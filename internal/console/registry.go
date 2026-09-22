package console

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	yaml "go.yaml.in/yaml/v2"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// registryVersion is the registry file schema version this binary writes and
// fully understands. A file with a HIGHER version loads read-only (list/get
// still work) and every mutating operation is refused — otherwise an older
// binary could rewrite a newer file through its narrower schema and lose
// non-additive changes. Purely additive fields don't need a version bump: they
// round-trip through ServerEntry.Extra.
const registryVersion = 1

// bootServerID is the reserved id of the ephemeral entry seeded from the
// command line (--index-dsn / `up`'s stream DSN). It is never written to the
// registry file and can never be edited or deleted over the API.
const bootServerID = "default"

var (
	// ErrDuplicateName rejects two registry entries sharing a name — the name
	// is the operator-facing label in the UI switcher, so it must be unique.
	ErrDuplicateName = errors.New("a server with this name already exists")
	// ErrUnknownServer is returned for an id with no registry entry.
	ErrUnknownServer = errors.New("unknown server id")
	// ErrRegistryReadOnly is returned for mutating operations when the on-disk
	// file was written by a newer bintrail (version > registryVersion).
	ErrRegistryReadOnly = errors.New("server registry was written by a newer bintrail; upgrade bintrail to edit it")
	// ErrS3StoreConflict: two servers name the same bucket but route it to
	// different stores (#1575). The store is per bucket, so one of them would
	// silently get the other's endpoint; the edit is refused instead.
	ErrS3StoreConflict = errors.New("S3 store conflict")
)

// ServerEntry is one named index connection in the registry. DSN holds the
// full secret (including the password) and is NEVER serialized into an HTTP
// response — see serverDTO in servers_api.go for the masked wire view.
type ServerEntry struct {
	// ID is a server-generated stable 8-byte hex key; it survives renames and
	// is what the browser sends in the X-Bintrail-Server header.
	ID string `yaml:"id"`
	// Name is the mutable operator-facing label, unique across the registry.
	Name string `yaml:"name"`
	// DSN is the full index DSN, password included — a secret at rest (the
	// file is 0600, same class as shim.yaml's mysql_password).
	DSN         string `yaml:"index_dsn"`
	BaselineDir string `yaml:"baseline_dir,omitempty"`
	BaselineS3  string `yaml:"baseline_s3,omitempty"`
	NoArchive   bool   `yaml:"no_archive,omitempty"`

	// ── Control-plane fields (phase 2 of the approved blueprint) ──
	// These configure MONITORING of a source MySQL; the supervisor in
	// `bintrail-console watch` consumes them (phase 3). On binaries that
	// predate them they round-trip untouched through Extra.

	// SourceDSN is the source MySQL to monitor — replication credentials,
	// a secret exactly like DSN, never serialized to any HTTP response.
	// Empty = a view-only entry (no monitoring configured).
	SourceDSN string `yaml:"source_dsn,omitempty"`
	// SourceServerID overrides the auto-derived replica server id (0 = derive
	// from the source DSN, the same rule as `bintrail up`).
	SourceServerID uint32 `yaml:"source_server_id,omitempty"`
	// Schemas is the optional comma-separated schema filter for monitoring.
	Schemas string `yaml:"schemas,omitempty"`

	// ── Source family (#1019) ──
	// Flavor selects the capture engine: "" or "mysql" (MySQL), "mariadb"
	// (MySQL DSN, MariaDB GTID at stream), "postgres" (pgstreamrun). Empty →
	// mysql keeps every pre-#1019 entry working (mirrors an empty SSLMode).
	// This is the generic "which source family" field #623 (MariaDB) also
	// needs — one implementation, not two. Accessed via SourceFlavor().
	Flavor string `yaml:"flavor,omitempty"`
	// SourceSlot / SourcePublication configure PostgreSQL logical replication
	// (postgres flavor only): the operator-created publication and the
	// replication slot the capturer streams from. Empty for MySQL/MariaDB.
	SourceSlot        string `yaml:"source_slot,omitempty"`
	SourcePublication string `yaml:"source_publication,omitempty"`

	// ── Source TLS (#879) ──
	// SSLMode/SSLCA/SSLCert/SSLKey configure the TLS the supervisor uses when
	// connecting to the SOURCE MySQL. Empty SSLMode = the daemon default
	// ("preferred"), preserving pre-#879 behavior; SSLCA/SSLCert/SSLKey are file
	// paths on the daemon host (verify-ca / mutual TLS), not secrets like the
	// DSNs. On binaries that predate these they round-trip untouched via Extra.
	SSLMode string `yaml:"ssl_mode,omitempty"`
	SSLCA   string `yaml:"ssl_ca,omitempty"`
	SSLCert string `yaml:"ssl_cert,omitempty"`
	SSLKey  string `yaml:"ssl_key,omitempty"`
	// ArchiveS3 is the S3 destination (s3://bucket/prefix/) the daemon's
	// built-in rotation uploads this source's rotated Parquet partitions to
	// BEFORE dropping them — so the forensic record survives retention and
	// stays queryable (the console auto-discovers it). Empty = drop-only.
	// Region/credentials come from the ambient AWS chain (env / ~/.aws / IAM
	// role); a local staging dir is used transiently. Non-secret (a bucket
	// URL): unlike DSNs it is serialized to the masked HTTP responses.
	ArchiveS3 string `yaml:"archive_s3,omitempty"`
	// ── S3 store (#1575) ──
	// S3Endpoint / S3PathStyle / S3Region say where this server's S3 buckets
	// (ArchiveS3 and BaselineS3) live when that is not AWS or the process-wide
	// BINTRAIL_S3_ENDPOINT: an S3-compatible store (MinIO, Wasabi), a pinned
	// region, or both. Applied PER BUCKET at runtime (storage.BucketStore):
	// every upload to and every DuckDB read of a bucket named here goes to
	// this store, whichever server asked. Two servers naming the same bucket
	// with different stores is refused (ErrS3StoreConflict). S3PathStyle is
	// "", "path" or "vhost"; "" means path style, what MinIO needs. Locations,
	// not secrets: serialized to the masked DTO.
	S3Endpoint  string `yaml:"s3_endpoint,omitempty"`
	S3PathStyle string `yaml:"s3_path_style,omitempty"`
	S3Region    string `yaml:"s3_region,omitempty"`
	// S3AccessKeyID / S3SecretAccessKey sign requests for those buckets
	// instead of the ambient credential chain. Both or neither. They belong
	// to the bucket like the endpoint: two servers on one bucket must carry
	// the same pair. The secret is at rest here like the DSN passwords (file
	// 0600) and never leaves the process: the DTO carries the access key and
	// has_s3_secret_access_key only.
	S3AccessKeyID     string `yaml:"s3_access_key_id,omitempty"`
	S3SecretAccessKey string `yaml:"s3_secret_access_key,omitempty"`
	// MonitorDesired records the operator's intent to monitor this source.
	// The supervisor reconciles running streams against it at boot and on
	// every edit; nothing reads it until phase 3.
	MonitorDesired bool `yaml:"monitor_desired,omitempty"`
	// BackupSchedule is this server's unattended backup timer (#1442): a full
	// backup from the source, or a rebuild from the change history, on a
	// fixed grid. Nil = no schedule. Non-secret. Edited only through the
	// schedule endpoints; the server edit form carries it over untouched.
	// On binaries that predate it, it round-trips through Extra.
	BackupSchedule *BackupSchedule `yaml:"backup_schedule,omitempty"`

	// Extra is the forward-compat catch-all: unknown fields written by a NEWER
	// bintrail (e.g. the phase-2 control plane's source_dsn / server_id /
	// monitor_state) land here on load and re-emit verbatim on save, so an
	// older binary editing the file never drops them. yaml:",inline" requires
	// map[string]any specifically; non-strict Unmarshal alone would only
	// tolerate unknown fields on read — a re-marshal would lose them.
	Extra map[string]any `yaml:",inline"`
}

// RotationConfig is the daemon-global built-in-rotation policy, editable from
// the console UI. It is stored once in the registry envelope, NOT per-server:
// the rotation loop is a single shared ticker, so retain/interval/add-future
// necessarily apply to every index the daemon rotates. Absent (nil) = the
// daemon's --rotate-* flags / BINTRAIL_ROTATE_* env stay in force. The fields
// hold the operator-typed strings (e.g. "30d", "1h") so they round-trip
// exactly and the engine parses them with the same grammar as the flags.
type RotationConfig struct {
	Retain    string `yaml:"retain"`
	Interval  string `yaml:"interval"`
	AddFuture int    `yaml:"add_future"`
}

// registryFile is the versioned on-disk envelope.
type registryFile struct {
	Version int `yaml:"version"`
	// Rotation is the optional global rotation override (omitted when the
	// daemon flags/env are in force). Additive at registryVersion 1 (no bump):
	// binaries from this release on preserve it (this field, plus the Extra
	// catch-all below for any future envelope key). A downgrade to a
	// PRE-rotation binary that re-saves the file would drop it — the version
	// gate, not round-tripping, is the cross-version safety net, since that
	// older binary has neither this field nor the inline catch-all.
	Rotation *RotationConfig `yaml:"rotation,omitempty"`
	// There is no baseline_refresh section here any more (#1681): reusing the
	// file of a table that did not change is always on, so the console has
	// nothing to override. A `baseline_refresh:` key written by an older
	// binary is carried by the Extra catch-all below and ignored.
	// BackupSettings is the optional daemon-wide backup settings section
	// (#1682): the values the Backup settings page used to show read-only
	// because they existed only as flags and environment of the process.
	// Same additive story as the two sections above, and the same reason for
	// living here rather than in a file of its own — see the BackupSettings
	// type for why a second home would have been the wrong shape.
	BackupSettings *BackupSettings `yaml:"backup_settings,omitempty"`
	Servers        []ServerEntry   `yaml:"servers"`
	// Extra preserves any FUTURE envelope-level key a (future) older binary
	// doesn't model, exactly as ServerEntry.Extra does at the entry level — so
	// the next additive envelope field is downgrade-safe from here on. (It does
	// not retroactively help binaries released before it.)
	Extra map[string]any `yaml:",inline"`
}

// Registry is the console's named-server store: a local YAML file, the ONLY
// thing the console ever writes. All mutations rewrite the file atomically
// (temp file + fsync + rename) under a mutex — this is bintrail's first
// programmatically-mutated config file, so a plain os.WriteFile (which can
// interleave concurrent writers) is not enough.
type Registry struct {
	path string // "" = in-memory only (unit tests); Save skips the disk
	mu   sync.Mutex
	file registryFile
	// readOnly is set when the on-disk version is newer than this binary
	// understands; see ErrRegistryReadOnly.
	readOnly bool
	// processBuckets are buckets the daemon itself reads with the
	// process-wide endpoint (bucket → the setting's name), which no per-server
	// store may claim; see SetProcessS3Location. Guarded by mu.
	processBuckets map[string]string
}

// DefaultRegistryPath returns ~/.config/bintrail/console-servers.yaml, with
// configPath's working-directory fallback for homeless environments. The
// verify and baseline run histories are named as siblings of this path, so
// they inherit whatever it resolves to.
func DefaultRegistryPath() string {
	return configPath("console-servers.yaml")
}

// LoadRegistry reads the registry file at path. A missing or empty file is an
// empty registry, not an error. The parse is deliberately NON-strict (unlike
// shim.yaml's UnmarshalStrict): an older binary must tolerate top-level fields
// a newer one added. path == "" creates an in-memory registry that never
// touches disk — for unit tests and callers without persistence.
func LoadRegistry(path string) (*Registry, error) {
	r := &Registry{path: path, file: registryFile{Version: registryVersion}}
	if path == "" {
		return r, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read server registry %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &r.file); err != nil {
		return nil, fmt.Errorf("parse server registry %s: %w", path, err)
	}
	if r.file.Version > registryVersion {
		r.readOnly = true
	}
	if r.file.Version == 0 {
		// Unset/zero version: a hand-written file; normalize on the next save.
		r.file.Version = registryVersion
	}
	r.syncBucketStores()
	return r, nil
}

// BucketStore builds the per-bucket store this entry's S3 settings describe.
// The zero store (no endpoint, no region, no keys) means the ambient
// configuration.
func (e ServerEntry) BucketStore() (storage.BucketStore, error) {
	st, err := storage.NewBucketStore(e.S3Endpoint, e.S3PathStyle, e.S3Region)
	if err != nil {
		return storage.BucketStore{}, err
	}
	return st.WithKeys(e.S3AccessKeyID, e.S3SecretAccessKey)
}

// s3Buckets lists the buckets this entry's S3 locations name, deduplicated.
// A location that does not parse names no bucket: it is refused elsewhere or
// ignored, and either way routes nothing.
func (e ServerEntry) s3Buckets() []string {
	var out []string
	for _, loc := range []string{e.ArchiveS3, e.BaselineS3} {
		if loc == "" {
			continue
		}
		b, _, err := storage.ParseS3URL(loc)
		if err != nil || b == "" || slices.Contains(out, b) {
			continue
		}
		out = append(out, b)
	}
	return out
}

// bucketStoreConflict is one bucket two entries route differently.
type bucketStoreConflict struct {
	Bucket, ServerA, ServerB string
}

// bucketStoresOf derives the bucket→store table from entries. A bucket has
// ONE store, and "no store" counts as one (the ambient endpoint): a bucket two
// entries read from different stores, a store-less entry included, is a
// conflict and is left OUT of the table, so no server silently takes another
// over. Add/Update refuse that shape, so it can only come from a hand-edited
// file. An entry whose store fields do not parse counts as store-less (its
// buckets use the ambient endpoint, as its warning says); a store naming no
// bucket of its own is reported and routes nothing.
//
// process holds the daemon's own store-less buckets (bucket → setting name):
// they are seen first, as a reader with no store.
func bucketStoresOf(entries []ServerEntry, process map[string]string) (map[string]storage.BucketStore, []bucketStoreConflict) {
	seen := map[string]storage.BucketStore{}
	owner := map[string]string{}
	conflicted := map[string]bool{}
	var conflicts []bucketStoreConflict
	processNames := make([]string, 0, len(process))
	for b := range process {
		processNames = append(processNames, b)
	}
	slices.Sort(processNames)
	for _, b := range processNames {
		seen[b] = storage.BucketStore{}
		owner[b] = process[b]
	}
	for _, e := range entries {
		st, err := e.BucketStore()
		if err != nil {
			// Only a hand-edited file gets here: Add/Update refuse the shape.
			slog.Warn("server registry: this server's S3 store settings are invalid and are ignored; its buckets use the ambient endpoint",
				"server", e.Name, "error", err)
			st = storage.BucketStore{}
		}
		buckets := e.s3Buckets()
		if !st.IsZero() && len(buckets) == 0 {
			slog.Warn("server registry: this server has S3 store settings but no Archive to S3 or Backups S3 location of its own; the store routes nothing",
				"server", e.Name)
		}
		for _, b := range buckets {
			if conflicted[b] {
				continue
			}
			if prev, ok := seen[b]; ok {
				if !prev.Equal(st) {
					conflicts = append(conflicts, bucketStoreConflict{Bucket: b, ServerA: owner[b], ServerB: e.Name})
					conflicted[b] = true
				}
				continue
			}
			seen[b] = st
			owner[b] = e.Name
		}
	}
	table := map[string]storage.BucketStore{}
	for b, st := range seen {
		if !conflicted[b] && !st.IsZero() {
			table[b] = st
		}
	}
	return table, conflicts
}

// syncBucketStores publishes the registry's bucket→store table to the
// storage package, where every S3 client and DuckDB session in this process
// reads it. Called on load and after every successful save. Callers hold
// r.mu (or own r exclusively, as LoadRegistry does).
func (r *Registry) syncBucketStores() {
	table, conflicts := bucketStoresOf(r.file.Servers, r.processBuckets)
	for _, c := range conflicts {
		if label, ok := r.processBuckets[c.Bucket]; ok && label == c.ServerA {
			// Not a disagreement two servers can resolve: a store on this
			// bucket is refused outright (checkBucketStore).
			slog.Warn("server registry: a server's S3 store names the daemon's --baseline-s3 bucket, which servers with no Backups location of their own read with the process-wide endpoint; the store is not applied to that bucket. Give the server another bucket, or set BINTRAIL_S3_ENDPOINT for the whole process",
				"bucket", c.Bucket, "server", c.ServerB)
			continue
		}
		slog.Warn("server registry: the same S3 bucket is read from different stores (one side may have no store set, or be the daemon's --baseline-s3 default); the bucket uses the ambient endpoint until they agree",
			"bucket", c.Bucket, "server_a", c.ServerA, "server_b", c.ServerB)
	}
	storage.SetBucketStores(table)
}

// DaemonBaselineS3Label names the daemon's --baseline-s3 bucket in refusals
// and warnings about a store claiming it.
const DaemonBaselineS3Label = "the daemon's --baseline-s3 default"

// SetProcessS3Location registers an S3 location the daemon reads for servers
// with none of their own: the --baseline-s3 fallback (withBaselineDefaults)
// that every server with no Backups location inherits, and that the boot
// entry reads. Its bucket counts as a bucket with no store: a server naming
// it with a store is refused, and a hand-edited one leaves it unrouted. label
// names the setting in refusals and warnings. Called before serving.
func (r *Registry) SetProcessS3Location(label, loc string) {
	// Parsed as typed: the daemon's readers test the raw value for an s3://
	// prefix, so a padded one is a local directory to them, not a bucket.
	bucket, _, err := storage.ParseS3URL(loc)
	if err != nil {
		if loc != "" {
			slog.Warn("server registry: the daemon's default S3 location is not an s3://bucket/prefix/ URL, so no bucket is reserved for it",
				"setting", label, "location", loc, "error", err)
		}
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.processBuckets == nil {
		r.processBuckets = map[string]string{}
	}
	r.processBuckets[bucket] = label
	r.syncBucketStores()
}

// storeInputsChanged reports whether an edit touches anything the bucket
// store depends on: the store fields, keys included, or the S3 locations they
// apply to. Whitespace-only differences count (the check normalizes them away).
func storeInputsChanged(old, e ServerEntry) bool {
	return old.S3Endpoint != e.S3Endpoint || old.S3PathStyle != e.S3PathStyle || old.S3Region != e.S3Region ||
		old.S3AccessKeyID != e.S3AccessKeyID || old.S3SecretAccessKey != e.S3SecretAccessKey ||
		old.ArchiveS3 != e.ArchiveS3 || old.BaselineS3 != e.BaselineS3
}

// checkBucketStore validates e's S3 store fields, normalizes them in place,
// and refuses a shape that would save and then route a bucket somewhere its
// servers do not agree on. selfID exempts the entry being updated. Callers
// hold r.mu. Three refusals:
//   - a store with no S3 location of its own routes nothing. A location-less
//     server's Backups read the daemon's --baseline-s3 at run time, and that
//     bucket is not this server's to route;
//   - a store beside a location that does not parse would route only the
//     half that does;
//   - a bucket another server reads from a different store, "no store"
//     included: a store-less server on a routed bucket would be taken over,
//     then handed back when the store's server is deleted.
func (r *Registry) checkBucketStore(e *ServerEntry, selfID string) error {
	st, err := e.BucketStore()
	if err != nil {
		return err
	}
	e.S3Endpoint = st.Endpoint.URL
	e.S3PathStyle = strings.ToLower(strings.TrimSpace(e.S3PathStyle))
	e.S3Region = st.Region
	e.S3AccessKeyID, e.S3SecretAccessKey = st.AccessKeyID, st.SecretKey
	if !st.IsZero() {
		for _, loc := range []struct{ field, value string }{{"Archive to S3", e.ArchiveS3}, {"Backups S3", e.BaselineS3}} {
			if loc.value == "" {
				continue
			}
			if _, _, err := storage.ParseS3URL(loc.value); err != nil {
				return fmt.Errorf("%w: the %s location %q is not an s3://bucket/prefix/ URL, so the S3 store cannot apply to it", storage.ErrBucketStoreConfig, loc.field, loc.value)
			}
		}
		if len(e.s3Buckets()) == 0 {
			return fmt.Errorf("%w: an S3 store applies to this server's own buckets; set its Archive to S3 or Backups S3 location, or clear the S3 store fields", storage.ErrBucketStoreConfig)
		}
	}
	for _, b := range e.s3Buckets() {
		if label, ok := r.processBuckets[b]; ok && !st.IsZero() {
			return fmt.Errorf("%w: bucket %q is %s, which servers with no Backups S3 location of their own read from AWS or the process-wide endpoint; a per-server store would take them all over, so use another bucket, or set BINTRAIL_S3_ENDPOINT for the whole process",
				ErrS3StoreConflict, b, label)
		}
		for _, other := range r.file.Servers {
			if other.ID == selfID || !slices.Contains(other.s3Buckets(), b) {
				continue
			}
			ost, err := other.BucketStore()
			if err != nil {
				// It loads as store-less (bucketStoresOf). Say why: the
				// operator would otherwise copy settings that are ignored.
				if st.IsZero() {
					continue
				}
				return fmt.Errorf("%w: bucket %q is also used by server %q, whose S3 store settings are invalid and ignored (%v), so it reads the bucket from AWS or the process-wide endpoint; fix that server's S3 store first",
					ErrS3StoreConflict, b, other.Name, err)
			}
			if ost.Equal(st) {
				continue
			}
			return fmt.Errorf("%w: bucket %q is also used by server %q, %s; a bucket has one store, so give both servers the same S3 store settings or use another bucket",
				ErrS3StoreConflict, b, other.Name, storeDifference(st, ost))
		}
	}
	return nil
}

// storeDifference says how other's store differs from this one's, in the
// server form's words. Never a key value.
func storeDifference(this, other storage.BucketStore) string {
	switch {
	case other.IsZero():
		return "which has no S3 store set and reads it from AWS or the process-wide endpoint"
	case this.IsZero():
		return "which reads it from its own S3 store"
	case this.SameRouting(other):
		return "with different S3 keys"
	default:
		return "with a different S3 endpoint, addressing or region"
	}
}

// List returns a copy of the entries, in file order. The copy is shallow:
// each entry's Extra map is shared with the registry — treat returned entries
// as read-only (nothing in the console mutates Extra off a copy today).
func (r *Registry) List() []ServerEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ServerEntry, len(r.file.Servers))
	copy(out, r.file.Servers)
	return out
}

// Get returns the entry with the given id. Like List, the entry's Extra map
// is shared with the registry — treat it as read-only.
func (r *Registry) Get(id string) (ServerEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.file.Servers {
		if e.ID == id {
			return e, true
		}
	}
	return ServerEntry{}, false
}

// Len reports the number of registry entries.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.file.Servers)
}

// ReadOnly reports whether mutating operations are refused (newer-version file).
func (r *Registry) ReadOnly() bool { return r.readOnly }

// Rotation returns the saved global rotation policy, or false when none is set
// (the daemon's --rotate-* flags/env are in force). The rotation loop's
// settings provider reads this every cycle, so an override applies live.
func (r *Registry) Rotation() (RotationConfig, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file.Rotation == nil {
		return RotationConfig{}, false
	}
	return *r.file.Rotation, true
}

// SetRotation persists the global rotation policy. Like every registry
// mutation it rewrites the file atomically and is refused on a newer-version
// (read-only) file. The caller validates the field grammar.
func (r *Registry) SetRotation(rc RotationConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readOnly {
		return ErrRegistryReadOnly
	}
	prev := r.file.Rotation
	r.file.Rotation = &rc
	if err := r.save(); err != nil {
		r.file.Rotation = prev // roll back
		return err
	}
	return nil
}

// Add validates, mints an id, appends, and persists the entry. The returned
// entry carries the generated ID.
func (r *Registry) Add(e ServerEntry) (ServerEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readOnly {
		return ServerEntry{}, ErrRegistryReadOnly
	}
	if err := r.checkName(e.Name, ""); err != nil {
		return ServerEntry{}, err
	}
	if err := r.checkBucketStore(&e, ""); err != nil {
		return ServerEntry{}, err
	}
	id, err := genServerID()
	if err != nil {
		return ServerEntry{}, fmt.Errorf("generate server id: %w", err)
	}
	e.ID = id
	r.file.Servers = append(r.file.Servers, e)
	if err := r.save(); err != nil {
		r.file.Servers = r.file.Servers[:len(r.file.Servers)-1] // roll back
		return ServerEntry{}, err
	}
	return e, nil
}

// Update replaces the entry with e.ID and persists. The caller is responsible
// for keep-password merging — the registry stores exactly what it is given.
// Unknown phase-2 fields survive: the stored entry's Extra is carried over
// unless the caller supplied its own.
func (r *Registry) Update(e ServerEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readOnly {
		return ErrRegistryReadOnly
	}
	if err := r.checkName(e.Name, e.ID); err != nil {
		return err
	}
	for i, old := range r.file.Servers {
		if old.ID != e.ID {
			continue
		}
		// The store is re-validated only when the edit TOUCHES it (the three
		// fields or the S3 locations they apply to). A hand-edited file with
		// an invalid s3_endpoint is warned about at load; refusing every
		// other save of that entry over it would block "stop monitoring" and
		// the backup schedule until the YAML is fixed.
		if storeInputsChanged(old, e) {
			if err := r.checkBucketStore(&e, e.ID); err != nil {
				return err
			}
		}
		if e.Extra == nil {
			e.Extra = old.Extra // preserve forward-compat fields across edits
		}
		r.file.Servers[i] = e
		if err := r.save(); err != nil {
			r.file.Servers[i] = old // roll back
			return err
		}
		return nil
	}
	return ErrUnknownServer
}

// Delete removes the entry with the given id and persists.
func (r *Registry) Delete(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readOnly {
		return ErrRegistryReadOnly
	}
	for i, old := range r.file.Servers {
		if old.ID != id {
			continue
		}
		r.file.Servers = append(r.file.Servers[:i], r.file.Servers[i+1:]...)
		if err := r.save(); err != nil {
			// Roll back: re-insert at the original position.
			r.file.Servers = append(r.file.Servers[:i], append([]ServerEntry{old}, r.file.Servers[i:]...)...)
			return err
		}
		return nil
	}
	return ErrUnknownServer
}

// checkName enforces non-empty unique names; selfID exempts the entry being
// updated. Callers hold r.mu. The boot entry's reserved id doubles as a
// reserved name so the switcher never shows two entries labeled "default".
func (r *Registry) checkName(name, selfID string) error {
	if name == "" {
		return errors.New("server name is required")
	}
	if name == bootServerID {
		return fmt.Errorf("%q is reserved for the command-line server", bootServerID)
	}
	for _, e := range r.file.Servers {
		if e.Name == name && e.ID != selfID {
			return ErrDuplicateName
		}
	}
	return nil
}

// save writes the registry atomically: marshal → temp file in the same
// directory → fsync → rename. Callers hold r.mu. The file is 0600 and its
// directory 0700 — it holds DSN passwords (same class as shim.yaml/dump.key).
func (r *Registry) save() error {
	if r.path == "" {
		r.syncBucketStores()
		return nil // in-memory registry
	}
	data, err := yaml.Marshal(&r.file)
	if err != nil {
		return fmt.Errorf("marshal server registry: %w", err)
	}
	dir := filepath.Dir(r.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create registry directory %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".console-servers-*.yaml")
	if err != nil {
		return fmt.Errorf("create temp registry file: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod temp registry file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write server registry: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync server registry: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp registry file: %w", err)
	}
	if err := os.Rename(tmp.Name(), r.path); err != nil {
		return fmt.Errorf("replace server registry %s: %w", r.path, err)
	}
	r.syncBucketStores()
	return nil
}

// genServerID returns a random 8-byte hex id (16 chars) — stable across
// renames, unguessable, and short enough to read in a YAML file.
func genServerID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
