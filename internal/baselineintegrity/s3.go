// baselineintegrity — s3.go: the S3 half of the at-rest integrity story
// (#698, follow-up to #636's local validation).
//
// The manifest's CRC-32C is over the raw Parquet object BYTES, and no S3 read
// path keeps a byte-identical local copy to hash against — the row paths
// stream directly via DuckDB parquet_scan or re-encode via DuckDB COPY to a
// temp, and the footer read (baseline.ReadParquetMetadataAny) streams too. So
// the S3 validation is a PRE-PASS: before any reader touches the object, the
// ORIGINAL object is streamed once through CRC-32C via the AWS SDK and
// compared against the snapshot's _MANIFEST (also fetched from S3). This
// validates exact bytes on every manifest-covered read path — the
// arbitrary-SQL surfaces (`reconstruct --sql`, the console SQL panel) validate
// nothing, exactly as they don't locally — without forcing a download-to-disk
// (the DuckDB read that follows keeps whatever streaming mode the caller
// chose), at the cost of one extra full read of the object — memoized per
// process, see the caching policy below.
//
// Scope is unchanged from #636: bit-rot / truncated or partial writes, NOT
// tamper-evidence — an attacker who rewrites the object can rewrite the
// manifest too.
package baselineintegrity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// OpenS3Object fetches an S3 object as a byte stream. It is a package variable
// ONLY so the read-path wiring tests can stub S3 without a mock server;
// production code never reassigns it. The default implementation uses the AWS
// SDK default credential chain with the shared IMDS region fallback
// (storage.NewS3Client) — the same resolution every other SDK-side S3 caller
// in this codebase uses.
var OpenS3Object = sdkOpenS3Object

// Caching policy. ValidateS3File sits on hot paths that loop — the shim
// `_snapshot` validates once per client MySQL query, cascade Phase-2 once per
// parent PK — so every verdict that cost an S3 round-trip is memoized per
// process, in two layers (the no-traffic skips — unparseable URL, shallow
// layout — are recomputed per call; and a verdict born of the CALLER's context
// dying is never stored at all, see ValidateS3File):
//
//   - s3VerdictCache (full object path → verdict): match, legacy-no-manifest,
//     file-unlisted and unrecognized-version verdicts are TERMINAL — a
//     snapshot is immutable once its _SUCCESS marker exists, so they never
//     expire. A CRC MISMATCH and a validator-side READ FAILURE are cached only
//     for failureVerdictTTL: long enough that a tight loop neither re-downloads
//     the object nor emits a warn per call (the WarnS3IntegrityNotValidated
//     this replaced was once-per-process for exactly that reason), short
//     enough that an operator who repairs a corrupt object in place — the
//     path this feature itself creates — is un-bricked without restarting the
//     daemon, and that a transient S3 blip does not disable validation for
//     the rest of the process.
//   - s3ManifestCache ("bucket\x00snapshotKey" → *Manifest, nil = absent):
//     one _MANIFEST GET covers every table of the snapshot instead of one per
//     object; fetch errors are never cached here (they surface as a TTL'd
//     verdict above).
type verdict struct {
	err     error
	expires time.Time // zero = terminal, never expires
}

// failureVerdictTTL bounds how long a mismatch or a validator-read-failure
// verdict is trusted before re-checking. Variable only for tests.
var failureVerdictTTL = time.Minute

var (
	s3VerdictCache  sync.Map
	s3ManifestCache sync.Map
)

// maxManifestBytes caps the _MANIFEST read: a real manifest is one small JSON
// entry per table (a few MiB covers ~100k tables). Anything larger at that key
// is not a manifest; without the cap a wrong multi-GB object parked there
// would be buffered wholesale inside the long-lived shim/watch daemons.
const maxManifestBytes = 16 << 20

// ValidateS3File checks an s3:// baseline Parquet object against its
// snapshot's _MANIFEST — the S3 mirror of ValidateLocalFile, with the same
// outcomes: nil on match, ErrIntegrity (wrapped) on a CRC mismatch, and a
// degrade-to-skip (nil) whenever the check CANNOT run — no manifest (legacy
// snapshot), unreadable/unparseable manifest, unrecognized version/algo, file
// not listed, an s3:// URL that doesn't parse, or a layout that isn't
// <snapshot>/<db>/<table>.parquet. All of
// those mean "unverified", never "corrupt"; a rotted sidecar must not brick a
// good baseline.
//
// One DELIBERATE divergence from ValidateLocalFile: a failure to READ the data
// object here degrades to a logged skip instead of an error. Locally, an
// unopenable file means the subsequent read fails on the same syscall path
// anyway. On S3 the validator (AWS SDK default chain) and the reader (DuckDB
// httpfs credential_chain) are DIFFERENT clients that can diverge on region
// resolution or IAM path, so failing here could deny recovery of intact data
// that DuckDB can perfectly well read. Only a completed hash that mismatches
// the manifest is proof of corruption — and note a truncated-at-rest object
// still produces exactly that (a complete, shorter read → CRC mismatch), so
// the skip does not hide the partial-write case this exists to catch.
func ValidateS3File(ctx context.Context, s3Path string) error {
	expiredMismatch := false
	if v, ok := s3VerdictCache.Load(s3Path); ok {
		ve := v.(verdict)
		if ve.expires.IsZero() || time.Now().Before(ve.expires) {
			return ve.err
		}
		expiredMismatch = ve.err != nil
		s3VerdictCache.Delete(s3Path)
	}
	bucket, key, err := storage.ParseS3URL(s3Path)
	if err != nil {
		// Not an interpretable s3:// URL — nothing to locate a manifest by.
		slog.Debug("S3 baseline path not interpretable; integrity not verified", "path", s3Path, "error", err)
		return nil
	}
	// path.Dir below CLEANS the path, so the rel lookup must run on the same
	// cleaned key: a non-canonical key (a double slash from a trailing-slash
	// prefix join, a "./" segment) would otherwise make TrimPrefix a no-op,
	// miss the manifest entry, and switch validation off for that object
	// terminally and silently.
	key = path.Clean(key)
	// Snapshot layout is <prefix>/<timestamp>/<schema>/<table>.parquet with the
	// _MANIFEST directly under <timestamp>/ — the object's grandparent, exactly
	// like the local layout. Fewer than two segments above the object means the
	// path is not under a snapshot directory; skip like ValidateLocalFile's
	// filepath.Rel failure.
	tableDir := path.Dir(key)
	snapKey := path.Dir(tableDir)
	if tableDir == "." || snapKey == "." {
		slog.Debug("S3 baseline path too shallow for the snapshot layout; integrity not verified", "path", s3Path)
		return nil
	}
	rel := strings.TrimPrefix(key, snapKey+"/")
	snapshotLabel := "s3://" + bucket + "/" + snapKey

	if expiredMismatch {
		// A mismatch verdict just expired. The operator may have repaired
		// EITHER side in place: the object (the re-hash below catches that) or
		// a rotted-but-parseable _MANIFEST digest (regenerated sidecar) — so
		// drop the cached manifest and re-read both fresh. This only fires on
		// the already-failing path, so the extra GET costs nothing in the
		// steady state.
		s3ManifestCache.Delete(bucket + "\x00" + snapKey)
	}

	m, ok, err := loadManifestS3(ctx, bucket, snapKey)
	if err != nil {
		if ctx.Err() != nil {
			// The CALLER died mid-fetch (client disconnect, request deadline) —
			// that says nothing about the object, and storing it would switch
			// validation off for every OTHER caller for a full TTL. Don't
			// cache, don't warn as corruption; the caller's own read dies on
			// the same context right after.
			slog.Debug("S3 manifest fetch canceled by caller; integrity not verified for this call",
				"snapshot", snapshotLabel, "error", err)
			return nil
		}
		if errors.Is(err, storage.ErrS3EndpointConfig) {
			// A malformed BINTRAIL_S3_ENDPOINT is a configuration fault, not a
			// storage one: it is knowable before any byte moves and says
			// nothing about whether the data is intact. Degrading here would
			// switch integrity validation off for a TTL and blame sidecar rot
			// for a typo, so this one refuses instead (#1453).
			return err
		}
		// Unreadable / unparseable manifest = cannot verify, not data corruption
		// (same degrade as the local path). This branch also absorbs an ABSENT
		// manifest that S3 reports as AccessDenied (a GetObject-only policy
		// without s3:ListBucket returns 403 for missing keys — indistinguishable
		// from a real denial), hence the IAM mention in the warning.
		slog.Warn("S3 integrity manifest unreadable (sidecar rot, IAM, or transient S3 error); treating baseline as integrity-not-verified",
			"snapshot", snapshotLabel, "error", err)
		storeVerdict(s3Path, nil, false)
		return nil
	}
	if !ok {
		// Legacy snapshot written before #636 — no manifest will ever appear
		// (snapshots are immutable), so the skip verdict is terminal.
		storeVerdict(s3Path, nil, true)
		return nil
	}
	want, verify := m.digestFor(rel, snapshotLabel)
	if !verify {
		storeVerdict(s3Path, nil, true)
		return nil
	}
	got, err := crc32cS3Object(ctx, bucket, key)
	if err != nil {
		if ctx.Err() != nil {
			// Caller-scoped cancellation, not an object-scoped verdict — the
			// multi-GB hash pass is the widest cancellation window in the
			// request, and caching this would disable validation for every
			// other caller for a TTL. See the manifest branch above.
			slog.Debug("S3 baseline hash pass canceled by caller; integrity not verified for this call",
				"path", s3Path, "error", err)
			return nil
		}
		// The deliberate divergence documented above: validator-side read
		// failure ≠ corruption. Warn, then proceed unverified until the TTL
		// re-check.
		slog.Warn("could not re-read S3 baseline for integrity check; treating as integrity-not-verified",
			"path", s3Path, "error", err)
		storeVerdict(s3Path, nil, false)
		return nil
	}
	if got != want {
		verr := fmt.Errorf("%w: %s (crc32c %s, manifest %s)", ErrIntegrity, s3Path, got, want)
		storeVerdict(s3Path, verr, false)
		return verr
	}
	storeVerdict(s3Path, nil, true)
	return nil
}

// storeVerdict caches a validation outcome for s3Path. terminal verdicts
// never expire (the snapshot is immutable); non-terminal ones (mismatch,
// validator-read failure) expire after failureVerdictTTL and re-check.
func storeVerdict(s3Path string, err error, terminal bool) {
	v := verdict{err: err}
	if !terminal {
		v.expires = time.Now().Add(failureVerdictTTL)
	}
	s3VerdictCache.Store(s3Path, v)
}

// loadManifestS3 is LoadManifest over S3, memoized per snapshot (immutable
// once _SUCCESS exists). ok=false with a nil error means the manifest object
// is ABSENT — a legacy snapshot; any other fetch or parse failure is returned
// for the caller to degrade on, and is NOT cached, since an
// AccessDenied/throttle/network error hides a manifest that may exist.
func loadManifestS3(ctx context.Context, bucket, snapKey string) (*Manifest, bool, error) {
	cacheKey := bucket + "\x00" + snapKey
	if v, ok := s3ManifestCache.Load(cacheKey); ok {
		m := v.(*Manifest)
		return m, m != nil, nil
	}
	rc, err := OpenS3Object(ctx, bucket, path.Join(snapKey, ManifestName))
	if err != nil {
		if s3ObjectAbsent(err) {
			s3ManifestCache.Store(cacheKey, (*Manifest)(nil))
			return nil, false, nil
		}
		return nil, false, err
	}
	defer rc.Close()
	b, err := io.ReadAll(io.LimitReader(rc, maxManifestBytes+1))
	if err != nil {
		return nil, false, err
	}
	if len(b) > maxManifestBytes {
		return nil, false, fmt.Errorf("%s exceeds %d bytes — not an integrity manifest", ManifestName, maxManifestBytes)
	}
	var parsed Manifest
	if err := json.Unmarshal(b, &parsed); err != nil {
		return nil, false, fmt.Errorf("parse %s: %w", ManifestName, err)
	}
	s3ManifestCache.Store(cacheKey, &parsed)
	return &parsed, true, nil
}

// s3ObjectAbsent reports whether err means the object does not exist, across
// the shapes real backends produce: the modeled SDK types, a bare S3 error
// code, and the plain HTTP 404 some S3-compatible backends (Ceph, Wasabi)
// return without a code — the same discrimination internal/storage does for
// Exists. NOT matched: AccessDenied for a missing key under a
// GetObject-only policy (indistinguishable from a real denial; degrades via
// the caller's error path instead).
func s3ObjectAbsent(err error) bool {
	var noKey *s3types.NoSuchKey
	var notFound *s3types.NotFound
	if errors.As(err, &noKey) || errors.As(err, &notFound) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if c := apiErr.ErrorCode(); c == "NoSuchKey" || c == "NotFound" {
			return true
		}
	}
	var re *smithyhttp.ResponseError
	return errors.As(err, &re) && re.Response.StatusCode == 404
}

// crc32cS3Object streams the object through CRC-32C (Castagnoli) and returns
// the 8-char lowercase hex digest — CRC32CFile over a GET body.
func crc32cS3Object(ctx context.Context, bucket, key string) (string, error) {
	rc, err := OpenS3Object(ctx, bucket, key)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	h := crc32.New(crc32cTable)
	if _, err := io.Copy(h, rc); err != nil {
		return "", err
	}
	return fmt.Sprintf("%08x", h.Sum32()), nil
}

var (
	s3ClientMu sync.Mutex
	s3Client   *s3.Client
)

// sharedS3Client lazily builds one process-wide S3 client on the DEFAULT
// credential-chain region (env/config/IMDS via storage.NewS3Client). It
// deliberately does NOT chase a bucket's region across a 301 redirect: the
// baseline READ paths this validator guards resolve region the same way
// (duckdbutil.EnableS3CredentialChain pins no region either), so a
// cross-region baseline bucket fails the read itself — this is validator
// parity, not a new gap, and a redirect failure degrades to the logged skip
// like any other validator-side read error. If the read side ever grows
// region pinning for baselines (the #511 EnableS3CredentialChainRegion
// treatment), thread the same region in here. A failed build is not cached,
// so a transient config error doesn't pin validation off forever.
func sharedS3Client(ctx context.Context) (*s3.Client, error) {
	s3ClientMu.Lock()
	defer s3ClientMu.Unlock()
	if s3Client != nil {
		return s3Client, nil
	}
	c, err := storage.NewS3Client(ctx, "")
	if err != nil {
		return nil, err
	}
	s3Client = c
	return c, nil
}

func sdkOpenS3Object(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	var client *s3.Client
	var err error
	if _, routed := storage.BucketStoreFor(bucket); routed {
		// A bucket with its own store (#1575) cannot share the default-region
		// client: it has its own endpoint and region.
		client, err = storage.NewS3ClientForBucket(ctx, bucket, "")
	} else {
		client, err = sharedS3Client(ctx)
	}
	if err != nil {
		return nil, err
	}
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}
