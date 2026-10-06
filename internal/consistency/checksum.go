// Package consistency provides primitives that detect, with overwhelming
// probability, whether a Parquet snapshot diverges from the MySQL table it was
// taken from.
//
// The foundation is ConsistentTableChecksum: a point-in-time, order-independent,
// type-canonical fingerprint of a source table. The fingerprint is a 64-bit
// non-cryptographic multiset hash — sized to catch accidental corruption and
// divergence (collision ≈ 2⁻⁶⁴ per comparison), not to resist an adversary who
// can forge a colliding table. Fidelity is only checkable at a frozen consistent
// point — you cannot compare a checksum of the Parquet against a live table that
// has moved on. So the fingerprint is computed inside
// START TRANSACTION WITH CONSISTENT SNAPSHOT and bound to the @@gtid_executed
// captured at that same point. The baseline writer (issue #633) and the verify
// capstone (issue #634) both call this primitive — at dump time and at audit
// time respectively — and compare the result against a Parquet-derived digest.
//
// Part of the data-consistency guarantee epic (#631).
package consistency

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// erUnknownSystemVariable is MySQL's error code for an unknown system variable
// (ER_UNKNOWN_SYSTEM_VARIABLE). Matched by number rather than message text,
// which is locale-dependent.
const erUnknownSystemVariable = 1193

// TableChecksum is a point-in-time fingerprint of a single source table.
//
// GTIDSet is the source's executed GTID position captured just after the
// consistent snapshot opens, the point against which a Parquet snapshot can
// later be compared. GTIDFlavor names its format: @@gtid_executed on MySQL
// (empty with gtid_mode=OFF), @@gtid_binlog_pos on MariaDB (empty when the
// binlog holds no GTID yet). Both are empty on a server with neither
// variable; callers that need a position anchor there capture it separately.
//
// The variable is global state, not MVCC-filtered, so a commit landing in the
// brief window between the snapshot opening and this read is reflected in the
// GTID but not in the snapshot data. This lock-free window is the same one every
// bintrail baseline carries (mydumper dumps with NO_LOCK and reads its metadata
// GTID the same way); the digest itself is computed entirely over the snapshot
// and is unaffected — only the anchor's precision is bounded by this window.
//
// Digest is a version-tagged, order-independent multiset hash of the row
// contents (see digestVersion). Two tables holding the same rows in any physical
// or primary-key order produce the same Digest; a single changed byte produces a
// different one with overwhelming probability; a representation-only difference
// (e.g. JSON whitespace, which MySQL normalizes on storage) produces the same
// one.
type TableChecksum struct {
	Schema  string
	Table   string
	GTIDSet string
	// GTIDFlavor is GTIDFlavorMySQL, GTIDFlavorMariaDB, or "" when the
	// server has neither variable (and on the PostgreSQL path).
	GTIDFlavor string
	RowCount   int64
	Digest     string
	// LSN is the PostgreSQL WAL anchor (pg_current_wal_lsn) captured when the
	// snapshot opens — the PG sibling of GTIDSet, set only by
	// ConsistentTableChecksumPG (zero on the MySQL paths, which leave the
	// anchor in GTIDSet). It carries the same lock-free-window caveat as
	// GTIDSet: pg_current_wal_lsn is global state, not MVCC-filtered.
	LSN uint64
	// Anchor says how GTIDSet is tied to the snapshot (#2150): AnchorNative
	// or AnchorTableLock when it is the snapshot's exact position; "" when it
	// is the set read just after the snapshot opened, which can differ from
	// the snapshot by the transactions committing at that moment. Set only by
	// ConsistentTableChecksumAnchored.
	Anchor string
	// AnchorLockRefused: an exact anchor needed LOCK TABLES on the table and
	// the account may not take it, so Anchor is "".
	AnchorLockRefused bool
	// GTIDMode is the source's @@gtid_mode when it is not ON (MySQL; "" when
	// ON, on MariaDB, or when not read). A set read under any other mode is
	// not anchored.
	GTIDMode string
	// Columns is the ordered set of column names the digest was computed over
	// (ordinal order, generated columns excluded). A consumer that recomputes a
	// digest to compare (the verify capstone #634) must hash exactly this set in
	// this order, rather than re-deriving it — re-deriving from a schema snapshot
	// risks a different generated-column membership and a spurious mismatch.
	Columns []string
}

// ConsistentTableChecksum computes a TableChecksum for schema.table against the
// live source db. The whole computation — GTID capture, column introspection,
// and the table scan — runs on a single pinned connection inside
// START TRANSACTION WITH CONSISTENT SNAPSHOT, so the digest and the row count
// describe one snapshot of the data and the captured GTID anchors it (modulo the
// lock-free window documented on TableChecksum.GTIDSet).
//
// The canonical form of every value is MySQL's text-protocol rendering with the
// session time zone pinned to UTC. That rendering is already type-exact —
// UNSIGNED integers print unsigned, DATETIME/TIMESTAMP carry their declared
// fractional precision, DECIMAL is pre-formatted, JSON is normalized (MySQL;
// MariaDB stores JSON as LONGTEXT and renders it verbatim) — so no
// per-type canonicalization is reimplemented here. String columns are read with
// character_set_results = binary, so their RAW STORED BYTES are hashed (no
// transcoding to the connection charset) — matching mydumper's `SET NAMES
// binary` dump contract (issue #792). The Parquet side of the comparison (#634)
// must reproduce this same contract: "MySQL text rendering, session time zone
// UTC, raw string bytes". Two digests remain FLOAT/DOUBLE-text-rendering and
// server-family dependent, which is the natural case since the baseline and its
// verify run against the same source; the charset dependency is now removed by
// the binary pin.
//
// Generated columns (VIRTUAL/STORED) are excluded: mydumper does not dump them,
// so they are absent from the baseline Parquet and must be absent here too.
func ConsistentTableChecksum(ctx context.Context, db *sql.DB, schema, table string) (TableChecksum, error) {
	return consistentTableChecksum(ctx, db, schema, table, nil, false)
}

// ConsistentTableChecksumNormalized is ConsistentTableChecksum with an extra
// per-column hook: normalize, when non-nil, rewrites a scanned column's raw
// text-protocol bytes before it is folded into the digest.
//
// This exists so a caller comparing this digest against a digest computed by
// a DIFFERENT renderer (internal/verify's reconstruct, in the live-source
// verify path) can rewrite away known representation-only gaps between the
// two — e.g. a MySQL zero-date sentinel the reconstruct side normalizes to
// NULL, or a JSON-shaped TEXT value the reconstruct side re-serializes in
// canonical key order — symmetrically on THIS side too, the same way
// internal/verify's own renderCellNormalized does for its side. Applying the
// SAME normalization asymmetrically (only one side) would trade one false
// mismatch for a different one; this package intentionally does not import
// internal/verify to decide what "normalized" means — the caller supplies
// that policy as a closure, keeping this package's only knowledge of the
// concept a raw-bytes-in, raw-bytes-out hook.
//
// normalize receives the column's information_schema.COLUMNS DATA_TYPE
// (lower-case) and is never called for a genuine SQL NULL (nil raw bytes) —
// there is nothing to rewrite. It is not called by ConsistentTableChecksum
// (nil hook, unmodified behavior) — callers that persist this digest
// independently of internal/verify (e.g. the baseline writer, #633) must not
// normalize, since their digest is compared against ANOTHER raw, unnormalized
// rendering of the same contract, not against a reconstruct digest.
func ConsistentTableChecksumNormalized(ctx context.Context, db *sql.DB, schema, table string, normalize func(raw []byte, dataType string) []byte) (TableChecksum, error) {
	return consistentTableChecksum(ctx, db, schema, table, normalize, false)
}

// ConsistentTableChecksumAnchored is ConsistentTableChecksumNormalized whose
// GTIDSet is the snapshot's exact position where the server allows it (see
// openAnchoredSnapshot and TableChecksum.Anchor) (#2150). On stock MySQL that
// takes a brief read lock on the table; ErrAnchorBusy means the
// lock could not be had, and no scan ran.
func ConsistentTableChecksumAnchored(ctx context.Context, db *sql.DB, schema, table string, normalize func(raw []byte, dataType string) []byte) (TableChecksum, error) {
	return consistentTableChecksum(ctx, db, schema, table, normalize, true)
}

func consistentTableChecksum(ctx context.Context, db *sql.DB, schema, table string, normalize func(raw []byte, dataType string) []byte, anchored bool) (TableChecksum, error) {
	res := TableChecksum{Schema: schema, Table: table}

	conn, err := db.Conn(ctx)
	if err != nil {
		return res, fmt.Errorf("pin connection: %w", err)
	}
	defer conn.Close()

	// Pin the session time zone so TIMESTAMP values render deterministically
	// (TIMESTAMP is stored in UTC and rendered in the session zone).
	if _, err := conn.ExecContext(ctx, "SET SESSION time_zone = '+00:00'"); err != nil {
		return res, fmt.Errorf("set session time_zone: %w", err)
	}

	// Pin character_set_results = binary so the server returns each string
	// column's RAW STORED BYTES without transcoding them to the connection
	// charset (go-sql-driver's default is utf8mb4). This matches mydumper's
	// `SET NAMES binary` dump contract, which the baseline Parquet is written
	// under: a latin1 'é' (0xE9) then hashes as 0xE9 on BOTH the baseline side
	// and this live-scan side, instead of the server transcoding it to utf8mb4
	// (0xC3A9) here and producing a permanent, conclusive false-MISMATCH on
	// every non-ASCII row of a legacy-charset table (issue #792). The pin is the
	// v2 half of the digest contract — see digestVersion.
	if _, err := conn.ExecContext(ctx, "SET SESSION character_set_results = binary"); err != nil {
		return res, fmt.Errorf("set session character_set_results: %w", err)
	}

	// Open the consistent snapshot. Everything below reads the same view.
	committed := false
	defer func() {
		if !committed {
			// Read-only transaction; rollback is best-effort cleanup (also
			// when the snapshot never opened: a ROLLBACK outside a
			// transaction is a no-op).
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	if anchored {
		a, err := openAnchoredSnapshot(ctx, db, conn, schema, table)
		if err != nil {
			return res, err
		}
		res.GTIDSet, res.GTIDFlavor, res.Anchor, res.AnchorLockRefused, res.GTIDMode = a.set, a.flavor, a.method, a.lockRefused, a.gtidMode
	} else {
		if err := startSnapshot(ctx, conn); err != nil {
			return res, err
		}
		// Capture the GTID anchor inside the snapshot.
		res.GTIDSet, res.GTIDFlavor, err = capturedGTID(ctx, conn)
		if err != nil {
			return res, err
		}
	}

	// Introspect the non-generated columns, in ordinal order.
	cols, err := tableColumns(ctx, conn, schema, table)
	if err != nil {
		return res, err
	}
	if len(cols) == 0 {
		return res, fmt.Errorf("table %s.%s has no columns (does it exist?)", schema, table)
	}

	// Scan every row, hashing as we stream — no full-table buffering.
	//
	// promoteFloat is scoped to the normalize != nil (cross-renderer) callers
	// only — see selectExpr's FLOAT case for why.
	promoteFloat := normalize != nil
	selectList := make([]string, len(cols))
	res.Columns = make([]string, len(cols))
	for i, c := range cols {
		selectList[i] = selectExpr(c, promoteFloat)
		res.Columns[i] = c.name
	}
	query := fmt.Sprintf("SELECT %s FROM %s.%s",
		strings.Join(selectList, ","), quoteIdent(schema), quoteIdent(table))

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return res, fmt.Errorf("scan %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	dest := make([]sql.RawBytes, len(cols))
	ptrs := make([]any, len(cols))
	for i := range dest {
		ptrs[i] = &dest[i]
	}
	values := make([][]byte, len(cols))

	hasher := newRowHasher()
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return res, fmt.Errorf("scan row of %s.%s: %w", schema, table, err)
		}
		for i := range dest {
			// nil RawBytes is SQL NULL; a non-nil empty slice is an empty value.
			// normalize (when it returns its input unchanged) and the
			// fallthrough default both alias dest[i]'s backing buffer, which
			// the driver reuses on the NEXT rows.Scan — safe only because
			// hasher.add below runs synchronously and copies every byte
			// before this loop iterates again. Do not defer or buffer this.
			switch {
			case dest[i] == nil:
				values[i] = nil
			case normalize != nil:
				values[i] = normalize(dest[i], cols[i].dataType)
			default:
				values[i] = dest[i]
			}
		}
		hasher.add(values)
	}
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("iterate rows of %s.%s: %w", schema, table, err)
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return res, fmt.Errorf("commit snapshot: %w", err)
	}
	committed = true

	res.RowCount = hasher.count()
	res.Digest = digestVersion + hasher.digest()
	return res, nil
}

// digestVersion tags the digest with the contract it was computed under — both
// the Go-side encoding (field tagging, FNV-1a/64, additive fold) and the
// MySQL-side rendering (text protocol, session tz UTC, and
// character_set_results = binary so string columns hash as their raw stored
// bytes). Two digests are only comparable when their tags match; a version skew
// must be treated as needs-rebaseline, not a false mismatch — DigestVersionOf
// lets a consumer detect the skew before it byte-compares (see the verify
// capstone's classify). Persisted baselines (#633) carry this tag.
//
// Bumped v1 → v2 for the character_set_results = binary pin (issue #792): the v1
// live scan transcoded string columns to the connection charset (utf8mb4) while
// the persisted baseline digest was always over mydumper's raw `SET NAMES
// binary` bytes, so the two silently disagreed for any non-ASCII legacy-charset
// value. v2 makes both sides read raw bytes. A v1 baseline digest is NOT
// byte-comparable to a v2 scan — regenerate the baseline (its raw-byte content
// is unchanged; only the tag differs).
const digestVersion = "v2:"

// DigestVersion is the current digest contract tag (see digestVersion). Exported
// so a consumer comparing a persisted digest against a freshly computed one can
// tell whether they share a contract.
const DigestVersion = digestVersion

// DigestVersionOf returns the "vN:" version-tag prefix of a digest produced by
// this package (ConsistentTableChecksum / Hasher.Digest) — the text up to and
// including the first ':'. It returns "" when the string carries no recognizable
// tag (e.g. an empty or pre-tag legacy value). Comparing two digests whose tags
// differ would byte-differ even on identical data, so a consumer must degrade to
// a needs-rebaseline signal rather than report a false mismatch.
func DigestVersionOf(digest string) string {
	if i := strings.IndexByte(digest, ':'); i >= 0 {
		return digest[:i+1]
	}
	return ""
}

// The GTID formats TableChecksum.GTIDFlavor names.
const (
	// GTIDFlavorMySQL: GTIDSet is @@gtid_executed (uuid:interval sets).
	GTIDFlavorMySQL = "mysql"
	// GTIDFlavorMariaDB: GTIDSet is @@gtid_binlog_pos (domain-server-seq,
	// one per domain).
	GTIDFlavorMariaDB = "mariadb"
)

// capturedGTID reads the source's executed GTID position inside the snapshot,
// and names its format. MySQL always exposes @@gtid_executed (empty string
// when gtid_mode=OFF). MariaDB has no such variable (1193); its analog is
// @@gtid_binlog_pos, the last GTID written to the binlog per domain, which is
// what the capture reads and what its own gap detection compares with
// (detectMariaDBGTIDGap). A server with neither variable reports an empty set
// and no flavor rather than an error: a missing GTID is a legitimate server
// configuration, not a checksum failure. Any other failure to read the
// variable is an error, on both flavors: a position that could not be read
// is never reported as "no position".
func capturedGTID(ctx context.Context, conn *sql.Conn) (set, flavor string, err error) {
	var gtid sql.NullString
	err = conn.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&gtid)
	if err == nil {
		return strings.TrimSpace(gtid.String), GTIDFlavorMySQL, nil
	}
	if !isUnknownSystemVariable(err) {
		return "", "", fmt.Errorf("read @@gtid_executed: %w", err)
	}
	err = conn.QueryRowContext(ctx, "SELECT @@global.gtid_binlog_pos").Scan(&gtid)
	if err == nil {
		// NULL or empty: a MariaDB whose binlog holds no GTID yet. Still
		// MariaDB, so the caller cannot mistake it for "GTIDs are off".
		return strings.TrimSpace(gtid.String), GTIDFlavorMariaDB, nil
	}
	if !isUnknownSystemVariable(err) {
		return "", "", fmt.Errorf("read @@gtid_binlog_pos: %w", err)
	}
	return "", "", nil
}

func isUnknownSystemVariable(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == erUnknownSystemVariable
}

// column is a non-generated source column: its name and information_schema
// DATA_TYPE (lower-case, e.g. "datetime", "bigint", "varchar").
type column struct {
	name     string
	dataType string
}

// tableColumns returns the non-generated columns of schema.table in ordinal
// order. Generated columns are excluded because mydumper omits them from the
// dump, so they never reach the baseline Parquet.
//
// Generated-ness is read from GENERATION_EXPRESSION, not the EXTRA column: EXTRA
// also reports "DEFAULT_GENERATED" for an ordinary column with an expression
// default (e.g. created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP), so a substring
// match on "GENERATED" would wrongly drop those real, dumped data columns and
// make their corruption invisible to the fingerprint. GENERATION_EXPRESSION is
// non-empty only for true VIRTUAL/STORED generated columns (empty in MySQL, NULL
// in MariaDB for everything else).
func tableColumns(ctx context.Context, conn *sql.Conn, schema, table string) ([]column, error) {
	const q = `
		SELECT COLUMN_NAME, DATA_TYPE, GENERATION_EXPRESSION
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`
	rows, err := conn.QueryContext(ctx, q, schema, table)
	if err != nil {
		return nil, fmt.Errorf("introspect columns of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var cols []column
	for rows.Next() {
		var name, dataType string
		var genExpr sql.NullString
		if err := rows.Scan(&name, &dataType, &genExpr); err != nil {
			return nil, fmt.Errorf("scan column metadata of %s.%s: %w", schema, table, err)
		}
		if genExpr.Valid && strings.TrimSpace(genExpr.String) != "" {
			continue // VIRTUAL/STORED generated column — not in the dump
		}
		cols = append(cols, column{name: name, dataType: strings.ToLower(dataType)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate column metadata of %s.%s: %w", schema, table, err)
	}
	return cols, nil
}

// selectExpr returns the SELECT expression for a column. DATE/DATETIME/TIMESTAMP
// columns are wrapped in CAST(... AS CHAR) to force MySQL's native text
// rendering (e.g. "2021-01-01 00:00:00"). Without it, a connection opened with
// parseTime=true (config.Connect and the test DSNs both do) makes the driver
// decode these into time.Time and re-render them as RFC3339 ("2021-01-01T..Z"),
// which is NOT what mydumper dumps or the binlog carries — so the digest would
// not match a baseline. The CAST makes the canonical form parseTime-independent.
// Other types (including TIME and YEAR, which the driver never parses) are read
// as-is, EXCEPT FLOAT when promoteFloat is set (see below).
//
// promoteFloat controls a FLOAT-only widening: bare `SELECT f` renders through
// MySQL's my_gcvt truncated to ~6 significant digits (FLT_DIG) — lossy
// relative to the underlying binary float32 for any value that needs more
// precision to round-trip (#795). `f+0e0` promotes the read to DOUBLE
// arithmetic, so my_gcvt renders the value's full double-precision decimal
// expansion (enough digits to identify the exact float32), which
// canonicalFloatText(_, 32) then folds back to the same shortest float32
// text a Go-side renderer produces for the identical stored value — closing
// the gap without touching the recon side (see internal/verify/render.go's
// canonicalFloatText doc).
//
// Scoped to promoteFloat (wired to normalize != nil, i.e. only
// ConsistentTableChecksumNormalized's live-source verify caller) rather than
// applied unconditionally: ConsistentTableChecksum's un-normalized digest must
// stay byte-identical to mydumper's own FLOAT text (both MySQL's ordinary
// my_gcvt truncation, matching today) so the baseline-dump-time fidelity
// check (internal/baseline, #633) keeps comparing like renderings — widening
// unconditionally would turn THAT comparison into a false MISMATCH instead
// of fixing this one. `f+0e0` (not CAST(f AS DOUBLE), which needs MySQL
// 8.0.17+) is plain arithmetic promotion, safe on the whole MySQL 8.0+ floor
// and on MariaDB.
func selectExpr(c column, promoteFloat bool) string {
	switch {
	case c.dataType == "date" || c.dataType == "datetime" || c.dataType == "timestamp":
		return "CAST(" + quoteIdent(c.name) + " AS CHAR)"
	case c.dataType == "float" && promoteFloat:
		return quoteIdent(c.name) + "+0e0"
	default:
		return quoteIdent(c.name)
	}
}

// quoteIdent backtick-quotes a MySQL identifier, doubling any embedded backtick.
func quoteIdent(id string) string {
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}

// rowHasher accumulates an order-independent multiset hash over rows.
//
// Each row is hashed independently with FNV-1a/64 (deterministic, no random
// seed — unlike hash/maphash), then folded into the accumulator by addition.
// Addition is commutative (so row order does not matter) and, unlike XOR, does
// not cancel out two identical rows. Each field is length-prefixed and tagged so
// a SQL NULL (tag 0x00) is distinct from an empty value (tag 0x01, length 0) and
// no field-value boundary is ambiguous.
//
// The accumulator is 64-bit: this is a multiset fingerprint for accidental
// corruption/divergence, not a tamper-resistant digest.
type rowHasher struct {
	h   hash.Hash64
	acc uint64
	cnt int64
}

func newRowHasher() *rowHasher { return &rowHasher{h: fnv.New64a()} }

// add folds one row into the accumulator. It hashes values synchronously and
// must not retain the slice — the caller reuses one backing buffer per row.
func (r *rowHasher) add(values [][]byte) {
	r.h.Reset()
	var lb [binary.MaxVarintLen64]byte
	for _, v := range values {
		if v == nil {
			_, _ = r.h.Write([]byte{0x00})
			continue
		}
		_, _ = r.h.Write([]byte{0x01})
		n := binary.PutUvarint(lb[:], uint64(len(v)))
		_, _ = r.h.Write(lb[:n])
		_, _ = r.h.Write(v)
	}
	r.acc += r.h.Sum64()
	r.cnt++
}

func (r *rowHasher) digest() string { return fmt.Sprintf("%016x", r.acc) }
func (r *rowHasher) count() int64   { return r.cnt }
