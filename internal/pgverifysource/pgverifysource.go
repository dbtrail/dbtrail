// Package pgverifysource is the PostgreSQL live-source provider for
// internal/verify (#1024): the PG-native table checksum plus the pinned
// connection and the verify.PGSourceChecksum factory that wires it.
//
// It is a SEPARATE package — not part of internal/consistency — for a
// dependency-boundary reason, not a stylistic one: it links the PostgreSQL
// driver stack (jackc/pgx, pglogrepl, internal/pgcapture), which two guards
// ban from the packages the core binary and the read layer link —
// cliapp's TestCoreBinaryIsPostgresFree (#534: cmd/bintrail must stay
// postgres-free; PG capture lives in cmd/bintrail-pg) and internal/event's
// TestReadLayerDoesNotLinkGoMySQL (#528: the read stack, which reaches
// internal/consistency via reconstruct → baseline, must link no capture
// library). Putting this code in internal/consistency put pgx into both.
// Only pgx-linking binaries import this package: cmd/bintrail-pg (via the
// internal/cli seam, cli.SetPGLiveVerifyConnect) and consoleapp (the watch
// daemon already links the PG capture stack).
package pgverifysource

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// connectPinned opens the live-source PostgreSQL connection with the capture
// plane's pinned render GUCs (TimeZone=UTC, DateStyle=ISO,
// extra_float_digits=3, bytea_output=hex, IntervalStyle=postgres). The pin is
// load-bearing, not a convenience: the reconstructed side's text (pgbaseline
// COPY + pgoutput deltas) was rendered under exactly these GUCs, so a source
// scanned without them (server-local TimeZone, default float digits) would
// digest-differ on identical data — every timestamptz row a conclusive false
// MISMATCH. Unexported on purpose, like the checksum below: LiveSource is the
// ONLY composition this package offers, so the pin invariant is structural —
// no external caller can pair the checksum with an unpinned connection.
func connectPinned(ctx context.Context, dsn string) (*pgx.Conn, error) {
	return pgcapture.ConnectQueryPinned(ctx, dsn)
}

// LiveSource connects to dsn (pinned, via connectPinned) and returns the
// verify.PGSourceChecksum a verify.PGLiveConfig needs, plus the close func
// the caller must defer. This is the one-call wiring both consumers use
// (cmd/bintrail-pg's cli.SetPGLiveVerifyConnect hook and consoleapp's
// runLiveSourcePG), so the connect-pin-checksum composition cannot drift
// between them. The returned checksum func uses the connection serially —
// pgx.Conn is not concurrency-safe — which matches VerifyTablePG's
// one-table-at-a-time loop.
//
// Each table is read at the snapshot of its own temporary logical replication
// slot (#2198, see pinSnapshot), opened on a replication connection derived
// from dsn: the same connection settings plus replication=database.
func LiveSource(ctx context.Context, dsn string) (verify.PGSourceChecksum, func() error, error) {
	repl, err := replConfig(dsn)
	if err != nil {
		return nil, nil, err
	}
	conn, err := connectPinned(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	checksum := func(ctx context.Context, schema, table string, normalize func(raw []byte) []byte) (consistency.TableChecksum, error) {
		return consistentTableChecksumPG(ctx, conn, repl, schema, table, normalize)
	}
	return checksum, func() error { return conn.Close(context.Background()) }, nil
}

// replConfig is the replication connection's config: dsn parsed as it is,
// with replication=database set (replacing any replication value it
// carries). Parsed, not built by text, so a key=value DSN and a URL both
// work.
func replConfig(dsn string) (*pgconn.Config, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgverifysource: parse the source DSN for the replication connection: %w", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["replication"] = "database"
	return cfg, nil
}

// snapshotWait bounds how long creating the temporary slot may wait for the
// write transactions open on the server to end (see pinSnapshot). A variable
// so tests do not wait.
var snapshotWait = 10 * time.Second

// slotPrefix starts the name of every temporary slot verify creates.
const slotPrefix = "bintrail_verify_"

// slotNamed is told the name of every temporary slot before it is created,
// so a test can check that exactly that slot is gone afterwards.
var slotNamed = func(string) {}

// pinnedSnapshot is a temporary slot's exported snapshot, held open on its
// replication connection until it is imported.
type pinnedSnapshot struct {
	name string // the exported snapshot, for SET TRANSACTION SNAPSHOT
	lsn  uint64 // the slot's consistent point
	conn *pgconn.PgConn
}

// release closes the replication connection: the server drops the
// temporary slot with it, so nothing pins WAL after verify, crash or not.
func (p *pinnedSnapshot) release() {
	if p == nil || p.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = p.conn.Close(ctx) // the slot goes with the session either way
	p.conn = nil
}

// pinSnapshot creates a TEMPORARY logical slot with the pgoutput plugin
// (built into PostgreSQL: nothing is installed in the source) and returns
// the snapshot it exported and its consistent point (#2198). That snapshot
// holds exactly the transactions that committed before the consistent point,
// which is how PostgreSQL's own logical replication copies a table and then
// streams from the slot with no gap and no change applied twice. Every row a
// PostgreSQL capture indexes carries its transaction's commit LSN, so the
// reconstruction is cut at the same point by number.
//
// Three outcomes besides the pinned snapshot:
//   - unpinned != "": the slot could not be had: the replication connection
//     did not open (the query connection on the same DSN works, so that
//     failure is about replication: no REPLICATION attribute, no
//     pg_hba.conf replication entry, a pooler that refuses it), or the
//     server refused the slot for a lasting reason (slotUnavailable). The
//     caller reads the table the way it did before #2198 and the verdict
//     says why the read is not cut.
//   - consistency.ErrPGSnapshotWait: creating the slot waits for every write
//     transaction open on the server at that moment, and one outlived
//     snapshotWait (or the server's statement_timeout). The error names the
//     oldest one it can see.
//   - any other error: the check cannot go on.
func pinSnapshot(ctx context.Context, repl *pgconn.Config, queryConn *pgx.Conn) (_ *pinnedSnapshot, unpinned string, _ error) {
	rc, err := pgconn.ConnectConfig(ctx, repl.Copy())
	if err != nil {
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
		return nil, "the replication connection a temporary slot needs could not be opened: " + replErrorText(err), nil
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		_ = rc.Close(context.Background())
		return nil, "", fmt.Errorf("pgverifysource: name the temporary slot: %w", err)
	}
	name := slotPrefix + hex.EncodeToString(b[:])
	slotNamed(name)
	waitCtx, cancel := context.WithTimeout(ctx, snapshotWait)
	defer cancel()
	res, err := pglogrepl.CreateReplicationSlot(waitCtx, rc, name, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: true, Mode: pglogrepl.LogicalReplication, SnapshotAction: "EXPORT_SNAPSHOT"})
	if err != nil {
		_ = rc.Close(context.Background())
		var pgErr *pgconn.PgError
		switch {
		case ctx.Err() != nil:
			return nil, "", ctx.Err()
		case waitCtx.Err() != nil:
			return nil, "", fmt.Errorf("%w (waited %s); %s", consistency.ErrPGSnapshotWait, snapshotWait, oldestOpenTransaction(ctx, queryConn))
		case errors.As(err, &pgErr) && pgErr.Code == "57014":
			// The server's own statement_timeout ended the wait first.
			return nil, "", fmt.Errorf("%w (the server cancelled it: %s); %s", consistency.ErrPGSnapshotWait, pgErr.Message, oldestOpenTransaction(ctx, queryConn))
		case errors.As(err, &pgErr) && slotUnavailable[pgErr.Code]:
			return nil, "a temporary replication slot could not be created: " + replErrorText(err), nil
		}
		return nil, "", fmt.Errorf("pgverifysource: create the temporary slot: %w", err)
	}
	lsn, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil || lsn == 0 || res.SnapshotName == "" {
		_ = rc.Close(context.Background())
		return nil, "", fmt.Errorf("pgverifysource: the temporary slot reported consistent point %q and snapshot %q, which cannot anchor a read", res.ConsistentPoint, res.SnapshotName)
	}
	return &pinnedSnapshot{name: res.SnapshotName, lsn: uint64(lsn), conn: rc}, "", nil
}

// slotUnavailable are the SQLSTATEs of CREATE_REPLICATION_SLOT that mean
// this source cannot give the check a slot at all, so it reads unpinned: no
// privilege (42501), every slot in use (53400), wal_level below logical or a
// standby that cannot decode (55000), logical decoding not supported here
// (0A000). Anything else (a lock or deadlock, the server shutting down) is
// an error: reading unpinned then would blame the slot for a passing
// condition.
var slotUnavailable = map[string]bool{"42501": true, "53400": true, "55000": true, "0A000": true}

// replErrorText words a replication connect or slot error for the verdict,
// with what to grant for the common SQLSTATEs.
func replErrorText(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42501":
			return pgErr.Message + " (the role needs the REPLICATION attribute; on Amazon RDS, the rds_replication role)"
		case "28000":
			return pgErr.Message + " (pg_hba.conf has no replication entry for this role and host)"
		case "53400":
			return pgErr.Message + " (every replication slot is in use: raise max_replication_slots by one)"
		case "55000":
			return pgErr.Message
		}
		return pgErr.Message + " (SQLSTATE " + pgErr.Code + ")"
	}
	return err.Error()
}

// oldestOpenTransaction names the transaction the slot most likely waited
// for: the oldest one holding a transaction id (a prepared transaction
// counts, and outlives its session), as far as this role can see.
func oldestOpenTransaction(ctx context.Context, conn *pgx.Conn) string {
	var (
		gid      string
		prepared time.Time
	)
	err := conn.QueryRow(ctx, "SELECT gid, prepared FROM pg_prepared_xacts ORDER BY prepared LIMIT 1").Scan(&gid, &prepared)
	var preparedNote string
	switch {
	case err == nil:
		return fmt.Sprintf("a prepared transaction %q has been open since %s: it holds the slot until it is committed or rolled back", gid, prepared.UTC().Format(time.RFC3339))
	case !errors.Is(err, pgx.ErrNoRows):
		preparedNote = " (prepared transactions could not be listed: " + err.Error() + ")"
	}
	var (
		pid        int32
		user, app  string
		openFor    *int64
		backendXID *string
	)
	err = conn.QueryRow(ctx, `SELECT pid, coalesce(usename, ''), coalesce(application_name, ''),
		extract(epoch FROM now() - xact_start)::bigint, backend_xid::text
		FROM pg_stat_activity
		WHERE backend_xid IS NOT NULL AND pid <> pg_backend_pid()
		ORDER BY xact_start NULLS LAST LIMIT 1`).Scan(&pid, &user, &app, &openFor, &backendXID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "no open write transaction is visible to this role (pg_stat_activity shows other roles' sessions only with pg_read_all_stats)" + preparedNote + "; run the check again"
	case err != nil:
		return "the open transactions could not be listed: " + err.Error() + preparedNote
	}
	what := fmt.Sprintf("the oldest open write transaction is pid %d", pid)
	if user != "" {
		what += ", user " + user
	}
	if app != "" {
		what += ", application " + app
	}
	if openFor != nil {
		what += fmt.Sprintf(", open for %ds", *openFor)
	}
	return what + preparedNote + "; run the check again when it has ended"
}

// consistentTableChecksumPG is the PostgreSQL sibling of
// consistency.ConsistentTableChecksumNormalized (#1024): a point-in-time,
// order-independent fingerprint of a live PG source table, byte-comparable to
// the digest internal/verify's reconstruct computes from a pgbaseline
// snapshot plus pgoutput deltas (it folds rows through the same
// consistency.Hasher, so the digests share the version-tagged contract).
//
// Why this needs almost no per-type canonicalization, where the MySQL scan
// needed plenty: for a PostgreSQL source BOTH sides of the comparison already
// speak the same rendering — PostgreSQL's own type output functions, run under
// the render GUCs internal/pgcapture pins (see connectPinned). The baseline is
// COPY ... (FORMAT text) under those GUCs (internal/pgbaseline), the deltas
// are pgoutput text under those GUCs, and this scan reads the SAME output
// functions under the SAME GUCs by forcing the wire format to text. The one
// trap this deliberately avoids: a `col::text` CAST is NOT the output function
// — boolean::text renders 'true' while bool_out (what COPY, pgoutput, and a
// text-format result all use) renders 't' — so the SELECT names bare columns
// and the text rendering comes from pgx.QueryResultFormats{TextFormatCode},
// never from casts.
//
// conn MUST be connected with the pinned render GUCs (connectPinned) — an unpinned
// session renders timestamptz in the server's zone and floats at default
// precision, silently breaking byte-comparability. The connection is used
// serially: a transaction is opened and committed within this call.
//
// The whole computation runs inside one REPEATABLE READ, READ ONLY transaction
// — PG's MVCC snapshot is the consistency anchor, the sibling of MySQL's
// START TRANSACTION WITH CONSISTENT SNAPSHOT. With repl set, that transaction
// imports the snapshot a temporary slot exported (pinSnapshot, #2198) and the
// anchor is the slot's consistent point: exact (Anchor = AnchorPGSlot).
// Without it, or when the slot cannot be had, the WAL anchor
// (pg_current_wal_lsn) is captured by the transaction's FIRST statement, which
// is also what materializes the snapshot, so the anchor and the data describe
// the same instant modulo the lock-free window documented on
// consistency.TableChecksum.GTIDSet (pg_current_wal_lsn is global, not
// MVCC-filtered — exactly like @@gtid_executed), and AnchorUnpinned says why.
// It errors on a standby (pg_current_wal_lsn requires a primary), same as
// pgbaseline's own anchor read.
//
// Column set: the live, non-dropped, NON-GENERATED columns in attnum order —
// the same contract as internal/pgbaseline's loadColumns, duplicated here
// rather than imported to keep this provider free of pgbaseline's Parquet
// stack. Generated columns must stay out for the same reason they are out of
// the baseline: pgoutput never streams them on PG 14–17, so the reconstructed
// side cannot carry them and including them here would digest-differ on every
// row.
//
// normalize, when non-nil, rewrites a scanned value's raw text bytes before
// hashing — the same symmetric-normalization hook
// consistency.ConsistentTableChecksumNormalized takes, minus the DATA_TYPE
// parameter: metadata.WritePGSnapshot stores an EMPTY data_type for every PG
// column, so the reconstruct side renders under dataType "" and the live side
// must apply the identical policy; a type-keyed hook here would invite an
// asymmetric normalization that can never trigger on the other side. It is
// never called for SQL NULL (nil raw bytes). VerifyTablePG passes its own
// render normalizer through verify.PGSourceChecksum, so the two sides are
// symmetric by construction.
func consistentTableChecksumPG(ctx context.Context, conn *pgx.Conn, repl *pgconn.Config, schema, table string, normalize func(raw []byte) []byte) (consistency.TableChecksum, error) {
	res := consistency.TableChecksum{Schema: schema, Table: table}
	if conn == nil {
		return res, fmt.Errorf("pgverifysource: no PostgreSQL source connection")
	}

	var pin *pinnedSnapshot
	if repl == nil {
		res.AnchorUnpinned = "no replication connection was configured for the check"
	} else {
		var err error
		pin, res.AnchorUnpinned, err = pinSnapshot(ctx, repl, conn)
		if err != nil {
			return res, err
		}
		defer pin.release()
	}

	if _, err := conn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		return res, fmt.Errorf("begin repeatable read snapshot: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			// Read-only transaction; rollback is best-effort cleanup.
			_, _ = conn.Exec(context.Background(), "ROLLBACK")
		}
	}()

	if pin != nil {
		// Must be the transaction's first statement. The snapshot name comes
		// from the server, quoted as a literal anyway.
		if _, err := conn.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+strings.ReplaceAll(pin.name, "'", "''")+"'"); err != nil {
			return res, fmt.Errorf("adopt the temporary slot's snapshot %q: %w", pin.name, err)
		}
		// Imported: the slot is no longer needed, and dropping it now keeps
		// it from holding anything back for the length of the scan.
		pin.release()
		res.LSN = pin.lsn
		res.Anchor = consistency.AnchorPGSlot
	} else {
		// First statement in the transaction: materializes the MVCC snapshot
		// AND captures the WAL anchor at that same instant.
		var lsnText string
		if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnText); err != nil {
			return res, fmt.Errorf("read WAL anchor (pg_current_wal_lsn requires a primary): %w", err)
		}
		lsn, err := pglogrepl.ParseLSN(lsnText)
		if err != nil {
			return res, fmt.Errorf("parse WAL anchor %q: %w", lsnText, err)
		}
		res.LSN = uint64(lsn)
	}

	cols, err := pgTableColumns(ctx, conn, schema, table)
	if err != nil {
		return res, err
	}
	if len(cols) == 0 {
		return res, fmt.Errorf("table %s.%s has no columns (does it exist?)", schema, table)
	}
	res.Columns = cols

	selectList := make([]string, len(cols))
	for i, c := range cols {
		selectList[i] = pgx.Identifier{c}.Sanitize()
	}
	query := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(selectList, ","), pgx.Identifier{schema, table}.Sanitize())

	// A single format code applies to every result column (PostgreSQL Bind
	// semantics) — the server renders each value through its type OUTPUT
	// function, the exact bytes COPY text and pgoutput produce for the same
	// stored value.
	rows, err := conn.Query(ctx, query, pgx.QueryResultFormats{pgx.TextFormatCode})
	if err != nil {
		return res, fmt.Errorf("scan %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	hasher := consistency.NewHasher()
	values := make([][]byte, len(cols))
	for rows.Next() {
		raw := rows.RawValues()
		if len(raw) != len(cols) {
			return res, fmt.Errorf("scan %s.%s: row has %d values, want %d", schema, table, len(raw), len(cols))
		}
		for i, v := range raw {
			// nil is SQL NULL; a non-nil empty slice is an empty value. Both
			// raw and a pass-through normalize alias pgx's row buffer, which
			// is reused on the NEXT rows.Next — safe only because AddBytes
			// below hashes synchronously and copies every byte. Do not defer
			// or buffer this (same aliasing contract as the MySQL scan).
			switch {
			case v == nil:
				values[i] = nil
			case normalize != nil:
				values[i] = normalize(v)
			default:
				values[i] = v
			}
		}
		hasher.AddBytes(values)
	}
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("iterate rows of %s.%s: %w", schema, table, err)
	}
	rows.Close()

	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		return res, fmt.Errorf("commit snapshot: %w", err)
	}
	committed = true

	res.RowCount = hasher.Count()
	res.Digest = hasher.Digest()
	return res, nil
}

// pgTableColumns returns the live, non-dropped, non-generated columns of
// schema.table in attnum order — the same catalog contract as
// internal/pgbaseline's loadColumns (see consistentTableChecksumPG's doc for
// why it is duplicated rather than imported, and why generated columns are
// excluded).
func pgTableColumns(ctx context.Context, conn *pgx.Conn, schema, table string) ([]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT a.attname
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
		  AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
		ORDER BY a.attnum`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("introspect columns of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan column metadata of %s.%s: %w", schema, table, err)
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate column metadata of %s.%s: %w", schema, table, err)
	}
	return cols, nil
}
