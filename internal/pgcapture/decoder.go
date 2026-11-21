package pgcapture

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// UnchangedToastKey is the single key of the structurally-distinct marker the
// decoder emits for an unchanged-TOAST value ('u') that could NOT be resolved from
// the before-image: always under a partly captured earlier row (a table below
// REPLICA IDENTITY FULL), never under a whole one. The canonical constant and its
// full rationale (why a one-key map, the #533 forward constraint) live in
// the source-neutral internal/event package: the read side — recovery, reconstruct,
// the shim — must detect a residual marker and fail loud (#592), and the #528
// depguard bans those packages from linking pgcapture. Aliased here so the decoder
// and its tests keep reading naturally.
const UnchangedToastKey = event.UnchangedToastKey

// Decoder turns a pgoutput logical-replication message stream into source-neutral
// event.Event values. It is stateful across messages within a stream: it caches
// each RelationMessage (column names + types + primary key) and the in-flight
// transaction's commit LSN and timestamp.
//
// Decode performs NO I/O. The one piece of state that needs the database — a
// relation's primary-key columns — is supplied by an injected PKResolver, so the
// decode logic is fully unit-testable without a live PostgreSQL: the capturer wires
// a catalog-query PKResolver, tests stub it.
type Decoder struct {
	resolvePK    PKResolver
	attrResolver AttrResolver // identity/generated flags (#557); nil = no flags
	filters      event.Filters
	logger       *slog.Logger

	relations map[uint32]*relationInfo
	txn       txnContext

	// pending buffers events a single Decode produced beyond its one return value.
	// Only a multi-relation TRUNCATE fills it: `TRUNCATE a, b` arrives as ONE
	// pgoutput message covering several relations, but Decode returns a single
	// event — so the first in-scope marker is returned and the rest are queued here
	// for the caller to drain via TakePending after every Decode (#827).
	pending []event.Event

	// infer, when set (WithDDLInference), turns Relation shape changes into
	// inferred schema_changes rows (#2441). nil = shapes change silently.
	infer *inferState

	// timescaleWarned makes the TimescaleDB out-of-scope guard (#559) warn ONCE per
	// stream: a busy hypertable has many chunk relations, and one warning per chunk
	// would flood the log.
	timescaleWarned bool

	// skips is the capture-skip ledger (WithSkipCounters): every dropped change of
	// a skipped relation (relationInfo.skipped) is recorded there under the
	// relation's reason, and every emitted row breaks the consecutive-skip run.
	// nil records nothing (all methods are nil-safe).
	skips *parser.SkipCounters

	// deferrable, when set (WithDeferrableResolver), says whether a relation's
	// primary key is declared DEFERRABLE, asked each time PostgreSQL describes
	// the relation. nil = not asked (the check at capture start is then the
	// only one).
	deferrable DeferrableResolver

	// partitions, when set (WithPartitionsResolver), says what the catalog
	// knows of the partitions of a relation described at REPLICA IDENTITY FULL:
	// their old tuples arrive labeled as whole rows whatever they hold. nil =
	// not asked.
	partitions PartitionsResolver
}

// The capture-skip reasons (parser.SkipCounters, persisted in
// stream_state.capture_skips). Stable strings: `status` shows them verbatim, and
// renaming one would split its history.
const (
	// SkipReplicaIdentityNotFull is the reason earlier versions recorded for
	// every change of a table that was not at REPLICA IDENTITY FULL, which they
	// dropped. Such a table is captured now (its earlier rows in part), so
	// nothing records this reason any more. It stays for the entries an index
	// already holds: those changes are still missing, and the readers of the
	// ledger still send their tables to a full read.
	SkipReplicaIdentityNotFull = "replica_identity_not_full"

	// SkipReplicaIdentityIndexWithoutPrimaryKey is recorded for a change of a
	// table whose replica identity does not hold every primary-key column: in
	// practice REPLICA IDENTITY USING INDEX on an index that leaves a
	// primary-key column out. PostgreSQL then sends no earlier row at all for an
	// UPDATE that changes the primary key, and an earlier row without the
	// primary key for a DELETE, so the change cannot be filed under the row it
	// changed. The table's changes are dropped until it is described again
	// with an identity that holds the key (DEFAULT, FULL, or a wider index).
	SkipReplicaIdentityIndexWithoutPrimaryKey = "replica_identity_index_without_primary_key"

	// SkipDeferrablePrimaryKey is recorded for a change of a table whose primary
	// key the catalog said was DEFERRABLE when PostgreSQL described the table
	// (deferrable.go: inside a transaction two rows can then hold one key, and
	// the copy is kept by key).
	SkipDeferrablePrimaryKey = "primary_key_deferrable"

	// SkipReplicaIdentityKeyNotSent is recorded for ONE change whose earlier
	// row, captured in part, came without a usable value for a primary-key
	// column. PostgreSQL is not known to send such a change (the identity
	// columns of a key-only old row are never NULL and never left out); should
	// one arrive, it is dropped and counted, never stored under a made-up key
	// and never the end of the stream.
	SkipReplicaIdentityKeyNotSent = "replica_identity_key_not_sent"

	// SkipPrimaryKeyNotInDescription is recorded for a change of a table whose
	// primary key, as the catalog has it now, names a column that PostgreSQL's
	// description of the table does not hold: capture is behind, and the key
	// was moved to a column added after these changes were written. Nothing is
	// to be run for it: the description that follows the change of key holds
	// the column, and the table is captured again from there.
	SkipPrimaryKeyNotInDescription = "primary_key_not_in_description"
)

// WithPartitionsResolver wires the catalog-backed read of a relation's
// partitions, asked on every description of a relation at FULL. The capturer
// passes it.
func WithPartitionsResolver(r PartitionsResolver) DecoderOption {
	return func(d *Decoder) { d.partitions = r }
}

// WithDeferrableResolver wires the catalog-backed check of a deferrable primary
// key, asked on every description of a relation. The capturer passes it; tests
// that do not exercise it leave it out.
func WithDeferrableResolver(r DeferrableResolver) DecoderOption {
	return func(d *Decoder) { d.deferrable = r }
}

// WithSkipCounters wires the capture-skip ledger the decoder records into. The
// capturer passes Config.Skips; tests pass their own.
func WithSkipCounters(s *parser.SkipCounters) DecoderOption {
	return func(d *Decoder) { d.skips = s }
}

// DecoderOption configures optional Decoder dependencies without churning the
// NewDecoder call sites that don't need them.
type DecoderOption func(*Decoder)

// WithAttrResolver wires the catalog-backed identity/generated lookup (#557). The
// capturer passes it; tests that don't exercise identity/generated omit it (the
// flags then stay false, which is the safe default for the recovery skip-sets).
func WithAttrResolver(r AttrResolver) DecoderOption {
	return func(d *Decoder) { d.attrResolver = r }
}

// txnContext carries the in-flight transaction's commit metadata. pgoutput puts
// the commit LSN and timestamp on the Begin/Commit messages, never on the row
// messages, so the decoder records them at Begin and stamps every row event in the
// transaction with them. All rows of a logical-replication transaction are
// delivered at commit and share its timestamp — the correct event_timestamp for
// partitioning (binlog_events partitions on TO_SECONDS(event_timestamp); a zero
// timestamp would land rows in the wrong partition).
type txnContext struct {
	commitLSN  pglogrepl.LSN
	commitTime time.Time
	open       bool
}

// NewDecoder constructs a Decoder. resolvePK supplies primary-key columns per
// relation (catalog-backed in the capturer, stubbed in tests) and must be non-nil.
// filters restricts which schema/table produce events (the zero Filters accepts
// all). logger may be nil (slog.Default() is used). Optional dependencies (e.g. the
// identity/generated AttrResolver, #557) are passed as DecoderOptions.
func NewDecoder(resolvePK PKResolver, filters event.Filters, logger *slog.Logger, opts ...DecoderOption) *Decoder {
	// resolvePK is mandatory (every row event needs a PK source). A nil resolver
	// would otherwise survive construction and panic deep inside cacheRelation on
	// the first RelationMessage; fail at the wiring site instead.
	if resolvePK == nil {
		panic("pgcapture: NewDecoder requires a non-nil PKResolver")
	}
	if logger == nil {
		logger = slog.Default()
	}
	d := &Decoder{
		resolvePK: resolvePK,
		filters:   filters,
		logger:    logger,
		relations: make(map[uint32]*relationInfo),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Decode processes one pgoutput message. It returns:
//   - (event, true, nil) when the message produces a row, commit, or DDL-marker
//     event (a TRUNCATE covering several relations returns its first marker here
//     and queues the rest for TakePending);
//   - (zero, false, nil) when the message is consumed for internal state only
//     (Begin/Relation) or produces nothing to index (Type/Origin, or a TRUNCATE
//     whose relations are all out of scope);
//   - (zero, false, err) on a decode-invariant violation (unknown relation, a row
//     outside a transaction, a binary tuple datum, a tuple column-count mismatch,
//     or a primary-key lookup failure). The caller must treat a non-nil error as
//     fatal — never skip the message and continue, or the stream desynchronizes.
//
// After every Decode the caller MUST drain TakePending: a multi-relation TRUNCATE
// buffers all but its first marker there.
func (d *Decoder) Decode(msg pglogrepl.Message) (event.Event, bool, error) {
	switch m := msg.(type) {
	case *pglogrepl.BeginMessage:
		// FinalLSN is the transaction's commit LSN; CommitTime its commit timestamp.
		d.txn = txnContext{commitLSN: m.FinalLSN, commitTime: m.CommitTime, open: true}
		return event.Event{}, false, nil

	case *pglogrepl.RelationMessage:
		if err := d.cacheRelation(m); err != nil {
			return event.Event{}, false, err
		}
		// A skipped relation (a deferrable primary key, or a replica identity
		// without the primary key) produces nothing: no schema snapshot (no row
		// will reference it) and no DDL inference (the description that resumes
		// it is then its first observed shape, the same as a table entering
		// capture).
		if d.relations[m.RelationID].skipped {
			return event.Event{}, false, nil
		}
		// Inferred DDL first (#2441): the consumer writes each event in order,
		// and a schema-change row written before the new shape's snapshot
		// survives a crash between the two (the redelivered Relation writes a
		// duplicate row); the reverse order would lose the change, because the
		// restart would find the new shape already known. Inference tracks
		// every relation; only in-scope rows are emitted.
		var out []event.Event
		if d.infer != nil {
			inScope := func(n relName) bool { return d.filters.Matches(n.schema, n.table) }
			inferred, err := d.infer.observe(m, d.txn, inScope, d.logger.Warn)
			if err != nil {
				return event.Event{}, false, err
			}
			for _, ev := range inferred {
				if d.ddlInScope(ev) {
					out = append(out, ev)
				}
			}
		}
		// Emit a schema snapshot for the consumer to persist (#533), but only for an
		// in-scope relation: cacheRelation caches EVERY relation so row decoding can
		// resolve any OID, yet a filtered-out table must not write snapshot rows or
		// stamp a SchemaVersion. Gate the EMIT, not the cache.
		rel := d.relations[m.RelationID]
		if d.filters.Matches(rel.schema, rel.table) {
			out = append(out, relationEvent(rel, d.txn, d.infer != nil))
		}
		if len(out) == 0 {
			return event.Event{}, false, nil
		}
		d.pending = append(d.pending, out[1:]...)
		return out[0], true, nil

	case *pglogrepl.InsertMessage:
		return d.decodeInsert(m)

	case *pglogrepl.UpdateMessage:
		return d.decodeUpdate(m)

	case *pglogrepl.DeleteMessage:
		return d.decodeDelete(m)

	case *pglogrepl.CommitMessage:
		// The commit boundary: the ONLY event the durable cursor advances on (the
		// #491 invariant in PostgreSQL clothing). The commit LSN travels in GTID —
		// the field the consumer reads to advance stream_state.gtid_set.
		ev := event.Event{
			BinlogFile: m.CommitLSN.String(),
			EndPos:     uint64(m.CommitLSN),
			Timestamp:  m.CommitTime,
			GTID:       m.CommitLSN.String(),
			EventType:  event.EventCommit,
		}
		d.txn = txnContext{}
		return ev, true, nil

	case *pglogrepl.TruncateMessage:
		// A TRUNCATE removes every row of each listed relation but emits NO per-row
		// DELETE — so left uncaptured it is indistinguishable from "nothing happened",
		// and a later reconstruct crossing it would silently resurrect the rows (#827).
		// Record it as a durable audit MARKER: an EventDDL(DDLTruncateTable) the
		// consumer writes to schema_changes, the source-neutral analog of the MySQL
		// DDL path. This is a RECORD that the truncate happened, not a replayable
		// change (DDL replay stays out of #530 scope; recover has no per-row images to
		// put back). One message can list several relations (TRUNCATE a, b); emit one
		// marker per in-scope relation, returning the first and buffering the rest for
		// TakePending so none is silently dropped.
		var first event.Event
		haveFirst := false
		for _, oid := range m.RelationIDs {
			rel, ok := d.relations[oid]
			if !ok {
				// No cached shape (no preceding Relation message) — the relation cannot
				// be named, so there is nothing meaningful to record. Warn rather than
				// fabricate a blank marker.
				d.logger.Warn("pgcapture: TRUNCATE on uncached relation OID — not recorded", "oid", oid)
				continue
			}
			if !d.filters.Matches(rel.schema, rel.table) {
				continue
			}
			ev := d.truncateEvent(rel)
			if !haveFirst {
				first, haveFirst = ev, true
				continue
			}
			d.pending = append(d.pending, ev)
		}
		if !haveFirst {
			return event.Event{}, false, nil
		}
		return first, true, nil

	case *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
		// No row data. Type/Origin are not needed for capture fidelity under
		// proto_version 1 (column names + text values arrive regardless).
		return event.Event{}, false, nil

	default:
		// The slice-2 capturer requests proto_version 1, which emits no in-progress-
		// transaction streaming (v2) messages, so any other type is unexpected; log
		// rather than fail (it carries no row data we could be silently dropping). A
		// future bump to proto v2 would route real row-bearing stream messages here —
		// revisit this branch before negotiating v2.
		d.logger.Debug("pgcapture: ignoring unhandled message", "type", fmt.Sprintf("%T", msg))
		return event.Event{}, false, nil
	}
}

// cacheRelation records (or refreshes) a relation's columns + primary key. PG emits
// a fresh RelationMessage before the first change to a relation in a session and
// again after a relation's shape changes, so this doubles as cache invalidation —
// the analog of the MySQL parser's SwapResolver-on-DDL.
func (d *Decoder) cacheRelation(m *pglogrepl.RelationMessage) error {
	// TimescaleDB out-of-scope guard (#559): a hypertable's data lives in physical
	// CHUNK tables (_hyper_<id>_<n>_chunk) under the _timescaledb_internal schema, and
	// pgoutput streams those chunk relations by their physical names — so bintrail would
	// index changes under _timescaledb_internal._hyper_* rather than the logical
	// hypertable (a decode-granularity mismatch). We do NOT silently skip them, but the
	// operator MUST know the captured target is the chunk, not the parent table — warn
	// once, before the checks below so the signal survives a chunk that ends up
	// skipped. This is BEFORE the cache write: the warning is about target identity,
	// not a reason to stop.
	if !d.timescaleWarned && isTimescaleChunk(m.Namespace, m.RelationName) {
		d.logger.Warn("pgcapture: TimescaleDB hypertable chunk detected — bintrail indexes raw chunk relations under _timescaledb_internal, NOT the logical hypertable; TimescaleDB is out of scope (decode-granularity mismatch)",
			"schema", m.Namespace, "table", m.RelationName)
		d.timescaleWarned = true
	}

	inScope := d.filters.Matches(m.Namespace, m.RelationName)

	// A primary key declared DEFERRABLE, asked of the catalog at every
	// description of the relation and not only when capture starts: a table
	// that joins the publication, or whose key is rebuilt as deferrable while
	// capture runs, is described before its next change. The table is SKIPPED
	// under a reason of its own; every other table keeps flowing. What this
	// reads is the catalog as it is now, which is all that can be asked: a
	// change PostgreSQL does not describe the relation again for is seen at the
	// next start (CheckDeferrableKeys).
	if d.deferrable != nil {
		constraint, err := d.deferrable(m.RelationID, m.Namespace, m.RelationName)
		if err != nil {
			return fmt.Errorf("pgcapture: deferrable-key lookup for %s.%s (oid %d): %w",
				m.Namespace, m.RelationName, m.RelationID, err)
		}
		if constraint != "" {
			fix := deferrableKeyFix(m.Namespace, m.RelationName, constraint)
			d.skipRelation(m, SkipDeferrablePrimaryKey,
				fmt.Sprintf("primary key (constraint %s) is declared DEFERRABLE; fix: %s", pgx.Identifier{constraint}.Sanitize(), fix))
			if inScope {
				d.logger.Warn("pgcapture: table has a primary key declared DEFERRABLE; its changes are NOT captured (each one is counted as skipped in capture health) until the key is not deferrable, the other tables keep flowing. Inside a transaction such a key lets two rows hold the same value, and the copy is kept by key",
					"table", m.Namespace+"."+m.RelationName,
					"constraint", constraint,
					"fix", fix)
			}
			return nil
		}
	}

	cols := make([]relColumn, len(m.Columns))
	hasIdentity := false
	for i, c := range m.Columns {
		cols[i] = relColumn{name: c.Name, typeOID: c.DataType, typeMod: c.TypeModifier, inIdentity: c.Flags&1 != 0}
		hasIdentity = hasIdentity || cols[i].inIdentity
	}

	// Identity/generated flags (#557) — not in the RelationMessage, so a catalog
	// lookup, same as the PK. A genuine lookup failure fails loud rather than index
	// rows recovery would emit un-runnable SQL for. (Absent resolver → flags stay
	// false, the safe default for the recovery skip-sets.)
	if d.attrResolver != nil {
		attrs, err := d.attrResolver(m.RelationID, m.Namespace, m.RelationName)
		if err != nil {
			return fmt.Errorf("pgcapture: column-attr lookup for %s.%s (oid %d): %w",
				m.Namespace, m.RelationName, m.RelationID, err)
		}
		for i := range cols {
			if a, ok := attrs[cols[i].name]; ok {
				cols[i].isIdentityAlways = a.IsIdentityAlways
				cols[i].isGenerated = a.IsGenerated
			}
		}
	}

	pkCols, err := d.resolvePK(m.RelationID, m.Namespace, m.RelationName)
	if err != nil {
		return fmt.Errorf("pgcapture: primary-key lookup for %s.%s (oid %d): %w",
			m.Namespace, m.RelationName, m.RelationID, err)
	}

	// The catalog PK lookup reads the CURRENT schema on a separate connection, while
	// the in-band column names belong to this RelationMessage. If a PK column name is
	// absent from the relation's columns, the two have diverged: capture is
	// behind, and the primary key has since moved to a column added after these
	// changes were written. A key cannot be built for them, so the TABLE is
	// skipped under a reason of its own, and the other tables go on. It used to
	// be an error that ended the stream, which PostgreSQL then repeated at
	// every restart (it re-sends the same description) for every table; while
	// only tables at FULL reached this line that was rare, and tables at
	// DEFAULT reach it now. The description that follows the change of key
	// replaces this entry.
	colSet := make(map[string]bool, len(cols))
	for _, c := range cols {
		colSet[c.name] = true
	}
	var absent []string
	for _, pk := range pkCols {
		if !colSet[pk.Name] {
			absent = append(absent, pk.Name)
		}
	}
	if len(absent) > 0 {
		d.skipRelation(m, SkipPrimaryKeyNotInDescription,
			fmt.Sprintf("primary-key column(s) %s of the catalog are not among the columns PostgreSQL described for these changes (the key was changed after they were written); they cannot be filed under a key. Nothing to run: capture of the table resumes at the changes made after the key was changed",
				strings.Join(absent, ", ")))
		if inScope {
			d.logger.Warn("pgcapture: table's primary key is not among the columns PostgreSQL described for its changes (capture is behind, and the key was changed after they were written); these changes are NOT captured (each one is counted as skipped in capture health), the other tables keep flowing, and the table resumes by itself at the changes made after the key was changed",
				"table", m.Namespace+"."+m.RelationName,
				"primary_key_columns_not_described", strings.Join(absent, ", "))
		}
		return nil
	}

	// Order PK columns by their table-column (ordinal) position. The catalog query
	// returns them in PK-KEY order, which diverges from table order for a composite
	// PRIMARY KEY declared out of column order (e.g. PRIMARY KEY (b, a) on a table
	// (a, b)). event.BuildPKValues here would then build pk_values in key order,
	// while the offline resolver (metadata.PKColumnMetas) yields the PK columns in
	// table-ordinal order — and reconstruct pairs the two POSITIONALLY, silently
	// corrupting the merge. Reordering to table-ordinal keeps the cross-source
	// invariant pk_values == BuildPKValues(resolver PKColumnMetas, row). Single-column
	// PKs are unaffected; all pkCols names are present in cols (drift check above).
	if len(pkCols) > 1 {
		ordinalOf := make(map[string]int, len(cols))
		for i, c := range cols {
			ordinalOf[c.name] = i
		}
		sort.SliceStable(pkCols, func(a, b int) bool {
			return ordinalOf[pkCols[a].Name] < ordinalOf[pkCols[b].Name]
		})
	}

	// Below REPLICA IDENTITY FULL ('d' default, 'i' using index, 'n' nothing)
	// PostgreSQL sends, of the row as it was before an UPDATE or a DELETE, the
	// identity columns only, or no earlier row at all. The table is captured
	// all the same (decodeUpdate / decodeDelete store that earlier row in part,
	// and only undo needs it whole), on one condition: the identity holds every
	// primary-key column. Where it does not (USING INDEX on an index that leaves
	// a primary-key column out; or a description whose key differs from the
	// primary key the catalog has now), an UPDATE of the primary key arrives
	// with no earlier row and a DELETE with one that lacks the key, so the
	// change could not be filed under the row it changed, and the copy would
	// keep a row the source no longer has. Such a table stays SKIPPED, under a
	// reason of its own. A table with no identity at all sends INSERT and
	// TRUNCATE only (PostgreSQL refuses the rest) and needs no key.
	//
	// Skipped, not fatal, for the reason the old gate was: PostgreSQL decodes
	// the WAL with the catalog as it was when the change was written, so the
	// slot re-sends the same description on every restart, even after the
	// ALTER. The next description of the table replaces this entry.
	if m.ReplicaIdentity != 'f' && hasIdentity {
		var missing []string
		for _, pk := range pkCols {
			for _, c := range cols {
				if c.name == pk.Name && !c.inIdentity {
					missing = append(missing, pk.Name)
				}
			}
		}
		if len(missing) > 0 {
			fix := identityWithoutKeyFix(m.Namespace, m.RelationName, m.ReplicaIdentity)
			d.skipRelation(m, SkipReplicaIdentityIndexWithoutPrimaryKey,
				fmt.Sprintf("replica identity %s does not hold primary-key column(s) %s; fix: %s",
					replicaIdentityName(m.ReplicaIdentity), strings.Join(missing, ", "), fix))
			if inScope {
				d.logger.Warn("pgcapture: table's replica identity does not hold its primary key; its changes are NOT captured (each one is counted as skipped in capture health) until it does, the other tables keep flowing. PostgreSQL would send no earlier row for an UPDATE of the primary key, and one without the key for a DELETE",
					"table", m.Namespace+"."+m.RelationName,
					"replica_identity", replicaIdentityName(m.ReplicaIdentity),
					"primary_key_columns_left_out", strings.Join(missing, ", "),
					"fix", fix)
			}
			return nil
		}
	}

	// A description at FULL flags every column, which says nothing about what
	// a key-only tuple would hold. One can still arrive, labeled as a whole
	// row: a partition below FULL under a root at FULL (PartitionsResolver).
	// The columns such a tuple is trusted for are the primary key's, the
	// identity of a partition left at DEFAULT.
	//
	// What the catalog says decides how the old rows of this description are
	// read, and is asked again at every description (nothing is remembered
	// from an earlier one):
	//   - not partitioned: every old row is whole, as ever;
	//   - partitioned: each old row by what it holds (keyOnlyShaped). A row
	//     from a partition at FULL is whole and can be undone, one from a
	//     partition at DEFAULT is key-only, whatever the other partitions are;
	//   - partitioned, with a partition whose own identity index holds a
	//     column outside the primary key: its key-only rows carry a value
	//     outside the key and look whole, so every old row is stored in part;
	//   - partitioned, with a partition whose own identity index leaves the
	//     primary key out: the table is skipped, as a table with such an
	//     identity is, since a change of key made there has no earlier row;
	//   - the catalog has no row for it (dropped since), or could not be read:
	//     not known, so each old row by what it holds. Capture goes on.
	var parts Partitions
	byShape := false
	if m.ReplicaIdentity == 'f' {
		isPK := make(map[string]bool, len(pkCols))
		for _, pk := range pkCols {
			isPK[pk.Name] = true
		}
		for i := range cols {
			cols[i].inIdentity = isPK[cols[i].name]
		}
		if d.partitions != nil {
			var perr error
			parts, perr = d.partitions(m.RelationID, m.Namespace, m.RelationName)
			switch {
			case perr != nil:
				parts = Partitions{}
				byShape = true
				d.logger.Warn("pgcapture: could not read from the catalog whether the table is partitioned; capture goes on, and until the table is described again an earlier row of it that holds nothing outside the primary key is stored with its key only (undo refuses that change)",
					"table", m.Namespace+"."+m.RelationName, "error", perr)
			case !parts.Known:
				byShape = true
			default:
				byShape = parts.Partitioned
			}
		}
	}
	if parts.MissesKey > 0 {
		d.skipRelation(m, SkipReplicaIdentityIndexWithoutPrimaryKey,
			fmt.Sprintf("%d partition(s) of the table have a replica identity of their own (USING INDEX) that does not hold the primary key; fix: ALTER TABLE <partition> REPLICA IDENTITY DEFAULT (or FULL) on each (`bintrail-pg doctor` lists them)", parts.MissesKey))
		if inScope {
			d.logger.Warn("pgcapture: a partition of the table has a replica identity of its own that does not hold the primary key; the table's changes are NOT captured (each one is counted as skipped in capture health) until it does, the other tables keep flowing. PostgreSQL would send no earlier row for an UPDATE of the primary key made in that partition. Run ALTER TABLE <partition> REPLICA IDENTITY DEFAULT (or FULL) on each such partition; `bintrail-pg doctor` lists them",
				"table", m.Namespace+"."+m.RelationName,
				"partitions", parts.MissesKey)
		}
		return nil
	}

	d.relations[m.RelationID] = &relationInfo{
		oid:             m.RelationID,
		schema:          m.Namespace,
		table:           m.RelationName,
		columns:         cols,
		pkCols:          pkCols,
		replicaIdentity: m.ReplicaIdentity,
		partial:         m.ReplicaIdentity != 'f' || parts.Wide > 0,
		byShape:         byShape,
	}
	if parts.BelowFull > 0 && inScope {
		d.logger.Warn("pgcapture: partitioned table with partitions without undo: it is published through its root, which is at REPLICA IDENTITY FULL, and some of its partitions are not (REPLICA IDENTITY is set per partition; ALTER TABLE on the parent does not reach them). The changes of every partition are captured and everything else works on them; undo refuses the UPDATE and DELETE made in a partition that is not at FULL. Run ALTER TABLE <partition> REPLICA IDENTITY FULL on each to make its later changes undoable",
			"table", m.Namespace+"."+m.RelationName,
			"partitions_not_at_full", parts.BelowFull,
			"every_change_stored_with_its_key_only", parts.Wide > 0)
	}
	if m.ReplicaIdentity != 'f' && inScope {
		if hasIdentity {
			d.logger.Warn("pgcapture: table without undo: not at REPLICA IDENTITY FULL. Its changes are captured and everything else works on them; undo refuses its UPDATE and DELETE. The statement in fix makes later changes undoable (the table then writes more WAL; on PostgreSQL 18 run `bintrail-pg doctor` first, a FULL table with a generated column the publication does not publish refuses UPDATE and DELETE)",
				"table", m.Namespace+"."+m.RelationName,
				"replica_identity", replicaIdentityName(m.ReplicaIdentity),
				"fix", replicaIdentityFix(m.Namespace, m.RelationName))
		} else {
			d.logger.Warn("pgcapture: table has no replica identity (NOTHING, or DEFAULT without a primary key): PostgreSQL itself refuses UPDATE and DELETE on it while it is published, so only its INSERT and TRUNCATE are captured. Give it a primary key, or run the statement in fix",
				"table", m.Namespace+"."+m.RelationName,
				"replica_identity", replicaIdentityName(m.ReplicaIdentity),
				"fix", replicaIdentityFix(m.Namespace, m.RelationName))
		}
	}
	return nil
}

// skipRelation caches m as a relation whose changes are dropped and recorded
// under reason, with detail as what the ledger shows for it.
func (d *Decoder) skipRelation(m *pglogrepl.RelationMessage, reason, detail string) {
	d.relations[m.RelationID] = &relationInfo{
		oid:             m.RelationID,
		schema:          m.Namespace,
		table:           m.RelationName,
		replicaIdentity: m.ReplicaIdentity,
		skipped:         true,
		skipReason:      reason,
		skipDetail:      detail,
	}
}

// replicaIdentityFix is the statement that puts a table at REPLICA IDENTITY FULL,
// with both names quoted so a mixed-case or unusual name works as printed.
func replicaIdentityFix(schema, table string) string {
	return "ALTER TABLE " + pgx.Identifier{schema, table}.Sanitize() + " REPLICA IDENTITY FULL"
}

// identityWithoutKeyFix is what to run on a table whose replica identity leaves
// a primary-key column out: the whole row, or, when the identity is an index,
// back to the primary key. (A table already at DEFAULT is here when its
// primary key has a column the identity does not send: FULL is its way out.)
func identityWithoutKeyFix(schema, table string, identity uint8) string {
	name := pgx.Identifier{schema, table}.Sanitize()
	if identity != 'i' {
		return "ALTER TABLE " + name + " REPLICA IDENTITY FULL"
	}
	return "ALTER TABLE " + name + " REPLICA IDENTITY DEFAULT (or ALTER TABLE " + name + " REPLICA IDENTITY FULL, which also makes its changes undoable)"
}

// deferrableKeyFix is what to run on a table whose primary key is deferrable.
func deferrableKeyFix(schema, table, constraint string) string {
	name := pgx.Identifier{schema, table}.Sanitize()
	return "ALTER TABLE " + name + " DROP CONSTRAINT " + pgx.Identifier{constraint}.Sanitize() +
		", ADD PRIMARY KEY (<the same columns>) (it rebuilds the key's index and locks the table while it does), or take the table out of the publication"
}

// replicaIdentityName renders a pg_class.relreplident code for a person: the code
// itself (what the catalog shows) and its meaning.
func replicaIdentityName(code uint8) string {
	meaning := "unknown"
	switch code {
	case 'd':
		meaning = "default: primary key only"
	case 'i':
		meaning = "using index"
	case 'n':
		meaning = "nothing"
	case 'f':
		meaning = "full"
	}
	return fmt.Sprintf("'%c' (%s)", rune(code), meaning)
}

// recordSkip records one dropped change of rel under reason, attributed to the
// table and to the transaction's commit LSN (in the shape the PostgreSQL
// checkpoint uses: the LSN text as the file, its number as the position).
func (d *Decoder) recordSkip(rel *relationInfo, reason, detail string) {
	lsn := d.txn.commitLSN
	d.skips.RecordSkipAttributed(reason, parser.SkipAttribution{
		File:   lsn.String(),
		Pos:    uint64(lsn),
		Schema: rel.schema,
		Table:  rel.table,
		Detail: detail,
	})
}

// isTimescaleChunk reports whether a relation is a TimescaleDB hypertable chunk — the
// internal physical tables (_hyper_<id>_<n>_chunk, or _dist_hyper_* for distributed
// hypertables) under the _timescaledb_internal schema that pgoutput streams in place
// of the logical hypertable. Used by the #559 out-of-scope guard. (TimescaleDB's
// catalog/config schemas — _timescaledb_catalog, _timescaledb_config — are not chunk
// data and are not flagged here.)
func isTimescaleChunk(schema, table string) bool {
	return schema == "_timescaledb_internal" &&
		(strings.HasPrefix(table, "_hyper_") || strings.HasPrefix(table, "_dist_hyper_"))
}

// relationEvent builds an EventRelation carrying the relation's shape for the
// consumer to persist as a schema snapshot (the source-neutral, in-band analog of
// MySQL's TakeSnapshot — no information_schema). IsPK is by name; rel.pkCols is
// already in table-ordinal order (cacheRelation reordered it), so the snapshot's PK
// columns align with the resolver. Ordinal is the table-column position.
func relationEvent(rel *relationInfo, txn txnContext, inferring bool) event.Event {
	pkNames := make(map[string]bool, len(rel.pkCols))
	for _, pk := range rel.pkCols {
		pkNames[pk.Name] = true
	}
	var seenLSN uint64
	var seenAt time.Time
	// Where the shape was seen. While DDL inference is off, the time is left
	// out: such a shape was never compared, so the next start does not use it
	// as the reference (it compares against the last shape inference saw,
	// and a change made meanwhile is still found), and its position marks a
	// stretch where the index's own rows cannot bound a change (some of them
	// may already be under a new shape).
	if txn.open {
		seenLSN = uint64(txn.commitLSN)
		if inferring {
			seenAt = txn.commitTime
		}
	}
	cols := make([]metadata.PGRelationColumn, len(rel.columns))
	for i, c := range rel.columns {
		cols[i] = metadata.PGRelationColumn{
			Name:             c.name,
			Ordinal:          i + 1,
			IsPK:             pkNames[c.name],
			TypeOID:          c.typeOID,
			TypeMod:          c.typeMod,
			IsIdentityAlways: c.isIdentityAlways,
			IsGenerated:      c.isGenerated,
		}
	}
	return event.Event{
		Schema:    rel.schema,
		Table:     rel.table,
		EventType: event.EventRelation,
		Relation: &metadata.PGRelationSchema{
			OID:     rel.oid,
			SeenLSN: seenLSN,
			SeenAt:  seenAt,
			Schema:  rel.schema,
			Table:   rel.table,
			Columns: cols,
		},
	}
}

func (d *Decoder) decodeInsert(m *pglogrepl.InsertMessage) (event.Event, bool, error) {
	rel, err := d.relationFor(m.RelationID)
	if err != nil {
		return event.Event{}, false, err
	}
	if !d.filters.Matches(rel.schema, rel.table) {
		return event.Event{}, false, nil
	}
	if rel.skipped {
		// Before any tuple is decoded: nothing of a skipped relation is read.
		d.recordSkip(rel, rel.skipReason, rel.skipDetail)
		return event.Event{}, false, nil
	}
	// INSERT carries only a new tuple, fully written — pgoutput never emits 'u' in
	// an INSERT, so there is no before-image to resolve from.
	after, err := d.decodeTuple(rel, m.Tuple, roleAfter, nil)
	if err != nil {
		return event.Event{}, false, err
	}
	return d.rowEvent(rel, event.EventInsert, nil, after), true, nil
}

func (d *Decoder) decodeUpdate(m *pglogrepl.UpdateMessage) (event.Event, bool, error) {
	rel, err := d.relationFor(m.RelationID)
	if err != nil {
		return event.Event{}, false, err
	}
	if !d.filters.Matches(rel.schema, rel.table) {
		return event.Event{}, false, nil
	}
	if rel.skipped {
		// Before any tuple is decoded: nothing of a skipped relation is read.
		d.recordSkip(rel, rel.skipReason, rel.skipDetail)
		return event.Event{}, false, nil
	}
	// With no old tuple the earlier row was not sent at all: REPLICA IDENTITY
	// FULL always sends it whole.
	if m.OldTuple == nil || earlierRowIsPartial(rel, m.OldTupleType) || keyOnlyShaped(rel, m.OldTuple) {
		return d.decodePartialUpdate(rel, m)
	}
	// REPLICA IDENTITY FULL: OldTuple is the whole old row ('O'). Decoded as a
	// before-image: a 'u' here fails loud (see decodeTuple), since under FULL it
	// cannot occur.
	before, err := d.decodeTuple(rel, m.OldTuple, roleBefore, nil)
	if err != nil {
		return event.Event{}, false, err
	}
	// Resolve any 'u' in the new tuple from the before-image (Option B): under RI
	// FULL the unchanged column's real value is in the (now guaranteed 'u'-free)
	// before-image, so the after-image holds the TRUE post-update row state — making
	// changed_columns correct and needing zero downstream handling.
	after, err := d.decodeTuple(rel, m.NewTuple, roleAfter, before)
	if err != nil {
		return event.Event{}, false, err
	}
	return d.rowEvent(rel, event.EventUpdate, before, after), true, nil
}

// earlierRowIsPartial reports whether the old tuple of a change holds the
// earlier row in part: the relation's earlier rows are (relationInfo.partial),
// or PostgreSQL labels this tuple as holding the key only ('K').
// Decided per change, so a table that goes DEFAULT, FULL, DEFAULT in mid
// stream is exact at each boundary: every description of the relation
// replaces its entry before the next change.
func earlierRowIsPartial(rel *relationInfo, oldTupleType uint8) bool {
	return rel.partial || oldTupleType == pglogrepl.UpdateMessageTupleTypeKey
}

// keyOnlyShaped reports whether an old tuple of a relation read row by row
// (relationInfo.byShape: a partitioned table described at FULL, or one the
// catalog had no answer for) holds nothing outside the primary key: every
// other column is 'n', and there is one. That is what a partition below FULL
// logs, under a root whose description says FULL (PartitionsResolver). A row
// from a partition at FULL holds its values and is whole.
//
// A key-only tuple cannot be told from a whole row whose other columns really
// are NULL, so that row is stored in part too: undo refuses a change it could
// have undone, and never writes NULL over values that were there. A table the
// catalog says is not partitioned is never asked: its description is its own.
func keyOnlyShaped(rel *relationInfo, t *pglogrepl.TupleData) bool {
	if !rel.byShape || len(t.Columns) != len(rel.columns) {
		return false
	}
	outside := 0
	for i, col := range t.Columns {
		if rel.columns[i].inIdentity {
			continue
		}
		if col.DataType != pglogrepl.TupleDataTypeNull {
			return false
		}
		outside++
	}
	return outside > 0
}

// decodePartialUpdate is decodeUpdate for a change whose earlier row was not
// sent whole: a table below REPLICA IDENTITY FULL.
//
// PostgreSQL sends the old row only when an identity column changed (or an
// unchanged identity column is stored out of line), and then with the
// identity columns alone ('K'). The before-image stored is partialBefore of
// that tuple; with no old tuple at all the identity did not change, so it is
// partialBefore of the NEW tuple: the same key, every other column not
// captured. Either way it holds exactly the columns the after-image holds,
// which the fold's whole-row checks rely on (reconstruct.checkPGEventMarkers,
// splitPGKeyChange, and the dropped-column guard).
//
// A value the UPDATE left as it was and PostgreSQL did not send ('u') is
// stored as the unchanged-TOAST marker, never taken from this before-image:
// it does not hold it, and a NULL stored in its stead would be carried by
// every reader as the row's value. The one exception is an identity column,
// whose value the 'K' tuple carries in full.
func (d *Decoder) decodePartialUpdate(rel *relationInfo, m *pglogrepl.UpdateMessage) (event.Event, bool, error) {
	src := m.OldTuple
	if src == nil {
		src = m.NewTuple
	}
	before, err := d.partialBefore(rel, src)
	if err != nil {
		return event.Event{}, false, err
	}
	after, err := d.decodeTuple(rel, m.NewTuple, roleAfterPartial, before)
	if err != nil {
		return event.Event{}, false, err
	}
	if d.skipKeyNotSent(rel, before, after) {
		return event.Event{}, false, nil
	}
	return d.rowEvent(rel, event.EventUpdate, before, after), true, nil
}

func (d *Decoder) decodeDelete(m *pglogrepl.DeleteMessage) (event.Event, bool, error) {
	rel, err := d.relationFor(m.RelationID)
	if err != nil {
		return event.Event{}, false, err
	}
	if !d.filters.Matches(rel.schema, rel.table) {
		return event.Event{}, false, nil
	}
	if rel.skipped {
		// Before any tuple is decoded: nothing of a skipped relation is read.
		d.recordSkip(rel, rel.skipReason, rel.skipDetail)
		return event.Event{}, false, nil
	}
	// A DELETE's before-image is the ONLY source for its reversal INSERT. pgoutput
	// always sends an old tuple for a DELETE (its decode requires 'K' or 'O'), and a
	// table with no usable replica identity can't be DELETEd from under logical
	// replication at all — so a missing old tuple is a broken invariant, not a
	// supported case: fail loud rather than index an un-keyed, un-reversible row.
	if m.OldTuple == nil {
		return event.Event{}, false, fmt.Errorf("pgcapture: DELETE on %s.%s carries no before-image (no replica identity?) — cannot index an un-reversible delete", rel.schema, rel.table)
	}
	if earlierRowIsPartial(rel, m.OldTupleType) || keyOnlyShaped(rel, m.OldTuple) {
		// Below REPLICA IDENTITY FULL: the key of the deleted row and nothing
		// else. Enough to delete it from the copy and to show it in history;
		// undo refuses it (event.CheckUnresolvedToast).
		before, err := d.partialBefore(rel, m.OldTuple)
		if err != nil {
			return event.Event{}, false, err
		}
		if d.skipKeyNotSent(rel, before, nil) {
			return event.Event{}, false, nil
		}
		return d.rowEvent(rel, event.EventDelete, before, nil), true, nil
	}
	before, err := d.decodeTuple(rel, m.OldTuple, roleBefore, nil)
	if err != nil {
		return event.Event{}, false, err
	}
	return d.rowEvent(rel, event.EventDelete, before, nil), true, nil
}

// partialBefore builds the before-image of a change whose earlier row
// PostgreSQL sent in part, from t: the 'K' old tuple, or, for an UPDATE with
// no old tuple, the new tuple (the identity did not change, so its identity
// values are the earlier row's).
//
// THE CONTRACT, which the readers are built on and a test pins: the image
// holds every column of the relation, the same set an after-image holds. An
// identity column holds its real value; EVERY other column holds the
// not-captured marker (event.NotCapturedKey), whatever t carries there. Never
// nil, never absent: nil would be read as a NULL the row held.
//
// An identity column that t does not give a value for ('n' or 'u') is the
// marker too, never NULL. PostgreSQL is not known to send one (identity
// columns are NOT NULL, and a key-only tuple carries them in full even when
// stored out of line); skipKeyNotSent drops the change when it is a
// primary-key column.
func (d *Decoder) partialBefore(rel *relationInfo, t *pglogrepl.TupleData) (map[string]any, error) {
	if len(t.Columns) != len(rel.columns) {
		return nil, fmt.Errorf("pgcapture: tuple has %d columns but relation %s.%s has %d",
			len(t.Columns), rel.schema, rel.table, len(rel.columns))
	}
	row := make(map[string]any, len(t.Columns))
	for i, col := range t.Columns {
		c := rel.columns[i]
		if !c.inIdentity {
			row[c.name] = notCapturedMarker()
			continue
		}
		switch col.DataType {
		case pglogrepl.TupleDataTypeText:
			row[c.name] = string(col.Data)
		case pglogrepl.TupleDataTypeNull, pglogrepl.TupleDataTypeToast:
			// No value for an identity column: see the comment above.
			row[c.name] = notCapturedMarker()
		case pglogrepl.TupleDataTypeBinary:
			return nil, fmt.Errorf("pgcapture: binary tuple datum for column %q in %s.%s — pgcapture streams text format only",
				c.name, rel.schema, rel.table)
		default:
			return nil, fmt.Errorf("pgcapture: unknown tuple data type %q for column %q in %s.%s",
				col.DataType, c.name, rel.schema, rel.table)
		}
	}
	return row, nil
}

// skipKeyNotSent drops, and records under SkipReplicaIdentityKeyNotSent, a
// change of a table below REPLICA IDENTITY FULL whose images do not give a
// real value for every primary-key column: the earlier row (before), and for
// an UPDATE the row it leaves (after; nil for a DELETE). It reports whether
// it dropped the change. Stored, such a change would sit in the index under a
// key spelled from a marker. See SkipReplicaIdentityKeyNotSent.
func (d *Decoder) skipKeyNotSent(rel *relationInfo, before, after map[string]any) bool {
	var missing []string
	for _, pk := range rel.pkCols {
		_, okBefore := before[pk.Name].(string)
		okAfter := true
		if after != nil {
			_, okAfter = after[pk.Name].(string)
		}
		if !okBefore || !okAfter {
			missing = append(missing, pk.Name)
		}
	}
	if len(missing) == 0 {
		return false
	}
	detail := fmt.Sprintf("replica identity %s: PostgreSQL sent a change without a value for primary-key column(s) %s; fix: %s",
		replicaIdentityName(rel.replicaIdentity), strings.Join(missing, ", "), replicaIdentityFix(rel.schema, rel.table))
	d.recordSkip(rel, SkipReplicaIdentityKeyNotSent, detail)
	d.logger.Warn("pgcapture: a change arrived without a value for a primary-key column of its row; it is NOT captured (counted as skipped in capture health), the stream goes on",
		"table", rel.schema+"."+rel.table,
		"replica_identity", replicaIdentityName(rel.replicaIdentity),
		"primary_key_columns", strings.Join(missing, ", "),
		"commit_lsn", d.txn.commitLSN.String(),
		"fix", replicaIdentityFix(rel.schema, rel.table))
	return true
}

// relationFor returns the cached relation for a row message, erroring if no
// RelationMessage preceded it (a protocol invariant) or if no transaction is open
// (a row must arrive between Begin and Commit; without an open txn the event would
// get a zero commit timestamp and land in the wrong partition).
func (d *Decoder) relationFor(oid uint32) (*relationInfo, error) {
	if !d.txn.open {
		return nil, fmt.Errorf("pgcapture: row event for relation OID %d outside a transaction (no preceding Begin)", oid)
	}
	rel, ok := d.relations[oid]
	if !ok {
		return nil, fmt.Errorf("pgcapture: row event for unknown relation OID %d (no preceding Relation message)", oid)
	}
	return rel, nil
}

// tupleRole distinguishes how the decoder treats an unchanged-TOAST ('u') datum,
// which is the only kind whose handling depends on which image a tuple is.
type tupleRole uint8

const (
	// roleBefore = an old/before-image tuple. A 'u' here is a HARD ERROR (see
	// decodeTuple): under REPLICA IDENTITY FULL the before-image always carries the
	// real value, so a 'u' means the value bintrail needs for recovery is absent.
	roleBefore tupleRole = iota
	// roleAfter = a new/after-image tuple beside a WHOLE before-image (REPLICA
	// IDENTITY FULL), or with none (an INSERT). A 'u' is resolved from the
	// before-image (guaranteed real, since roleBefore rejects 'u'), falling back
	// to the marker only when there is no before-image.
	roleAfter
	// roleAfterPartial = a new/after-image tuple beside a before-image captured
	// in part (partialBefore). A 'u' is the unchanged-TOAST marker, except in a
	// column whose real value the before-image does hold (an identity column):
	// what the before-image holds elsewhere is the not-captured marker, and
	// taking that, or a NULL, for the value would store it as the row's.
	roleAfterPartial
)

// decodeTuple converts a pgoutput tuple to a column-name→value map in the cached
// relation's column order:
//   - 'n' (null)   → Go nil (SQL NULL);
//   - 't' (text)   → Go string (lossless; type-faithful rendering is #533's);
//   - 'b' (binary) → error: the slice-2 capturer requests text format, so a binary
//     datum is a misconfiguration we refuse rather than silently mishandle;
//   - 'u' (unchanged TOAST) → in a before-image (roleBefore) a HARD ERROR; in an
//     after-image beside a whole before-image (roleAfter) resolved from it
//     (Option B); in an after-image beside a partial one (roleAfterPartial) the
//     structurally distinct unchanged-TOAST marker (never a plain string, never
//     NULL), unless the partial before-image holds the column's real value.
//
// A before-image captured in part is not decoded here: see partialBefore.
//
// Why a 'u' in a before-image is a hard error: under REPLICA IDENTITY FULL
// PostgreSQL detoasts and WAL-logs every replica-identity
// column (all columns, under FULL), so the before-image carries the real value,
// never 'u' (proven at the protocol level in the spike; PG commit 1cd5802, back-
// patched to PG10+). A 'u' in a before-image therefore means the real value — the
// ONLY source for a DELETE's reversal INSERT — is gone, so we fail loud rather than
// silently store a marker. Unreachable in support.
//
// before is the before-image, consulted only for roleAfter; nil otherwise.
func (d *Decoder) decodeTuple(rel *relationInfo, t *pglogrepl.TupleData, role tupleRole, before map[string]any) (map[string]any, error) {
	if t == nil {
		return nil, nil
	}
	if len(t.Columns) != len(rel.columns) {
		return nil, fmt.Errorf("pgcapture: tuple has %d columns but relation %s.%s has %d",
			len(t.Columns), rel.schema, rel.table, len(rel.columns))
	}
	row := make(map[string]any, len(t.Columns))
	for i, col := range t.Columns {
		name := rel.columns[i].name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			row[name] = nil
		case pglogrepl.TupleDataTypeText:
			row[name] = string(col.Data)
		case pglogrepl.TupleDataTypeToast:
			if role == roleBefore {
				return nil, fmt.Errorf("pgcapture: unchanged-TOAST datum in the before-image of %s.%s column %q — the source must use REPLICA IDENTITY FULL so the real value is captured for recovery",
					rel.schema, rel.table, name)
			}
			if role == roleAfterPartial {
				// Only a value the partial before-image really holds: text
				// in an identity column. Its other columns are the
				// not-captured marker, which is no value.
				if v, ok := before[name].(string); ok {
					row[name] = v
					continue
				}
				row[name] = unchangedToastMarker()
				continue
			}
			if before != nil {
				if v, ok := before[name]; ok {
					row[name] = v
					continue
				}
			}
			row[name] = unchangedToastMarker()
		case pglogrepl.TupleDataTypeBinary:
			return nil, fmt.Errorf("pgcapture: binary tuple datum for column %q in %s.%s — pgcapture streams text format only",
				name, rel.schema, rel.table)
		default:
			return nil, fmt.Errorf("pgcapture: unknown tuple data type %q for column %q in %s.%s",
				col.DataType, name, rel.schema, rel.table)
		}
	}
	return row, nil
}

// rowEvent assembles a row event.Event, stamping it with the in-flight
// transaction's commit LSN and timestamp.
func (d *Decoder) rowEvent(rel *relationInfo, typ event.EventType, before, after map[string]any) event.Event {
	// PKValues from the before-image for UPDATE/DELETE (under RI FULL it is complete
	// and free of the unchanged-TOAST marker, so a TOASTed-but-unchanged PK column
	// still yields its real value; below FULL it holds the identity columns, which
	// include every primary-key column, each checked to be real by skipKeyNotSent),
	// from the after-image for INSERT (no before-image)
	// — mirroring the MySQL parser (parser.go emitInserts/emitDeletes/emitUpdates).
	pkSource := before
	if pkSource == nil {
		pkSource = after
	}
	var pkValues string
	if len(rel.pkCols) > 0 {
		pkValues = event.BuildPKValues(rel.pkCols, pkSource)
	}

	// All rows of a logical-replication transaction are delivered at commit and
	// share its LSN, so StartPos == EndPos == GTID for every row of a txn (unlike
	// MySQL's distinct per-row byte offsets). This is intentional and safe: the
	// downstream stack treats position fields as opaque metadata it never orders or
	// compares on (see event.Event); the durable cursor advances on EventCommit.
	if d.infer != nil {
		d.infer.noteChange(rel.oid, d.txn)
	}
	// A captured row breaks the ledger's consecutive-skip run, so a busy skipped
	// table beside captured ones does not read as "capture stopped".
	d.skips.RecordCaptured()
	lsn := d.txn.commitLSN.String()
	return event.Event{
		BinlogFile: lsn,
		StartPos:   uint64(d.txn.commitLSN),
		EndPos:     uint64(d.txn.commitLSN),
		Timestamp:  d.txn.commitTime,
		GTID:       lsn,
		Schema:     rel.schema,
		Table:      rel.table,
		EventType:  typ,
		PKValues:   pkValues,
		RowBefore:  before,
		RowAfter:   after,
	}
}

// truncateEvent builds the EventDDL(DDLTruncateTable) audit marker for one
// truncated relation (#827), stamped with the in-flight transaction's commit LSN
// and timestamp — a TRUNCATE always arrives between Begin and Commit, so d.txn
// carries them. DDLQuery is a synthetic statement (pgoutput carries no SQL text)
// so the schema_changes row reads naturally. Carries no row data: recover has
// nothing to reverse; it is a record that the truncate happened.
func (d *Decoder) truncateEvent(rel *relationInfo) event.Event {
	lsn := d.txn.commitLSN.String()
	return event.Event{
		BinlogFile: lsn,
		StartPos:   uint64(d.txn.commitLSN),
		EndPos:     uint64(d.txn.commitLSN),
		Timestamp:  d.txn.commitTime,
		GTID:       lsn,
		Schema:     rel.schema,
		Table:      rel.table,
		EventType:  event.EventDDL,
		DDLType:    event.DDLTruncateTable,
		DDLQuery:   fmt.Sprintf("TRUNCATE TABLE %s.%s", rel.schema, rel.table),
	}
}

// ddlInScope reports whether an inferred DDL event names an in-scope table; a
// rename is in scope when either name is.
func (d *Decoder) ddlInScope(ev event.Event) bool {
	if d.filters.Matches(ev.Schema, ev.Table) {
		return true
	}
	for _, t := range ev.DDLTables {
		if d.filters.Matches(t.Schema, t.Table) {
			return true
		}
	}
	return false
}

// TakePending returns and clears the events the last Decode buffered beyond its
// single return value, then empties the buffer. Only a multi-relation TRUNCATE
// fills it (one marker per in-scope relation past the first). The caller drains it
// after every Decode; a nil return is the common no-op case.
func (d *Decoder) TakePending() []event.Event {
	if len(d.pending) == 0 {
		return nil
	}
	p := d.pending
	d.pending = nil
	return p
}

// unchangedToastMarker is the structurally-distinct stand-in for an unchanged-TOAST
// value that could not be resolved from the before-image. See UnchangedToastKey.
func unchangedToastMarker() map[string]any {
	return map[string]any{UnchangedToastKey: true}
}

// notCapturedMarker is the stand-in for a column of an earlier row that
// PostgreSQL did not send (event.NotCapturedKey). A map of its own per column:
// the consumer serializes each, and none is shared.
func notCapturedMarker() map[string]any {
	return map[string]any{event.NotCapturedKey: true}
}
