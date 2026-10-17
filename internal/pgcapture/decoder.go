package pgcapture

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// UnchangedToastKey is the single key of the structurally-distinct marker the
// decoder emits for an unchanged-TOAST value ('u') that could NOT be resolved from
// the before-image. The canonical constant and its full rationale (why a one-key
// map, the RI-FULL never-persisted invariant, the #533 forward constraint) live in
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
	// once, before the RI check below so the signal survives even if the chunk is not at
	// FULL. This is BEFORE the cache write: the warning is about target identity, not a
	// reason to stop.
	if !d.timescaleWarned && isTimescaleChunk(m.Namespace, m.RelationName) {
		d.logger.Warn("pgcapture: TimescaleDB hypertable chunk detected — bintrail indexes raw chunk relations under _timescaledb_internal, NOT the logical hypertable; TimescaleDB is out of scope (decode-granularity mismatch)",
			"schema", m.Namespace, "table", m.RelationName)
		d.timescaleWarned = true
	}

	// REPLICA IDENTITY FULL enforcement at the LIVE boundary, not just startup: a
	// table added to a FOR ALL TABLES publication after Run started (or any mid-stream
	// new relation) arrives here as a fresh RelationMessage at PostgreSQL's default
	// identity ('d'), which the one-shot startup validator (validateReplicaIdentity)
	// never re-checks. Without FULL the before-image is partial — an unchanged
	// out-of-line TOAST value is gone — so fail loud here too rather than index
	// unrecoverable rows (whose recovery WHERE would carry the unchanged-TOAST marker
	// and match nothing). 'f' = FULL; 'd' default, 'i' using-index, 'n' nothing.
	if m.ReplicaIdentity != 'f' {
		return fmt.Errorf("pgcapture: relation %s.%s is not at REPLICA IDENTITY FULL (replica identity %q) — before-images would be partial; run ALTER TABLE %s.%s REPLICA IDENTITY FULL",
			m.Namespace, m.RelationName, string(rune(m.ReplicaIdentity)), m.Namespace, m.RelationName)
	}

	cols := make([]relColumn, len(m.Columns))
	for i, c := range m.Columns {
		cols[i] = relColumn{name: c.Name, typeOID: c.DataType, typeMod: c.TypeModifier}
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
	// absent from the relation's columns, the two have diverged (a PK-changing DDL
	// during stream lag) — fail loud rather than build a wrong/empty PK.
	colSet := make(map[string]bool, len(cols))
	for _, c := range cols {
		colSet[c.name] = true
	}
	for _, pk := range pkCols {
		if !colSet[pk.Name] {
			return fmt.Errorf("pgcapture: primary-key column %q absent from relation %s.%s columns (schema drift?)",
				pk.Name, m.Namespace, m.RelationName)
		}
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

	d.relations[m.RelationID] = &relationInfo{
		oid:     m.RelationID,
		schema:  m.Namespace,
		table:   m.RelationName,
		columns: cols,
		pkCols:  pkCols,
	}
	return nil
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
	// OldTuple is the full old tuple ('O') under REPLICA IDENTITY FULL, a key-only
	// tuple ('K') when a replica-identity column changed under a weaker identity, or
	// absent when an UPDATE under a weaker identity (e.g. RI DEFAULT) left the
	// replica-identity columns unchanged. Decoded as a
	// before-image: a 'u' here fails loud (see decodeTuple) — under RI FULL it can't
	// occur, and under a weaker identity it would mean a lost value.
	var before map[string]any
	if m.OldTuple != nil {
		before, err = d.decodeTuple(rel, m.OldTuple, roleBefore, nil)
		if err != nil {
			return event.Event{}, false, err
		}
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

func (d *Decoder) decodeDelete(m *pglogrepl.DeleteMessage) (event.Event, bool, error) {
	rel, err := d.relationFor(m.RelationID)
	if err != nil {
		return event.Event{}, false, err
	}
	if !d.filters.Matches(rel.schema, rel.table) {
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
	before, err := d.decodeTuple(rel, m.OldTuple, roleBefore, nil)
	if err != nil {
		return event.Event{}, false, err
	}
	return d.rowEvent(rel, event.EventDelete, before, nil), true, nil
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
	// roleAfter = a new/after-image tuple. A 'u' is resolved from the before-image
	// (guaranteed real, since roleBefore rejects 'u'), falling back to the marker
	// only when no before-image is available (a weaker-than-FULL replica identity).
	roleAfter
)

// decodeTuple converts a pgoutput tuple to a column-name→value map in the cached
// relation's column order:
//   - 'n' (null)   → Go nil (SQL NULL);
//   - 't' (text)   → Go string (lossless; type-faithful rendering is #533's);
//   - 'b' (binary) → error: the slice-2 capturer requests text format, so a binary
//     datum is a misconfiguration we refuse rather than silently mishandle;
//   - 'u' (unchanged TOAST) → in a before-image (roleBefore) a HARD ERROR; in an
//     after-image (roleAfter) resolved from the before-image (Option B), else the
//     structurally distinct unchanged-TOAST marker (never a plain string).
//
// Why a 'u' in a before-image is a hard error: under REPLICA IDENTITY FULL — the
// required mode (#531) — PostgreSQL detoasts and WAL-logs every replica-identity
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
	// still yields its real value), from the after-image for INSERT (no before-image)
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
