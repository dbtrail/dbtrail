package cascade

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Logging is what the index knows about whether the source wrote the rows its
// foreign keys cascade to into the binary log, per start of the source server
// (metadata.DDLSourceFKLogging). MySQL 9.6 and later do, unless started with
// innodb_native_foreign_keys = ON; MySQL before 9.6 and MariaDB do not.
//
// A parent change is judged by the server start that wrote it:
//
//   - logged: the cascade's child rows are ordinary events of the parent's
//     transaction, and they are what is reversed (collectLogged). Nothing is
//     synthesized: the children's last state before the parent change is
//     their own delete or rewrite, so synthesis would find nothing.
//   - not logged: synthesis, as before this existed.
//   - unknown (the change is older than every recorded start, or it lies
//     within a second or two of a start whose setting differs from the one
//     before): synthesis, and the index is searched for child changes in the
//     parent's transaction as if they were logged. Finding any makes the
//     recovery incomplete, naming them: either the source logged that
//     cascade and synthesis restored less than it removed, or the
//     application deleted those children itself in the same transaction.
//     The index cannot tell the two apart, so it says so instead of
//     choosing.
//
// The zero value (no record at all) judges every change unknown.
type Logging struct {
	starts []metadata.SourceFKLogging // oldest first
}

// LoadLogging reads the index's record of the source's cascade logging. It
// returns nil (synthesize every root, unchanged) for a PostgreSQL index:
// there the record is never written, and what recover-cascade does with
// PostgreSQL's own logged cascades is its own path.
func LoadLogging(ctx context.Context, indexDB *sql.DB) (*Logging, error) {
	if query.SourceFlavor(indexDB) == "postgres" { // the flavor PostgreSQL capture stamps
		return nil, nil
	}
	recs, err := metadata.LoadSourceFKLogging(ctx, indexDB)
	if err != nil {
		return nil, err
	}
	return NewLogging(recs), nil
}

// NewLogging is a Logging over the given records, in any order.
func NewLogging(recs []metadata.SourceFKLogging) *Logging {
	s := append([]metadata.SourceFKLogging(nil), recs...)
	sort.Slice(s, func(i, j int) bool { return s[i].ServerStarted.Before(s[j].ServerStarted) })
	return &Logging{starts: s}
}

type loggingMode int

const (
	modeUnknown loggingMode = iota
	modeNotLogged
	modeLogged
)

// loggingStartSlack: a server start is read as the clock minus Uptime, both
// to the second, and event timestamps are seconds too, so the first seconds
// of a start could belong to the server before it.
const loggingStartSlack = 2 * time.Second

// at is the mode of the server start that wrote an event at t: the one whose
// proven span (from its start to its last reading, metadata.DDLSourceFKLogging)
// holds t. Outside every span, or inside two spans that disagree (two servers
// recorded at once, as across a failover), it is unknown. It never falls back
// to the nearest start: a restart nobody read sits between them, and its
// setting is exactly what is not known.
//
// The time is the event's own timestamp, which is the writing server's clock
// unless a session set TIMESTAMP itself or the source is a replica writing
// its primary's events; such a change can land in the wrong span. In a span
// that says logged the synthesis still runs beside the logged rows
// (synthesizeByLogging), so a wrong span there costs a warning, not a row. In
// a span that says not logged the logged rows are not looked for: a change
// with a borrowed timestamp, from a server that logs its cascades, falling
// inside the span of one that did not, is the case this cannot see.
func (l *Logging) at(t time.Time) loggingMode {
	if l == nil {
		return modeUnknown
	}
	mode := modeUnknown
	for _, s := range l.starts {
		if t.Before(s.ServerStarted.Add(loggingStartSlack)) || t.After(s.LastSeen) {
			continue
		}
		m := modeOf(s)
		if mode != modeUnknown && m != mode {
			return modeUnknown
		}
		mode = m
	}
	return mode
}

func modeOf(s metadata.SourceFKLogging) loggingMode {
	if s.LogsCascades() {
		return modeLogged
	}
	return modeNotLogged
}

// synthesizeByLogging is SynthesizeVictims with opts.Logging set: each root is
// judged by the server start that wrote it (see Logging).
func synthesizeByLogging(ctx context.Context, eng query.Fetcher, fks []CascadeFK, parentEvents []query.ResultRow, opts Options) (Result, error) {
	var logged, other, unknown []query.ResultRow
	for _, r := range parentEvents {
		switch opts.Logging.at(r.EventTimestamp) {
		case modeLogged:
			logged = append(logged, r)
		case modeUnknown:
			unknown = append(unknown, r)
			other = append(other, r)
		default:
			other = append(other, r)
		}
	}
	legacyOpts := opts
	legacyOpts.Logging = nil
	var results []Result
	var errs []error
	if len(other) > 0 {
		r, err := SynthesizeVictims(ctx, eng, fks, other, legacyOpts)
		results = append(results, r)
		if err != nil {
			errs = append(errs, err)
		}
	}
	var loggedVictims []query.ResultRow
	if len(logged) > 0 {
		lr, err := collectLogged(ctx, eng, fks, logged, opts, false)
		loggedVictims = lr.Victims
		lr.Victims = nil
		results = append(results, lr)
		if err != nil {
			errs = append(errs, err)
		}
		// The synthesis beside it: on a server that logged the cascade it
		// finds nothing (each child's last change up to the parent's is its
		// own logged delete or rewrite). Anything it does find means the
		// record was wrong for these parents (a borrowed timestamp, a
		// restart nobody read), and those rows are kept, with a warning,
		// rather than dropped.
		sr, err := SynthesizeVictims(ctx, eng, fks, logged, legacyOpts)
		if err != nil {
			errs = append(errs, err)
		}
		if n := len(sr.Victims) + len(sr.SetNullRows) + len(sr.KeyUpdates); n > 0 {
			sr.Warnings = append(sr.Warnings, fmt.Sprintf(
				"the index records that the source wrote its foreign-key cascades to the binary log when these parent "+
					"rows changed, and their logged child changes are reversed; %d more child row(s) still referenced a "+
					"parent when it changed and were reconstructed as well, as on a source that does not log them. "+
					"Review them: either the record does not fit these changes, or the application changed those "+
					"children in a way the binary log shows differently", n))
		}
		sr.KeyUpdateParents = nil // the logged walk already named the key roots
		results = append(results, sr)
	}
	if len(unknown) > 0 {
		pr, err := collectLogged(ctx, eng, fks, unknown, opts, true)
		if err != nil {
			errs = append(errs, err)
		}
		probe := Result{Incomplete: pr.Incomplete}
		if n := len(pr.Victims); n > 0 {
			probe.Incomplete = append(probe.Incomplete, fmt.Sprintf(
				"the index does not record whether the source wrote its foreign-key cascades to the binary log when "+
					"these parent rows changed, and %d child row change(s) of the same transactions are in the index "+
					"(first: %s.%s %s). They were NOT reversed here: either the source logged that cascade (MySQL 9.6 "+
					"and later with innodb_native_foreign_keys = OFF) and the rows synthesized here are fewer than it "+
					"removed, or the application changed those children itself in the same transaction. Review them "+
					"with `bintrail query`, and reverse them with `bintrail recover` over the same window if they "+
					"belong to the cascade. The setting is recorded while capture runs: a change outside every reading "+
					"(before the first one, or around a restart nobody read) is one it cannot attribute",
				n, pr.Victims[0].SchemaName, pr.Victims[0].TableName, pr.Victims[0].PKValues))
		}
		results = append(results, probe)
	}
	if len(results) == 0 {
		return Result{}, nil
	}
	out := MergeResults(results...)
	if len(loggedVictims) > 0 {
		// One restore per deleted row, whichever arm found it (a synthesized
		// victim and the logged delete of the same row would INSERT its key
		// twice); every logged UPDATE stays (dedupVictimsNewest).
		out.Victims = dedupVictimsNewest(append(out.Victims, loggedVictims...))
	}
	var err error
	if len(errs) > 0 {
		err = errors.Join(errs...)
	}
	return out, err
}

// collectLogged finds, for parent changes written by a server that logs its
// cascades, the child row changes their cascades made: events of the child
// table in the parent's transaction (same binary log file, same commit time,
// same connection) whose before-image references the parent's key, of the
// shape the rule produces. It follows them down (a deleted child that is
// itself a parent), bounded by MaxDepth. The returned Victims are the events
// themselves, oldest first, ready for the generator to reverse.
//
// A child change of the same transaction that the application made itself
// (it deleted the children, then the parent) has the same shape and is
// returned too: reversing the parent's transaction puts those rows back as
// well, which is what restoring the parent with its children asks for.
//
// probe is the unknown case's search (synthesizeByLogging): only whether such
// rows exist matters, so the limits of the search itself (no commit time,
// depth, composite or excluded tables) are not caveats there. A failed or
// truncated read still is: then nobody knows whether the rows exist.
func collectLogged(ctx context.Context, eng query.Fetcher, fks []CascadeFK, roots []query.ResultRow, opts Options, probe bool) (Result, error) {
	opts = opts.withDefaults()
	var res Result
	reported := map[string]bool{}
	addIncomplete := func(key, msg string) {
		if probe && !strings.HasPrefix(key, "fetch:") && !strings.HasPrefix(key, "limit:") {
			return
		}
		if !reported[key] {
			reported[key] = true
			res.Incomplete = append(res.Incomplete, msg)
		}
	}
	colsPerConstraint := map[string]int{}
	for _, fk := range fks {
		colsPerConstraint[fk.Schema+"."+fk.Table+"."+fk.ConstraintName]++
	}
	byParentDelete := map[string][]CascadeFK{}
	byParentUpdate := map[string][]CascadeFK{}
	for _, fk := range fks {
		delCascades := fk.DeleteRule == "CASCADE" || fk.DeleteRule == "SET NULL"
		updCascades := fk.UpdateRule == "CASCADE" || fk.UpdateRule == "SET NULL"
		if !delCascades && !updCascades {
			continue
		}
		ckey := fk.Schema + "." + fk.Table + "." + fk.ConstraintName
		if colsPerConstraint[ckey] > 1 {
			addIncomplete("composite:"+ckey, fmt.Sprintf(
				"composite FK %q on %s.%s not supported; the rows its cascade touched were NOT reconstructed",
				fk.ConstraintName, fk.Schema, fk.Table))
			continue
		}
		pk := fk.ReferencedSchema + "." + fk.ReferencedTable
		if delCascades {
			byParentDelete[pk] = append(byParentDelete[pk], fk)
		}
		if updCascades {
			byParentUpdate[pk] = append(byParentUpdate[pk], fk)
		}
	}

	seen := map[uint64]bool{}
	keyParentSeen := map[uint64]bool{}
	for _, r := range roots {
		seen[r.EventID] = true
	}
	var walk func(p query.ResultRow, depth int, isRoot bool) error
	walk = func(p query.ResultRow, depth int, isRoot bool) error {
		table := p.SchemaName + "." + p.TableName
		type want struct {
			fk     CascadeFK
			typ    event.EventType
			key    any  // the value the child's FK held before
			newKey any  // ON UPDATE CASCADE: the value it holds after
			toNull bool // the rule nulls the FK
		}
		var wants []want
		switch p.EventType {
		case event.EventDelete:
			for _, fk := range byParentDelete[table] {
				key := p.RowBefore[fk.ReferencedColumn]
				if key == nil {
					continue
				}
				if fk.DeleteRule == "CASCADE" {
					wants = append(wants, want{fk: fk, typ: event.EventDelete, key: key})
				} else {
					wants = append(wants, want{fk: fk, typ: event.EventUpdate, key: key, toNull: true})
				}
			}
		case event.EventUpdate:
			for _, fk := range byParentUpdate[table] {
				oldKey, newKey := p.RowBefore[fk.ReferencedColumn], p.RowAfter[fk.ReferencedColumn]
				if !refKeyChanged(oldKey, newKey) || oldKey == nil {
					continue
				}
				if isRoot && !keyParentSeen[p.EventID] {
					keyParentSeen[p.EventID] = true
					res.KeyUpdateParents = append(res.KeyUpdateParents, p)
				}
				wants = append(wants, want{fk: fk, typ: event.EventUpdate, key: oldKey, newKey: newKey, toNull: fk.UpdateRule == "SET NULL"})
			}
		}
		if len(wants) == 0 {
			return nil
		}
		if depth > opts.MaxDepth {
			addIncomplete("depth:"+table, fmt.Sprintf(
				"cascade below %s goes deeper than %d levels; deeper child changes were NOT reversed", table, opts.MaxDepth))
			return nil
		}
		if p.CommitTsUS == nil && !probe {
			addIncomplete("nocommit:"+table, fmt.Sprintf(
				"the index has no commit time for a change of %s (row %s), so the child changes of its transaction "+
					"cannot be told from others in the same second; its cascade was NOT reversed", table, p.PKValues))
			return nil
		}
		for _, w := range wants {
			if w.fk.ChildExcludedFromSnapshot {
				addIncomplete("excluded:"+w.fk.Schema+"."+w.fk.Table, fmt.Sprintf(
					"%s.%s was excluded from the schema snapshot, so its changes were never captured and the cascade's "+
						"changes to it were NOT reversed", w.fk.Schema, w.fk.Table))
				continue
			}
			ts := p.EventTimestamp
			typ := w.typ
			rows, err := eng.Fetch(ctx, query.Options{
				Schema: w.fk.Schema, Table: w.fk.Table, EventType: &typ,
				Since: &ts, Until: &ts, Order: "ASC", Limit: opts.CandidateLimit + 1,
			})
			if err != nil {
				addIncomplete("fetch:"+w.fk.Schema+"."+w.fk.Table, fmt.Sprintf(
					"reading the changes of %s.%s failed (%v); the cascade's changes to it were NOT reversed",
					w.fk.Schema, w.fk.Table, err))
				return fmt.Errorf("fetch logged cascade rows of %s.%s: %w", w.fk.Schema, w.fk.Table, err)
			}
			if len(rows) > opts.CandidateLimit {
				addIncomplete("limit:"+w.fk.Schema+"."+w.fk.Table, fmt.Sprintf(
					"%s.%s has more than %d changes in the second of a parent change; only the first were read, so the "+
						"cascade's changes to it may be partly NOT reversed", w.fk.Schema, w.fk.Table, opts.CandidateLimit))
				rows = rows[:opts.CandidateLimit]
			}
			for _, c := range rows {
				same := sameTransaction(p, c)
				if !same && probe && p.CommitTsUS == nil {
					// The unknown case's search over changes indexed with no
					// commit time: the same file, connection and second is the
					// closest the index can say to "the same transaction", and
					// a false match here only adds a caveat.
					same = c.BinlogFile == p.BinlogFile && c.EventTimestamp.Equal(p.EventTimestamp) &&
						(p.ConnectionID == nil || c.ConnectionID == nil || *p.ConnectionID == *c.ConnectionID)
				}
				if seen[c.EventID] || !same || !matchesRule(c, w.fk.Column, w.key, w.newKey, w.toNull, w.typ) {
					continue
				}
				seen[c.EventID] = true
				res.Victims = append(res.Victims, c)
				if err := walk(c, depth+1, false); err != nil {
					return err
				}
			}
		}
		return nil
	}
	var firstErr error
	for _, r := range roots {
		if err := walk(r, 1, true); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	sort.SliceStable(res.Victims, func(i, j int) bool {
		a, b := res.Victims[i], res.Victims[j]
		if !a.EventTimestamp.Equal(b.EventTimestamp) {
			return a.EventTimestamp.Before(b.EventTimestamp)
		}
		return a.EventID < b.EventID
	})
	return res, firstErr
}

// sameTransaction: the child event was written in the parent's transaction.
// The commit time identifies the transaction (every event of one carries its
// GTID event's commit timestamp), the file and the connection guard against
// two sessions committing in the same microsecond.
func sameTransaction(p, c query.ResultRow) bool {
	if p.CommitTsUS == nil || c.CommitTsUS == nil || *p.CommitTsUS != *c.CommitTsUS {
		return false
	}
	if p.BinlogFile != c.BinlogFile {
		return false
	}
	if p.ConnectionID != nil && c.ConnectionID != nil && *p.ConnectionID != *c.ConnectionID {
		return false
	}
	return true
}

// matchesRule: the child event is what the rule does to a child that
// referenced key: deleted (CASCADE on delete), its FK nulled (SET NULL), or
// its FK rewritten to newKey (ON UPDATE CASCADE).
func matchesRule(c query.ResultRow, col string, key, newKey any, toNull bool, typ event.EventType) bool {
	if c.EventType != typ || c.RowBefore == nil {
		return false
	}
	before, ok := c.RowBefore[col]
	if !ok || before == nil || !sameKey(before, key) {
		return false
	}
	if typ == event.EventDelete {
		return true
	}
	after, ok := c.RowAfter[col]
	if !ok {
		return false
	}
	if toNull {
		return after == nil
	}
	return after != nil && sameKey(after, newKey)
}

// sameKey compares a child's FK value with the parent's key the way the
// common text collations do: MySQL cascades from 'abc' to a child holding
// 'ABC ' under a case-insensitive PAD SPACE collation, and the logged child
// row holds the child's own spelling. Accent folding is not attempted. Under
// a binary collation this can also match a child the cascade did not touch
// but the same transaction changed, which reverses that change too.
func sameKey(a, b any) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	x, y := valToString(a), valToString(b)
	if x == y {
		return true
	}
	_, as := a.(string)
	_, bs := b.(string)
	return as && bs && strings.EqualFold(strings.TrimRight(x, " "), strings.TrimRight(y, " "))
}
