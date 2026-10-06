// Package reconstruct implements row state reconstruction by combining baseline
// Parquet snapshots with indexed binlog change events.
//
// The core idea: a baseline Parquet file captures the full row state at a point
// in time (produced by "bintrail baseline"). Binlog events captured after that
// snapshot each carry a full row_after image. Applying them in order up to a
// target timestamp yields the exact row state at that moment.
package reconstruct

import (
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// StateEntry is the row state at a single point in time.
type StateEntry struct {
	Time    time.Time      // when this state was established
	Source  string         // "baseline" | "INSERT" | "UPDATE" | "DELETE"
	EventID uint64         // 0 for the baseline entry
	GTID    string         // empty when not applicable
	State   map[string]any // nil means the row was deleted
}

// ApplyAt applies binlog events to initialState up to and including at,
// returning the final row state. Returns a nil state if the row was deleted.
// Events are folded in the order given. A caller that wants the row's changes
// in binary log order, and not in the (event_timestamp, event_id) order a
// fetch returns, passes them through EventsInBinlogOrder first (#2156).
//
// The `ev.EventTimestamp.After(at)` check below is a defensive backstop, not
// the transaction-boundary cut: callers are expected to have already fetched
// events via FetchEventsAtomic (or trimmed them with
// TrimPartialTailTransaction), which drops a trailing PARTIAL transaction —
// one whose statements straddle `at` — wholesale rather than letting it be
// half-applied here (#783). A caller that passes raw query.FetchMerged
// output gets the pre-#783 per-row cut, which can apply half of a
// multi-statement transaction.
//
// It fails loud on a residual unchanged-TOAST marker (#592) in any folded
// event's row image: the marker means the capture invariant (REPLICA IDENTITY
// FULL resolves every unchanged TOAST value at decode time) was violated, so
// the reconstructed state would silently carry the marker instead of the real
// value. Events after `at` are never inspected — they are not folded.
func ApplyAt(initialState map[string]any, events []query.ResultRow, at time.Time) (map[string]any, error) {
	state := copyMap(initialState)
	for _, ev := range events {
		if ev.EventTimestamp.After(at) {
			break
		}
		if err := checkEventToast(ev); err != nil {
			return nil, err
		}
		state = applyEvent(state, ev)
	}
	return state, nil
}

// BuildHistory applies events chronologically up to at and returns a StateEntry
// for each state transition, starting with the baseline as the first entry.
// Events are folded and listed in the order given: see ApplyAt for binary log
// order (EventsInBinlogOrder, #2156). Like ApplyAt, it fails
// loud on a residual unchanged-TOAST marker (#592) in any folded event.
//
// Same transaction-boundary caveat as ApplyAt: pass events already fetched
// via FetchEventsAtomic / trimmed via TrimPartialTailTransaction (#783), or
// a straddling transaction can appear here as two history entries (T1, T2)
// when it should appear as none.
func BuildHistory(initialState map[string]any, baselineTime time.Time, events []query.ResultRow, at time.Time) ([]StateEntry, error) {
	entries := []StateEntry{{
		Time:   baselineTime,
		Source: "baseline",
		State:  copyMap(initialState),
	}}
	current := copyMap(initialState)
	for _, ev := range events {
		if ev.EventTimestamp.After(at) {
			break
		}
		if err := checkEventToast(ev); err != nil {
			return nil, err
		}
		current = applyEvent(current, ev)
		gtid := ""
		if ev.GTID != nil {
			gtid = *ev.GTID
		}
		entries = append(entries, StateEntry{
			Time:    ev.EventTimestamp,
			Source:  eventTypeName(ev.EventType),
			EventID: ev.EventID,
			GTID:    gtid,
			State:   copyMap(current),
		})
	}
	return entries, nil
}

// ─── Formatters ───────────────────────────────────────────────────────────────

// WriteStateJSON writes a single row state as indented JSON.
func WriteStateJSON(state map[string]any, w io.Writer) error {
	if state == nil {
		_, err := fmt.Fprintln(w, "null")
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(state)
}

// WriteHistoryJSON writes history entries as an indented JSON array.
func WriteHistoryJSON(entries []StateEntry, w io.Writer) error {
	type jsonEntry struct {
		Time    string         `json:"time"`
		Source  string         `json:"source"`
		EventID uint64         `json:"event_id,omitempty"`
		GTID    string         `json:"gtid,omitempty"`
		State   map[string]any `json:"state"`
	}
	out := make([]jsonEntry, len(entries))
	for i, e := range entries {
		out[i] = jsonEntry{
			Time:    e.Time.UTC().Format(time.RFC3339),
			Source:  e.Source,
			EventID: e.EventID,
			GTID:    e.GTID,
			State:   e.State,
		}
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// formatCell renders a state value as text for the table/CSV formatters. A
// []byte — a decoded BLOB column (#666), or a baseline BLOB scanned by DuckDB —
// is rendered as base64 to match the JSON output of the same command; fmt's
// "%v" would otherwise print a raw decimal byte array (e.g. "[72 101 ...]").
// Everything else uses the default "%v".
func formatCell(v any) string {
	if b, ok := v.([]byte); ok {
		return base64.StdEncoding.EncodeToString(b)
	}
	return fmt.Sprintf("%v", v)
}

// WriteStateTable writes a single row state as aligned key-value pairs.
func WriteStateTable(state map[string]any, w io.Writer) error {
	if state == nil {
		_, err := fmt.Fprintln(w, "(row deleted)")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	defer tw.Flush()
	fmt.Fprintln(tw, "COLUMN\tVALUE")
	fmt.Fprintln(tw, "──────\t─────")
	for _, col := range sortedKeys(state) {
		fmt.Fprintf(tw, "%s\t%s\n", col, formatCell(state[col]))
	}
	return nil
}

// WriteHistoryTable writes history entries as a tabwriter table.
// The STATE column renders as compact "key=value" pairs.
func WriteHistoryTable(entries []StateEntry, w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	defer tw.Flush()
	fmt.Fprintln(tw, "TIMESTAMP\tSOURCE\tEVENT_ID\tGTID\tSTATE")
	fmt.Fprintln(tw, "─────────────────────\t──────\t────────\t────\t─────")
	for _, e := range entries {
		eid := "-"
		if e.EventID > 0 {
			eid = fmt.Sprintf("%d", e.EventID)
		}
		gtid := "-"
		if e.GTID != "" {
			gtid = e.GTID
		}
		stateStr := "(deleted)"
		if e.State != nil {
			var parts []string
			for _, col := range sortedKeys(e.State) {
				parts = append(parts, fmt.Sprintf("%s=%s", col, formatCell(e.State[col])))
			}
			stateStr = strings.Join(parts, " ")
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			e.Time.UTC().Format("2006-01-02 15:04:05"),
			e.Source, eid, gtid, stateStr)
	}
	return nil
}

// WriteStateCSV writes a single row state as CSV (header + one data row).
func WriteStateCSV(state map[string]any, w io.Writer) error {
	cw := csv.NewWriter(w)
	if state == nil {
		if err := cw.Write([]string{"(deleted)"}); err != nil {
			return err
		}
		cw.Flush()
		return cw.Error()
	}
	keys := sortedKeys(state)
	if err := cw.Write(keys); err != nil {
		return err
	}
	vals := make([]string, len(keys))
	for i, k := range keys {
		vals[i] = formatCell(state[k])
	}
	if err := cw.Write(vals); err != nil {
		return err
	}
	cw.Flush()
	return cw.Error()
}

// WriteHistoryCSV writes history entries as CSV. Columns are: time, source,
// event_id, gtid, followed by all state columns (union of all entries, sorted).
func WriteHistoryCSV(entries []StateEntry, w io.Writer) error {
	// Collect all unique state columns across every entry.
	colSet := make(map[string]struct{})
	for _, e := range entries {
		for col := range e.State {
			colSet[col] = struct{}{}
		}
	}
	stateCols := sortedKeysBool(colSet)
	headers := append([]string{"time", "source", "event_id", "gtid"}, stateCols...)

	cw := csv.NewWriter(w)
	if err := cw.Write(headers); err != nil {
		return err
	}
	for _, e := range entries {
		record := []string{
			e.Time.UTC().Format(time.RFC3339),
			e.Source,
			fmt.Sprintf("%d", e.EventID),
			e.GTID,
		}
		for _, col := range stateCols {
			if e.State == nil {
				record = append(record, "")
			} else {
				record = append(record, formatCell(e.State[col]))
			}
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// ─── SQL result formatters ────────────────────────────────────────────────────

// WriteSQLResultsJSON writes dynamic DuckDB SQL result rows as an indented JSON array.
func WriteSQLResultsJSON(rows []map[string]any, w io.Writer) error {
	if len(rows) == 0 {
		_, err := fmt.Fprintln(w, "[]")
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rows)
}

// WriteSQLResultsTable writes dynamic DuckDB SQL results as an aligned table.
func WriteSQLResultsTable(rows []map[string]any, cols []string, w io.Writer) error {
	if len(rows) == 0 {
		_, err := fmt.Fprintln(w, "No results.")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	defer tw.Flush()
	headers := make([]string, len(cols))
	for i, c := range cols {
		headers[i] = strings.ToUpper(c)
	}
	fmt.Fprintln(tw, strings.Join(headers, "\t"))
	seps := make([]string, len(cols))
	for i, c := range cols {
		seps[i] = strings.Repeat("─", len(c))
	}
	fmt.Fprintln(tw, strings.Join(seps, "\t"))
	for _, row := range rows {
		vals := make([]string, len(cols))
		for i, col := range cols {
			vals[i] = fmt.Sprintf("%v", row[col])
		}
		fmt.Fprintln(tw, strings.Join(vals, "\t"))
	}
	return nil
}

// WriteSQLResultsCSV writes dynamic DuckDB SQL results as CSV.
func WriteSQLResultsCSV(rows []map[string]any, cols []string, w io.Writer) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(cols); err != nil {
		return err
	}
	for _, row := range rows {
		record := make([]string, len(cols))
		for i, col := range cols {
			record[i] = fmt.Sprintf("%v", row[col])
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// ─── internal helpers ─────────────────────────────────────────────────────────

// checkEventToast fails loud when either row image of an event carries a
// residual unchanged-TOAST marker (#592). Both images are checked — only
// row_after is folded into state, but a marker in EITHER image proves the
// capture invariant was violated for this event, and refusing is always safer
// than emitting a state whose provenance is broken.
func checkEventToast(ev query.ResultRow) error {
	return event.CheckUnresolvedToast(ev.SchemaName, ev.TableName, ev.PKValues, ev.RowBefore, ev.RowAfter)
}

func applyEvent(state map[string]any, ev query.ResultRow) map[string]any {
	switch ev.EventType {
	case event.EventInsert:
		return copyMap(ev.RowAfter)
	case event.EventUpdate:
		return copyMap(ev.RowAfter)
	case event.EventDelete:
		return nil
	case 0:
		// EventType==0 is the zero value, produced by the defensive
		// scanRows in internal/query and internal/parquetquery when the
		// row's event_type column is NULL (dbtrail/bintrail#318 drift).
		// Without an explicit case the default branch would silently
		// no-op, producing wrong reconstructed state with no operator signal.
		slog.Warn("reconstruct: skipping drift row with NULL event_type — reconstructed state may be incomplete",
			"event_id", ev.EventID,
			"schema", ev.SchemaName,
			"table", ev.TableName,
			"timestamp", ev.EventTimestamp)
		return state
	default:
		return state
	}
}

func copyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func eventTypeName(et event.EventType) string {
	switch et {
	case event.EventInsert:
		return "INSERT"
	case event.EventUpdate:
		return "UPDATE"
	case event.EventDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func sortedKeysBool(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
