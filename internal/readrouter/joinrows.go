package readrouter

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// The rows a MariaDB plan reads across its joins.
//
// MariaDB's plan has no cost the router can compare (see Plan.CostUnknown),
// and it resolves a heavy join by walking the small table and probing the
// big one by key: no single table shows a scan over the threshold while the
// join reads millions of rows. This file adds the missing number, from the
// same EXPLAIN FORMAT=JSON.
//
// What MariaDB reports per table, read off real plans (testdata/joins):
//
//   - rows: the rows read EACH TIME the table is entered. 1 for eq_ref and
//     const; the average per key for ref (MariaDB 10.11 reports about half
//     of what 11.4 does for the same index); the whole range, index or
//     table for range, index and ALL. Under an ORDER BY an index serves
//     with a LIMIT, the first table's rows is already cut to the rows the
//     LIMIT needs.
//   - filtered: the percentage of those rows left after the table's own
//     condition.
//   - loops (11.0 and later only): how many times the server expects to
//     enter the table. Not used: 10.11 has none, and under a LIMIT it is
//     not cut the way the first table's rows is (an index walk reported as
//     10 rows is followed by a table with loops 1,991,571).
//
// The estimate: down a list of joined tables, the rows read by a table are
// the rows the tables before it produce (the product of their rows x
// filtered, never taken below one entry) times its rows; the plan reads
// the sum. A subquery or derived table is a block of its own, estimated the
// same way and added once when the server builds it once (materialized, a
// derived table, a subquery with no reference outward) and once per row of
// the block around it when it is run for each row (inside a subquery
// cache, an IN probe, a reference to an outer table, a lateral derived
// table).
//
// A shape this file does not know is never guessed: the estimate is
// withheld with the reason (Plan.RowsReadUnknown), and the plan is decided
// by the scan rules alone, as before.

// maxRowsRead caps the estimate: a product of large row counts stays a
// number an int64 holds, never +Inf or NaN.
const maxRowsRead = 1e18

// rowsEstimator walks one plan. tables counts the table reads it covered,
// to compare with every table read the plan has (Plan.Tables): a table
// under a node the estimator does not descend into would otherwise be
// left out of the sum in silence.
type rowsEstimator struct {
	tables int
	joined bool
}

// estimateRowsRead fills Plan.RowsRead, Plan.Joined and
// Plan.RowsReadUnknown from the top query block.
func estimateRowsRead(qb map[string]any, p *Plan) {
	var e rowsEstimator
	read, _, err := e.block(qb)
	if err == nil && e.tables != p.Tables {
		err = fmt.Errorf("the plan has %d table reads, %d of them where a join step is expected", p.Tables, e.tables)
	}
	if err != nil {
		p.RowsReadUnknown = err.Error()
		return
	}
	p.RowsRead = int64(capRows(read))
	p.Joined = e.joined
}

func capRows(n float64) float64 {
	if math.IsNaN(n) || n > maxRowsRead {
		return maxRowsRead
	}
	return n
}

// blockWrappers are the nodes MariaDB wraps a block's tables in: they sort
// or buffer what the tables below produce and read nothing themselves.
var blockWrappers = []string{"filesort", "temporary_table", "window_functions_computation"}

// block estimates one query block: read is the rows it reads, out the rows
// it produces.
func (e *rowsEstimator) block(qb map[string]any) (read, out float64, err error) {
	read, out, err = e.tablesOf(qb, 0)
	if err != nil {
		return 0, 0, err
	}
	subs, present := qb["subqueries"]
	if !present {
		return read, out, nil
	}
	list, ok := subs.([]any)
	if !ok {
		return 0, 0, fmt.Errorf("subqueries is not a list")
	}
	for _, s := range list {
		sub, perRow, err := subqueryBlock(s)
		if err != nil {
			return 0, 0, err
		}
		subRead, _, err := e.block(sub)
		if err != nil {
			return 0, 0, err
		}
		if perRow {
			// Run for each row of this block (an upper bound: a subquery
			// cache answers a repeated value without running it).
			e.joined = true
			subRead *= math.Max(out, 1)
		}
		read = capRows(read + subRead)
	}
	return read, out, nil
}

// subqueryBlock unwraps one entry of a block's "subqueries" list and says
// whether the server runs it once or for each outer row.
func subqueryBlock(s any) (qb map[string]any, perRow bool, err error) {
	m, ok := s.(map[string]any)
	if !ok || len(m) != 1 {
		return nil, false, fmt.Errorf("a subquery entry is not a single node")
	}
	for key, v := range m {
		node, _ := v.(map[string]any)
		switch key {
		case "query_block":
			// No wrapper: a subquery the server runs once, unless it reads
			// a table by a value of the block around it.
			if node == nil {
				return nil, false, fmt.Errorf("a subquery has no query_block")
			}
			return node, refersOutward(node), nil
		case "materialization":
			// An IN subquery built once into a lookup table.
			qb, _ = node["query_block"].(map[string]any)
		case "subquery_cache", "expression_cache":
			// The cache MariaDB puts in front of a subquery that depends
			// on the outer row (subquery_cache from 11.0, expression_cache
			// before).
			qb, _ = node["query_block"].(map[string]any)
			perRow = true
		default:
			return nil, false, fmt.Errorf("unknown subquery node %q", key)
		}
		if qb == nil {
			return nil, false, fmt.Errorf("%s has no query_block", key)
		}
	}
	return qb, perRow, nil
}

// refersOutward reports whether a subquery block reads a table by a value
// from outside itself: an IN probe (access_type index_subquery or
// unique_subquery), or a "ref" naming a table the block does not contain.
// A subquery tied to the outer row only by a condition no index serves has
// neither; MariaDB puts that one behind a subquery cache unless the cache
// is switched off (optimizer_switch subquery_cache=off), and then it is
// counted once here: too low, which leaves the statement on the source as
// before.
func refersOutward(qb map[string]any) bool {
	names := map[string]bool{}
	var refs []string
	probe := false
	var visit func(v any)
	visit = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if name, ok := x["table_name"].(string); ok {
				names[name] = true
			}
			if at, _ := x["access_type"].(string); at == "index_subquery" || at == "unique_subquery" {
				probe = true
			}
			if list, ok := x["ref"].([]any); ok {
				for _, r := range list {
					if s, ok := r.(string); ok {
						refs = append(refs, s)
					}
				}
			}
			for _, child := range x {
				visit(child)
			}
		case []any:
			for _, child := range x {
				visit(child)
			}
		}
	}
	visit(qb)
	if probe {
		return true
	}
	for _, r := range refs {
		// "const", "func", or "schema.table.column" ("table.column" for a
		// derived table).
		parts := strings.Split(r, ".")
		if len(parts) >= 2 && !names[parts[len(parts)-2]] {
			return true
		}
	}
	return false
}

// tablesOf finds the tables of a block under the wrappers MariaDB puts
// around them and estimates them.
func (e *rowsEstimator) tablesOf(node map[string]any, depth int) (read, out float64, err error) {
	if depth > 8 {
		return 0, 0, fmt.Errorf("wrappers nested too deep")
	}
	if steps, ok := node["nested_loop"].([]any); ok {
		c := chain{e: e, fanout: 1}
		if err := c.steps(steps); err != nil {
			return 0, 0, err
		}
		return c.read, c.fanout, nil
	}
	if t, ok := node["table"].(map[string]any); ok {
		// A block with one table and no list around it.
		c := chain{e: e, fanout: 1}
		if err := c.table(t, false); err != nil {
			return 0, 0, err
		}
		return c.read, c.fanout, nil
	}
	if u, ok := node["union_result"].(map[string]any); ok {
		specs, ok := u["query_specifications"].([]any)
		if !ok {
			return 0, 0, fmt.Errorf("union_result has no query_specifications")
		}
		for _, s := range specs {
			m, _ := s.(map[string]any)
			qb, ok := m["query_block"].(map[string]any)
			if !ok || len(m) != 1 {
				return 0, 0, fmt.Errorf("a UNION branch is not a query_block")
			}
			r, o, err := e.block(qb)
			if err != nil {
				return 0, 0, err
			}
			read, out = capRows(read+r), capRows(out+o)
		}
		return read, out, nil
	}
	for _, w := range blockWrappers {
		if inner, ok := node[w].(map[string]any); ok {
			return e.tablesOf(inner, depth+1)
		}
	}
	if _, ok := node["recursive_union"]; ok {
		return 0, 0, fmt.Errorf("a recursive CTE runs its block an unknown number of times")
	}
	return 0, 0, fmt.Errorf("no list of tables found in a query block")
}

// chain is one list of joined tables being walked: read is the rows read
// so far, fanout the rows the tables so far produce.
type chain struct {
	e      *rowsEstimator
	read   float64
	fanout float64
	n      int // tables in this chain
}

func (c *chain) steps(steps []any) error {
	for _, s := range steps {
		m, ok := s.(map[string]any)
		if !ok || len(m) != 1 {
			return fmt.Errorf("a join step is not a single node")
		}
		for key, v := range m {
			switch key {
			case "table":
				t, ok := v.(map[string]any)
				if !ok {
					return fmt.Errorf("a table step is not an object")
				}
				if err := c.table(t, false); err != nil {
					return err
				}
			case "block-nl-join":
				// The table is joined through a join buffer.
				b, _ := v.(map[string]any)
				t, ok := b["table"].(map[string]any)
				if !ok {
					return fmt.Errorf("block-nl-join has no table")
				}
				if err := c.table(t, true); err != nil {
					return err
				}
			case "read_sorted_file":
				// A table read through a sort of its own rows.
				f, _ := v.(map[string]any)
				fs, _ := f["filesort"].(map[string]any)
				t, ok := fs["table"].(map[string]any)
				if !ok {
					return fmt.Errorf("read_sorted_file has no table")
				}
				if err := c.table(t, false); err != nil {
					return err
				}
			case "duplicates_removal":
				// Semi-join duplicate weedout: the tables inside are part
				// of this same list.
				inner, ok := v.([]any)
				if !ok {
					return fmt.Errorf("duplicates_removal is not a list of tables")
				}
				if err := c.steps(inner); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown join step %q", key)
			}
		}
	}
	return nil
}

// table adds one table read to the chain. buffered is true under a
// block-nl-join.
func (c *chain) table(t map[string]any, buffered bool) error {
	at, ok := t["access_type"].(string)
	if !ok {
		if _, isMessage := t["message"].(string); isMessage {
			return nil // "No tables used", "Impossible WHERE": nothing is read
		}
		return fmt.Errorf("a table has no access_type")
	}
	name, _ := t["table_name"].(string)
	rows, ok := planNumber(t["rows"])
	if !ok {
		return fmt.Errorf("table %s has no usable rows estimate", name)
	}
	filtered := 100.0
	if f, present := t["filtered"]; present {
		if filtered, ok = planNumber(f); !ok || filtered > 100 {
			return fmt.Errorf("table %s has no usable filtered percentage", name)
		}
	}
	entries := math.Max(c.fanout, 1) // a table is entered at least once
	if buffered && strings.HasPrefix(at, "hash_") {
		// A hash join reads the table once to build the hash and probes
		// it in memory.
		entries = 1
	}
	// Without a hash, a buffered join compares every buffered row with
	// every row of the table: counted like any other entry per row.
	c.read = capRows(c.read + entries*rows)
	if rf, present := t["rowid_filter"]; present {
		// The filter is built once from a range of another index.
		m, _ := rf.(map[string]any)
		n, ok := planNumber(m["rows"])
		if !ok {
			return fmt.Errorf("table %s has a rowid_filter with no rows estimate", name)
		}
		c.read = capRows(c.read + n)
	}
	if mat, present := t["materialized"]; present {
		m, _ := mat.(map[string]any)
		qb, ok := m["query_block"].(map[string]any)
		if !ok {
			return fmt.Errorf("table %s is materialized from no query_block", name)
		}
		subRead, _, err := c.e.block(qb)
		if err != nil {
			return err
		}
		if _, lateral := m["lateral"]; lateral {
			// Built again for each row of the tables before it.
			c.e.joined = true
			subRead *= entries
		}
		c.read = capRows(c.read + subRead)
	}
	c.fanout = capRows(c.fanout * rows * filtered / 100)
	c.n++
	c.e.tables++
	if c.n > 1 {
		c.e.joined = true
	}
	return nil
}

// planNumber reads a row count or percentage: a JSON number that is finite
// and not negative. Anything else (absent, a string, negative) is not a
// number to estimate from.
func planNumber(v any) (float64, bool) {
	f, ok := v.(float64)
	if !ok || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 {
		return 0, false
	}
	return f, true
}

// groupDigits writes n with thousands separators, for the reason text.
func groupDigits(n int64) string {
	s := strconv.FormatInt(n, 10)
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

// roundRows keeps three significant digits: the estimate is one, and the
// reason text should not read as a count.
func roundRows(n int64) int64 {
	if n < 1000 {
		return n
	}
	unit := int64(1)
	for n/unit >= 1000 {
		unit *= 10
	}
	return (n + unit/2) / unit * unit
}
