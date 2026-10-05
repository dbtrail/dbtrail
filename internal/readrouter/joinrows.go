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
// filtered, never taken below one entry) times its rows; the list reads
// the sum. A table the server leaves at the first match (first_match with
// no condition of its own to test, not_exists, the index probe of an IN or
// EXISTS subquery) reads one row per entry and passes on at most one row
// per row that came in. A const or system table is a value read before
// the plan starts, not a table of the join. A table read whole under a
// condition, with filtered reported as 100, is taken to pass a tenth.
//
// A subquery or derived table is a block of its own, estimated the same
// way and added once when the server builds it once (materialized, a
// derived table, a subquery with no reference outward) and once per row of
// the block around it when it is run for each row (inside a subquery
// cache, an IN probe, a reference to an outer table, a lateral derived
// table).
//
// Only the parts that multiply are counted (Plan.RowsRead): a list of two
// tables or more, and what is run once per row. A table read alone, as a
// whole statement, a UNION branch or a derived table built once, is the
// scan rules' business, however many rows it reads through an index, and
// a small join beside it does not change that.
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
	sum, err := e.block(qb)
	if err == nil && e.tables != p.Tables {
		err = fmt.Errorf("the plan has %d table reads, %d of them where a join step is expected", p.Tables, e.tables)
	}
	if err != nil {
		p.RowsReadUnknown = err.Error()
		return
	}
	p.RowsRead = int64(capRows(sum.across))
	p.topSort, p.sortedFirstRows = topSortOf(qb)
	p.Joined = e.joined
}

func capRows(n float64) float64 {
	if math.IsNaN(n) || n > maxRowsRead {
		return maxRowsRead
	}
	return n
}

// How the top block sorts, for a statement whose LIMIT could end its join
// early (DecideStatement).
const (
	sortNone       = iota // nothing sorts or buffers the joined rows
	sortFirstTable        // the first table is sorted alone, then joined
	sortWholeJoin         // a sort or a temporary table takes the whole join
)

// topSortOf reads how the top block sorts. For sortFirstTable, rows is the
// sorted table's row estimate.
func topSortOf(qb map[string]any) (kind int8, rows int64) {
	for _, w := range blockWrappers {
		if _, ok := qb[w]; ok {
			return sortWholeJoin, 0
		}
	}
	steps, _ := qb["nested_loop"].([]any)
	for i, s := range steps {
		m, _ := s.(map[string]any)
		f, ok := m["read_sorted_file"].(map[string]any)
		if !ok {
			continue
		}
		fs, _ := f["filesort"].(map[string]any)
		t, _ := fs["table"].(map[string]any)
		n, ok := planNumber(t["rows"])
		if i > 0 || !ok {
			// A sort further down the list is fed by the tables before
			// it: taken as a sort of the whole join.
			return sortWholeJoin, 0
		}
		kind, rows = sortFirstTable, int64(capRows(n))
	}
	return kind, rows
}

// blockWrappers are the nodes MariaDB wraps a block's tables in: they sort
// or buffer what the tables below produce and read nothing themselves.
var blockWrappers = []string{"filesort", "temporary_table", "window_functions_computation"}

// estimate is what one block or list of tables adds up to: read is every
// row it reads, across the rows read by its parts that multiply (the
// number the rule compares), out the rows it produces.
type estimate struct{ read, across, out float64 }

// block estimates one query block.
func (e *rowsEstimator) block(qb map[string]any) (estimate, error) {
	sum, err := e.tablesOf(qb, 0)
	if err != nil {
		return estimate{}, err
	}
	subs, present := qb["subqueries"]
	if !present {
		return sum, nil
	}
	list, ok := subs.([]any)
	if !ok {
		return estimate{}, fmt.Errorf("subqueries is not a list")
	}
	for _, s := range list {
		sub, perRow, err := subqueryBlock(s)
		if err != nil {
			return estimate{}, err
		}
		subSum, err := e.block(sub)
		if err != nil {
			return estimate{}, err
		}
		if perRow {
			// Run for each row of this block (an upper bound: a subquery
			// cache answers a repeated value without running it).
			e.joined = true
			subSum.read = capRows(subSum.read * math.Max(sum.out, 1))
			subSum.across = subSum.read
		}
		sum.read = capRows(sum.read + subSum.read)
		sum.across = capRows(sum.across + subSum.across)
	}
	return sum, nil
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
// unique_subquery), or a "ref" naming a base table the block does not
// contain ("schema.table.column"; table_name is the alias the ref uses).
// A ref to a derived table ("alias.column") names an alias the plan shows
// nowhere else (its table_name is "<derived2>"), so it cannot be placed
// and is not taken for an outer one.
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
		parts := strings.Split(r, ".")
		if len(parts) == 3 && !names[parts[1]] {
			return true
		}
	}
	return false
}

// tablesOf finds the tables of a block under the wrappers MariaDB puts
// around them and estimates them.
func (e *rowsEstimator) tablesOf(node map[string]any, depth int) (estimate, error) {
	if depth > 8 {
		return estimate{}, fmt.Errorf("wrappers nested too deep")
	}
	if steps, ok := node["nested_loop"].([]any); ok {
		c := chain{e: e, fanout: 1}
		if err := c.steps(steps); err != nil {
			return estimate{}, err
		}
		return c.sum(), nil
	}
	if t, ok := node["table"].(map[string]any); ok {
		// A block with one table and no list around it.
		c := chain{e: e, fanout: 1}
		if err := c.table(t, false); err != nil {
			return estimate{}, err
		}
		return c.sum(), nil
	}
	if u, ok := node["union_result"].(map[string]any); ok {
		specs, ok := u["query_specifications"].([]any)
		if !ok {
			return estimate{}, fmt.Errorf("union_result has no query_specifications")
		}
		var sum estimate
		for _, s := range specs {
			m, _ := s.(map[string]any)
			qb, ok := m["query_block"].(map[string]any)
			if !ok || len(m) != 1 {
				return estimate{}, fmt.Errorf("a UNION branch is not a query_block")
			}
			b, err := e.block(qb)
			if err != nil {
				return estimate{}, err
			}
			sum = estimate{capRows(sum.read + b.read), capRows(sum.across + b.across), capRows(sum.out + b.out)}
		}
		return sum, nil
	}
	for _, w := range blockWrappers {
		if inner, ok := node[w].(map[string]any); ok {
			return e.tablesOf(inner, depth+1)
		}
	}
	if _, ok := node["recursive_union"]; ok {
		return estimate{}, fmt.Errorf("a recursive CTE runs its block an unknown number of times")
	}
	return estimate{}, fmt.Errorf("no list of tables found in a query block")
}

// chain is one list of joined tables being walked: own is the rows its
// tables read so far, fanout the rows they produce; subRead and subAcross
// are what the derived tables among them read to be built.
type chain struct {
	e                  *rowsEstimator
	own, fanout        float64
	subRead, subAcross float64
	n                  int // tables in this chain, const and system ones left out
}

// sum is the chain's total. Its own tables count as rows read across a
// join only when there are two or more of them.
func (c *chain) sum() estimate {
	across := c.subAcross
	if c.n > 1 {
		across = capRows(across + c.own)
	}
	return estimate{read: capRows(c.own + c.subRead), across: across, out: c.fanout}
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
	cond, _ := t["attached_condition"].(string)
	passed := filtered
	if (at == "ALL" || at == "index") && cond != "" && filtered == 100 && !strings.Contains(cond, "subquery#") {
		// A condition on a table read whole, with every row reported as
		// passing it: MariaDB has no statistics on the column and says
		// 100. Taken as a tenth, the guess MySQL makes for the same
		// filter, so that a small table scanned under a selective filter
		// is not counted as if each of its rows were joined.
		passed = 10
	}
	entries := math.Max(c.fanout, 1) // a table is entered at least once
	if buffered && strings.HasPrefix(at, "hash_") {
		// A hash join reads the table once to build the hash and probes
		// it in memory.
		entries = 1
	}
	// Without a hash, a buffered join compares every buffered row with
	// every row of the table: counted like any other entry per row.
	perEntry := rows
	_, firstMatch := t["first_match"]
	_, notExists := t["not_exists"]
	probe := at == "index_subquery" || at == "unique_subquery"
	if notExists || probe || (firstMatch && cond == "") {
		// The server leaves the table at the first row it finds for each
		// entry: a semi-join with nothing more to test on the row, an
		// anti-join, or the index probe of an IN or EXISTS subquery. (A
		// first_match table with a condition of its own reads on until a
		// row passes it: counted in full.)
		perEntry = math.Min(rows, 1)
	}
	c.own = capRows(c.own + entries*perEntry)
	if rf, present := t["rowid_filter"]; present {
		// The filter is built once from a range of another index.
		m, _ := rf.(map[string]any)
		n, ok := planNumber(m["rows"])
		if !ok {
			return fmt.Errorf("table %s has a rowid_filter with no rows estimate", name)
		}
		c.own = capRows(c.own + n)
	}
	if mat, present := t["materialized"]; present {
		m, _ := mat.(map[string]any)
		qb, ok := m["query_block"].(map[string]any)
		if !ok {
			return fmt.Errorf("table %s is materialized from no query_block", name)
		}
		sub, err := c.e.block(qb)
		if err != nil {
			return err
		}
		if _, lateral := m["lateral"]; lateral {
			// Built again for each row of the tables before it.
			c.e.joined = true
			sub.read = capRows(sub.read * entries)
			sub.across = sub.read
		}
		c.subRead = capRows(c.subRead + sub.read)
		c.subAcross = capRows(c.subAcross + sub.across)
	}
	out := rows * passed / 100
	if notExists || firstMatch || probe {
		// At most one row goes on for each row that came in.
		out = math.Min(out, 1)
	}
	c.fanout = capRows(c.fanout * out)
	c.e.tables++
	if at == "const" || at == "system" {
		// One row the optimizer read before the plan started: a value,
		// not a table of the join.
		return nil
	}
	c.n++
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
