package pgshim

// The copy's fast lane. The copy runs a few statements at once, and a short
// statement for it waited behind every report that held its workers: a sum
// of one warehouse, answered in a second alone, took 13 to 31 seconds under
// 30 connections with reports running. The host (the pg-router) keeps one of
// the copy's workers for short statements, and this says which are short,
// before they run, from the plan the source already gave to route them
// (route_cost.go): one that reads one table and joins nothing
// (RunOptions.FastLane). On the copy, what makes a report long is its joins;
// a read of one table is one pass over one table, whatever its plan costs on
// the source, so the source's cost cannot tell them apart: the reports cost
// from 98,367 to 13,351,258 there, and a sum of one table of 30 million rows
// about 500,000.
//
// "One table" is counted on the safe side: when in doubt, a statement is not
// short.
//   - Exactly one scan of a table, of the kinds that read a table: Seq Scan,
//     Index Scan, Index Only Scan, Bitmap Heap Scan. A self-join reads it
//     twice, and is not.
//   - Above it only nodes that read nothing of their own and have one child:
//     Aggregate, Limit, Sort, Incremental Sort, Gather, Gather Merge, Unique,
//     Group, WindowAgg. Under a Bitmap Heap Scan, only the index nodes it is
//     built from (Bitmap Index Scan, BitmapAnd, BitmapOr).
//   - A SubPlan or an InitPlan (a subquery the planner kept apart, which can
//     read another table, or the same table again, with no Join node in the
//     plan), a CTE Scan, an Append or a Merge Append (a partitioned table
//     read in more than one partition, a UNION, even an Append of one), a
//     Subquery Scan, a function, a Result, a Materialize, and any node not
//     named above: not short. It is the plan that counts, not the text:
//     PostgreSQL folds a WITH used once, a subquery in FROM and a partition
//     pruned to one into the query, and a plan that then reads one table
//     alone is one pass over one table on the copy too.
//   - No plan (routing by plan off, a connection with no source, the plan
//     refused or unreadable, the capture account's gate): not short.
//
// Nothing is cut short in the lane: a short statement that runs long runs to
// its end, as any other does.

// oneTableScans read a table.
var oneTableScans = map[string]bool{"Seq Scan": true, "Index Scan": true, "Index Only Scan": true, "Bitmap Heap Scan": true}

// oneTableAbove read nothing of their own and pass their one child's rows on.
var oneTableAbove = map[string]bool{"Aggregate": true, "Limit": true, "Sort": true, "Incremental Sort": true,
	"Gather": true, "Gather Merge": true, "Unique": true, "Group": true, "WindowAgg": true}

// bitmapIndexNodes are what a Bitmap Heap Scan is built from: they read the
// indexes of its table.
var bitmapIndexNodes = map[string]bool{"Bitmap Index Scan": true, "BitmapAnd": true, "BitmapOr": true}

// readsOneTable reports whether a plan, its top node as EXPLAIN (FORMAT
// JSON) writes it, reads one table and joins nothing, by the rules above.
func readsOneTable(top map[string]any) bool {
	scans := 0
	return walkOneTable(top, &scans, false, 0) && scans == 1
}

// walkOneTable checks node and its children; underBitmap is set below a
// Bitmap Heap Scan, where only its index nodes may stand.
func walkOneTable(node map[string]any, scans *int, underBitmap bool, depth int) bool {
	if depth > maxPlanDepth {
		return false
	}
	if rel, _ := node["Parent Relationship"].(string); rel == "InitPlan" || rel == "SubPlan" {
		return false
	}
	if _, ok := node["Subplan Name"]; ok {
		return false
	}
	kind, _ := node["Node Type"].(string)
	var children []any
	if raw, ok := node["Plans"]; ok {
		list, ok := raw.([]any)
		if !ok {
			return false
		}
		children = list
	}
	childrenUnderBitmap := false
	switch {
	case underBitmap:
		if !bitmapIndexNodes[kind] {
			return false
		}
		// A Bitmap Index Scan has no children; BitmapAnd and BitmapOr have
		// index nodes.
		childrenUnderBitmap = true
	case kind == "Bitmap Heap Scan":
		*scans++
		if len(children) != 1 {
			return false
		}
		childrenUnderBitmap = true
	case oneTableScans[kind]:
		*scans++
		// A child under a scan is a subplan, which the check above refuses
		// by its relationship; anything else is not a plan this knows.
		if len(children) != 0 {
			return false
		}
	case oneTableAbove[kind]:
		if len(children) != 1 {
			return false
		}
	default:
		return false
	}
	for _, c := range children {
		child, ok := c.(map[string]any)
		if !ok {
			return false
		}
		if !walkOneTable(child, scans, childrenUnderBitmap, depth+1) {
			return false
		}
	}
	return true
}
