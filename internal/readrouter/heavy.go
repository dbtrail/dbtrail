package readrouter

// What is expensive on the source goes to the copy (product decision,
// 2026-11-26, the MySQL side of what the PostgreSQL port does since
// 0.140.0): a statement sent as text whose plan on the source is expensive
// is answered by the copy even where a veto would keep it on MySQL only
// for how the copy computes its answer. The copy may then answer otherwise
// than MySQL: other decimals, another order of text or of ties, a LIKE
// that tells letter case apart, a NULL argument skipped. That is accepted,
// and said in docs/time-travel-sql.md.
//
// Not relaxed, whatever the plan: a veto under which the copy would read
// another statement than MySQL (a comment MySQL executes, a string or a
// name written otherwise, ||, ^, --, $...$, #, a word the copy reserves), or
// one that needs what only the live server has (a variable, the session's
// user, database or clock, FOUND_ROWS, a locking read, INTO, the system
// schemas), or that the copy cannot run as MySQL means it (a recursion
// MySQL stops and the copy may not, JSON, MATCH, BINARY, COLLATE, a CAST to
// SIGNED or UNSIGNED, DIV, a date read from text, a DATEDIFF or
// TIMESTAMPDIFF the copy is not sent as its macro, a DATE_ADD or an AVG the
// rewrite refuses), and RAND and UUID. A veto added to the list is held to
// by default: it is relaxed only once it is named here.

// heavyRelaxes are the names on the veto list (vetoes) an expensive
// statement is let past. The other relaxed vetoes (CONCAT/LEAST/GREATEST,
// SUBSTRING, LEFT/RIGHT, division, UNSIGNED arithmetic) are checks of their
// own in vetoOpts, skipped there.
var heavyRelaxes = map[string]bool{
	// MySQL cuts the text at group_concat_max_len.
	"GROUP_CONCAT": true,
	// Other week numbers.
	"WEEK/YEARWEEK": true,
	// The same.
	"EXTRACT(WEEK ...)": true,
	// The copy tells letter case apart.
	likeVeto: true,
	// The same.
	"INSTR/LOCATE/POSITION/STRCMP (case-sensitive on the copy)": true,
	// Duplicates removed by bytes or by the copy's case folding.
	"UNION/INTERSECT/EXCEPT without ALL (duplicates removed by the copy's rules, not by MySQL's collation)": true,
}

// HardVetoOpts is VetoOpts without the vetoes an expensive statement is let
// past (heavyRelaxes and the value checks after them): the first construct
// that keeps the statement on MySQL whatever its plan, or "". It is not the
// first veto VetoOpts names: a statement with a LIKE and a || is held by
// the ||, which VetoOpts names second.
func HardVetoOpts(stmt string, o CopyOptions) string {
	return vetoOpts(stmt, o, true)
}
