package pgshim

// What is expensive on the source goes to the copy (product decision,
// 2026-11-24): on a connection that reaches its source, a statement sent on
// the simple protocol whose plan on the source is over the port's cost
// thresholds is answered by the copy even when the screen would send it to
// the source for how the copy answers it (an order of text, a char(n)'s
// padding, a decimal, an alias, a division the screen cannot prove). The
// copy may then answer otherwise than PostgreSQL would; that is accepted,
// and said in docs/postgres.md. What the copy cannot run at all is not
// relaxed: a statement the port does not read whole, one under a session
// time zone, one in a transaction block, one whose plan is cheap or could
// not be had. A statement the copy fails on still goes to the source
// (copyFallback), and nothing of the copy's answer was sent by then.
//
// Not on the extended protocol: a statement prepared there is described by
// one side and may be run by the other, and a copy that announces other
// types than the source's (a division, a length, a sum of integers) would
// leave a client that keeps the statement with "cached plan must not change
// result type" for good.

// expensive reports whether d, what the source's plan decided, sends the
// statement to the copy for its cost: the plan was asked and found it over
// the thresholds.
func (s *session) expensive(d routeDecision) bool {
	return d.side == RouteCopy && d.reason == ReasonAnswersTheSame && !s.costPolicy().off()
}
