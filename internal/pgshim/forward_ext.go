package pgshim

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

// The extended query protocol toward the source (#2771). A statement the
// client prepared is kept here as always (extended.go), and parsed on the
// source the first time it is described or bound there, under the client's
// own name: a driver that prepares once and executes many times (pgx in its
// default mode, JDBC after a few executions) prepares outside a transaction
// and executes inside one as a matter of course.
//
// Each message is sent to the source with a Flush and its answer read before
// the next one is taken from the client, so the answers the port gives and
// the ones the source gives reach the client in the order it asked. The
// client's Sync is sent on when the batch touched the source, and the
// transaction status the source reports is what the client is told.

// stmtToSource decides whether a prepared statement is described and run on
// the source.
func (s *session) stmtToSource(st *preparedStatement) bool {
	d := s.stmtRoute(st)
	return d.side == RouteSource || d.side == RouteRefused
}

// stmtRoute is the side that answers a prepared statement now, and why. A
// statement the copy refused once (st.fallback) goes to the source from
// then on: the client keeps the description it was given, so the decision
// stays with the statement.
func (s *session) stmtRoute(st *preparedStatement) routeDecision {
	if st.fallback != "" && s.forwarding() {
		if s.pinned() {
			return routeDecision{RouteSource, ReasonInTransaction}
		}
		return routeDecision{RouteSource, st.fallback}
	}
	// Classified with a stand-in for each argument, as Parse classifies it;
	// screened as the client wrote it, each $n a value of its own type.
	d := s.route(s.classify(st.probeText()), st.query)
	if !byCostApplies(d) {
		return d
	}
	// The source's plan, asked once for the statement, before any argument
	// exists (route_cost.go): its Describe and every execution are answered
	// by the same side.
	if st.byCost.side != "" {
		return st.byCost
	}
	d, keep, stop := s.routeByCost(st.query, len(st.paramOIDs) > 0)
	if keep || stop != nil && stop.code != "57014" {
		// A source that could not be reached is kept too: this is asked again
		// by every message of the statement (its Bind, its note), and each
		// would wait for the source to answer once more. It goes there, whose
		// error the client gets.
		st.byCost = d
	}
	return d
}

// cloneBind copies a Bind message, whose buffers the next message reuses.
func cloneBind(m *pgproto3.Bind) *pgproto3.Bind {
	c := &pgproto3.Bind{
		DestinationPortal:    m.DestinationPortal,
		PreparedStatement:    m.PreparedStatement,
		ParameterFormatCodes: append([]int16(nil), m.ParameterFormatCodes...),
		ResultFormatCodes:    append([]int16(nil), m.ResultFormatCodes...),
		Parameters:           make([][]byte, len(m.Parameters)),
	}
	for i, p := range m.Parameters {
		if p != nil {
			c.Parameters[i] = append([]byte{}, p...)
		}
	}
	return c
}

// fallBackPortal moves to the source a portal of the copy whose statement
// the copy just failed (session.fallback), before any of its rows was sent.
// tried false: it stays on the copy, and its error is the client's; true:
// the portal is on the source (p.onSource), or the client was sent the
// error that kept it from getting there.
func (s *session) fallBackPortal(p *portal) (tried bool) {
	why := s.fallback
	s.fallback = ""
	if why == "" || p.bind == nil || p.sent != 0 || !s.canFallBack() || !isSelectLike(p.stmt.query) {
		return false
	}
	s.logger.Debug("pgshim: the copy did not answer a statement; the source does", "reason", why)
	if !transientFallback(why) {
		// The statement's own reason: it stays with the source.
		p.stmt.fallback = why
	}
	p.route = routeDecision{RouteSource, why}
	s.portalToSource(p)
	return true
}

// portalToSource binds on the source a portal the copy could not answer
// (route.go), with the client's own Bind, before anything of its answer was
// sent. The client already has its BindComplete. ok false: the client was
// sent an error.
func (s *session) portalToSource(p *portal) (ok bool) {
	if p.bind == nil {
		return false
	}
	if perr := forwardRefusal(p.stmt.query); perr != nil {
		s.extFail(perr)
		return false
	}
	up, perr := s.prepareSource()
	if perr != nil {
		s.extFail(perr)
		return false
	}
	if perr := s.captureRefusal(up, p.stmt.query); perr != nil {
		// The capture account's gate (#3054), before the statement is
		// parsed there.
		s.extFail(perr)
		return false
	}
	if !s.ensureParsed(up, p.stmt) {
		return false
	}
	if !s.sendBindToSource(up, p.stmt, p.bind) {
		return false
	}
	p.onSource = true
	p.sourcePath = up.sent["search_path"]
	p.cols, p.types, p.rows, p.tag = nil, nil, nil, ""
	return true
}

// textish are the types whose binary form is their text: a column of one is
// read the same whichever of them it is announced as.
var textish = map[uint32]bool{oidText: true, 1043: true, 1042: true, 19: true}

// relayErrorsOnly relays to the client the source's errors and notices only,
// as the source wrote them: the answers to messages the client already got
// the port's answer to.
func (s *session) relayErrorsOnly(up *upstream, done func(pgproto3.BackendMessage) bool) (*pgErr, *errSourceLost) {
	return s.relay(up, relayErrors, done)
}

// endsOn is a done function for a relay that ends on any of the given
// message kinds, or on an error.
func endsOn(kinds ...func(pgproto3.BackendMessage) bool) func(pgproto3.BackendMessage) bool {
	return func(m pgproto3.BackendMessage) bool {
		if _, ok := m.(*pgproto3.ErrorResponse); ok {
			return true
		}
		for _, k := range kinds {
			if k(m) {
				return true
			}
		}
		return false
	}
}

func isParseComplete(m pgproto3.BackendMessage) bool { _, ok := m.(*pgproto3.ParseComplete); return ok }
func isBindComplete(m pgproto3.BackendMessage) bool  { _, ok := m.(*pgproto3.BindComplete); return ok }
func isCloseComplete(m pgproto3.BackendMessage) bool { _, ok := m.(*pgproto3.CloseComplete); return ok }
func isRowShape(m pgproto3.BackendMessage) bool {
	switch m.(type) {
	case *pgproto3.RowDescription, *pgproto3.NoData:
		return true
	}
	return false
}
func endsExecute(m pgproto3.BackendMessage) bool {
	switch m.(type) {
	case *pgproto3.CommandComplete, *pgproto3.PortalSuspended, *pgproto3.EmptyQueryResponse:
		return true
	}
	return false
}

// sourceFailed is a source error in the middle of a batch: the client's
// batch is discarded up to its Sync, as the source discards its own.
func (s *session) sourceFailed(up *upstream) {
	up.failed = true
	s.markFailed()
	s.ext.skip = true
}

// sourceLostExt is a connection to the source lost in a batch.
func (s *session) sourceLostExt(lost *errSourceLost) {
	s.loseSource(lost.why)
	s.extFail(s.lostError(lost))
}

// ensureParsed parses st on the source under its name, once.
//
// Its callers send an extended-protocol message to the source right after
// it, parsed or not: the batch is open there from here, and the client's
// Sync must reach the source (an error there discards everything up to it).
func (s *session) ensureParsed(up *upstream, st *preparedStatement) (ok bool) {
	up.inCycle = true
	if up.stmts[st.name] == st {
		return true
	}
	var msgs []pgproto3.FrontendMessage
	if _, there := up.stmts[st.name]; there && st.name != "" {
		// Another statement of that name, closed here since: gone there too.
		msgs = append(msgs, &pgproto3.Close{ObjectType: 'S', Name: st.name})
	}
	msgs = append(msgs, &pgproto3.Parse{Name: st.name, Query: st.query, ParameterOIDs: st.declaredOIDs}, &pgproto3.Flush{})
	if err := up.send(msgs...); err != nil {
		s.sourceLostExt(&errSourceLost{"it could not be written to: " + err.Error()})
		return false
	}
	want := 1
	if len(msgs) == 3 {
		want = 2
	}
	n := 0
	perr, lost := s.relayErrorsOnly(up, func(m pgproto3.BackendMessage) bool {
		if _, ok := m.(*pgproto3.ErrorResponse); ok {
			return true
		}
		if isParseComplete(m) || isCloseComplete(m) {
			n++
		}
		return n == want
	})
	if lost != nil {
		s.sourceLostExt(lost)
		return false
	}
	if perr != nil {
		delete(up.stmts, st.name)
		s.sourceFailed(up)
		return false
	}
	up.stmts[st.name] = st
	return true
}

// describeOnSource answers Describe of a statement from the source.
func (s *session) describeOnSource(st *preparedStatement) {
	if perr := forwardRefusal(st.query); perr != nil {
		s.extFail(perr)
		return
	}
	up, perr := s.prepareSource()
	if perr != nil {
		s.extFail(perr)
		return
	}
	if perr := s.captureRefusal(up, st.query); perr != nil {
		// The capture account's gate (#3054), before the statement is
		// parsed there.
		s.extFail(perr)
		return
	}
	if !s.ensureParsed(up, st) {
		return
	}
	if err := up.send(&pgproto3.Describe{ObjectType: 'S', Name: st.name}, &pgproto3.Flush{}); err != nil {
		s.sourceLostExt(&errSourceLost{"it could not be written to: " + err.Error()})
		return
	}
	var fields []pgproto3.FieldDescription
	var params []uint32
	noData := false
	perr, lost := s.relay(up, relayClient, endsOn(func(m pgproto3.BackendMessage) bool {
		switch r := m.(type) {
		case *pgproto3.ParameterDescription:
			params = append([]uint32(nil), r.ParameterOIDs...)
		case *pgproto3.RowDescription:
			fields = append([]pgproto3.FieldDescription(nil), r.Fields...)
			return true
		case *pgproto3.NoData:
			noData = true
			return true
		}
		return false
	}))
	if lost != nil {
		s.sourceLostExt(lost)
		return
	}
	if perr != nil {
		s.sourceFailed(up)
		return
	}
	st.describedOnSource = true
	// The client encodes its arguments by the types it was told: an
	// execution the copy answers later reads them by the same ones.
	if len(params) == len(st.paramOIDs) {
		for i, oid := range params {
			if st.paramOIDs[i] == 0 {
				st.paramOIDs[i] = oid
			}
		}
	}
	st.describedNames, st.describedTypes = nil, nil
	switch {
	case noData:
		st.described = noRows
	default:
		st.described = len(fields)
		for _, f := range fields {
			st.describedNames = append(st.describedNames, string(f.Name))
			st.describedTypes = append(st.describedTypes, colType{oid: f.DataTypeOID, size: f.DataTypeSize, typmod: f.TypeModifier})
		}
	}
}

// bindOnSource answers Bind of a statement that runs on the source.
func (s *session) bindOnSource(st *preparedStatement, m *pgproto3.Bind) {
	if perr := forwardRefusal(st.query); perr != nil {
		s.extFail(perr)
		return
	}
	up, perr := s.prepareSource()
	if perr != nil {
		s.extFail(perr)
		return
	}
	if perr := s.captureRefusal(up, st.query); perr != nil {
		// The capture account's gate (#3054), before the statement is
		// parsed there.
		s.extFail(perr)
		return
	}
	if !s.ensureParsed(up, st) {
		return
	}
	if !s.sendBindToSource(up, st, m) {
		return
	}
	if s.ext.portals == nil {
		s.ext.portals = map[string]*portal{}
	}
	s.dropPortal(m.DestinationPortal)
	s.ext.portals[m.DestinationPortal] = &portal{
		name: m.DestinationPortal, stmt: st, onSource: true, sourcePath: up.sent["search_path"],
		resultFormats: append([]int16(nil), m.ResultFormatCodes...),
		route:         s.stmtRoute(st),
	}
	s.be.Send(&pgproto3.BindComplete{})
}

// sendBindToSource sends the client's Bind of st to the source, parsed
// there already, and reads its answer. ok false: the client was sent an
// error, and its batch is discarded up to its Sync.
func (s *session) sendBindToSource(up *upstream, st *preparedStatement, m *pgproto3.Bind) (ok bool) {
	msgs := []pgproto3.FrontendMessage{m}
	// The client decodes the rows by the Describe it got. One the port gave
	// from the copy is checked against what the source will send.
	check := st.described != -1 && !st.describedOnSource
	if check {
		msgs = append(msgs, &pgproto3.Describe{ObjectType: 'P', Name: m.DestinationPortal})
	}
	msgs = append(msgs, &pgproto3.Flush{})
	if err := up.send(msgs...); err != nil {
		s.sourceLostExt(&errSourceLost{"it could not be written to: " + err.Error()})
		return false
	}
	var fields []pgproto3.FieldDescription
	noData := false
	perr, lost := s.relayErrorsOnly(up, endsOn(func(msg pgproto3.BackendMessage) bool {
		if !check {
			return isBindComplete(msg)
		}
		switch r := msg.(type) {
		case *pgproto3.RowDescription:
			fields = append([]pgproto3.FieldDescription(nil), r.Fields...)
			return true
		case *pgproto3.NoData:
			noData = true
			return true
		}
		return false
	}))
	if lost != nil {
		s.sourceLostExt(lost)
		return false
	}
	if perr != nil {
		s.sourceFailed(up)
		return false
	}
	up.portals[m.DestinationPortal] = true
	if check && !sameShape(st, fields, noData, m.ResultFormatCodes) {
		// PostgreSQL's words, which drivers prepare the statement again on.
		s.extFail(&pgErr{"0A000", "cached plan must not change result type"})
		return false
	}
	return true
}

// sameShape reports whether the source's columns for a portal read the way
// the copy's Describe told the client they would: as many, and each one in
// binary of the same type (a column in text is read from its text whatever
// it is announced as).
func sameShape(st *preparedStatement, fields []pgproto3.FieldDescription, noData bool, formats []int16) bool {
	if st.described == noRows || noData {
		return st.described == noRows && noData
	}
	if len(fields) != st.described {
		return false
	}
	for i, f := range fields {
		if formatAt(formats, len(fields), i) != pgproto3.BinaryFormat {
			continue
		}
		want := typeAt(st.describedTypes, i).oid
		if want != f.DataTypeOID && !(textish[want] && textish[f.DataTypeOID]) {
			return false
		}
	}
	return true
}

// describePortalOnSource answers Describe of a portal bound on the source.
func (s *session) describePortalOnSource(p *portal) {
	s.leaveBarredSource()
	up := s.up
	if up == nil {
		s.extFail(&pgErr{"08006", "the connection to the source was lost (" + s.upLost + "); reconnect"})
		return
	}
	up.inCycle = true
	if err := up.send(&pgproto3.Describe{ObjectType: 'P', Name: p.name}, &pgproto3.Flush{}); err != nil {
		s.sourceLostExt(&errSourceLost{"it could not be written to: " + err.Error()})
		return
	}
	perr, lost := s.relay(up, relayClient, endsOn(isRowShape))
	if lost != nil {
		s.sourceLostExt(lost)
		return
	}
	if perr != nil {
		s.sourceFailed(up)
	}
}

// executeOnSource answers Execute of a portal bound on the source.
func (s *session) executeOnSource(p *portal, maxRows uint32) {
	s.leaveBarredSource()
	up := s.up
	if up == nil {
		s.extFail(&pgErr{"08006", "the connection to the source was lost (" + s.upLost + "); reconnect"})
		return
	}
	up.inCycle = true
	ctx, done := s.statementContext()
	defer done()
	up.ctx = ctx
	defer func() { up.ctx = nil }()
	// Which tables it reads is confirmed once the batch is over
	// (syncOnSource): until then what the source sends marks every table.
	if proof := s.proveReads(p.stmt.query); proof != nil && proof.path == p.sourcePath {
		// Only for a portal bound under the search_path of now: the source
		// resolved its names when it was bound.
		s.proving = proof
	}
	defer func() { s.proving = nil }()
	if err := up.send(&pgproto3.Execute{Portal: p.name, MaxRows: maxRows}, &pgproto3.Flush{}); err != nil {
		s.sourceLostExt(&errSourceLost{"it could not be written to: " + err.Error()})
		return
	}
	perr, lost := s.relay(up, relayClient, endsOn(endsExecute))
	if lost != nil {
		if perr != nil {
			// The source's error was sent: one answer, not two.
			s.loseSource(lost.why)
			s.markFailed()
			s.ext.skip = true
			return
		}
		s.sourceLostExt(lost)
		return
	}
	if perr != nil {
		s.sourceFailed(up)
		return
	}
	s.tookSavepointStmt(p.stmt.query)
	s.afterSource(up)
}

// closeOnSource closes on the source what the client closed here: a
// statement parsed there, or a portal bound there.
func (s *session) closeOnSource(kind byte, name string, st *preparedStatement) {
	up := s.up
	if up == nil {
		return
	}
	switch kind {
	case 'S':
		if st == nil || up.stmts[name] != st {
			return
		}
		delete(up.stmts, name)
	case 'P':
		if !up.portals[name] {
			return
		}
		delete(up.portals, name)
	}
	if up.failed {
		// Discarded there until the Sync anyway.
		return
	}
	up.inCycle = true
	if err := up.send(&pgproto3.Close{ObjectType: kind, Name: name}, &pgproto3.Flush{}); err != nil {
		s.sourceLostExt(&errSourceLost{"it could not be written to: " + err.Error()})
		return
	}
	perr, lost := s.relayErrorsOnly(up, endsOn(isCloseComplete))
	if lost != nil {
		s.sourceLostExt(lost)
		return
	}
	if perr != nil {
		s.sourceFailed(up)
	}
}

// syncOnSource sends the client's Sync on when its batch touched the source,
// and takes the transaction status the source reports.
func (s *session) syncOnSource() {
	up := s.up
	if up == nil || !up.inCycle {
		return
	}
	tx, lost := s.syncCycle(up, relayClient)
	if lost != nil {
		s.loseSource(lost.why)
		s.errorResponse("08006", s.lostError(lost).msg)
		return
	}
	s.followSourceStatus(up, tx)
	if !up.unsafe {
		if lost := s.confirmPending(up); lost != nil {
			s.loseSource(lost.why)
			s.afterSource(up)
			return
		}
	} else if len(s.src.pending) > 0 {
		s.src.markAll()
	}
	if s.src.any() && !s.knownValid && s.bound.CopyWait != nil && !up.unsafe {
		// The batch sent the client the source's rows: where the source
		// stands now, once the batch is over (#2985, knownLSN).
		if _, perr, lost := s.positionAfter(up); lost != nil {
			s.loseSource(lost.why)
		} else if perr != nil {
			s.logger.Debug("pgshim: the source did not say its WAL position after a batch", "code", perr.code, "err", perr.msg)
		}
	}
	s.afterSource(up)
}

// positionAfter reads the source's WAL position on up, outside any batch,
// and keeps it as the connection's known one.
func (s *session) positionAfter(up *upstream) (uint64, *pgErr, *errSourceLost) {
	cell, perr, lost := s.cellOnSource(up, sourceWALPositionSQL)
	if lost != nil || perr != nil {
		return 0, perr, lost
	}
	lsn, ok := parseLSN(string(cell))
	if !ok {
		return 0, &pgErr{"XX000", fmt.Sprintf("%q is not a WAL position", cell)}, nil
	}
	s.knownLSN, s.knownValid = lsn, true
	return lsn, nil, nil
}
