package pgshim

import (
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
	return s.forwarding() && s.toSource(s.classify(st.probeText()))
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
	if !s.ensureParsed(up, st) {
		return
	}
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
		return
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
		return
	}
	if perr != nil {
		s.sourceFailed(up)
		return
	}
	up.portals[m.DestinationPortal] = true
	if check && !sameShape(st, fields, noData, m.ResultFormatCodes) {
		// PostgreSQL's words, which drivers prepare the statement again on.
		s.extFail(&pgErr{"0A000", "cached plan must not change result type"})
		return
	}
	if s.ext.portals == nil {
		s.ext.portals = map[string]*portal{}
	}
	s.dropPortal(m.DestinationPortal)
	s.ext.portals[m.DestinationPortal] = &portal{
		name: m.DestinationPortal, stmt: st, onSource: true,
		resultFormats: append([]int16(nil), m.ResultFormatCodes...),
	}
	s.be.Send(&pgproto3.BindComplete{})
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
	s.afterSource(up)
}
