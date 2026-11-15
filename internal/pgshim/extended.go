package pgshim

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The extended query protocol (#2767): Parse, Bind, Describe, Execute, Close,
// Sync and Flush, which JDBC, pgx in its default mode, psycopg 3 and
// node-postgres with parameters speak. Every statement still runs the way a
// simple query does (session.run, the same refusals with the same codes):
// what this adds is statements and portals with names, arguments written into
// the statement (params.go), a statement's columns said before it runs, an
// Execute that hands its rows over a few at a time, and PostgreSQL's error
// rule: after an error everything up to the next Sync is discarded, and Sync
// says the transaction status.

// Caps on what one connection keeps open. PostgreSQL has none, but there each
// is a plan; here an open portal holds its whole answer in memory.
const (
	maxStatements = 1000
	maxPortals    = 64
	// flushAfterBytes: an Execute that sends more than this flushes at its
	// end instead of keeping the rows in the buffer until the client's Sync.
	flushAfterBytes = 1 << 20
)

// maxStatementBytes is the most text the prepared statements of one
// connection may hold together, and maxPortalBytes the most answer its open
// portals may: the port runs in the process that may also capture, so one
// client must not be able to fill its memory. Variables so a test can lower
// them.
var (
	maxStatementBytes = 64 << 20
	maxPortalBytes    = 256 << 20
)

// preparedStatement is a statement Parse made.
type preparedStatement struct {
	name  string
	query string
	tmpl  template
	// paramOIDs is one type per parameter: the one the client declared, or
	// 0 when it declared none.
	paramOIDs []uint32
	// rd is who reads the statement once its arguments are written in.
	rd reader
	// describedNames are the columns a Describe of the statement named,
	// which a time-travel execution is put into (runPortal).
	describedNames []string
	// describedTypes are the types a Describe of a statement for the copy
	// announced its columns with (nil for time travel, whose columns are
	// text): the client decodes every execution's cells by them.
	describedTypes []colType
	// described is the column count a Describe of the statement told the
	// client (-1 for none, and for no rows: noRows), which every portal
	// bound from it must keep: the client decodes the rows by that answer.
	described int
}

// portal is a statement with its arguments, ready to run.
type portal struct {
	name          string
	stmt          *preparedStatement
	text          string
	resultFormats []int16
	// ran is set once the statement ran; what it answered is below.
	ran   bool
	empty bool
	cols  []string // nil: the statement returns no rows
	types []colType
	rows  [][][]byte
	tag   string
	// sent is how many of rows earlier Executes handed over.
	sent int
	// bytes is about how much memory rows hold.
	bytes int
	// held is what rows count in the port's result budget while the portal
	// keeps them (resultbudget.go): from the run until the last of them is
	// sent, or the portal ends.
	held int64
}

// extState is the extended protocol's state on one connection.
type extState struct {
	stmts   map[string]*preparedStatement
	portals map[string]*portal
	// skip is set by an error: messages are discarded up to the next Sync.
	skip bool
}

// noRows is preparedStatement.described for a statement said to return no
// rows (NoData).
const noRows = -2

// statementBytes is the text the named statements hold.
func (e *extState) statementBytes() int {
	n := 0
	for name, st := range e.stmts {
		if name != "" {
			n += len(st.query)
		}
	}
	return n
}

// portalBytes is the answer the open portals hold, but skip's.
func (e *extState) portalBytes(skip *portal) int {
	n := 0
	for _, p := range e.portals {
		if p != skip {
			n += p.bytes
		}
	}
	return n
}

// dropUnnamed ends the unnamed statement and portal.
func (s *session) dropUnnamed() {
	delete(s.ext.stmts, "")
	s.dropPortal("")
}

// dropPortals ends every portal: the end of a transaction, explicit or the
// implicit one a Sync closes, and of the connection.
func (s *session) dropPortals() {
	for name := range s.ext.portals {
		s.dropPortal(name)
	}
}

// dropPortal ends one portal, and gives back what its kept rows count in the
// port's result budget. Every way a portal ends goes through here.
func (s *session) dropPortal(name string) {
	p, ok := s.ext.portals[name]
	if !ok {
		return
	}
	delete(s.ext.portals, name)
	s.releasePortal(p)
}

// releasePortal gives back what p's rows count in the budget and lets them
// go.
func (s *session) releasePortal(p *portal) {
	if p.held != 0 && s.budget != nil {
		s.budget.held.Add(-p.held)
	}
	// An Execute after this one finds the portal at its end and sends no row.
	p.held, p.rows, p.sent = 0, nil, 0
}

// extFail sends an extended-protocol error and discards the rest of the
// client's messages up to its Sync.
func (s *session) extFail(perr *pgErr) {
	s.fail(perr)
	s.ext.skip = true
}

// handleParse answers Parse.
func (s *session) handleParse(m *pgproto3.Parse) {
	if perr := s.abortedRefusal(m.Query); perr != nil {
		s.extFail(perr)
		return
	}
	if m.Name != "" {
		if _, ok := s.ext.stmts[m.Name]; ok {
			s.extFail(&pgErr{"42P05", fmt.Sprintf("prepared statement %q already exists", m.Name)})
			return
		}
		if s.ext.statementBytes()+len(m.Query) > maxStatementBytes {
			s.extFail(&pgErr{"54000", fmt.Sprintf("this connection's prepared statements already hold close to %d MB of text, the most it keeps; close some", maxStatementBytes>>20)})
			return
		}
		if len(s.ext.stmts) >= maxStatements {
			s.extFail(&pgErr{"54000", fmt.Sprintf("this connection already has %d prepared statements, the most it keeps; close some", maxStatements)})
			return
		}
	}
	tmpl, perr := splitParams(m.Query)
	if perr != nil {
		s.extFail(perr)
		return
	}
	n := max(tmpl.highest, len(m.ParameterOIDs))
	if n > maxParams {
		s.extFail(&pgErr{"54000", fmt.Sprintf("a statement takes at most %d parameters", maxParams)})
		return
	}
	oids := make([]uint32, n)
	copy(oids, m.ParameterOIDs)
	st := &preparedStatement{name: m.Name, query: m.Query, tmpl: tmpl, paramOIDs: oids, described: -1}
	// Which parser reads the statement decides how an argument is written
	// into it, and the arguments do not exist yet: ask with a stand-in.
	if s.classify(st.probeText()).timeTravel {
		st.rd = readTimeTravel
	}
	if s.ext.stmts == nil {
		s.ext.stmts = map[string]*preparedStatement{}
	}
	s.ext.stmts[m.Name] = st
	s.be.Send(&pgproto3.ParseComplete{})
}

// probeArgument stands in for every placeholder when Parse asks which parser
// reads a statement: a quoted string the time-travel parser takes both as a
// key and as the moment of an AS OF.
const probeArgument = "'1 second ago'"

func (st *preparedStatement) probeText() string {
	var b strings.Builder
	for i, part := range st.tmpl.parts {
		b.WriteString(part)
		if i < len(st.tmpl.refs) {
			b.WriteString(" " + probeArgument + " ")
		}
	}
	return b.String()
}

// handleBind answers Bind.
func (s *session) handleBind(m *pgproto3.Bind) {
	st, ok := s.ext.stmts[m.PreparedStatement]
	if !ok {
		s.extFail(&pgErr{"26000", fmt.Sprintf("prepared statement %q does not exist", m.PreparedStatement)})
		return
	}
	if perr := s.abortedRefusal(st.query); perr != nil {
		s.extFail(perr)
		return
	}
	if len(m.Parameters) != len(st.paramOIDs) {
		s.extFail(&pgErr{"08P01", fmt.Sprintf("bind message supplies %d parameters, but prepared statement %q requires %d",
			len(m.Parameters), st.name, len(st.paramOIDs))})
		return
	}
	nf := len(m.ParameterFormatCodes)
	if nf != 0 && nf != 1 && nf != len(m.Parameters) {
		s.extFail(&pgErr{"08P01", fmt.Sprintf("bind message has %d parameter formats but %d parameters", nf, len(m.Parameters))})
		return
	}
	for _, f := range m.ResultFormatCodes {
		if f != 0 && f != 1 {
			s.extFail(&pgErr{"22023", fmt.Sprintf("unsupported format code: %d", f)})
			return
		}
	}
	if m.DestinationPortal != "" {
		if _, ok := s.ext.portals[m.DestinationPortal]; ok {
			s.extFail(&pgErr{"42P03", fmt.Sprintf("portal %q already exists", m.DestinationPortal)})
			return
		}
		if len(s.ext.portals) >= maxPortals {
			s.extFail(&pgErr{"54000", fmt.Sprintf("this connection already has %d open portals, the most it keeps; close some", maxPortals)})
			return
		}
	}
	args := make([]arg, len(m.Parameters))
	for i, raw := range m.Parameters {
		var format int16
		switch nf {
		case 1:
			format = m.ParameterFormatCodes[0]
		case len(m.Parameters):
			format = m.ParameterFormatCodes[i]
		}
		a, perr := decodeArg(i, st.paramOIDs[i], format, raw)
		if perr != nil {
			s.extFail(perr)
			return
		}
		args[i] = a
	}
	text, perr := st.tmpl.fill(args, st.rd)
	if perr != nil {
		s.extFail(perr)
		return
	}
	if s.classify(text).timeTravel != (st.rd == readTimeTravel) {
		// The arguments were written for one parser and the finished text
		// would be read by the other. Never run text under the parser it
		// was not written for.
		s.extFail(&pgErr{"22023", "an argument changes how this prepared statement is read; write the value in the statement's text"})
		return
	}
	if s.ext.portals == nil {
		s.ext.portals = map[string]*portal{}
	}
	// The unnamed portal a new Bind replaces gives back what it kept.
	s.dropPortal(m.DestinationPortal)
	s.ext.portals[m.DestinationPortal] = &portal{
		name: m.DestinationPortal, stmt: st, text: text,
		// The message's buffer is reused for the next one.
		resultFormats: append([]int16(nil), m.ResultFormatCodes...),
	}
	s.be.Send(&pgproto3.BindComplete{})
}

// handleDescribe answers Describe of a statement ('S') or a portal ('P').
func (s *session) handleDescribe(m *pgproto3.Describe) {
	switch m.ObjectType {
	case 'S':
		st, ok := s.ext.stmts[m.Name]
		if !ok {
			s.extFail(&pgErr{"26000", fmt.Sprintf("prepared statement %q does not exist", m.Name)})
			return
		}
		cols, types, held, perr := s.describeStatement(st)
		if perr != nil {
			// What fails here PostgreSQL reports at Parse, where the
			// statement then never exists. It goes, so that a client that
			// prepares it again under the same name (pgx's statement cache
			// does, after an error) is not told it already exists.
			s.closeStatement(m.Name)
			s.extFail(perr)
			return
		}
		s.be.Send(&pgproto3.ParameterDescription{ParameterOIDs: st.paramOIDs})
		st.described, st.describedTypes = -1, nil
		if cols == nil {
			if held {
				st.described = noRows
			}
			s.be.Send(&pgproto3.NoData{})
			return
		}
		st.described, st.describedNames, st.describedTypes = len(cols), cols, types
		s.rowDescription(cols, types, nil)
	case 'P':
		p, ok := s.ext.portals[m.Name]
		if !ok {
			s.extFail(&pgErr{"34000", fmt.Sprintf("portal %q does not exist", m.Name)})
			return
		}
		if !p.ran {
			// A statement that returns no rows says so without running: a
			// BEGIN described must not open its block before it executes.
			if c := s.classify(p.text); isEmptyQuery(p.text) || c.kind == kindProbe {
				s.be.Send(&pgproto3.NoData{})
				return
			}
		}
		if perr := s.runPortal(p); perr != nil {
			s.extFail(perr)
			return
		}
		if p.cols == nil {
			s.be.Send(&pgproto3.NoData{})
			return
		}
		if perr := checkResultFormats(p); perr != nil {
			s.extFail(perr)
			return
		}
		s.rowDescription(p.cols, p.types, p.resultFormats)
	default:
		s.extFail(&pgErr{"08P01", fmt.Sprintf("invalid DESCRIBE message subtype %d", m.ObjectType)})
	}
}

// describeStatement says, before any argument exists, what a statement
// returns: its columns, or nil for none. It reads no row, and it meets every
// gate the statement would. held is false when the answer is not one the
// statement's executions are held to (a statement the copy could not
// describe, see describeFree).
func (s *session) describeStatement(st *preparedStatement) (cols []string, types []colType, held bool, perr *pgErr) {
	if perr := s.abortedRefusal(st.query); perr != nil {
		return nil, nil, false, perr
	}
	if isEmptyQuery(st.query) {
		return nil, nil, true, nil
	}
	c := s.classify(st.probeText())
	switch c.kind {
	case kindRefused:
		return nil, nil, false, c.perr
	case kindProbe:
		// A session statement returns no rows; one that is refused is
		// refused when it executes, with its own arguments.
		return nil, nil, true, nil
	case kindTimeTravel:
		// The gates first: the table's columns are something said about it.
		if perr := s.timeTravelGates(c.q, false); perr != nil {
			return nil, nil, false, perr
		}
		cols := s.h.ColumnsFor(nil, c.q)
		if len(cols) == 0 {
			// No schema snapshot names the table's columns: there is
			// nothing true to describe, and an answer of no columns would
			// hold every execution to it.
			return nil, nil, false, &pgErr{"55000", fmt.Sprintf(
				"the columns of %s.%s cannot be described: no schema snapshot of the table is recorded; run the statement with the simple query protocol",
				c.q.Schema, c.q.Table)}
		}
		// Text, as the simple protocol announces them.
		return cols, nil, true, nil
	}
	return s.describeFree(st)
}

// describeFree finds the columns of a statement for the copy with NULL for
// every argument, through the executor the connection is bound to, so the
// copy's own rules (its views, what a server hides) hold for it as they do
// for the statement. It asks with the statement inside a subquery that
// returns no row, which costs the copy nothing, and never runs the statement
// itself: a Describe reads no row.
//
// A statement that cannot stand in a subquery (a write, a syntax error, SHOW)
// is described as returning no rows (NoData), and nothing is held against
// its Execute: that runs the statement as a simple query runs it, and
// answers the error a simple query gets, with its code, where the subquery's
// error would name another statement than the client's. described returns
// false then.
func (s *session) describeFree(st *preparedStatement) (cols []string, types []colType, described bool, perr *pgErr) {
	got, perr, err := s.describeWith(st, "")
	if perr != nil {
		return nil, nil, false, perr
	}
	if err == nil {
		types = columnTypes(sandboxTypes(got))
		if perr := s.untypedArgumentsAsText(st, types); perr != nil {
			return nil, nil, false, perr
		}
		return columnNames(got), types, true, nil
	}
	perr = freeError(err, s.logger)
	if strings.HasPrefix(perr.code, "42") || perr.code == "0A000" {
		// The statement's own shape or text: it cannot stand in the
		// subquery, and its Execute answers the error a simple query gets.
		return nil, nil, false, nil
	}
	// Anything else (the copy busy, behind, timed out, a worker that
	// failed) is the moment's, not the statement's: the Describe fails with
	// it, so the client does not keep "no rows" for a statement that has
	// them, and tries again.
	s.logger.Warn("pgshim: a statement could not be described", "code", perr.code, "err", err)
	return nil, nil, false, perr
}

// describeWith asks the copy for the columns of st with NULL for every
// argument, in a subquery that returns no row. untyped is the type a NULL
// stands as for an argument whose type the client did not declare: "" for
// none (the engine's own, an INTEGER in the answer). perr is a statement
// whose arguments cannot be written in, err what the copy answered.
func (s *session) describeWith(st *preparedStatement, untyped string) (cols []sqlsandbox.Column, perr *pgErr, err error) {
	nulls := make([]arg, len(st.paramOIDs))
	for i, oid := range st.paramOIDs {
		cast := typedCast[oid]
		if cast == "" {
			cast = untyped
		}
		nulls[i] = arg{null: true, cast: cast}
	}
	text, perr := st.tmpl.fill(nulls, readCopy)
	if perr != nil {
		return nil, perr, nil
	}
	body := strings.TrimRight(text, " \t\r\n;")
	// On lines of their own: a statement that ends in a -- comment must not
	// comment out the closing parenthesis.
	cols, err = s.columnsOf("SELECT * FROM (\n" + body + "\n) AS dbtrail_describe LIMIT 0")
	return cols, nil, err
}

// untypedArgumentsAsText announces as text each column whose type comes from
// an argument the client gave no type. Such an argument is described as a
// bare NULL, which the engine answers as an INTEGER, and runs as the string
// literal the client sent, which it answers as text: SELECT $1 would be
// announced as an integer and answer "hello". PostgreSQL reads an argument
// of unknown type there as text, and so does this port: the statement is
// described a second time with each such argument a NULL of text, and a
// column whose type changes with it is text. Only asked when it can matter:
// an argument without a type, and a column the first answer typed INTEGER or
// a list of INTEGER (SELECT [$1]). When the second description is refused
// for the statement's own shape or text (the text NULL meets a comparison
// with a number) the first answer stands, and an execution whose cells do
// not read as it is refused then (conformToDescribed). Any other failure is
// the moment's, as in describeFree: the Describe fails with it, rather than
// keep an INTEGER that an execution with text would be refused against for
// a reason that is not its own.
func (s *session) untypedArgumentsAsText(st *preparedStatement, types []colType) *pgErr {
	untyped := false
	for _, oid := range st.paramOIDs {
		if typedCast[oid] == "" {
			untyped = true
		}
	}
	integer := false
	for _, t := range types {
		if t.oid == oidInt4 || t.oid == arrayOIDs[oidInt4] {
			integer = true
		}
	}
	if !untyped || !integer {
		return nil
	}
	again, perr, err := s.describeWith(st, "VARCHAR")
	if perr != nil {
		return nil
	}
	if err != nil {
		perr = freeError(err, s.logger)
		if strings.HasPrefix(perr.code, "42") || perr.code == "0A000" {
			return nil
		}
		s.logger.Warn("pgshim: a statement could not be described a second time", "code", perr.code, "err", err)
		return perr
	}
	if len(again) != len(types) {
		return nil
	}
	for i, c := range again {
		if columnType(c.Type) != types[i] {
			types[i] = textColumn
		}
	}
	return nil
}

// columnsOf runs text on the copy, counted and recorded as every statement
// the port runs is (runFree), and returns its columns.
func (s *session) columnsOf(text string) ([]sqlsandbox.Column, error) {
	started := time.Now()
	res, err := s.bound.Run(s.ctx, text)
	if s.bound.Answered != nil && s.ctx.Err() == nil {
		s.bound.Answered(time.Since(started), err != nil)
	}
	if res.Spool != nil {
		_ = res.Spool.Close()
	}
	if err != nil {
		return nil, err
	}
	s.recordFree(text, res)
	return res.Columns, nil
}

func sandboxTypes(cols []sqlsandbox.Column) []string {
	types := make([]string, len(cols))
	for i, c := range cols {
		types[i] = c.Type
	}
	return types
}

func columnNames(cols []sqlsandbox.Column) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

// runPortal runs a portal's statement once, the way a simple query runs it,
// and keeps its answer for the Executes that hand it over.
func (s *session) runPortal(p *portal) *pgErr {
	if p.ran {
		return nil
	}
	p.ran = true
	if isEmptyQuery(p.text) {
		p.empty = true
		return nil
	}
	cols, types, rows, tag, perr := s.run(p.text)
	// What the statement counted in the port's budget is the portal's from
	// here: the connection's next statement must not give it back while the
	// portal still keeps these rows (an Execute with a row limit).
	held := s.heldResult
	s.heldResult = 0
	giveBack := func() {
		if held != 0 && s.budget != nil {
			s.budget.held.Add(-held)
		}
	}
	if perr != nil {
		giveBack()
		return perr
	}
	if p.stmt.rd == readTimeTravel && cols != nil && p.stmt.described >= 0 {
		cols, rows = s.intoDescribed(p.stmt.describedNames, cols, rows)
	}
	if d := p.stmt.described; cols != nil && d != -1 {
		if d == noRows || len(cols) != d {
			// The client decodes the rows by what Describe told it; rows of
			// another shape would be read into the wrong columns. The words
			// are PostgreSQL's, which drivers recognize and prepare again on.
			giveBack()
			return &pgErr{"0A000", "cached plan must not change result type"}
		}
	}
	if cols != nil && p.stmt.describedTypes != nil {
		if types, perr = conformToDescribed(p.stmt.describedTypes, types, rows); perr != nil {
			giveBack()
			return perr
		}
	}
	// In the formats the Bind asked for before anything is counted below: a
	// cell's binary form can take more bytes than its text (a numeric 1 is
	// one byte of text and ten of binary), and what the portal keeps is what
	// it will send.
	if perr := s.encodeResult(cols, types, p.resultFormats, rows); perr != nil {
		giveBack()
		return perr
	}
	// The statement was counted in the port's budget by its text; what the
	// binary form adds is counted now, so the budget holds what is really
	// kept. By the budget's own rule: the port holds at most its budget plus
	// one result, and the binary form is part of this one, so it is refused
	// only when the OTHER results already hold the budget (what this one
	// counted is taken out of the comparison), as holdResult refuses one.
	if b := s.budget; b != nil && held != 0 {
		if grown := resultBytes(rows) - held; grown > 0 {
			for {
				cur := b.held.Load()
				if cur-held >= b.max {
					giveBack()
					return b.refuse(s.logger)
				}
				if b.held.CompareAndSwap(cur, cur+grown) {
					break
				}
			}
			b.charged.Add(grown)
			held += grown
		}
	}
	bytes := 0
	for _, r := range rows {
		for _, c := range r {
			bytes += len(c) + 24
		}
	}
	if s.ext.portalBytes(p)+bytes > maxPortalBytes {
		giveBack()
		return &pgErr{"54000", fmt.Sprintf("this connection's open portals would hold more than %d MB of answers, the most it keeps; fetch or close the open ones", maxPortalBytes>>20)}
	}
	p.cols, p.types, p.rows, p.tag, p.bytes, p.held = cols, types, rows, tag, bytes, held
	return nil
}

// conformToDescribed holds an execution's columns to the types its
// statement's Describe announced, which is how the client decodes them (pgx
// decodes by the statement's description and never asks for the portal's).
// The copy can answer an execution with another type than its description:
// an argument the description stood in for with a NULL can change an
// expression's type (SELECT $1 + 1 with $1 a bigint). A cell of another type
// is kept when its text reads as the announced type, the way PostgreSQL
// reads a value as the type of its column; when one does not, the execution
// is refused with PostgreSQL's words, and no cell is sent that the client
// would read as another value.
func conformToDescribed(described, types []colType, rows [][][]byte) ([]colType, *pgErr) {
	out := make([]colType, len(described))
	for i, want := range described {
		out[i] = want
		if typeAt(types, i) == want || want.oid == oidText {
			continue
		}
		for _, r := range rows {
			if i >= len(r) || r[i] == nil {
				continue
			}
			if _, err := binaryCell(want, r[i]); err != nil {
				return nil, &pgErr{"0A000", "cached plan must not change result type"}
			}
		}
	}
	return out, nil
}

// encodeResult puts rows in the result formats a Bind asked for: a column
// asked for in binary gets each cell's binary form (binaryCell). A cell that
// has none is refused, before any row is sent, rather than sent in a format
// the client did not ask for: a client decodes by the format it asked, not
// by the one it is told. Formats that do not fit the columns are left for
// checkResultFormats to refuse.
func (s *session) encodeResult(cols []string, types []colType, formats []int16, rows [][][]byte) *pgErr {
	n := len(cols)
	if cols == nil || (len(formats) > 1 && len(formats) != n) {
		return nil
	}
	binaryAt := make([]bool, n)
	some := false
	for i := range n {
		if formatAt(formats, n, i) == pgproto3.BinaryFormat && typeAt(types, i).oid != oidText {
			binaryAt[i], some = true, true
		}
	}
	if !some {
		return nil
	}
	for _, r := range rows {
		for i, c := range r {
			if i >= n || !binaryAt[i] || c == nil {
				continue
			}
			b, err := binaryCell(typeAt(types, i), c)
			if err != nil {
				// A cell that does not read as its own column's type: the
				// copy's text and the type announced for it disagree. Logged
				// without the value, which is the client's data.
				s.logger.Warn("pgshim: a cell has no binary form for its column's type", "column", cols[i], "oid", typeAt(types, i).oid)
				return &pgErr{"0A000", fmt.Sprintf(
					"column %q cannot be sent in binary format: %v; ask for it in text format", cols[i], err)}
			}
			r[i] = b
		}
	}
	return nil
}

// intoDescribed puts a time-travel row into the columns its statement's
// Describe named, by name, which is how the client reads it. A row as it was
// can carry other columns than the table has now (renamed, dropped, added
// since): one the Describe named and the row lacks is NULL, as on the simple
// protocol, and one the row has that the Describe did not name cannot be
// sent, which a notice says.
func (s *session) intoDescribed(names, cols []string, rows [][][]byte) ([]string, [][][]byte) {
	at := make(map[string]int, len(cols))
	for i, c := range cols {
		at[c] = i
	}
	var left []string
	for _, c := range cols {
		if !slices.Contains(names, c) {
			left = append(left, c)
		}
	}
	if len(left) > 0 {
		s.notice(fmt.Sprintf("the row as it was has column(s) %s, which the table no longer has and this prepared statement was described without; run it with the simple query protocol to see them",
			strings.Join(left, ", ")))
	}
	out := make([][][]byte, len(rows))
	for r, row := range rows {
		cells := make([][]byte, len(names))
		for i, n := range names {
			if j, ok := at[n]; ok {
				cells[i] = row[j]
			}
		}
		out[r] = cells
	}
	return names, out
}

// checkResultFormats refuses a Bind whose result formats cannot apply to the
// statement's columns: none, one for all, or one per column.
func checkResultFormats(p *portal) *pgErr {
	if n := len(p.resultFormats); n > 1 && n != len(p.cols) {
		return &pgErr{"08P01", fmt.Sprintf("bind message has %d result formats but query has %d columns", n, len(p.cols))}
	}
	return nil
}

// handleExecute answers Execute: the portal's rows, at most maxRows of them
// when maxRows is not 0, then PortalSuspended if some are left, or the
// CommandComplete that ends it.
func (s *session) handleExecute(m *pgproto3.Execute) {
	p, ok := s.ext.portals[m.Portal]
	if !ok {
		if s.txStatus() == 'E' {
			// The failed block's portals were let go at its Sync; to the
			// client they are refused as on PostgreSQL, for the block.
			s.extFail(s.abortedRefusal(""))
			return
		}
		s.extFail(&pgErr{"34000", fmt.Sprintf("portal %q does not exist", m.Portal)})
		return
	}
	// Asked here too, not only when the statement runs: a portal that ran
	// before its block failed hands over nothing more.
	if perr := s.abortedRefusal(p.stmt.query); perr != nil {
		s.extFail(perr)
		return
	}
	if perr := s.runPortal(p); perr != nil {
		s.extFail(perr)
		return
	}
	switch {
	case p.empty:
		s.be.Send(&pgproto3.EmptyQueryResponse{})
		return
	case p.cols == nil:
		s.commandComplete(p.tag)
		return
	}
	if perr := checkResultFormats(p); perr != nil {
		s.extFail(perr)
		return
	}
	left := len(p.rows) - p.sent
	n := left
	if m.MaxRows > 0 && uint64(m.MaxRows) < uint64(left) {
		n = int(m.MaxRows)
	}
	sentBytes := 0
	for _, r := range p.rows[p.sent : p.sent+n] {
		s.dataRow(r)
		for _, c := range r {
			sentBytes += len(c)
		}
	}
	p.sent += n
	if sentBytes > flushAfterBytes {
		// A write failure here is seen by the next read, which ends the
		// connection.
		defer func() { _ = s.be.Flush() }()
	}
	if m.MaxRows > 0 && uint64(n) == uint64(m.MaxRows) {
		// PostgreSQL stops at the limit without looking past it: a portal
		// whose last row the limit reached exactly is suspended too, and
		// the next Execute ends it with no row.
		s.be.Send(&pgproto3.PortalSuspended{})
		return
	}
	// Every row is sent: what they counted in the budget is given back once
	// they are written (at the flush that follows), as on the simple path.
	defer s.releasePortal(p)
	// The count is this Execute's rows, as PostgreSQL counts them.
	word, _, _ := strings.Cut(p.tag, " ")
	s.commandComplete(word + " " + strconv.Itoa(n))
}

// handleClose answers Close of a statement ('S') or a portal ('P'). Closing
// one that does not exist is not an error. Closing a statement closes the
// portals made from it.
func (s *session) handleClose(m *pgproto3.Close) {
	switch m.ObjectType {
	case 'S':
		s.closeStatement(m.Name)
	case 'P':
		s.dropPortal(m.Name)
	default:
		s.extFail(&pgErr{"08P01", fmt.Sprintf("invalid CLOSE message subtype %d", m.ObjectType)})
		return
	}
	s.be.Send(&pgproto3.CloseComplete{})
}

// closeStatement ends a statement and the portals made from it.
func (s *session) closeStatement(name string) {
	st, ok := s.ext.stmts[name]
	if !ok {
		return
	}
	delete(s.ext.stmts, name)
	for pname, p := range s.ext.portals {
		if p.stmt == st {
			s.dropPortal(pname)
		}
	}
}

// handleSync answers Sync: the end of the client's batch. Outside a
// transaction block it ends the implicit transaction, and with it every
// portal; then ReadyForQuery says the status.
func (s *session) handleSync() error {
	s.ext.skip = false
	// Outside a block the Sync ends the implicit transaction, and every
	// portal with it. In a failed block no portal can run again (25P02):
	// what they keep is given back now, not at the ROLLBACK.
	if s.txStatus() != 'T' {
		s.dropPortals()
	}
	return s.readyFlush()
}
