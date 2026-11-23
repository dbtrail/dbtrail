package pgshim

import (
	"context"
	"strings"
)

// The capture gate (#3054): what a statement must be for the port to send it
// to the source as the account DBTrail captures with.
//
// That account can read far more than the copy holds: it can be the
// database's owner or its master user, and read every schema and the
// system catalog. A server of the registry may still say yes to forwarding
// with it (Forward.CaptureAccount), and then the port sends a statement only
// when all of this holds, checked before anything of it is sent:
//
//  1. its text is read whole by readsOf (source_tables.go): one SELECT whose
//     tables are all named in FROM clauses, calling only functions known to
//     read no table. Every name has one or two parts (a three-part name is a
//     database's, which the source refuses with an error that would end the
//     client's transaction block);
//  2. the session's search_path does not name pg_catalog or pg_temp, which
//     then no longer come first and would change what a name resolves to;
//  3. the source confirms it (proofSQL, sent before the statement on the same
//     connection, under the same search_path): each name is a plain
//     permanent table, with no child, no parent and no row-level security,
//     and nothing of the user's can run where the text shows nothing;
//  4. every table the source names in that answer is, by schema and name
//     exactly, a table of the copy (Binding.Catalog, read afresh), and none
//     is in a schema of the system.
//
// Anything that is not shown to be that is refused with SQLSTATE 0A000 and
// not sent: proving too little refuses a statement that was safe, proving
// too much would send one that reads what the copy does not hold. The
// connection to the source is still opened read-only (default_transaction_
// read_only, forward.go), so this gate is about WHAT may be read, on top of
// that.
//
// What it cannot see: a table altered on the source between the proof and
// the statement (a table swapped for a view in that instant). Doing that
// needs DDL on the source, which is beyond what a client of the port can
// do through it.

// captureNotPure are the functions of pureFuncs the gate does not let
// through.
var captureNotPure = map[string]bool{"current_setting": true, "pg_sleep": true}

// captureSystemSchemas are schemas no table of the copy can be in.
var captureSystemSchemas = map[string]bool{"pg_catalog": true, "information_schema": true, "pg_toast": true}

// captureRefusedPrefix starts every refusal of the gate.
const captureRefusedPrefix = "this server forwards to its source with the account DBTrail captures with, so the port sends there only a SELECT whose every table is a table of the copy, and "

// captureNotSent ends every refusal.
const captureNotSent = "; nothing was sent to the source"

// captureRefused is the gate's error for why.
func captureRefused(why string) *pgErr {
	return &pgErr{"0A000", captureRefusedPrefix + why + captureNotSent}
}

// captureTextRefusal reads what the statement's text says it reads, and why
// the gate refuses it on its text alone ("" when it does not).
func captureTextRefusal(qstr string, searchPath []string) (stmtReads, string) {
	for _, p := range searchPath {
		if p == "pg_catalog" || p == "pg_temp" || strings.HasPrefix(p, "pg_temp_") {
			return stmtReads{}, "this session's search_path names " + p + ", which changes what a name in the statement is"
		}
	}
	reads, why := readsOf(qstr, nil)
	if why != "" {
		return stmtReads{}, "this statement is not one whose tables the port can tell from its text (" + why + ")"
	}
	for _, r := range reads.refs {
		if len(r.parts) > 2 {
			return stmtReads{}, "this statement names a table with its database"
		}
	}
	// Functions readsOf takes as reading no table that still read past the
	// copy with this account: a server setting (archive_command,
	// primary_conninfo can hold credentials), or a session held open on the
	// source as long as asked.
	for _, f := range reads.funcs {
		if captureNotPure[f] {
			return stmtReads{}, "this statement calls " + f + "(), which reads beyond the copy's tables or holds the source"
		}
	}
	// A locking read (FOR UPDATE, FOR SHARE, FOR [NO] KEY ...) holds locks
	// other sessions wait on: not a plain read. readsOf already read the
	// whole text, so lexSQL reads it the same way here.
	toks, _ := lexSQL(qstr)
	for i := 0; i+1 < len(toks); i++ {
		// A cast to a reg type (regclass, regrole, regnamespace...) looks a
		// name up in the system catalog: it tells whether a table, a role
		// or a schema exists.
		if isOpTok(toks, i, "::") && toks[i+1].kind == rtWord && strings.HasPrefix(toks[i+1].text, "reg") {
			return stmtReads{}, "this statement casts to " + toks[i+1].text + ", which reads the system catalog"
		}
		if toks[i].kind == rtWord && toks[i].text == "for" && toks[i+1].kind == rtWord {
			switch toks[i+1].text {
			case "update", "share", "no", "key":
				return stmtReads{}, "this statement is a locking read (FOR UPDATE, FOR SHARE), which takes locks on the source"
			}
		}
	}
	return reads, ""
}

// captureTablesRefusal says why the tables the source confirmed are not all
// tables of the copy ("" when they are). Compared exactly, schema and name:
// the source's names are its catalog's, and a table the copy holds under
// another case is not taken to be it.
func captureTablesRefusal(proven [][2]string, copyTables [][2]string) string {
	held := make(map[[2]string]bool, len(copyTables))
	for _, t := range copyTables {
		held[t] = true
	}
	for _, p := range proven {
		if captureSystemSchemas[p[0]] || strings.HasPrefix(p[0], "pg_") {
			return "this statement reads a table of the system catalog"
		}
		if !held[p] {
			return "this statement reads a table the copy does not hold"
		}
	}
	return ""
}

// captureRefusal is the gate for a statement about to be sent on up: nil
// when it may be sent, or when the server forwards with an account of its
// own (the gate is for the capture account only). It sends the proof and
// reads the copy's catalog; up must have the session's settings on it
// (prepareSource) and no extended batch open, since a query of the port's
// own cannot be sent in the middle of one.
func (s *session) captureRefusal(up *upstream, qstr string) *pgErr {
	if s.bound == nil || s.bound.Forward == nil || !s.bound.Forward.CaptureAccount {
		return nil
	}
	reads, why := captureTextRefusal(qstr, s.set.searchPath)
	if why != "" {
		s.logger.Debug("pgshim: the capture gate refused a statement on its text", "why", why)
		return captureRefused(why)
	}
	if up == nil || up.inCycle {
		return captureRefused("this statement came while an extended batch was open on the source, where the port cannot ask the source about it first")
	}
	if up.sent == nil || up.sent["search_path"] != showSearchPath(s.set.searchPath) {
		return captureRefused("the source's search_path is not known to be this session's")
	}
	if s.bound.Catalog == nil {
		return captureRefused("the copy's tables cannot be listed for this connection")
	}
	cell, perr, lost := s.cellOnSource(up, proofSQL(reads))
	if lost != nil {
		s.loseSource(lost.why)
		return s.lostError(lost)
	}
	if perr != nil {
		// The source's own error, said: a timeout, a cancel or a privilege
		// is not a view the client should go looking for.
		s.logger.Warn("pgshim: the source refused the capture gate's check of a statement; the statement is not sent", "code", perr.code, "err", perr.msg)
		return captureRefused("the source could not check the names in it (" + perr.code + ": " + perr.msg + ")")
	}
	proven, ok := parseProof(cell)
	if !ok {
		return captureRefused("the source did not confirm that the names in it are plain tables and that nothing of a user's runs in it")
	}
	ctx, done := s.statementContext()
	copyTables, err := s.copyTableNames(ctx)
	done()
	if err != nil {
		s.logger.Warn("pgshim: the copy's tables could not be listed for the capture gate; the statement is not sent", "err", err)
		return captureRefused("the copy's tables could not be listed (" + err.Error() + ")")
	}
	if why := captureTablesRefusal(proven, copyTables); why != "" {
		return captureRefused(why)
	}
	return nil
}

// copyTableNames is the copy's tables by schema and name, without one of its
// workers where the host can say them so (Binding.CopyTableNames).
func (s *session) copyTableNames(ctx context.Context) ([][2]string, error) {
	if s.bound.CopyTableNames != nil {
		return s.bound.CopyTableNames(ctx)
	}
	tables, err := s.bound.Catalog(ctx)
	if err != nil {
		return nil, err
	}
	names := make([][2]string, len(tables))
	for i, t := range tables {
		names[i] = [2]string{t.Schema, t.Name}
	}
	return names, nil
}

// capturePass is one statement let through by the capture gate on up, in
// the batch numbered cycle, under search_path path.
type capturePass struct {
	up    *upstream
	cycle uint64
	path  string
}

// captureRefusalStmt is captureRefusal for a prepared statement of the
// extended protocol: a statement the gate let through in this same batch on
// this connection, under the same search_path, is not checked again. A
// driver sends Parse, Describe, Bind and Execute of one statement in one
// batch, and once the first of them went to the source the batch is open
// there and no check can be sent. A batch is short and the names were
// resolved there when the statement was parsed, right after its check.
func (s *session) captureRefusalStmt(up *upstream, st *preparedStatement) *pgErr {
	if s.bound == nil || s.bound.Forward == nil || !s.bound.Forward.CaptureAccount {
		return nil
	}
	if p := st.capturePassed; p != nil && up != nil && p.up == up && up.inCycle && p.cycle == up.cycle &&
		up.stmts[st.name] == st && up.sent != nil && p.path == up.sent["search_path"] {
		return nil
	}
	st.capturePassed = nil
	if perr := s.captureRefusal(up, st.query); perr != nil {
		return perr
	}
	st.capturePassed = &capturePass{up: up, cycle: up.cycle, path: up.sent["search_path"]}
	return nil
}
