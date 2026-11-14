package console

import "github.com/dbtrail/dbtrail/internal/sqlsandbox"

// sqlCopySession is the session of a statement written in the copy's own
// dialect (the SQL card, the MCP sql tool) for b's server: PostgreSQL's rules
// for NULL order, integer division and text comparison when the source is
// PostgreSQL (#2786), the session written for MySQL otherwise.
func (s *Server) sqlCopySession(b *bundle) sqlsandbox.Session {
	if b == nil {
		return sqlsandbox.Session{}
	}
	var get func(string) (ServerEntry, bool)
	if s.cm != nil && s.cm.reg != nil {
		get = s.cm.reg.Get
	}
	return sqlsandbox.Session{PostgresRules: sourceIsPostgres(b.serverID, b.pgSource, get)}
}

// sourceIsPostgres says whether a server's source is PostgreSQL, from what is
// held in memory: no statement reads the index for it.
//
// A registry server answers from its entry: the flavor is required when the
// entry is made and cannot be changed afterwards. The server given on the
// command line has no entry, and the flavor its capture reports is only ever
// MySQL or MariaDB, so it answers with what its index said when the console
// started (bootIsPostgres, bundle.pgSource). An index no stream had written
// to by then reads as not PostgreSQL until the console is started again.
// Anything else not known to be PostgreSQL (an entry no longer in the
// registry) is not.
func sourceIsPostgres(serverID string, bootIsPostgres bool, get func(string) (ServerEntry, bool)) bool {
	if serverID == "" || serverID == bootServerID {
		return bootIsPostgres
	}
	if get == nil {
		return false
	}
	e, ok := get(serverID)
	return ok && e.IsPostgres()
}

// flavorIsPostgres reads a flavor as an index's stream stamps it. Empty (no
// stream has written, or the read failed) and anything this build does not
// know are not PostgreSQL.
func flavorIsPostgres(flavor string) bool {
	f, err := NormalizeFlavor(flavor)
	return err == nil && f == FlavorPostgres
}

// SourceIsPostgres says whether the source of the server with this id is
// PostgreSQL, the way sqlCopySession decides it (sourceIsPostgres): from the
// registry entry, or for the server given on the command line from what its
// index said when the console started. A port that serves one family asks it
// before it binds a connection to the server.
func (s *Server) SourceIsPostgres(id string) bool {
	if s == nil || s.cm == nil {
		return false
	}
	boot := false
	if b, _ := s.cm.bootInfo(); b != nil {
		boot = b.pgSource
	}
	var get func(string) (ServerEntry, bool)
	if s.cm.reg != nil {
		get = s.cm.reg.Get
	}
	return sourceIsPostgres(id, boot, get)
}
