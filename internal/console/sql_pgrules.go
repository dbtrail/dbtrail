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
	return sqlsandbox.Session{PostgresRules: sourceIsPostgres(b.serverID, s.bootSourceFlavor, get)}
}

// sourceIsPostgres answers from what is held in memory, with no read of the
// index: the registry entry's flavor, or for the server given on the command
// line the flavor its capture runs as. Anything not known to be PostgreSQL
// (an entry no longer in the registry, no flavor reported) is not.
func sourceIsPostgres(serverID string, bootFlavor func() string, get func(string) (ServerEntry, bool)) bool {
	if serverID == "" || serverID == bootServerID {
		if bootFlavor == nil {
			return false
		}
		f, err := NormalizeFlavor(bootFlavor())
		return err == nil && f == FlavorPostgres
	}
	if get == nil {
		return false
	}
	e, ok := get(serverID)
	return ok && e.IsPostgres()
}
