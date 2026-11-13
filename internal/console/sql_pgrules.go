package console

import (
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

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
	indexFlavor := func() string { return query.SourceFlavor(b.db) }
	return sqlsandbox.Session{PostgresRules: sourceIsPostgres(b.serverID, indexFlavor, get)}
}

// sourceIsPostgres says whether a server's source is PostgreSQL.
//
// A registry server answers from its entry, held in memory: the flavor is
// required when the entry is made and cannot be changed afterwards. The
// server given on the command line has no entry, and the flavor its capture
// reports is only ever MySQL or MariaDB, so it answers from its index
// (indexFlavor: what the stream stamped there), one read per statement.
// Anything not known to be PostgreSQL (an entry no longer in the registry, an
// index no stream has written to, a read that failed) is not.
func sourceIsPostgres(serverID string, indexFlavor func() string, get func(string) (ServerEntry, bool)) bool {
	if serverID == "" || serverID == bootServerID {
		if indexFlavor == nil {
			return false
		}
		f, err := NormalizeFlavor(indexFlavor())
		return err == nil && f == FlavorPostgres
	}
	if get == nil {
		return false
	}
	e, ok := get(serverID)
	return ok && e.IsPostgres()
}
