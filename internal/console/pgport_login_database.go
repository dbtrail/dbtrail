package console

// PGPortDatabaseServer is the id of the server a database name selects on
// the PostgreSQL port: a
// registry id, a display name, or "default" for the boot entry, as a
// connection without an application login picks its server
// (ResolveFlashback). ok is false when the name selects no server. Nothing is
// opened: the PostgreSQL port asks it to tell a database name that is
// another server from one that is none (#3087).
func (s *Server) PGPortDatabaseServer(database string) (id string, ok bool) {
	return s.flashbackTarget(database)
}

// PGPortServerName is how the PostgreSQL port names server id in an error: its
// display name, or its id when it has none.
func (s *Server) PGPortServerName(id string) string { return s.pgPortServerName(id) }

func (s *Server) pgPortServerName(id string) string {
	if e, ok := s.cm.reg.Get(id); ok && e.Name != "" {
		return e.Name
	}
	return id
}
