package console

// PGPortDatabaseServer is the server a database name selects on the
// PostgreSQL port, by its id and its display name for a person to read: a
// registry id, a display name, or "default" for the boot entry, as a
// connection without an application login picks its server
// (ResolveFlashback). ok is false when the name selects no server. Nothing is
// opened: the PostgreSQL port asks it to tell a database name that is
// another server from one that is none (#3087).
func (s *Server) PGPortDatabaseServer(database string) (id, name string, ok bool) {
	id, ok = s.flashbackTarget(database)
	if !ok {
		return "", "", false
	}
	return id, s.pgPortServerName(id), true
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
