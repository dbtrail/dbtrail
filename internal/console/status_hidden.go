package console

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/dbtrail/dbtrail/internal/query"
)

// hiddenFromSQLStatus is /api/status's "hidden_from_sql" (#2529): what this
// server's rules leave out of SQL on the copy, the MySQL port and MCP sql,
// so an empty answer there is never read as "no data".
type hiddenFromSQLStatus struct {
	// Tables and Columns are the rules as saved. A session under a data
	// profile or a table scope gets the counts only: a name it may not see
	// elsewhere is not shown here either.
	Tables       []string `json:"tables,omitempty"`
	Columns      []string `json:"columns,omitempty"`
	TablesCount  int      `json:"tables_count"`
	ColumnsCount int      `json:"columns_count"`
	// UnreadableKeys: rules a newer build saved that this build cannot
	// read; while there are any, the SQL surfaces refuse this server.
	UnreadableKeys []string `json:"unreadable_keys,omitempty"`
	// Refused: the SQL surfaces refuse this server's statements (rules
	// this build cannot enforce, or an entry no longer in the registry).
	Refused string `json:"refused,omitempty"`
}

// hiddenFromSQL is the section for b's server, nil when it hides nothing.
func (s *Server) hiddenFromSQL(b *bundle, opts query.Options) *hiddenFromSQLStatus {
	if b == nil || b.serverID == "" || b.serverID == bootServerID || s.cm == nil || s.cm.reg == nil {
		return nil
	}
	e, ok := s.cm.reg.Get(b.serverID)
	if !ok || e.Hidden == nil {
		return nil
	}
	out := &hiddenFromSQLStatus{
		TablesCount:    len(e.Hidden.Tables),
		ColumnsCount:   len(e.Hidden.Columns),
		UnreadableKeys: e.Hidden.UnknownKeys(),
	}
	if out.TablesCount == 0 && out.ColumnsCount == 0 && len(out.UnreadableKeys) == 0 {
		return nil
	}
	if _, err := s.hiddenRulesFor(b); err != nil {
		out.Refused = err.Error()
	}
	scoped := opts.ProfileActive || len(opts.DenyTables) > 0 || len(opts.AllowTables) > 0 ||
		len(opts.RedactColumns) > 0 || len(opts.AllowColumns) > 0
	if !scoped {
		out.Tables = append([]string{}, e.Hidden.Tables...)
		out.Columns = append([]string{}, e.Hidden.Columns...)
	}
	return out
}

// addStatusSection adds one top-level key to the status document, leaving
// the rest of it byte for byte as the status renderer wrote it.
func addStatusSection(body []byte, key string, section any) ([]byte, error) {
	trimmed := bytes.TrimRight(body, " \t\r\n")
	if !json.Valid(trimmed) || len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return nil, errors.New("status document is not a JSON object")
	}
	raw, err := json.Marshal(section)
	if err != nil {
		return nil, err
	}
	k, _ := json.Marshal(key)
	inner := bytes.TrimSpace(trimmed[1 : len(trimmed)-1])
	out := append([]byte{}, trimmed[:len(trimmed)-1]...)
	out = bytes.TrimRight(out, " \t\r\n")
	if len(inner) > 0 {
		out = append(out, ',')
	}
	out = append(out, k...)
	out = append(out, ':')
	out = append(out, raw...)
	out = append(out, '}')
	return append(out, body[len(trimmed):]...), nil
}
