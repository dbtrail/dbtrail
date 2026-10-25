package console

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/dbtrail/dbtrail/internal/hiderules"
)

// hiddenFromSQLStatus is /api/status's "hidden_from_sql" (#2529): what this
// server's rules leave out of SQL on the copy, the MySQL port and MCP sql,
// so an empty answer there is never read as "no data". (What the port
// forwards to the source under Engine Routing obeys MySQL's grants instead.)
type hiddenFromSQLStatus struct {
	// Tables and Columns are the enforced rules. The NAMES are server
	// metadata, shown to anyone who may read the server (its settings and
	// GET /api/servers carry them too); the values are what the rules hide.
	Tables       []string `json:"tables,omitempty"`
	Columns      []string `json:"columns,omitempty"`
	TablesCount  int      `json:"tables_count"`
	ColumnsCount int      `json:"columns_count"`
	// UnreadableKeys: rules a newer build saved that this build cannot
	// read; while there are any, the SQL surfaces refuse this server.
	UnreadableKeys []string `json:"unreadable_keys,omitempty"`
	// Refused: the SQL surfaces refuse this server's statements (rules
	// this build cannot enforce, or an entry no longer in the registry), in
	// the words the SQL surfaces use.
	Refused string `json:"refused,omitempty"`
}

// hiddenFromSQL is the section for b's server, nil when it hides nothing.
func (s *Server) hiddenFromSQL(b *bundle) *hiddenFromSQLStatus {
	if b == nil || b.serverID == "" || b.serverID == bootServerID || s.cm == nil || s.cm.reg == nil {
		return nil
	}
	e, ok := s.cm.reg.Get(b.serverID)
	if !ok {
		// The SQL surfaces refuse a server whose entry is gone
		// (hiddenRulesFor); say so here too, in their words.
		_, err := s.hiddenRulesFor(b)
		return &hiddenFromSQLStatus{Refused: err.Error()}
	}
	if e.Hidden == nil {
		return nil
	}
	out := &hiddenFromSQLStatus{UnreadableKeys: e.Hidden.UnknownKeys()}
	set, err := s.hiddenRulesFor(b)
	if err != nil {
		out.Refused = err.Error()
	}
	// The counts and lists are what is enforced: the parsed rules, blanks
	// and repeats gone. Rules that do not parse are counted as saved.
	shown := hiderules.Rules{Tables: e.Hidden.Tables, Columns: e.Hidden.Columns}
	if err == nil {
		shown = set.Rules()
	}
	out.TablesCount, out.ColumnsCount = len(shown.Tables), len(shown.Columns)
	if out.TablesCount == 0 && out.ColumnsCount == 0 && len(out.UnreadableKeys) == 0 && out.Refused == "" {
		return nil
	}
	out.Tables = append([]string{}, shown.Tables...)
	out.Columns = append([]string{}, shown.Columns...)
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
