package console

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// routeGrantsResult is GET /api/servers/{id}/route-grants: what the server's
// forwarding account may do on its MySQL, as SHOW GRANTS says, read live with
// the account and the client the MySQL port forwards with. With Engine
// Routing on, the light statements sent to the port run on the source as this
// account, so its grants are what the port can do there; the port's own
// password is not a MySQL account and has no grants.
type routeGrantsResult struct {
	User string `json:"user,omitempty"`
	// Grants are SHOW GRANTS' lines, passwords and authentication strings
	// removed (MariaDB prints the hash in them).
	Grants []string `json:"grants,omitempty"`
	// Writes are the granted privileges that change data or schema, in the
	// order SHOW GRANTS gave them, each once.
	Writes []string `json:"writes,omitempty"`
	// Other are granted privileges that neither change table data nor only
	// read: server administration (SUPER, CREATE USER, BINLOG_ADMIN, ...),
	// PROXY, and any this build does not know.
	Other []string `json:"other,omitempty"`
	// Execute: EXECUTE is granted, so a SELECT can call a stored function,
	// which may change data with its creator's rights.
	Execute bool `json:"execute,omitempty"`
	// Roles are the roles granted to the account: SHOW GRANTS does not list
	// what a role allows, so the page cannot say the account only reads.
	Roles []string `json:"roles,omitempty"`
	// None: no forwarding account, and why; nothing was read.
	None string `json:"none,omitempty"`
	// Skipped: not read, and why (this process has no MySQL client).
	Skipped string `json:"skipped,omitempty"`
	// Error: the login or the statement failed, as the source said.
	Error string `json:"error,omitempty"`
}

// routeGrantsTimeout bounds the login and the SHOW GRANTS.
const routeGrantsTimeout = 8 * time.Second

// writePrivileges change table data or the schema.
var writePrivileges = map[string]bool{
	"ALL": true, "ALL PRIVILEGES": true,
	"INSERT": true, "UPDATE": true, "DELETE": true,
	"CREATE": true, "DROP": true, "ALTER": true, "INDEX": true,
	"CREATE VIEW": true, "CREATE ROUTINE": true, "ALTER ROUTINE": true,
	"TRIGGER": true, "EVENT": true, "FILE": true,
	"CREATE TEMPORARY TABLES": true, "LOCK TABLES": true,
	"DELETE HISTORY": true, "CREATE TABLESPACE": true,
}

// readPrivileges only read. A privilege in neither list (SUPER, CREATE USER,
// BINLOG_ADMIN, any dynamic one, one this build does not know) is reported
// as "other": the page never calls an account read-only because it did not
// recognise what it holds.
var readPrivileges = map[string]bool{
	"USAGE": true, "SELECT": true, "SHOW VIEW": true, "SHOW DATABASES": true,
	"PROCESS": true, "REPLICATION CLIENT": true, "REPLICATION SLAVE": true,
	"REFERENCES": true, "SHOW_ROUTINE": true, "BINLOG MONITOR": true,
	"SLAVE MONITOR": true, "REPLICA MONITOR": true,
}

// grants is what classifyGrants found.
type grants struct {
	shown, writes, other, roles []string
	// execute: EXECUTE is granted. A SELECT may call a stored function, and
	// a DEFINER function runs with its creator's rights, so a read-only
	// router does not stop what it writes.
	execute bool
}

// Authentication material SHOW GRANTS can print: MariaDB's
// IDENTIFIED BY PASSWORD '<hash>' and IDENTIFIED VIA <plugin> USING '<string>'
// (also `OR`-chained), MySQL 5.7's IDENTIFIED WITH ... AS '<hash>'.
var grantSecret = regexp.MustCompile(`(?i)(\bIDENTIFIED\s+BY\s+PASSWORD\s+|\bUSING\s+|\bIDENTIFIED\s+WITH\s+\S+\s+AS\s+|\bIDENTIFIED\s+BY\s+)'(?:[^'\\]|\\.|'')*'`)

// keywordAt finds the first " <kw> " in line outside quotes (` ' ") and
// parentheses, case-insensitive; -1 when there is none. A column or role
// name may contain " ON " or " TO " inside its quotes.
func keywordAt(line, kw string) int {
	up := strings.ToUpper(line)
	pat := " " + kw + " "
	var quote byte
	depth := 0
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '`' || c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			if depth > 0 {
				depth--
			}
		case depth == 0 && strings.HasPrefix(up[i:], pat):
			return i
		}
	}
	return -1
}

// classifyGrants sorts SHOW GRANTS' lines: secrets removed for display, and
// the granted privileges split into those that change data, those that only
// read, EXECUTE, and the rest; the roles granted apart.
func classifyGrants(lines []string) grants {
	var g grants
	seen := map[string]bool{}
	add := func(list *[]string, p string) {
		if !seen[p] {
			seen[p] = true
			*list = append(*list, p)
		}
	}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		g.shown = append(g.shown, grantSecret.ReplaceAllString(line, "${1}'<hidden>'"))
		up := strings.ToUpper(line)
		if !strings.HasPrefix(up, "GRANT ") {
			continue // REVOKE (partial revokes) takes away; it adds nothing
		}
		body := line[len("GRANT "):]
		on, to := keywordAt(body, "ON"), keywordAt(body, "TO")
		if on >= 0 && (to < 0 || on < to) {
			for _, p := range splitPrivileges(body[:on]) {
				switch {
				case p == "PROXY":
					add(&g.other, p)
				case writePrivileges[p]:
					add(&g.writes, p)
				case p == "EXECUTE":
					g.execute = true
				case readPrivileges[p]:
				default:
					add(&g.other, p)
				}
			}
			continue
		}
		if to >= 0 {
			for _, r := range splitQuoted(body[:to]) {
				g.roles = append(g.roles, r)
			}
		}
	}
	return g
}

// splitQuoted splits a role list at the commas outside quotes.
func splitQuoted(s string) []string {
	var out []string
	var quote byte
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '`' || c == '\'' || c == '"':
			quote = c
		case c == ',':
			if r := strings.TrimSpace(s[start:i]); r != "" {
				out = append(out, r)
			}
			start = i + 1
		}
	}
	if r := strings.TrimSpace(s[start:]); r != "" {
		out = append(out, r)
	}
	return out
}

// splitPrivileges splits "SELECT, UPDATE (a, b), INSERT" at the commas that
// are not inside a column list or quotes, upper-cased, the column list
// dropped.
func splitPrivileges(s string) []string {
	var out []string
	var quote byte
	depth, start := 0, 0
	flush := func(end int) {
		p := strings.TrimSpace(s[start:end])
		if i := strings.IndexByte(p, '('); i >= 0 {
			p = strings.TrimSpace(p[:i])
		}
		if p != "" {
			out = append(out, strings.ToUpper(strings.Join(strings.Fields(p), " ")))
		}
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '`' || c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			if depth > 0 {
				depth--
			}
		case c == ',' && depth == 0:
			flush(i)
			start = i + 1
		}
	}
	flush(len(s))
	return out
}

// handleRouteGrants serves GET /api/servers/{id}/route-grants.
func (s *Server) handleRouteGrants(w http.ResponseWriter, r *http.Request) {
	e, ok := s.cm.reg.Get(r.PathValue("id"))
	if !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.routeGrants(r.Context(), e))
}

func (s *Server) routeGrants(ctx context.Context, e ServerEntry) routeGrantsResult {
	if e.SourceFlavor() == FlavorPostgres {
		return routeGrantsResult{None: "a PostgreSQL server: the MySQL port does not forward to it"}
	}
	state, cfg := routeAccountState(e.RouteDSN, e.SourceDSN)
	switch state {
	case routeNone:
		return routeGrantsResult{None: "no forwarding account: the copy answers every statement for this server"}
	case routeUnreadable:
		return routeGrantsResult{Error: "the saved forwarding account cannot be read; enter its user and password again, or remove it"}
	case routeIsCapture:
		return routeGrantsResult{User: cfg.User, Error: "the forwarding account is the account DBTrail captures with, and the MySQL port never forwards with that one"}
	}
	res := routeGrantsResult{User: cfg.User}
	if s.routeAccountGrants == nil {
		res.Skipped = "this DBTrail process has no MySQL client to log in with"
		return res
	}
	ctx, cancel := context.WithTimeout(ctx, routeGrantsTimeout)
	defer cancel()
	lines, err := s.routeAccountGrants(ctx, e.RouteDSN, e.SourceSSL(), routeGrantsTimeout)
	if err != nil {
		res.Error = scrubDSNError(err, e.RouteDSN)
		slog.Info("console: forwarding account grants", "id", e.ID, "user", cfg.User, "error", res.Error)
		return res
	}
	g := classifyGrants(lines)
	if len(g.shown) == 0 {
		// Every account has at least its USAGE line: none read means the
		// answer was not understood, and nothing may be claimed from it.
		res.Error = "SHOW GRANTS returned no lines this page could read"
		return res
	}
	res.Grants, res.Writes, res.Other, res.Roles, res.Execute = g.shown, g.writes, g.other, g.roles, g.execute
	return res
}
