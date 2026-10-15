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

// writePrivileges are the static privileges that change data or schema.
// Dynamic admin privileges (MySQL 8) and SUPER are not listed: they are
// about the server, and the page names data.
var writePrivileges = map[string]bool{
	"ALL": true, "ALL PRIVILEGES": true,
	"INSERT": true, "UPDATE": true, "DELETE": true,
	"CREATE": true, "DROP": true, "ALTER": true, "INDEX": true,
	"CREATE VIEW": true, "CREATE ROUTINE": true, "ALTER ROUTINE": true,
	"TRIGGER": true, "EVENT": true, "FILE": true,
	"CREATE TEMPORARY TABLES": true, "LOCK TABLES": true,
	"DELETE HISTORY": true,
}

// Authentication material SHOW GRANTS can print: MariaDB's
// IDENTIFIED BY PASSWORD '<hash>' and IDENTIFIED VIA <plugin> USING '<string>'
// (also `OR`-chained), MySQL 5.7's IDENTIFIED WITH ... AS '<hash>'.
var grantSecret = regexp.MustCompile(`(?i)(\bIDENTIFIED\s+BY\s+PASSWORD\s+|\bUSING\s+|\bIDENTIFIED\s+WITH\s+\S+\s+AS\s+|\bIDENTIFIED\s+BY\s+)'(?:[^'\\]|\\.|'')*'`)

// grantPrivs takes the privilege list of a GRANT line: the text between
// GRANT and its first " ON ". A role grant has no " ON " and is "".
var grantPrivs = regexp.MustCompile(`(?is)^\s*GRANT\s+(.+?)\s+ON\s+`)

// grantRole takes the role of a role grant: GRANT `r`@`%` TO `u`@`h`.
var grantRole = regexp.MustCompile("(?is)^\\s*GRANT\\s+(.+?)\\s+TO\\s+")

// classifyGrants returns the lines with their secrets removed, the
// privileges that change data or schema, and the roles granted.
func classifyGrants(lines []string) (shown, writes, roles []string) {
	seen := map[string]bool{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		shown = append(shown, grantSecret.ReplaceAllString(line, "${1}'<hidden>'"))
		if !strings.HasPrefix(strings.ToUpper(line), "GRANT") {
			continue // REVOKE (partial revokes) takes away; it adds nothing
		}
		if m := grantPrivs.FindStringSubmatch(line); m != nil && !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(m[1])), "PROXY") {
			for _, p := range splitPrivileges(m[1]) {
				if writePrivileges[p] && !seen[p] {
					seen[p] = true
					writes = append(writes, p)
				}
			}
			continue
		}
		if m := grantRole.FindStringSubmatch(line); m != nil && !strings.Contains(strings.ToUpper(m[1]), " ON ") {
			for _, r := range strings.Split(m[1], ",") {
				if r = strings.TrimSpace(r); r != "" {
					roles = append(roles, r)
				}
			}
		}
	}
	return shown, writes, roles
}

// splitPrivileges splits "SELECT, UPDATE (a, b), INSERT" at the commas that
// are not inside a column list, upper-cased, the column list dropped.
func splitPrivileges(s string) []string {
	var out []string
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
	for i, c := range s {
		switch c {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				flush(i)
				start = i + 1
			}
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
	res.Grants, res.Writes, res.Roles = classifyGrants(lines)
	return res
}
