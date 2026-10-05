package shim

import (
	"math"
	"regexp"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// A driver asks the server about itself when it connects: MySQL Connector/J
// sends one SELECT of nineteen system variables and reads the row; the mysql
// client asks for @@version_comment. The port has no such variables, and the
// copy does not know the @@ syntax, so such a statement used to get an empty
// OK (which a driver expecting a row does not accept) or, when it opened
// with the driver's comment, was sent to the copy and failed, and the driver
// could not connect at all.
//
// sysVarSelect answers a SELECT made ONLY of system variables with one row
// of values that describe this port truthfully: UTF-8, UTC, no SQL mode, the
// packet size it accepts. A statement naming a variable this file does not
// know is not answered here (the caller keeps its older behaviour): making
// up a value for a variable nobody listed would be a guess.

// sysVars are the variables answered, with what the port is. Integers go out
// as integers: drivers parse them.
var sysVars = map[string]any{
	"auto_increment_increment": int64(1),
	"auto_increment_offset":    int64(1),
	"autocommit":               int64(1),
	"character_set_client":     "utf8mb4",
	"character_set_connection": "utf8mb4",
	"character_set_results":    "utf8mb4",
	"character_set_server":     "utf8mb4",
	"collation_connection":     "utf8mb4_general_ci",
	"collation_server":         "utf8mb4_general_ci",
	"init_connect":             "",
	"interactive_timeout":      int64(28800),
	"license":                  "Apache-2.0",
	"lower_case_table_names":   int64(0),
	"max_allowed_packet":       int64(64 << 20),
	"net_buffer_length":        int64(16384),
	"net_write_timeout":        int64(60),
	"performance_schema":       int64(0),
	"sql_mode":                 "",
	"sql_select_limit":         uint64(math.MaxUint64), // MySQL's "no limit"
	"system_time_zone":         "UTC",
	"time_zone":                "UTC",
	"transaction_isolation":    "REPEATABLE-READ",
	"tx_isolation":             "REPEATABLE-READ",
	"transaction_read_only":    int64(1),
	"tx_read_only":             int64(1),
	"version":                  portServerVersion,
	"version_comment":          "DBTrail time-travel port",
	"wait_timeout":             int64(28800),
}

// portServerVersion is the version the port's handshake announces, whatever
// the source is. It is fixed on purpose (#2110): the handshake is written
// before the port knows which server the client asked for, and one port
// serves every server, so it cannot be the source's. It is also the version
// go-mysql's default server announces (the native-password path uses that
// server), so both auth paths say the same; TestStatus_handshakeAnnounces*
// pin it. Under read routing SELECT VERSION() and @@version are forwarded
// and answer the source's own. What a driver that reads the handshake
// version does with the difference is in docs/time-travel-sql.md.
const portServerVersion = "8.0.11"

// Three groups per item: the scope, the variable, the alias.
const sysVarItem = `@@(?:(session|global|local)\.)?([a-z_0-9]+)(?:\s+as\s+` + "`?" + `([a-z_0-9]+)` + "`?" + `)?`

var (
	sysVarItemRE   = regexp.MustCompile(`(?i)` + sysVarItem)
	sysVarSelectRE = regexp.MustCompile(`(?is)^select\s+` + sysVarItem + `(?:\s*,\s*` + sysVarItem + `)*\s*(?:limit\s+1\s*)?;?\s*$`)
	leadingComment = regexp.MustCompile(`(?s)^\s*(?:/\*.*?\*/\s*)+`)
)

// sysVarSelect answers a SELECT of system variables this port knows. ok is
// false for any other statement, and for one naming an unknown variable.
// session holds the variables this connection SET (sessionVars.overrides):
// @@name and @@session.name answer from it first, @@global.name never does.
func sysVarSelect(stmt string, session map[string]any) (res *mysql.Result, ok bool) {
	stmt = leadingComment.ReplaceAllString(stmt, "")
	if !sysVarSelectRE.MatchString(stmt) {
		return nil, false
	}
	items := sysVarItemRE.FindAllStringSubmatch(stmt, -1)
	names := make([]string, len(items))
	row := make([]any, len(items))
	for i, m := range items {
		name := strings.ToLower(m[2])
		v, known := sysVars[name]
		if !known {
			return nil, false
		}
		if set, ok := session[name]; ok && !strings.EqualFold(m[1], "global") {
			v = set
		}
		// MySQL names an unaliased column by its expression text.
		names[i] = m[3]
		if names[i] == "" {
			names[i] = strings.TrimSpace(m[0])
		}
		if s, isString := v.(string); isString {
			// A []byte: the builder writes an empty Go string as NULL.
			v = []byte(s)
		}
		row[i] = v
	}
	rs, err := mysql.BuildSimpleTextResultset(names, [][]any{row})
	if err != nil {
		return nil, false
	}
	return &mysql.Result{Status: mysql.SERVER_STATUS_AUTOCOMMIT, Resultset: rs}, true
}
