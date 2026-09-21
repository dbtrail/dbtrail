package doctor

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

const proxySQLRulesCheckName = "ProxySQL time-travel routing rules"

// The ProxySQL query-rule id range `bintrail proxysql-config` installs
// (990001-990006). Duplicated from cliapp/proxysql_config.go because cliapp
// imports this package, so the constants cannot be shared without a cycle.
const (
	proxySQLRuleIDFirst = 990001
	proxySQLRuleIDLast  = 990006
)

const proxySQLRulesRemediation = "Re-apply the DBTrail routing rules and persist them across ProxySQL restarts:\n\n" +
	"  bintrail proxysql-config --out proxysql-setup.sql\n" +
	"  mysql -u admin -p -h <proxysql-host> -P 6032 < proxysql-setup.sql\n\n" +
	"The generated script LOADs the rules to runtime and SAVEs them to disk.\n" +
	"Until the rules are live, time-travel queries fall through to the live MySQL:\n" +
	"the /*+ DBTRAIL_AT */ hint form then silently returns PRESENT data (it is\n" +
	"valid vanilla SQL), while the other statement shapes fail with an error."

// CheckProxySQLRules verifies the six dbtrail query rules `bintrail
// proxysql-config` installs are live (present AND active) in ProxySQL's
// runtime_mysql_query_rules (#820). Opt-in via `doctor --proxysql-admin`;
// advisory only — every outcome short of all-six-live is WARN, never FAIL,
// because ProxySQL routing is an optional deployment and doctor's exit code
// must not go red over it.
//
// The check exists chiefly for the /*+ DBTRAIL_AT */ hint form: it is valid
// vanilla MySQL (an unknown optimizer hint is a warning, not an error), so
// when routing is missing the query executes against the live table and
// returns PRESENT data with no error — the only statement shape that
// degrades silently on a misroute. The other forms fail loud
// (`_flashback.*` → ER_BAD_DB 1049; bare AS OF → 1064).
func CheckProxySQLRules(parent context.Context, adminDSN string) CheckResult {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()

	db, err := connectProxySQLAdmin(ctx, adminDSN)
	if err != nil {
		return CheckResult{
			Name:   proxySQLRulesCheckName,
			Status: StatusWarn,
			Detail: "could not connect to the ProxySQL admin interface: " + err.Error(),
			Remediation: "Verify --proxysql-admin points at ProxySQL's ADMIN port (default 6032, not the MySQL\n" +
				"traffic port 6033), e.g. admin:admin@tcp(127.0.0.1:6032)/ — and that the credentials\n" +
				"match the admin_credentials in /etc/proxysql.cnf.",
		}
	}
	defer db.Close()
	return checkProxySQLRulesOn(ctx, db)
}

// connectProxySQLAdmin opens the ProxySQL admin interface. Deliberately NOT
// config.Connect: that helper sets maxAllowedPacket=0, which makes the driver
// probe `SELECT @@max_allowed_packet` during the handshake — a MySQL-server
// system variable the SQLite-backed admin interface does not serve.
func connectProxySQLAdmin(ctx context.Context, dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %w", err)
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	db, err := config.OpenMySQL(cfg)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// checkProxySQLRulesOn runs the rule probe against an already-open admin
// connection — the testable core (sqlmock in proxysql_test.go).
func checkProxySQLRulesOn(ctx context.Context, db *sql.DB) CheckResult {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"SELECT rule_id, active FROM runtime_mysql_query_rules WHERE rule_id BETWEEN %d AND %d",
		proxySQLRuleIDFirst, proxySQLRuleIDLast))
	if err != nil {
		return CheckResult{
			Name:   proxySQLRulesCheckName,
			Status: StatusWarn,
			Detail: "could not query runtime_mysql_query_rules: " + err.Error(),
			Remediation: "Confirm the DSN targets ProxySQL's ADMIN interface (default port 6032) — the MySQL\n" +
				"traffic port (6033) and a plain MySQL server have no runtime_mysql_query_rules table.",
		}
	}
	defer rows.Close()

	// The admin interface is SQLite-backed and returns TEXT columns; scan as
	// strings and compare rather than trusting numeric conversion.
	live := map[string]bool{}
	for rows.Next() {
		var id, active string
		if err := rows.Scan(&id, &active); err != nil {
			return CheckResult{
				Name:   proxySQLRulesCheckName,
				Status: StatusWarn,
				Detail: "could not scan runtime_mysql_query_rules row: " + err.Error(),
			}
		}
		live[strings.TrimSpace(id)] = strings.TrimSpace(active) == "1"
	}
	if err := rows.Err(); err != nil {
		return CheckResult{
			Name:   proxySQLRulesCheckName,
			Status: StatusWarn,
			Detail: "could not read runtime_mysql_query_rules rows: " + err.Error(),
		}
	}

	var missing, inactive []string
	for id := proxySQLRuleIDFirst; id <= proxySQLRuleIDLast; id++ {
		key := strconv.Itoa(id)
		active, ok := live[key]
		switch {
		case !ok:
			missing = append(missing, key)
		case !active:
			inactive = append(inactive, key)
		}
	}
	if len(missing) == 0 && len(inactive) == 0 {
		return CheckResult{
			Name:   proxySQLRulesCheckName,
			Status: StatusPass,
			Detail: fmt.Sprintf("rules %d-%d live in runtime_mysql_query_rules", proxySQLRuleIDFirst, proxySQLRuleIDLast),
		}
	}
	var frags []string
	if len(missing) > 0 {
		frags = append(frags, "missing: "+strings.Join(missing, ", "))
	}
	if len(inactive) > 0 {
		frags = append(frags, "inactive: "+strings.Join(inactive, ", "))
	}
	return CheckResult{
		Name:   proxySQLRulesCheckName,
		Status: StatusWarn,
		Detail: "rule(s) " + strings.Join(frags, "; ") +
			" — time-travel queries fall through to the live MySQL; the /*+ DBTRAIL_AT */ hint form then silently returns PRESENT data (see docs/time-travel-sql.md)",
		Remediation: proxySQLRulesRemediation,
	}
}
