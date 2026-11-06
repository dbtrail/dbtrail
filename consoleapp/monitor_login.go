package consoleapp

import (
	"regexp"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// What a connection opened with the port's monitor login answers
// (console/monitor_login.go). The statements are the ones ProxySQL 3.1.12's
// monitor sends (lib/MySQL_Monitor.cpp): COM_PING is answered by
// routingHandler.PingSource, and these by monitorAnswer.

// monitorWaitTimeoutRE: `SET wait_timeout=N`, sent on every new monitor
// connection after the first check made on it (mysql-monitor_wait_timeout, on by
// default). Refusing it makes ProxySQL drop the connection and log an error.
var monitorWaitTimeoutRE = regexp.MustCompile(`(?i)^SET\s+(?:SESSION\s+|@@SESSION\.|@@)?wait_timeout\s*=\s*\d+$`)

// monitorReadOnlyRE: the read_only check of a server in
// mysql_replication_hostgroups, in each form ProxySQL sends it.
var monitorReadOnlyRE = regexp.MustCompile(`(?i)^SELECT\s+@@global\.(?:read_only|innodb_read_only|super_read_only)(?:\s*[&|]\s*@@global\.(?:read_only|innodb_read_only))?\s+(?:AS\s+)?read_only$`)

// errMonitorOnly answers everything else on a monitor connection.
var errMonitorOnly = gomysql.NewError(gomysql.ER_SPECIFIC_ACCESS_DENIED_ERROR,
	"this connection logged in with the MySQL port's monitor login, which answers a proxy's health checks only (PING, SET wait_timeout, the read_only check); log in with an application login or a server's name to query")

// monitorAnswer answers a statement on a monitor connection. The read_only
// check is answered 1: the port is a reader, and a 0 would make ProxySQL move
// it to the writer hostgroup.
func monitorAnswer(query string) (*gomysql.Result, error) {
	q := strings.TrimSpace(query)
	q = strings.TrimSpace(strings.TrimSuffix(q, ";"))
	switch {
	case monitorWaitTimeoutRE.MatchString(q):
		return &gomysql.Result{Status: gomysql.SERVER_STATUS_AUTOCOMMIT}, nil
	case monitorReadOnlyRE.MatchString(q):
		rs, err := gomysql.BuildSimpleTextResultset([]string{"read_only"}, [][]any{{int64(1)}})
		if err != nil {
			return nil, err
		}
		return &gomysql.Result{Status: gomysql.SERVER_STATUS_AUTOCOMMIT, Resultset: rs}, nil
	}
	return nil, errMonitorOnly
}
