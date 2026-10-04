package shim

import (
	"strings"
	"testing"
)

// What MySQL Connector/J 9 sends on connect, comment and all.
const connectorJHandshake = `/* mysql-connector-j-9.1.0 (Revision: cf2917ea44ae2e43a4514a33771035aa99de73bf) */SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@transaction_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout`

func TestSysVarSelect(t *testing.T) {
	res, ok := sysVarSelect(connectorJHandshake)
	if !ok {
		t.Fatal("Connector/J's connect statement was not answered")
	}
	rows := textRows(t, res.Resultset)
	if len(rows) != 1 || len(rows[0]) != 19 || len(res.Fields) != 19 {
		t.Fatalf("answer = %d rows, %d fields; want one row of 19", len(rows), len(res.Fields))
	}
	got := map[string]string{}
	for i, f := range res.Fields {
		got[string(f.Name)] = rows[0][i]
	}
	for name, want := range map[string]string{
		"auto_increment_increment": "1",
		"character_set_client":     "utf8mb4",
		"max_allowed_packet":       "67108864",
		"time_zone":                "UTC",
		"transaction_isolation":    "REPEATABLE-READ",
		// Empty strings, not NULL: a driver reading sql_mode as NULL fails.
		"sql_mode":     "",
		"init_connect": "",
	} {
		if v, has := got[name]; !has || v != want {
			t.Errorf("%s = %q (present %v), want %q", name, v, has, want)
		}
	}

	// An unaliased variable is named by its expression, as on MySQL.
	res, ok = sysVarSelect("select @@version_comment limit 1")
	if !ok || string(res.Fields[0].Name) != "@@version_comment" {
		t.Fatalf("mysql client's statement: ok=%v fields=%v", ok, res)
	}
	if v := textRows(t, res.Resultset)[0][0]; !strings.Contains(v, "DBTrail") {
		t.Errorf("version_comment = %q", v)
	}
	if res, ok = sysVarSelect("SELECT @@GLOBAL.max_allowed_packet;"); !ok || textRows(t, res.Resultset)[0][0] != "67108864" {
		t.Error("a scoped variable with a semicolon was not answered")
	}

	// Not answered: anything that is not ONLY known variables.
	for _, stmt := range []string{
		"SELECT @@no_such_variable",
		"SELECT @@time_zone, @@no_such_variable",
		"SELECT @@time_zone, 1",
		"SELECT @@time_zone FROM orders",
		"SELECT * FROM orders WHERE note = '@@time_zone'",
		"SET @@time_zone = 'UTC'",
		"SELECT @x",
		"",
	} {
		if _, ok := sysVarSelect(stmt); ok {
			t.Errorf("%q was answered as a system-variable select", stmt)
		}
	}
}

// The handler answers it before the copy sees it, with or without a copy.
func TestSysVarSelect_handlerAnswersBeforeTheCopy(t *testing.T) {
	f := &fakeFreeSQL{}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	res, err := h.HandleQuery(connectorJHandshake)
	if err != nil || res == nil || res.Resultset == nil || len(res.Fields) != 19 {
		t.Fatalf("answer = %+v, %v", res, err)
	}
	if f.calls != 0 {
		t.Errorf("the connect statement reached the copy: %s", f.gotStmt)
	}
	if res, err := NewHandler(nil, nil).HandleQuery(connectorJHandshake); err != nil || res.Resultset == nil {
		t.Errorf("without a copy: %+v, %v", res, err)
	}
}
