package doctor

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/serverid"
)

func TestExtractGrantUser(t *testing.T) {
	tests := []struct {
		name  string
		grant string
		want  string
	}{
		{
			name:  "simple user@host with single quotes",
			grant: "GRANT USAGE ON *.* TO 'bintrail'@'%'",
			want:  "'bintrail'@'%'",
		},
		{
			name:  "user@host with localhost",
			grant: "GRANT SELECT ON db.* TO 'app'@'localhost'",
			want:  "'app'@'localhost'",
		},
		{
			name:  "trailing IDENTIFIED BY clause (older MySQL)",
			grant: "GRANT REPLICATION SLAVE ON *.* TO 'repl'@'10.0.0.5' IDENTIFIED BY '<secret>'",
			want:  "'repl'@'10.0.0.5'",
		},
		{
			name:  "WITH GRANT OPTION suffix",
			grant: "GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION",
			want:  "'admin'@'%'",
		},
		{
			name:  "lowercase TO is uppercased by ToUpper search",
			grant: "grant select on db.* to 'app'@'localhost'",
			want:  "'app'@'localhost'",
		},
		{
			name:  "backtick-quoted identifier",
			grant: "GRANT USAGE ON *.* TO `bintrail`@`%`",
			want:  "`bintrail`@`%`",
		},
		{
			name:  "no TO clause",
			grant: "REVOKE ALL PRIVILEGES",
			want:  "",
		},
		{
			name:  "empty",
			grant: "",
			want:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractGrantUser(tt.grant)
			if got != tt.want {
				t.Errorf("extractGrantUser(%q) = %q, want %q", tt.grant, got, tt.want)
			}
		})
	}
}

func TestDoctorReportAdd(t *testing.T) {
	r := &Report{}
	r.add(CheckResult{Name: "a", Status: StatusPass})
	r.add(CheckResult{Name: "b", Status: StatusFail})
	r.add(CheckResult{Name: "c", Status: StatusWarn})
	r.add(CheckResult{Name: "d", Status: StatusSkip})
	r.add(CheckResult{Name: "e", Status: StatusPass})

	if r.Passed != 2 || r.Failed != 1 || r.Warnings != 1 || r.Skipped != 1 {
		t.Errorf("counts wrong: %+v", r)
	}
	if len(r.Checks) != 5 {
		t.Errorf("expected 5 checks, got %d", len(r.Checks))
	}
}

func TestDoctorReportAddUnknownStatusDoesNotCountButAppends(t *testing.T) {
	r := &Report{}
	r.add(CheckResult{Name: "weird", Status: CheckStatus("UNKNOWN")})
	if len(r.Checks) != 1 {
		t.Errorf("expected unknown check appended for JSON visibility, got %d entries", len(r.Checks))
	}
	if r.Passed+r.Failed+r.Warnings+r.Skipped != 0 {
		t.Errorf("expected no counters incremented for unknown status, got %+v", r)
	}
}

func TestDoctorReportErr(t *testing.T) {
	// No failures → nil error (warnings are tolerated).
	r := &Report{Passed: 3, Warnings: 2}
	if err := r.Err(); err != nil {
		t.Errorf("expected nil error with no failures, got %v", err)
	}

	// One failure → error. Recorded through add, the only sanctioned mutator:
	// PreflightError derives its count from the failing checks (#1503), so a
	// Report whose Failed counter was set by hand is not a shape that exists.
	r2 := &Report{Passed: 3}
	r2.add(CheckResult{Name: "binlog_format=ROW", Status: StatusFail})
	err := r2.Err()
	if err == nil {
		t.Error("expected error when a check failed")
	}
	if err != nil && !strings.Contains(err.Error(), "1 preflight check") {
		t.Errorf("error message does not mention check count: %v", err)
	}
}

func TestDoctorReportWriteJSON(t *testing.T) {
	// Build a report, marshal via Write, unmarshal, assert deep equality.
	// Catches: missing JSON tags, dropped fields, type mismatches.
	in := &Report{
		Checks: []CheckResult{
			{Name: "ok", Status: StatusPass, Detail: "MySQL 8.0.36"},
			{Name: "bad", Status: StatusFail, Detail: "denied", Remediation: "GRANT X ON *.* ..."},
		},
		Passed: 1,
		Failed: 1,
	}
	var buf bytes.Buffer
	if err := in.Write(&buf, "json"); err != nil {
		t.Fatalf("Write(json) error: %v", err)
	}
	var out Report
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v\nraw: %s", err, buf.String())
	}
	if out.Passed != 1 || out.Failed != 1 {
		t.Errorf("counters lost in round-trip: %+v", out)
	}
	if len(out.Checks) != 2 {
		t.Fatalf("expected 2 checks, got %d", len(out.Checks))
	}
	if out.Checks[0].Status != StatusPass {
		t.Errorf("status round-trip failed: %q", out.Checks[0].Status)
	}
	if out.Checks[1].Remediation != "GRANT X ON *.* ..." {
		t.Errorf("remediation lost: %q", out.Checks[1].Remediation)
	}
}

// TestEveryFailCheckCarriesRemediation is the structural invariant guarding
// against the SCHEMATA-class regression where a FAIL slips through with no
// next action for the operator. Per-test wantRemediation flags let new bare
// FAILs sneak in because authors can opt out. This test does not opt out:
// every check that can be exercised with sqlmock is forced into FAIL and
// asserted to carry Remediation. Checks that open their own DB connection
// (checkSourceConnection, checkIndexConnection, checkIndexWriteAccess) are
// covered indirectly via the *On variants where they exist.
func TestEveryFailCheckCarriesRemediation(t *testing.T) {
	ctx := t.Context()
	forcedErr := errors.New("forced query failure")

	cases := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		run   func(db sqlDB) CheckResult
	}{
		{
			name:  "checkLogBin/query error",
			setup: func(m sqlmock.Sqlmock) { m.ExpectQuery("SELECT @@log_bin").WillReturnError(forcedErr) },
			run:   func(db sqlDB) CheckResult { return checkLogBin(ctx, db) },
		},
		{
			name:  "checkReplicationGrants/SHOW GRANTS error",
			setup: func(m sqlmock.Sqlmock) { m.ExpectQuery("SHOW GRANTS").WillReturnError(forcedErr) },
			run:   func(db sqlDB) CheckResult { return checkReplicationGrants(ctx, db) },
		},
		{
			name: "checkSchemaVisibility/query error",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT COUNT").WillReturnError(forcedErr)
			},
			run: func(db sqlDB) CheckResult { return checkSchemaVisibility(ctx, db, nil) },
		},
		{
			name: "checkIndexWriteAccessOn/SCHEMATA error",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("information_schema.SCHEMATA").WillReturnError(forcedErr)
			},
			run: func(db sqlDB) CheckResult { return checkIndexWriteAccessOn(ctx, db, "binlog_index") },
		},
		{
			name: "checkIndexWriteAccessOn/CREATE TABLE denied",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("information_schema.SCHEMATA").WillReturnRows(
					sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("binlog_index"))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnError(forcedErr)
			},
			run: func(db sqlDB) CheckResult { return checkIndexWriteAccessOn(ctx, db, "binlog_index") },
		},
		{
			name: "checkIndexWriteAccessOn/DROP denied (upgraded WARN→FAIL)",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("information_schema.SCHEMATA").WillReturnRows(
					sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("binlog_index"))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("DROP TABLE").WillReturnError(forcedErr)
			},
			run: func(db sqlDB) CheckResult { return checkIndexWriteAccessOn(ctx, db, "binlog_index") },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			c.setup(mock)
			got := c.run(db)
			if got.Status != StatusFail {
				t.Errorf("expected StatusFail, got %q (detail=%q)", got.Status, got.Detail)
			}
			if got.Remediation == "" {
				t.Errorf("FAIL with no Remediation — operator has no next step.\n  Detail: %q", got.Detail)
			}
		})
	}
}

// sqlDB is a tiny type alias to keep the runner-table signatures readable
// without importing the full database/sql path into every cell.
type sqlDB = *sql.DB

func TestCheckLogBin(t *testing.T) {
	// checkLogBin owns its own string-comparison logic (does not delegate to a
	// validator with its own tests), so the "1"/"ON" parsing and the OFF/0
	// rejection branches are the load-bearing assertions here.
	tests := []struct {
		name       string
		returnVal  string
		queryErr   error
		wantStatus CheckStatus
		wantDetail string
	}{
		{name: "ON via 1", returnVal: "1", wantStatus: StatusPass, wantDetail: "ON"},
		{name: "ON via literal", returnVal: "ON", wantStatus: StatusPass, wantDetail: "ON"},
		{name: "ON case-insensitive", returnVal: "on", wantStatus: StatusPass, wantDetail: "ON"},
		{name: "OFF literal", returnVal: "OFF", wantStatus: StatusFail, wantDetail: `log_bin="OFF"`},
		{name: "OFF via 0", returnVal: "0", wantStatus: StatusFail, wantDetail: `log_bin="0"`},
		{name: "empty string", returnVal: "", wantStatus: StatusFail, wantDetail: `log_bin=""`},
		{name: "query error", queryErr: errors.New("denied"), wantStatus: StatusFail, wantDetail: "denied"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			exp := mock.ExpectQuery("SELECT @@log_bin")
			if tt.queryErr != nil {
				exp.WillReturnError(tt.queryErr)
			} else {
				exp.WillReturnRows(sqlmock.NewRows([]string{"@@log_bin"}).AddRow(tt.returnVal))
			}

			got := checkLogBin(t.Context(), db)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q", got.Status, tt.wantStatus)
			}
			if !strings.Contains(got.Detail, tt.wantDetail) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetail)
			}
			if tt.wantStatus == StatusFail && got.Remediation == "" {
				t.Error("FAIL outcome with no remediation breaks doctor's promise")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

func TestCheckBinlogRetention(t *testing.T) {
	// Two branches: @@binlog_expire_logs_seconds (modern) and @@expire_logs_days
	// (fallback, still present on MySQL 8.0 as deprecated). Plus retry/error
	// paths. Retention failures are WARN, not FAIL — the property test does not
	// apply here; each row asserts wantRemediation explicitly.
	tests := []struct {
		name            string
		modern          mockSQLScalar // first query — @@binlog_expire_logs_seconds
		legacy          mockSQLScalar // second query — @@expire_logs_days (only invoked when modern errors)
		wantStatus      CheckStatus
		wantDetailFrag  string // substring assertion on detail
		wantRemediation bool   // must remediation be present?
	}{
		// 1. MySQL 8.0+ branches.
		{name: "modern: at threshold", modern: row("172800"), wantStatus: StatusPass, wantDetailFrag: "48h"},
		{name: "modern: above threshold", modern: row("259200"), wantStatus: StatusPass, wantDetailFrag: "72h"},
		{name: "modern: below threshold", modern: row("3600"), wantStatus: StatusWarn, wantDetailFrag: "1h", wantRemediation: true},
		{name: "modern: zero (never expire)", modern: row("0"), wantStatus: StatusWarn, wantDetailFrag: "no automatic expiration"},
		{name: "modern: unparseable", modern: row("not-an-int"), wantStatus: StatusWarn, wantDetailFrag: "could not parse"},
		// 2. Legacy fallback when modern errors.
		{name: "legacy: 7 days", modern: errResp("unknown variable"), legacy: row("7"), wantStatus: StatusPass, wantDetailFrag: "7 days"},
		{name: "legacy: 1 day", modern: errResp("unknown variable"), legacy: row("1"), wantStatus: StatusWarn, wantDetailFrag: "expire_logs_days=1", wantRemediation: true},
		{name: "legacy: unparseable", modern: errResp("unknown variable"), legacy: row("garbage"), wantStatus: StatusWarn, wantDetailFrag: "could not parse"},
		// 3. Both error → warn-only (no remediation; doctor proceeds with degraded info).
		{name: "both error", modern: errResp("conn lost"), legacy: errResp("conn lost"), wantStatus: StatusWarn, wantDetailFrag: "could not read"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			// Non-RDS source: the rds_configuration probe (which runs first)
			// errors with 1146 (no such table) so the standard probe path runs.
			mock.ExpectQuery("mysql.rds_configuration").
				WillReturnError(&mysql.MySQLError{Number: 1146, Message: "Table 'mysql.rds_configuration' doesn't exist"})

			expect := mock.ExpectQuery("SELECT @@binlog_expire_logs_seconds")
			tt.modern.apply(expect, "@@binlog_expire_logs_seconds")
			if tt.modern.err != nil {
				lexpect := mock.ExpectQuery("SELECT @@expire_logs_days")
				tt.legacy.apply(lexpect, "@@expire_logs_days")
			}

			got := checkBinlogRetention(t.Context(), db)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if tt.wantRemediation && got.Remediation == "" {
				t.Error("expected remediation but got none")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// TestCheckBinlogRetentionRDS pins #812: on RDS/Aurora the effective binlog
// retention is governed by mysql.rds_configuration 'binlog retention hours',
// not @@binlog_expire_logs_seconds. When that row is queryable the verdict is
// based on it (and @@binlog_expire_logs_seconds is never consulted). NULL (the
// RDS default) or a sub-2-day value → WARN with the rds_set_configuration
// remediation; >= 2 days → PASS.
func TestCheckBinlogRetentionRDS(t *testing.T) {
	tests := []struct {
		name            string
		value           sql.NullString // mysql.rds_configuration value column
		wantStatus      CheckStatus
		wantDetailFrag  string
		wantRemediation bool
	}{
		{"NULL default", sql.NullString{}, StatusWarn, "is NULL", true},
		{"below 2 days", sql.NullString{String: "24", Valid: true}, StatusWarn, "below 2 days", true},
		{"exactly 2 days", sql.NullString{String: "48", Valid: true}, StatusPass, "48h (RDS binlog retention hours)", false},
		{"well above", sql.NullString{String: "720", Valid: true}, StatusPass, "720h (RDS binlog retention hours)", false},
		{"unparseable", sql.NullString{String: "later", Valid: true}, StatusWarn, "could not parse", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			rows := sqlmock.NewRows([]string{"value"})
			if tt.value.Valid {
				rows.AddRow(tt.value.String)
			} else {
				rows.AddRow(nil)
			}
			// Only the RDS probe runs — the standard @@binlog_expire_logs_seconds
			// query must NOT be issued (ExpectationsWereMet would flag an
			// unexpected one via a leftover, and we register none).
			mock.ExpectQuery("mysql.rds_configuration").WillReturnRows(rows)

			got := checkBinlogRetention(t.Context(), db)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if tt.wantRemediation && got.Remediation == "" {
				t.Error("expected remediation but got none")
			}
			if got.Status == StatusFail {
				t.Error("binlog retention is advisory — the check must never FAIL")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// TestCheckBinlogRetentionRDSProbeError pins #812's silent-failure fix: a
// permission/transient error reading mysql.rds_configuration must surface as WARN,
// never be laundered into "not RDS" and fall through to the engine-variable PASS.
func TestCheckBinlogRetentionRDSProbeError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	// A permission error (1142), not 1146 no-such-table: the check must WARN and
	// must NOT fall through to the standard @@binlog_expire_logs_seconds probe
	// (none is registered, so ExpectationsWereMet catches a stray one).
	mock.ExpectQuery("mysql.rds_configuration").
		WillReturnError(&mysql.MySQLError{Number: 1142, Message: "SELECT command denied to user 'x'@'%' for table 'rds_configuration'"})

	got := checkBinlogRetention(t.Context(), db)
	if got.Status != StatusWarn {
		t.Errorf("Status = %q, want WARN (a permission error must not become a PASS)", got.Status)
	}
	if !strings.Contains(got.Detail, "could not read mysql.rds_configuration") {
		t.Errorf("Detail = %q, want it to surface the probe error", got.Detail)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (did it fall through to the engine probe?): %v", err)
	}
}

// TestCheckServerIDCollision pins #819: the check WARNs when the server-id
// derived from --source-dsn equals the source's own @@server_id, PASSes
// otherwise, and stays advisory (never FAIL). Read/parse failures degrade to
// WARN so a stalled source cannot green-light a hidden collision.
func TestCheckServerIDCollision(t *testing.T) {
	const dsn = "user:pass@tcp(db.example.com:3306)/appdb"
	derived, err := serverid.DeriveServerID(dsn)
	if err != nil {
		t.Fatalf("DeriveServerID: %v", err)
	}
	collidingID := fmt.Sprintf("%d", derived)

	tests := []struct {
		name           string
		serverID       mockSQLScalar
		wantStatus     CheckStatus
		wantDetailFrag string
	}{
		{"no collision", row("1"), StatusPass, "derived server-id"},
		{"collision with source @@server_id", row(collidingID), StatusWarn, "equals the source's own @@server_id"},
		{"read error", errResp("driver: bad connection"), StatusWarn, "could not read @@server_id"},
		{"unparseable server_id", row("not-a-number"), StatusWarn, "could not parse @@server_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			exp := mock.ExpectQuery("SELECT @@server_id")
			tt.serverID.apply(exp, "@@server_id")

			got := checkServerIDCollision(t.Context(), db, dsn)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if got.Status == StatusFail {
				t.Error("server-id collision is advisory — the check must never FAIL")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// TestCheckServerIDCollisionBadDSN covers the DeriveServerID parse-error branch:
// an unparseable --source-dsn cannot reach @@server_id, so the check WARNs
// without issuing a query.
func TestCheckServerIDCollisionBadDSN(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	got := checkServerIDCollision(t.Context(), db, "not-a-dsn")
	if got.Status != StatusWarn {
		t.Errorf("Status = %q, want %q", got.Status, StatusWarn)
	}
	if !strings.Contains(got.Detail, "could not derive replication server-id") {
		t.Errorf("Detail = %q, want derive-error substring", got.Detail)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestCheckSyncBinlog pins the source-side sync_binlog advisory (#814):
// PASS at sync_binlog=1, WARN (never FAIL) at any other value or on a read
// error — a source crash-tail loss is a source-operator tradeoff bintrail can
// only surface, not block on.
func TestCheckSyncBinlog(t *testing.T) {
	tests := []struct {
		name            string
		resp            mockSQLScalar
		wantStatus      CheckStatus
		wantDetailFrag  string
		wantRemediation bool
	}{
		{name: "sync_binlog=1", resp: row("1"), wantStatus: StatusPass, wantDetailFrag: "sync_binlog=1"},
		{name: "sync_binlog=0", resp: row("0"), wantStatus: StatusWarn, wantDetailFrag: "sync_binlog=0", wantRemediation: true},
		{name: "sync_binlog=100", resp: row("100"), wantStatus: StatusWarn, wantDetailFrag: "sync_binlog=100", wantRemediation: true},
		{name: "query error", resp: errResp("conn lost"), wantStatus: StatusWarn, wantDetailFrag: "could not read"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			exp := mock.ExpectQuery("SELECT @@sync_binlog")
			tt.resp.apply(exp, "@@sync_binlog")

			got := checkSyncBinlog(t.Context(), db)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if tt.wantRemediation && got.Remediation == "" {
				t.Error("expected remediation but got none")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// TestCheckSourceIndexColocation pins the #978 advisory check: WARN when the
// source and index resolve to the same host:port, PASS when they differ, and
// a graceful SKIP (never a panic or an invented FAIL) on an unparseable DSN.
func TestCheckSourceIndexColocation(t *testing.T) {
	tests := []struct {
		name           string
		sourceDSN      string
		indexDSN       string
		wantStatus     CheckStatus
		wantDetailFrag string
	}{
		{
			name:           "same host:port",
			sourceDSN:      "dbtrail:pass@tcp(127.0.0.1:3306)/",
			indexDSN:       "root:secret@tcp(127.0.0.1:3306)/binlog_index",
			wantStatus:     StatusWarn,
			wantDetailFrag: "share a disk",
		},
		{
			name:           "different host",
			sourceDSN:      "dbtrail:pass@tcp(source.example.com:3306)/",
			indexDSN:       "root:secret@tcp(index.example.com:3306)/binlog_index",
			wantStatus:     StatusPass,
			wantDetailFrag: "separate hosts",
		},
		{
			name:           "same host, different port",
			sourceDSN:      "dbtrail:pass@tcp(127.0.0.1:3306)/",
			indexDSN:       "root:secret@tcp(127.0.0.1:3307)/binlog_index",
			wantStatus:     StatusPass,
			wantDetailFrag: "separate hosts",
		},
		{
			name:           "same host, different case",
			sourceDSN:      "dbtrail:pass@tcp(DB.EXAMPLE.COM:3306)/",
			indexDSN:       "root:secret@tcp(db.example.com:3306)/binlog_index",
			wantStatus:     StatusWarn,
			wantDetailFrag: "share a disk",
		},
		{
			name:           "unparseable source DSN",
			sourceDSN:      "not-a-dsn",
			indexDSN:       "root:secret@tcp(127.0.0.1:3306)/binlog_index",
			wantStatus:     StatusSkip,
			wantDetailFrag: "could not parse --source-dsn",
		},
		{
			name:           "unparseable index DSN",
			sourceDSN:      "dbtrail:pass@tcp(127.0.0.1:3306)/",
			indexDSN:       "not-a-dsn",
			wantStatus:     StatusSkip,
			wantDetailFrag: "could not parse --index-dsn",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checkSourceIndexColocation(tt.sourceDSN, tt.indexDSN)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if got.Status == StatusFail {
				t.Error("source/index colocation is advisory — the check must never FAIL")
			}
		})
	}
}

// TestCheckStatementCapture pins the #699 advisory check: PASS when the
// source logs statement text, WARN (never FAIL) when it doesn't, MariaDB
// fallback probe when the MySQL variable is absent, SKIP when neither exists.
func TestCheckStatementCapture(t *testing.T) {
	tests := []struct {
		name            string
		mysqlVar        mockSQLScalar  // SELECT @@binlog_rows_query_log_events
		mariaVar        *mockSQLScalar // SELECT @@binlog_annotate_row_events (only probed on a 1193 from mysqlVar)
		wantStatus      CheckStatus
		wantDetailFrag  string
		wantRemediation bool
	}{
		{"mysql ON", row("1"), nil, StatusPass, "binlog_rows_query_log_events=ON", false},
		{"mysql OFF", row("0"), nil, StatusWarn, "binlog_rows_query_log_events=OFF", true},
		{"mariadb ON", mysqlErrResp(1193, "Unknown system variable 'binlog_rows_query_log_events'"), ptr(row("1")), StatusPass, "binlog_annotate_row_events=ON", false},
		{"mariadb OFF", mysqlErrResp(1193, "Unknown system variable 'binlog_rows_query_log_events'"), ptr(row("OFF")), StatusWarn, "binlog_annotate_row_events=OFF", true},
		{"neither variable", mysqlErrResp(1193, "Unknown system variable"), ptr(mysqlErrResp(1193, "Unknown system variable")), StatusSkip, "neither", false},
		// A non-1193 failure is a READ problem, not a flavor fact — the real
		// error must surface instead of a fabricated "not available" claim.
		{"transient read failure", errResp("driver: bad connection"), nil, StatusWarn, "could not read binlog_rows_query_log_events: driver: bad connection", false},
		{"mariadb probe read failure", mysqlErrResp(1193, "Unknown system variable"), ptr(errResp("driver: bad connection")), StatusWarn, "could not read binlog_annotate_row_events: driver: bad connection", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			exp := mock.ExpectQuery("SELECT @@binlog_rows_query_log_events")
			tt.mysqlVar.apply(exp, "@@binlog_rows_query_log_events")
			if tt.mariaVar != nil {
				mexp := mock.ExpectQuery("SELECT @@binlog_annotate_row_events")
				tt.mariaVar.apply(mexp, "@@binlog_annotate_row_events")
			}

			got := checkStatementCapture(t.Context(), db)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if tt.wantRemediation && got.Remediation == "" {
				t.Error("expected remediation but got none")
			}
			// Enabling is NOT retroactive (#1437): only events written after
			// the change carry text, so a remediation that omits this sends
			// the operator to re-query a window that can never match.
			if tt.wantRemediation && !strings.Contains(got.Remediation, "Only changes made after this carry it.") {
				t.Errorf("Remediation = %q, missing the non-retroactivity caveat", got.Remediation)
			}
			if got.Status == StatusFail {
				t.Error("statement capture is optional — the check must never FAIL")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

// TestCheckBinlogRowValueOptions pins the #777 advisory check: PASS on the
// empty default, WARN (never FAIL) on PARTIAL_JSON, SKIP when the variable is
// absent (MySQL <8.0 / MariaDB), WARN on a transient read failure.
func TestCheckBinlogRowValueOptions(t *testing.T) {
	tests := []struct {
		name            string
		probe           mockSQLScalar
		wantStatus      CheckStatus
		wantDetailFrag  string
		wantRemediation bool
	}{
		{"default empty", row(""), StatusPass, "full JSON row images", false},
		{"whitespace only treated as empty", row("  "), StatusPass, "full JSON row images", false},
		{"PARTIAL_JSON", row("PARTIAL_JSON"), StatusWarn, `binlog_row_value_options="PARTIAL_JSON"`, true},
		{"unrecognized non-empty value warns, never PASS", row("SOMETHING_NEW"), StatusWarn, "is unrecognized", true},
		{"absent variable", mysqlErrResp(1193, "Unknown system variable 'binlog_row_value_options'"), StatusSkip, "not available", false},
		{"transient read failure", errResp("driver: bad connection"), StatusWarn, "could not read binlog_row_value_options: driver: bad connection", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			exp := mock.ExpectQuery("SELECT @@binlog_row_value_options")
			tt.probe.apply(exp, "@@binlog_row_value_options")

			got := checkBinlogRowValueOptions(t.Context(), db)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if tt.wantRemediation && got.Remediation == "" {
				t.Error("expected remediation but got none")
			}
			if got.Status == StatusFail {
				t.Error("binlog_row_value_options is optional — the check must never FAIL")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// TestSourceChecksHonorContext pins #813: the source-side probes run their
// queries under the caller's context, so a stalled source cannot hang doctor
// indefinitely. With an already-canceled context and a query rigged to block,
// checkLogBin returns at once (FAIL) instead of waiting on the socket — before
// the fix these checks used QueryRow (no context) and would block on the delay.
func TestSourceChecksHonorContext(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@log_bin").
		WillDelayFor(time.Hour).
		WillReturnRows(sqlmock.NewRows([]string{"@@log_bin"}).AddRow("1"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled: QueryRowContext must return ctx.Err() at once

	done := make(chan CheckResult, 1)
	go func() { done <- checkLogBin(ctx, db) }()
	select {
	case got := <-done:
		if got.Status != StatusFail {
			t.Errorf("expected FAIL on a canceled context, got %q (detail=%q)", got.Status, got.Detail)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("checkLogBin ignored the canceled context and blocked on the delayed query — a stalled source would hang doctor")
	}
}

// TestCheckRowMetadata pins the #700 advisory check: PASS on FULL, WARN
// (never FAIL) on MINIMAL, SKIP when the variable does not exist.
func TestCheckRowMetadata(t *testing.T) {
	tests := []struct {
		name            string
		probe           mockSQLScalar
		wantStatus      CheckStatus
		wantDetailFrag  string
		wantRemediation bool
	}{
		{"FULL", row("FULL"), StatusPass, "binlog_row_metadata=FULL", false},
		{"MINIMAL", row("MINIMAL"), StatusWarn, "binlog_row_metadata=MINIMAL", true},
		{"MariaDB NO_LOG default", row("NO_LOG"), StatusWarn, "binlog_row_metadata=NO_LOG", true},
		{"variable absent", mysqlErrResp(1193, "Unknown system variable 'binlog_row_metadata'"), StatusSkip, "not available", false},
		{"transient read failure", errResp("driver: bad connection"), StatusWarn, "could not read binlog_row_metadata: driver: bad connection", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			exp := mock.ExpectQuery("SELECT @@binlog_row_metadata")
			tt.probe.apply(exp, "@@binlog_row_metadata")

			got := checkRowMetadata(t.Context(), db)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if tt.wantRemediation && got.Remediation == "" {
				t.Error("expected remediation but got none")
			}
			if got.Status == StatusFail {
				t.Error("binlog_row_metadata is optional — the check must never FAIL")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// mockSQLScalar is a single-value SELECT mock — value xor err.
type mockSQLScalar struct {
	value string
	err   error
}

func row(v string) mockSQLScalar       { return mockSQLScalar{value: v} }
func errResp(msg string) mockSQLScalar { return mockSQLScalar{err: errors.New(msg)} }

// mysqlErrResp mocks a typed MySQL server error (e.g. 1193 unknown system
// variable) so checks that discriminate by error number can be exercised.
func mysqlErrResp(number uint16, msg string) mockSQLScalar {
	return mockSQLScalar{err: &mysql.MySQLError{Number: number, Message: msg}}
}

func (r mockSQLScalar) apply(exp *sqlmock.ExpectedQuery, col string) {
	if r.err != nil {
		exp.WillReturnError(r.err)
		return
	}
	exp.WillReturnRows(sqlmock.NewRows([]string{col}).AddRow(r.value))
}

// TestCheckIndexWriteAccessOnNeverDropsPreexistingDB is the data-loss guard
// for #384's probe cleanup: when the database PRE-EXISTS (SCHEMATA finds it),
// no DROP DATABASE may ever be issued. The trick: register a DROP DATABASE
// expectation and assert ExpectationsWereMet() ERRORS with it unfulfilled —
// a spurious drop would fulfil it and turn this test red. (The production
// dropErr is swallowed into slog.Warn, so an unexpected-call error from
// sqlmock would otherwise be invisible to assertions.)
func TestCheckIndexWriteAccessOnNeverDropsPreexistingDB(t *testing.T) {
	const dbName = "binlog_index"
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow(dbName))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DROP TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	// Sentinel: must remain UNFULFILLED.
	mock.ExpectExec("DROP DATABASE").WillReturnResult(sqlmock.NewResult(0, 0))

	got := checkIndexWriteAccessOn(t.Context(), db, dbName)
	if got.Status != StatusPass {
		t.Fatalf("Status = %q, want pass (detail=%q)", got.Status, got.Detail)
	}
	err = mock.ExpectationsWereMet()
	if err == nil {
		t.Fatal("DROP DATABASE expectation was fulfilled — a pre-existing database was dropped by the probe")
	}
	if !strings.Contains(err.Error(), "DROP DATABASE") {
		t.Fatalf("expected the unmet expectation to be DROP DATABASE, got: %v", err)
	}
}

// TestIsUnknownDatabaseErr pins the 1049 detection (#384): it must see
// through config.Connect's %w wrapping and must NOT match other MySQL
// errors or plain errors.
func TestIsUnknownDatabaseErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"wrapped 1049", fmt.Errorf("failed to ping MySQL: %w", &mysql.MySQLError{Number: 1049, Message: "Unknown database 'binlog_index'"}), true},
		{"bare 1049", &mysql.MySQLError{Number: 1049}, true},
		{"other mysql error", &mysql.MySQLError{Number: 1064}, false},
		{"plain error", errors.New("connection refused"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isUnknownDatabaseErr(tc.err); got != tc.want {
				t.Errorf("isUnknownDatabaseErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestCheckIndexWriteAccessOn(t *testing.T) {
	const dbName = "binlog_index"

	// Each subtest sets up the sqlmock expectation chain that mirrors the
	// branch under test. The probe table name is fixed in checkIndexWriteAccessOn
	// as `binlog_index`.`_bintrail_doctor_probe`.
	tests := []struct {
		name           string
		setup          func(mock sqlmock.Sqlmock)
		wantStatus     CheckStatus
		wantDetailFrag string
	}{
		{
			name: "db exists, create+drop OK",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow(dbName))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("DROP TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantStatus:     StatusPass,
			wantDetailFrag: "CREATE/DROP TABLE OK",
		},
		{
			name: "db missing, create database succeeds, then create+drop OK, probe db dropped",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				m.ExpectExec("CREATE DATABASE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("DROP TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
				// A diagnostic must not leave server state behind (#384):
				// the probe-created database is dropped via defer.
				m.ExpectExec("DROP DATABASE IF EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantStatus:     StatusPass,
			wantDetailFrag: "CREATE/DROP TABLE OK",
		},
		{
			name: "db missing, created by probe, CREATE TABLE denied — probe db still dropped",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				m.ExpectExec("CREATE DATABASE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").
					WillReturnError(errors.New("CREATE command denied"))
				// Cleanup runs on the FAIL path too (#384).
				m.ExpectExec("DROP DATABASE IF EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantStatus:     StatusFail,
			wantDetailFrag: "cannot CREATE TABLE",
		},
		{
			name: "db missing, create database denied",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				m.ExpectExec("CREATE DATABASE IF NOT EXISTS").
					WillReturnError(errors.New("Access denied for user"))
			},
			wantStatus:     StatusFail,
			wantDetailFrag: "cannot CREATE DATABASE",
		},
		{
			name: "db exists, create table denied",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow(dbName))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").
					WillReturnError(errors.New("CREATE command denied"))
			},
			wantStatus:     StatusFail,
			wantDetailFrag: "cannot CREATE TABLE",
		},
		{
			name: "create OK but drop denied — must FAIL (catches partition-rotate bites at runtime)",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow(dbName))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("DROP TABLE").WillReturnError(errors.New("DROP command denied"))
			},
			wantStatus:     StatusFail,
			wantDetailFrag: "user has CREATE but not DROP",
		},
		{
			name: "schemata query errors",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnError(errors.New("conn lost"))
			},
			wantStatus:     StatusFail,
			wantDetailFrag: "conn lost",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			tt.setup(mock)

			got := checkIndexWriteAccessOn(t.Context(), db, dbName)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if tt.wantStatus == StatusFail && got.Remediation == "" {
				t.Error("FAIL outcome with no remediation breaks doctor's promise")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

func TestDoctorReportWriteText(t *testing.T) {
	// Text formatter emits a specific status glyph per status. Any UI scraper
	// or grep workflow depends on this contract — assert per-glyph.
	r := &Report{}
	r.add(CheckResult{Name: "p", Status: StatusPass, Detail: "ok"})
	r.add(CheckResult{Name: "f", Status: StatusFail, Detail: "bad", Remediation: "fix it"})
	r.add(CheckResult{Name: "w", Status: StatusWarn, Detail: "meh"})
	r.add(CheckResult{Name: "s", Status: StatusSkip, Detail: "n/a"})

	var buf bytes.Buffer
	if err := r.Write(&buf, "text"); err != nil {
		t.Fatalf("Write(text): %v", err)
	}
	out := buf.String()
	for _, want := range []string{"✓ p", "✗ f", "! w", "- s", "    fix it"} {
		if !strings.Contains(out, want) {
			t.Errorf("text output missing %q\n--- output ---\n%s", want, out)
		}
	}
}

// TestDoctorReportFooter pins the parameterized text footer (#532): a non-MySQL
// caller (e.g. bintrail-pg doctor) can override the trailing guidance, and an empty
// footer falls back to the default MySQL-oriented text — so existing callers (and the
// JSON output) are unaffected.
func TestDoctorReportFooter(t *testing.T) {
	render := func(r *Report) string {
		var buf bytes.Buffer
		if err := r.Write(&buf, "text"); err != nil {
			t.Fatalf("Write: %v", err)
		}
		return buf.String()
	}

	// Custom footers: all-passed → ReadyFooter; has-failure → FixFooter.
	passed := &Report{ReadyFooter: "CUSTOM READY", FixFooter: "CUSTOM FIX"}
	passed.Add(CheckResult{Name: "p", Status: StatusPass})
	if out := render(passed); !strings.Contains(out, "CUSTOM READY") || strings.Contains(out, "Ready to stream") {
		t.Errorf("all-passed should render the custom ReadyFooter, not the default:\n%s", out)
	}
	failed := &Report{ReadyFooter: "CUSTOM READY", FixFooter: "CUSTOM FIX"}
	failed.Add(CheckResult{Name: "f", Status: StatusFail})
	if out := render(failed); !strings.Contains(out, "CUSTOM FIX") {
		t.Errorf("has-failure should render the custom FixFooter:\n%s", out)
	}

	// Empty footers → the default MySQL guidance (back-compat for existing callers).
	def := &Report{}
	def.Add(CheckResult{Name: "p", Status: StatusPass})
	if out := render(def); !strings.Contains(out, "Ready to stream. Run `bintrail up") {
		t.Errorf("empty ReadyFooter should fall back to the MySQL default:\n%s", out)
	}
	defFail := &Report{}
	defFail.Add(CheckResult{Name: "f", Status: StatusFail})
	if out := render(defFail); !strings.Contains(out, "re-run `bintrail doctor`") {
		t.Errorf("empty FixFooter should fall back to the MySQL default:\n%s", out)
	}

	// The footer fields must not leak into JSON.
	var jbuf bytes.Buffer
	if err := passed.Write(&jbuf, "json"); err != nil {
		t.Fatalf("Write(json): %v", err)
	}
	if strings.Contains(jbuf.String(), "CUSTOM") {
		t.Errorf("footer fields leaked into JSON:\n%s", jbuf.String())
	}
}

// TestCheckSchemaVisibility_emptyVsInvisible pins the #402 discrimination:
// zero tables because the schema is EMPTY routes the operator to "create a
// table", zero tables because the schema is INVISIBLE routes to grants — the
// wrong remediation sends a 3am operator down the wrong path.
func TestCheckSchemaVisibility_emptyVsInvisible(t *testing.T) {
	ctx := t.Context()
	countRows := func(tables, schemas int) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"COUNT(*)", "COUNT(DISTINCT TABLE_SCHEMA)"}).AddRow(tables, schemas)
	}
	schemataRows := func(n int) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(n)
	}

	// wantName is a LITERAL on purpose: PreflightError.TelemetryClass keys on
	// the check's name (#1503), so the producer's bytes are pinned here, not
	// the constant it happens to share with the classifier.
	cases := []struct {
		name            string
		setup           func(sqlmock.Sqlmock)
		wantName        string
		wantStatus      CheckStatus
		wantDetail      string
		wantRemediation string
	}{
		{
			name: "schema exists but is empty → create-a-table",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT COUNT").WillReturnRows(countRows(0, 0))
				m.ExpectQuery("SELECT COUNT").WillReturnRows(schemataRows(1))
			},
			wantName:        "Schema visibility",
			wantStatus:      StatusFail,
			wantDetail:      "no tables yet",
			wantRemediation: "Create at least one table",
		},
		{
			name: "schema invisible → grants",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT COUNT").WillReturnRows(countRows(0, 0))
				m.ExpectQuery("SELECT COUNT").WillReturnRows(schemataRows(0))
			},
			wantName:        "Schema access",
			wantStatus:      StatusFail,
			wantDetail:      "no tables visible",
			wantRemediation: "GRANT SELECT",
		},
		{
			// The probe error is NOT a verified grants problem: the name
			// stays "Schema visibility" (config_invalid for telemetry, not
			// db_permission) and the error is shown, not swallowed.
			name: "SCHEMATA probe fails → degrade to grants, never crash",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT COUNT").WillReturnRows(countRows(0, 0))
				m.ExpectQuery("SELECT COUNT").WillReturnError(errors.New("denied"))
			},
			wantName:        "Schema visibility",
			wantStatus:      StatusFail,
			wantDetail:      "schema probe failed: denied",
			wantRemediation: "GRANT SELECT",
		},
		{
			name: "tables visible → pass",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT COUNT").WillReturnRows(countRows(5, 2))
			},
			wantName:   "Schema visibility",
			wantStatus: StatusPass,
			wantDetail: "5 tables across 2 schemas",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			c.setup(mock)
			got := checkSchemaVisibility(ctx, db, []string{"shop"})
			if got.Name != c.wantName {
				t.Errorf("Name = %q, want %q", got.Name, c.wantName)
			}
			if got.Status != c.wantStatus {
				t.Errorf("status = %q, want %q (detail=%q)", got.Status, c.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, c.wantDetail) {
				t.Errorf("Detail = %q, want it to contain %q", got.Detail, c.wantDetail)
			}
			if c.wantRemediation != "" && !strings.Contains(got.Remediation, c.wantRemediation) {
				t.Errorf("Remediation = %q, want it to contain %q", got.Remediation, c.wantRemediation)
			}
		})
	}
}
