package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// DrillTarget is the scratch server a drill loads its rehearsal into. MySQL
// is built in; PostgreSQL is set by the binary that links its driver
// (SetPostgresDrillTarget), so this package never imports one.
type DrillTarget interface {
	// OutputFormat is the reconstruct output this target loads.
	OutputFormat() string
	// Empty refuses a target that already holds anything in one of the
	// drill's schemas.
	Empty(ctx context.Context, schemas []string) error
	// Load applies one table's reconstructed files.
	Load(ctx context.Context, outDir string, rep *reconstruct.TableReport) error
	// Count is the number of rows the loaded table holds.
	Count(ctx context.Context, schema, table string) (int64, error)
	Close() error
}

var postgresDrillTarget func(ctx context.Context, dsn string) (DrillTarget, error)

// SetPostgresDrillTarget gives drill a way to open a postgres:// target.
// Called from an init() of the binary that links the PostgreSQL driver.
func SetPostgresDrillTarget(open func(ctx context.Context, dsn string) (DrillTarget, error)) {
	postgresDrillTarget = open
}

// drillTargetIsPostgres says whether --target-dsn names a PostgreSQL server.
// Only the URL form is recognized; anything else is a MySQL DSN.
func drillTargetIsPostgres(dsn string) bool {
	l := strings.ToLower(strings.TrimSpace(dsn))
	return strings.HasPrefix(l, "postgres://") || strings.HasPrefix(l, "postgresql://")
}

func openDrillTarget(ctx context.Context, dsn string) (DrillTarget, error) {
	if drillTargetIsPostgres(dsn) {
		if postgresDrillTarget == nil {
			return nil, fmt.Errorf("--target-dsn names a PostgreSQL server, and this binary restores into MySQL only; drill a PostgreSQL source with bintrail-pg")
		}
		t, err := postgresDrillTarget(ctx, strings.TrimSpace(dsn))
		if err == nil && t == nil {
			err = fmt.Errorf("the PostgreSQL drill target opened nothing")
		}
		return t, err
	}
	tcfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid --target-dsn: %w (a MySQL DSN looks like user:pw@tcp(host:port)/; a PostgreSQL one is a URL, postgres://user:pw@host:port/db)", err)
	}
	// The dump's schema and chunk files are multi-statement scripts.
	tcfg.MultiStatements = true
	db, err := config.Connect(tcfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("connect to --target-dsn: %w", err)
	}
	return &mysqlDrillTarget{db: db}, nil
}

// drillReconstructError rewrites the reconstruct refusals that name
// --output-format, which drill picks from the target and does not take:
// what the operator changes is the kind of server --target-dsn names. Only
// when the refusal is ALL that failed (the run joins every table's error):
// beside other failures it is left as is, so they stay visible. Nil when
// there is nothing to rewrite.
func drillReconstructError(err error) error {
	switch {
	case onlyRefusal(err, reconstruct.ErrPGFullTableMydumper):
		return &drillRefusal{msg: "these tables were captured from PostgreSQL, and --target-dsn names a MySQL server; drill them into a scratch PostgreSQL with bintrail-pg (--target-dsn postgres://user:pw@host:port/db)", err: err}
	case onlyRefusal(err, reconstruct.ErrPsqlNeedsPostgres):
		return &drillRefusal{msg: "these tables were captured from MySQL, and --target-dsn names a PostgreSQL server; point it at a scratch MySQL", err: err}
	}
	return nil
}

// onlyRefusal says whether every error joined in err is target.
func onlyRefusal(err, target error) bool {
	if j, ok := err.(interface{ Unwrap() []error }); ok {
		errs := j.Unwrap()
		for _, e := range errs {
			if !onlyRefusal(e, target) {
				return false
			}
		}
		return len(errs) > 0
	}
	if u := errors.Unwrap(err); u != nil {
		return err == target || onlyRefusal(u, target)
	}
	return err == target
}

// drillRefusal says drill's own words and keeps the refusal underneath for
// errors.Is; its text is left out, since it names a flag drill has not.
type drillRefusal struct {
	msg string
	err error
}

func (r *drillRefusal) Error() string { return r.msg }
func (r *drillRefusal) Unwrap() error { return r.err }

type mysqlDrillTarget struct{ db *sql.DB }

func (m *mysqlDrillTarget) OutputFormat() string { return reconstruct.OutputFormatMydumper }

func (m *mysqlDrillTarget) Empty(ctx context.Context, schemas []string) error {
	return drillTargetEmpty(ctx, m.db, schemas)
}

func (m *mysqlDrillTarget) Load(ctx context.Context, outDir string, rep *reconstruct.TableReport) error {
	return drillLoadTable(ctx, m.db, outDir, rep)
}

func (m *mysqlDrillTarget) Count(ctx context.Context, schema, table string) (int64, error) {
	var n int64
	err := m.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+schema+"`.`"+table+"`").Scan(&n)
	return n, err
}

func (m *mysqlDrillTarget) Close() error { return m.db.Close() }
