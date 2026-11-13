package console

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2611: under a named zone the now-family is MySQL's; UTC and fixed offsets
// stay on the copy, a statement without a call is not refused for it, and a
// column CURDATE() is compared with must be a date of the tables read.
func TestSQLNowRefusalFor(t *testing.T) {
	in := views.Input{Baselines: []views.BaselineTable{
		{Schema: "s", Table: "ev", Path: "/c/s/ev.parquet", SchemaKnown: true,
			Columns:  []string{"id", "d", "dt", "ts", "note"},
			Temporal: []baseline.TemporalColumn{{Name: "d", Type: "date"}, {Name: "dt", Type: "datetime"}, {Name: "ts", Type: "timestamp"}}},
		{Schema: "s", Table: "log", Path: "/c/s/log.parquet", SchemaKnown: true,
			Columns: []string{"id", "d"}}, // d is text here
		{Schema: "s", Table: "raw", Path: "/c/s/raw.parquet",
			Columns: []string{"d"}},
	}}
	only := func(names ...string) views.Input {
		var refs sqlsandbox.Refs
		for _, n := range names {
			refs.Tables = append(refs.Tables, sqlsandbox.TableRef{Schema: "s", Name: n})
		}
		narrowed := in
		narrowed.OnlyViews = sqlWantedViews(in, refs)
		return narrowed
	}
	for _, c := range []struct {
		stmt, zone string
		tables     []string
		refused    bool
	}{
		{"SELECT * FROM ev WHERE ts >= NOW() - INTERVAL 1 DAY", "", []string{"ev"}, false},
		{"SELECT * FROM ev WHERE ts >= NOW() - INTERVAL 1 DAY", "Etc/GMT+5", []string{"ev"}, false},
		{"SELECT * FROM ev WHERE ts >= NOW() - INTERVAL 1 DAY", "America/New_York", []string{"ev"}, true},
		{"SELECT * FROM ev WHERE ts >= NOW(/**/) - INTERVAL 1 DAY", "America/New_York", []string{"ev"}, true},
		{"SELECT curdate ( )", "America/Bogota", nil, true},
		{"SELECT * FROM ev", "America/New_York", []string{"ev"}, false},
		{"SELECT `now` FROM ev", "America/New_York", []string{"ev"}, false},
		{"SELECT * FROM ev WHERE ev.d >= CURDATE() - INTERVAL 7 DAY AND UTC_DATE() > dt", "", []string{"ev"}, false},
		{"SELECT * FROM ev WHERE ts BETWEEN CURDATE() - INTERVAL 7 DAY AND CURDATE()", "", []string{"ev"}, false},
		{"SELECT * FROM ev WHERE note = CURDATE()", "", []string{"ev"}, true},
		{"SELECT * FROM ev WHERE id = CURDATE()", "", []string{"ev"}, true},
		{"SELECT * FROM ev WHERE missing = CURDATE()", "", []string{"ev"}, true},
		{"SELECT * FROM ev JOIN log USING (id) WHERE d = CURDATE()", "", []string{"ev", "log"}, true},
		{"SELECT * FROM raw WHERE d = CURDATE()", "", []string{"raw"}, true},
		// The keywords the router writes as NOW() and CURDATE() are held to
		// the same refusals.
		{"SELECT * FROM ev WHERE ts >= CURRENT_TIMESTAMP - INTERVAL 1 DAY", "America/New_York", []string{"ev"}, true},
		{"SELECT * FROM ev WHERE ts >= LOCALTIMESTAMP - INTERVAL 1 DAY", "Etc/GMT+5", []string{"ev"}, false},
		{"SELECT * FROM ev WHERE d >= CURRENT_DATE - INTERVAL 7 DAY", "", []string{"ev"}, false},
		{"SELECT * FROM ev WHERE note = CURRENT_DATE", "", []string{"ev"}, true},
		{"SELECT * FROM log WHERE d >= CURRENT_DATE() - INTERVAL 7 DAY", "", []string{"log"}, true},
	} {
		if got := sqlNowRefusalFor(only(c.tables...), c.stmt, readrouter.ShapeOf(c.stmt), c.zone); (got != "") != c.refused {
			t.Errorf("sqlNowRefusalFor(%q, %q) = %q, want refused=%v", c.stmt, c.zone, got, c.refused)
		}
	}
}

// Through runSQL and the real worker: the refusals above are the ones a
// caller that asked for MySQL's answer meets, and the copy answers the rest
// with MySQL's clock functions.
// With no reading of the statement, one that may call the four is refused.
func TestSQLNowRefusalFor_noShape(t *testing.T) {
	if sqlNowRefusalFor(views.Input{}, "SELECT now()", nil, "") == "" {
		t.Error("no reading, NOW(): not refused")
	}
	if sqlNowRefusalFor(views.Input{}, "SELECT CURRENT_DATE", nil, "") == "" {
		t.Error("no reading, CURRENT_DATE: not refused")
	}
	if got := sqlNowRefusalFor(views.Input{}, "SELECT 1", nil, ""); got != "" {
		t.Errorf("no reading, no call: %q", got)
	}
}

func TestSQL_realWorkerNow_2611(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLStarTable(t, f.root, "ev", "CREATE TABLE `ev` (\n  `id` int NOT NULL,\n  `created_on` date DEFAULT NULL,\n"+
		"  `note` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "2026-01-01", "2026-01-01 10:00:00"})
	for _, c := range []struct {
		stmt, zone, refused string
	}{
		{"SELECT id FROM shop.ev WHERE created_on >= CURDATE() - INTERVAL 7 DAY", "", ""},
		{"SELECT id FROM shop.ev WHERE created_on >= CURDATE() - INTERVAL 7 DAY", "Etc/GMT-5", ""},
		{"SELECT id FROM shop.ev WHERE created_on >= CURDATE() - INTERVAL 7 DAY", "America/New_York", "named time zone"},
		{"SELECT id FROM shop.ev WHERE note = CURDATE()", "", "compared with note"},
	} {
		sess := sqlsandbox.Session{StrictStar: true, Types: readrouter.ShapeOf(c.stmt), TimeZone: c.zone, MySQLClock: true}
		_, err := f.s.runSQL(context.Background(), f.s.cm.boot, "u", c.stmt, "", 0, sess)
		var refusal *sqlStarRefusal
		switch {
		case c.refused == "" && err != nil:
			t.Errorf("%s [%s]: %v, want the copy's answer", c.stmt, c.zone, err)
		case c.refused != "" && (!errors.As(err, &refusal) || !strings.Contains(refusal.Message, c.refused)):
			t.Errorf("%s [%s]: err = %v, want a refusal saying %q", c.stmt, c.zone, err, c.refused)
		}
	}
}
