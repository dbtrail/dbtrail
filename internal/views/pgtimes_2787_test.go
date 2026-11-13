package views

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2787: time, timetz and interval columns of a PostgreSQL source, each
// expression run on the engine over the text PostgreSQL writes.

func pgColumnSQL(t *testing.T, as string) string {
	t.Helper()
	parts := pgParts(BaselineTable{Schema: "public", Table: "t", PGCasts: []baseline.PGCast{{Name: "c", As: as}}})
	if len(parts) != 1 {
		t.Fatalf("pgParts gave %d parts", len(parts))
	}
	return parts[0].expr
}

func TestPGTimeAndInterval_readWhatPostgresWrites(t *testing.T) {
	db := openDuck(t)
	cases := []struct {
		as, text, want string
	}{
		{"TIME", "10:00:00", "10:00:00"},
		{"TIME", "00:00:00", "00:00:00"},
		{"TIME", "23:59:59.999999", "23:59:59.999999"},
		{"TIME", "12:00:00.5", "12:00:00.5"},
		{"TIMETZ", "10:00:00+00", "10:00:00+00"},
		{"TIMETZ", "08:30:00-05:30", "08:30:00-05:30"},
		{"TIMETZ", "00:00:00+14", "00:00:00+14"},
		{"TIMETZ", "24:00:00+00", "24:00:00+00"},
		{"TIMETZ", "24:00:00-05", "24:00:00-05"},
		{"INTERVAL", "00:00:00", "00:00:00"},
		{"INTERVAL", "1 year 2 mons 3 days 04:05:06.789", "1 year 2 months 3 days 04:05:06.789"},
		{"INTERVAL", "1 mon", "1 month"},
		{"INTERVAL", "-1 mons", "-1 month"},
		// A plus on a part that follows a negative one.
		{"INTERVAL", "-1 years -2 mons +3 days -04:05:06", "-1 year -2 months 3 days -04:05:06"},
		{"INTERVAL", "-3 days +01:00:00", "-3 days 01:00:00"},
		{"INTERVAL", "3 days -01:00:00", "3 days -01:00:00"},
		{"INTERVAL", "-00:00:00.5", "-00:00:00.5"},
		{"INTERVAL", "100:00:00", "100:00:00"},
		{"INTERVAL", "178000000 years", "178000000 years"},
	}
	for _, c := range cases {
		var got sql.NullString
		q := "SELECT CAST(" + pgColumnSQL(t, c.as) + " AS VARCHAR) FROM (SELECT ? AS c) t"
		if err := db.QueryRow(q, c.text).Scan(&got); err != nil {
			t.Errorf("%s %s: %v", c.as, c.text, err)
			continue
		}
		if got.String != c.want {
			t.Errorf("%s %s: %q, want %q", c.as, c.text, got.String, c.want)
		}
	}
}

// The parts of an interval are what PostgreSQL holds: months, days and
// microseconds, each with its own sign.
func TestPGInterval_keepsItsParts(t *testing.T) {
	db := openDuck(t)
	var months, days, micros int64
	q := "SELECT datepart('year', x) * 12 + datepart('month', x), datepart('day', x), epoch_us(x - to_months(CAST(datepart('year', x) * 12 + datepart('month', x) AS INTEGER)) - to_days(CAST(datepart('day', x) AS INTEGER))) FROM (SELECT " +
		pgColumnSQL(t, "INTERVAL") + " AS x FROM (SELECT '-1 years -2 mons +3 days -04:05:06' AS c) t) u"
	if err := db.QueryRow(q).Scan(&months, &days, &micros); err != nil {
		t.Fatal(err)
	}
	if months != -14 || days != 3 || micros != -(4*3600+5*60+6)*1_000_000 {
		t.Errorf("months %d, days %d, micros %d", months, days, micros)
	}
}

func TestPGTimeAndInterval_nullStaysNull(t *testing.T) {
	db := openDuck(t)
	for _, as := range []string{"TIME", "TIMETZ", "INTERVAL"} {
		var isNull bool
		var typ string
		q := "SELECT x IS NULL, typeof(x) FROM (SELECT " + pgColumnSQL(t, as) + " AS x FROM (SELECT CAST(NULL AS VARCHAR) AS c) t) u"
		if err := db.QueryRow(q).Scan(&isNull, &typ); err != nil {
			t.Fatalf("%s: %v", as, err)
		}
		want := as
		if as == "TIMETZ" {
			want = "TIME WITH TIME ZONE"
		}
		if !isNull || typ != want {
			t.Errorf("%s: is null %v, type %s", as, isNull, typ)
		}
	}
}

// 24:00:00 is a time of day in PostgreSQL. The copy's surfaces would show a
// TIME of it as midnight with no error (internal/sqlsandbox pins that), so
// the view refuses it; and what is not a time or an interval at all is the
// engine's to refuse.
func TestPGTimeAndInterval_refuseWhatTheyCannotHold(t *testing.T) {
	db := openDuck(t)
	for _, c := range []struct {
		as, text, says string
	}{
		{"TIME", "24:00:00", "public.t: column c holds the time 24:00:00"},
		{"TIME", "noon", ""},
		{"INTERVAL", "infinity", ""},
		{"INTERVAL", "-infinity", ""},
		{"INTERVAL", "1 fortnight", ""},
	} {
		var got sql.NullString
		err := db.QueryRow("SELECT CAST("+pgColumnSQL(t, c.as)+" AS VARCHAR) FROM (SELECT ? AS c) t", c.text).Scan(&got)
		if err == nil {
			t.Errorf("%s %s was read as %q", c.as, c.text, got.String)
			continue
		}
		if !strings.Contains(err.Error(), c.says) {
			t.Errorf("%s %s: %v, want it to say %q", c.as, c.text, err, c.says)
		}
	}
}
