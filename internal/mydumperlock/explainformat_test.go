package mydumperlock

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// #2211: what the source answers about explain_format.
func TestSourceExplainFormat(t *testing.T) {
	const q = "SHOW SESSION VARIABLES WHERE Variable_name = 'explain_format'"
	cases := []struct {
		name        string
		rows        *sqlmock.Rows
		err         error
		wantValue   string
		wantPresent bool
		wantErr     bool
	}{
		{name: "tree", rows: sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("explain_format", "TREE"), wantValue: "TREE", wantPresent: true},
		{name: "traditional", rows: sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("explain_format", "TRADITIONAL"), wantValue: "TRADITIONAL", wantPresent: true},
		// MariaDB and MySQL before 8.0.32 have no such variable: no row.
		{name: "no such variable", rows: sqlmock.NewRows([]string{"Variable_name", "Value"})},
		{name: "query fails", err: errors.New("connection reset"), wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			e := mock.ExpectQuery(q)
			if c.err != nil {
				e.WillReturnError(c.err)
			} else {
				e.WillReturnRows(c.rows)
			}
			v, present, err := SourceExplainFormat(context.Background(), db)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, want error %v", err, c.wantErr)
			}
			if v != c.wantValue || present != c.wantPresent {
				t.Fatalf("got (%q, %v), want (%q, %v)", v, present, c.wantValue, c.wantPresent)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPlanExplainFormat(t *testing.T) {
	modern := Version{1, 0, 3}
	cases := []struct {
		name       string
		present    bool
		value      string
		known      bool
		build      Version
		wantSend   bool
		wantWarnIn string // "" = no warning
		wantRemedy string
	}{
		{name: "tree, modern build", present: true, value: "TREE", known: true, build: modern, wantSend: true},
		{name: "json, modern build", present: true, value: "JSON", known: true, build: modern, wantSend: true},
		// Sent on TRADITIONAL too: it changes nothing there, and the session
		// value is not re-read between this check and the dump.
		{name: "traditional, modern build", present: true, value: "TRADITIONAL", known: true, build: modern, wantSend: true},
		{name: "at the floor", present: true, value: "TREE", known: true, build: Version{0, 16, 1}, wantSend: true},
		{name: "variable absent", present: false, known: true, build: modern},
		{name: "variable absent, old build", present: false, known: true, build: Version{0, 10, 0}},
		{name: "below the floor, tree", present: true, value: "TREE", known: true, build: Version{0, 16, 0}, wantWarnIn: "(0.16.0) is older than 0.16.1", wantRemedy: "install mydumper 0.16.1 or newer"},
		{name: "below the floor, lower-case tree with spaces", present: true, value: " tree ", known: true, build: Version{0, 10, 0}, wantWarnIn: "explain_format=tree,", wantRemedy: "install mydumper 0.16.1 or newer"},
		{name: "below the floor, traditional any case", present: true, value: "traditional", known: true, build: Version{0, 10, 0}},
		// An unreadable version may be a modern build: telling it to install
		// a newer one would be false.
		{name: "unknown build, tree", present: true, value: "TREE", known: false, wantWarnIn: "version could not be read", wantRemedy: "use a mydumper whose --version this release can read"},
		{name: "unknown build, traditional", present: true, value: "TRADITIONAL", known: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := PlanExplainFormat(c.present, c.value, c.known, c.build)
			if p.Send != c.wantSend {
				t.Fatalf("Send = %v, want %v", p.Send, c.wantSend)
			}
			if c.wantWarnIn == "" {
				if p.Warning != "" {
					t.Fatalf("unexpected warning: %s", p.Warning)
				}
				return
			}
			for _, w := range []string{c.wantWarnIn, c.wantRemedy, "or set explain_format=TRADITIONAL on the source"} {
				if !strings.Contains(p.Warning, w) {
					t.Fatalf("warning %q, want it to hold %q", p.Warning, w)
				}
			}
			if !c.known && strings.Contains(p.Warning, "install mydumper") {
				t.Fatalf("warning %q tells a build of unknown version to install a newer one", p.Warning)
			}
		})
	}
}

// The section is what mydumper reads: its own group name, the variable, the
// value. A typo in any of them is silently ignored by mydumper and the dump
// crashes again.
func TestExplainFormatSection(t *testing.T) {
	if ExplainFormatSection != "[mydumper_session_variables]\nexplain_format = TRADITIONAL\n" {
		t.Fatalf("section changed: %q", ExplainFormatSection)
	}
}
