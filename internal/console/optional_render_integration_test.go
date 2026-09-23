//go:build integration

package console

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// optionalRenderJS renders the three notices a start or a Test opens, fed a
// report the real doctor produced, and reads back what a person would see:
// the tone, the summary, and every folded section with its cards. Each Copy
// button in the optional section is pressed, and what it copied is compared
// with the statement shown above it.
const optionalRenderJS = `
FakeEl.prototype.addEventListener = function (type, fn) { (this.__h ||= {})[type] = fn; };
const copied = [];
vm.runInContext("copyText = (t) => __copy(t);", Object.assign(ctx, { __copy: (t) => copied.push(t) }));
const walk = (n, f) => { f(n); for (const c of n.children || []) if (c && c.nodeType === 1) walk(c, f); };
const text = (n) => n.textContent;
(async () => {
  const { bannedHits, countWords } = await import(process.argv[3]);
  const report = JSON.parse(process.argv[4]);
  const read = (n) => {
    const folds = [];
    for (const c of [].concat(n.content || [])) {
      if (!c || c.tag !== "details") continue;
      const cards = [];
      for (const card of c.children[1].children) {
        const pres = [], paras = [];
        walk(card, (x) => { if (x.tag === "pre") pres.push(x._text); if (x.tag === "p") paras.push(x._text); });
        const before = copied.length;
        walk(card, (x) => { if (x.tag === "button" && x._text === "Copy" && x.__h && x.__h.click) x.__h.click(); });
        cards.push({ cls: card.className, paras, pres, copied: copied.slice(before), mark: (card.children[0] && card.children[0]._text) || "",
          banned: bannedHits(paras.join(" ")).map((h) => h.word), headWords: countWords(paras[0] || "") });
      }
      folds.push({ cls: c.className, summary: text(c.children[0]), open: c.attrs.open !== undefined, cards });
    }
    return { tone: n.tone, summary: n.summary || "", lines: n.lines || [], folds };
  };
  const out = {
    connect: read(vm.runInContext("connectNotice", ctx)({ ok: true, started: true, name: "shop-db", doctor: report })),
    startup: read(vm.runInContext("startupNotice", ctx)({ started: true, doctor: report })),
    test: read(vm.runInContext("unsavedTestNotice", ctx)({ doctor: report })),
    optionalFlag: vm.runInContext("doctorOptional", ctx)(report),
    warningFlag: vm.runInContext("doctorWarnings", ctx)(report),
  };
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`

type renderedCard struct {
	Cls, Mark      string
	Paras, Pres    []string
	Copied, Banned []string
	HeadWords      int
}

type renderedNotice struct {
	Tone, Summary string
	Lines         []string
	Folds         []struct {
		Cls, Summary string
		Open         bool
		Cards        []renderedCard
	}
}

// TestOptionalImprovementsRenderFromRealDoctor runs the real doctor against
// the test MySQL (a stock 8.4 or 8.0: statement logging off, row metadata
// MINIMAL) and renders the Connect, Save and Test notices from what it
// returned. The two optional items must be in a closed "Optional
// improvements" fold, in plain words with the statement to copy, and must
// not be counted in "with N warnings" or color the notice.
func TestOptionalImprovementsRenderFromRealDoctor(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	if _, err := db.Exec("CREATE TABLE keyed (id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	var stmtLog, rowMeta string
	if err := db.QueryRow("SELECT @@global.binlog_rows_query_log_events, @@global.binlog_row_metadata").Scan(&stmtLog, &rowMeta); err != nil {
		t.Fatal(err)
	}
	if (stmtLog != "0" && !strings.EqualFold(stmtLog, "OFF")) || !strings.EqualFold(rowMeta, "MINIMAL") {
		t.Skipf("the test MySQL is not stock (binlog_rows_query_log_events=%s, binlog_row_metadata=%s); nothing optional to render", stmtLog, rowMeta)
	}

	rep := doctor.Build(t.Context(), testutil.BaseDSN()+"/", "", name, 0, doctor.ForUnsavedServer())
	raw, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	// The doctor's JSON and the console's DoctorReport are the same wire
	// shape; decoding one into the other is the seam this test crosses.
	var report DoctorReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatal(err)
	}
	if report.Optional != 2 {
		t.Fatalf("optional = %d, want 2 (statement capture and row metadata): %s", report.Optional, raw)
	}
	nonOptionalWarns := 0
	for _, c := range report.Checks {
		if c.Status == "warn" && !c.Optional {
			nonOptionalWarns++
		}
		t.Logf("%s %s optional=%v %s", c.Status, c.Name, c.Optional, c.Detail)
	}
	if report.Warnings != nonOptionalWarns {
		t.Errorf("warnings = %d, but %d non-optional warns are in the report", report.Warnings, nonOptionalWarns)
	}

	script := renderHarnessJS + optionalRenderJS
	out := runNodeConnectArgs(t, script, string(raw))
	var got struct {
		Connect, Startup, Test    renderedNotice
		OptionalFlag, WarningFlag bool
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %s: %v", out, err)
	}
	if !got.OptionalFlag || got.WarningFlag != (report.Warnings > 0) {
		t.Errorf("doctorOptional %v, doctorWarnings %v for warnings=%d", got.OptionalFlag, got.WarningFlag, report.Warnings)
	}

	for label, n := range map[string]renderedNotice{"connect": got.Connect, "startup": got.Startup, "test": got.Test} {
		t.Logf("%s: tone=%s summary=%q lines=%q", label, n.Tone, n.Summary, n.Lines)
		if report.Warnings == 0 && n.Tone != "ok" {
			t.Errorf("%s: tone %q with only optional items; they must not color the notice", label, n.Tone)
		}
		if strings.Contains(n.Summary, "warning") && report.Warnings == 0 {
			t.Errorf("%s: summary %q counts optional items as warnings", label, n.Summary)
		}
		var opt *renderedCard
		var fold int = -1
		for i, f := range n.Folds {
			if strings.Contains(f.Cls, "notice-optional") {
				fold = i
			}
			for _, c := range f.Cards {
				if strings.Contains(f.Summary, "All ") && strings.Contains(strings.Join(c.Paras, " "), "Show the SQL statement") && strings.Contains(c.Cls, "warn") {
					t.Errorf("%s: an optional item is drawn amber under All checks: %+v", label, c)
				}
			}
		}
		if fold < 0 {
			t.Fatalf("%s: no Optional improvements fold: %+v", label, n.Folds)
		}
		f := n.Folds[fold]
		if f.Open || f.Summary != "Optional improvements (2)" {
			t.Errorf("%s: fold %q open=%v; want a closed \"Optional improvements (2)\"", label, f.Summary, f.Open)
		}
		want := map[string]string{
			"Show the SQL statement behind each change. To turn it on:": "SET PERSIST binlog_rows_query_log_events = ON;",
			"Notice if someone renames a column. To turn it on:":        "SET PERSIST binlog_row_metadata = 'FULL';",
		}
		for i := range f.Cards {
			c := &f.Cards[i]
			t.Logf("%s card: %q %q copied=%q", label, c.Paras, c.Pres, c.Copied)
			if len(c.Paras) == 0 || len(c.Pres) == 0 {
				t.Errorf("%s: card without a line or a statement: %+v", label, c)
				continue
			}
			stmt, ok := want[c.Paras[0]]
			if !ok {
				t.Errorf("%s: unexpected headline %q", label, c.Paras[0])
				continue
			}
			delete(want, c.Paras[0])
			opt = c
			if c.Pres[0] != stmt {
				t.Errorf("%s: statement %q, want %q", label, c.Pres[0], stmt)
			}
			if strings.Join(c.Copied, "\n") != strings.Join(c.Pres, "\n") {
				t.Errorf("%s: Copy copies %q, the card shows %q", label, c.Copied, c.Pres)
			}
			if c.HeadWords > 25 || len(c.Banned) > 0 {
				t.Errorf("%s: %d words before the statement, banned %v", label, c.HeadWords, c.Banned)
			}
			for _, p := range c.Paras {
				if strings.Contains(p, "—") || strings.Contains(p, "TABLE_MAP") {
					t.Errorf("%s: jargon or em dash in %q", label, p)
				}
			}
			if strings.Contains(c.Cls, "warn") {
				t.Errorf("%s: optional card class %q is amber", label, c.Cls)
			}
		}
		if len(want) > 0 || opt == nil {
			t.Errorf("%s: missing optional items %v", label, want)
		}
	}
}
