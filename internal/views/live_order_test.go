package views

import (
	"strings"
	"testing"
	"time"
)

// The ATTACH is the one statement in this file that depends on reaching another
// machine, and `duckdb -init` aborts the session at the first error. Emitted
// ahead of the views it cost the reader everything, including views that read
// only Parquet and needed nothing from the index (#1536). Verified in DuckDB
// separately: statements that ran before an aborting one stay committed, so the
// ordering below is what turns a total loss into a degrade.

// orderedInput is the shape the ordering matters for: a cold leg AND a baseline
// AND an index, so the file carries all three kinds of statement.
func orderedInput() Input {
	in := liveInput(liveIdx())
	in.BaselineSource = "/baselines"
	in.BaselineSnapshot = time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC)
	in.Baselines = []BaselineTable{{Schema: "shop", Table: "orders", Path: "/baselines/2026-04-30/shop/orders.parquet"}}
	return in
}

func TestLiveLeg_parquetOnlyViewsPrecedeTheAttach(t *testing.T) {
	out := Generate(orderedInput())

	attach := strings.Index(out, "\nATTACH ")
	if attach < 0 {
		t.Fatalf("no ATTACH in a file generated with an index:\n%s", out)
	}
	events := strings.Index(out, `CREATE OR REPLACE VIEW "events"`)
	state := strings.Index(out, `CREATE OR REPLACE VIEW "shop"."orders"`)

	if events < 0 || state < 0 {
		t.Fatalf("expected both an events view and a state view:\n%s", out)
	}
	if state > attach {
		t.Error("a state view is defined after the ATTACH. State views read baseline " +
			"Parquet and never touch the index, so a failed ATTACH must not take them")
	}
	if events < attach {
		t.Error("the two-leg events view is defined before the ATTACH it reads through")
	}

	// EXACTLY ONE definition, counted rather than located. The events view is
	// the expensive statement in the file — union_by_name opens a Parquet
	// footer per archived file at CREATE VIEW time (#1535), measured at ~7s
	// over 120 files with a second definition costing ~3.6s more whether it
	// repeats the literal or references the first view. A change that reverts
	// to defining it archives-only and CREATE OR REPLACEing it after the ATTACH
	// still satisfies every ordering assertion above, so only the count catches
	// it.
	if n := strings.Count(out, `CREATE OR REPLACE VIEW "events"`); n != 1 {
		t.Errorf("the events view is defined %d times, want 1: each definition binds "+
			"the whole archive file list", n)
	}

	// The catalog name must appear only on the far side of the ATTACH that
	// creates it. This is the assertion that would catch a reorder that moved
	// the statements but left a view reading a catalog that does not exist yet.
	if i := strings.Index(out, `"bintrail_live"."binlog_events"`); i >= 0 && i < attach {
		t.Error("a view reads the attached catalog before the ATTACH creates it")
	}
}

// TestLiveLeg_saysWhatAFailedAttachCosts: the file is the only thing a reader
// whose ATTACH failed still has. It must state the trade the ordering makes —
// the state views survive, the events view does not — because that reader
// cannot otherwise tell an intended degrade from a truncated download.
//
// The second half is the older guarantee: this render is the same code path
// that serves a file with NO index, where it tells the reader to regenerate
// with --include-live. In a file that already has a live leg, that advice asks
// for something already done.
func TestLiveLeg_saysWhatAFailedAttachCosts(t *testing.T) {
	in := orderedInput()
	in.LiveLegHowTo = "tick the Include the live index box"
	out := Generate(in)

	for _, unwanted := range []string{"--include-live", "tick the Include the live index box"} {
		if strings.Contains(out, unwanted) {
			t.Errorf("the file tells the reader to add a live leg (%q) in a file that "+
				"already has one", unwanted)
		}
	}
	for _, want := range []string{
		"cannot reach",
		"the table views above already created",
		// The degrade is CONDITIONAL on how the file is run, and this is the
		// invocation that breaks it: `duckdb -init file.sql` with no database
		// file exits on the error and the in-memory catalog dies with it, so
		// nothing survives. Verified against DuckDB v1.5.5. `bintrail views
		// --help` names that invocation, so a file that promised the state
		// views survive would be lying to the reader who followed the help.
		"in-memory database goes with it",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the file never says %q, so a reader whose ATTACH failed cannot "+
				"tell what they are left holding:\n%s", want, out)
		}
	}
}

// TestAttachDegrade_saysNothingSurvivesWhenNoStateViewExist: archives + an index
// + no baseline location is a shape both producers reach (`bintrail views
// --include-live` with no --baseline-dir; the console serves it whenever a
// server has archived partitions and no baseline root). It defines NO state
// view, so the reassurance the other shape earns would send this reader hunting
// for views that were never written.
func TestAttachDegrade_saysNothingSurvivesWhenNoStateViewsExist(t *testing.T) {
	in := orderedInput()
	in.Baselines, in.BaselineSource = nil, ""
	out := Generate(in)

	if strings.Contains(out, `CREATE OR REPLACE VIEW "shop"."orders"`) {
		t.Fatalf("fixture defines a state view, so it cannot test the no-state degrade:\n%s", out)
	}
	if !strings.Contains(out, "no view at all") {
		t.Errorf("a file with no state view still tells the reader something survives "+
			"a failed ATTACH:\n%s", out)
	}
	if strings.Contains(out, "already created") {
		t.Error("the file claims state views survive a failed ATTACH while defining none")
	}
}

// TestAttachDegrade_liveOnlyBranchSaysItToo: the live-only events view is
// defined after the same ATTACH and loses the same way, so it needs the same
// note. It was written for the two-leg branch only, which is exactly the kind
// of omission a per-branch switch invites.
func TestAttachDegrade_liveOnlyBranchSaysItToo(t *testing.T) {
	in := orderedInput()
	in.ArchiveSources = nil
	out := Generate(in)

	if !strings.Contains(out, "Defined AFTER the ATTACH above") {
		t.Errorf("the live-only events view does not say what a failed ATTACH costs:\n%s", out)
	}
	// No cold leg exists, so regenerating without the live index would define
	// no events view at all. Offering it as a remedy would be advice to
	// generate an empty file.
	if strings.Contains(out, "Regenerating WITHOUT the live index") {
		t.Error("a file with no archive source offers an archives-only fallback that " +
			"would define nothing")
	}
}

// TestLiveOnly_definesEventsOnce guards the OTHER shape the single definition
// has to hold for: an index with nothing archived, where the archives-only
// render would emit a "(skipped: no archive sources are registered)" comment.
// Emitted alongside a live-only definition, that comment sits directly above a
// CREATE OR REPLACE VIEW "events" and flatly contradicts it.
func TestLiveOnly_definesEventsOnce(t *testing.T) {
	in := orderedInput()
	in.ArchiveSources = nil
	out := Generate(in)

	if n := strings.Count(out, "-- events:"); n != 1 {
		t.Errorf("a live-only file has %d events blocks, want 1:\n%s", n, out)
	}
	if n := strings.Count(out, `CREATE OR REPLACE VIEW "events"`); n != 1 {
		t.Errorf("a live-only file defines the events view %d times, want 1", n)
	}
	if strings.Contains(out, "skipped:") {
		t.Errorf("a file that DOES define the events view also says it was skipped:\n%s", out)
	}
}

// TestLiveLeg_singleLabelHostWarns covers the shape that produced #1536: a
// console running under Docker Compose emits its compose service name, which
// resolves for containers on that network and nowhere else.
func TestLiveLeg_singleLabelHostWarns(t *testing.T) {
	const marker = "bare name with no domain"
	const loopbackMarker = "is a loopback address"
	cases := []struct {
		name         string
		host         string
		want         bool
		wantLoopback bool
	}{
		{"compose service name", "index-mysql", true, false},
		{"kubernetes short name", "bintrail-index", true, false},
		{"fully qualified", "index.example.com", false, false},
		{"trailing dot is still qualified", "index.example.com.", false, false},
		{"ipv4 literal", "10.0.0.5", false, false},
		{"ipv6 literal has no dots but is an address", "[2001:db8::1]", false, false},
		// localhost is single-label too, and it gets the LOUDER warning: it
		// resolves everywhere and answers with a different index. wantLoopback
		// is asserted POSITIVELY here: with only the want:false above, emptying
		// the loopback branch leaves this test passing while the file warns
		// about nothing at all.
		{"localhost keeps the loopback warning", "localhost", false, true},
		{"loopback literal keeps the loopback warning", "127.0.0.1", false, true},
		// The DNS root-anchored forms. Only isSingleLabelHost trimmed the
		// trailing dot, so "localhost." used to be told it resolves in one
		// network and nowhere else -- about a name that resolves EVERYWHERE, to
		// the wrong index. "127.0.0.1." got no warning at all.
		{"root-anchored localhost is still loopback", "localhost.", false, true},
		{"root-anchored loopback literal is still loopback", "127.0.0.1.", false, true},
		// An empty host reaches the driver as localhost, so it earns the
		// loopback warning rather than none.
		{"empty host is loopback to the driver", "", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			li := liveIdx()
			li.Host = tc.host
			out := Generate(liveInput(li))
			if got := strings.Contains(out, marker); got != tc.want {
				t.Errorf("single-label warning present = %v, want %v for host %q",
					got, tc.want, tc.host)
			}
			if got := strings.Contains(out, loopbackMarker); got != tc.wantLoopback {
				t.Errorf("loopback warning present = %v, want %v for host %q",
					got, tc.wantLoopback, tc.host)
			}
			// Exactly one warning, never both: they name different failures and
			// a reader handed both cannot tell which one they are in.
			if strings.Contains(out, marker) && strings.Contains(out, loopbackMarker) {
				t.Errorf("host %q got both the loopback and the single-label warning", tc.host)
			}
		})
	}
}

func TestIsSingleLabelHost(t *testing.T) {
	for host, want := range map[string]bool{
		"index-mysql": true,
		"db":          true,
		// The trailing dot is the only case TrimSuffix discriminates: without it
		// "db." reads as qualified because it contains a dot.
		"db.":                true,
		"index.example.com":  false,
		"index.example.com.": false,
		"10.0.0.5":           false,
		"2001:db8::1":        false,
		"[2001:db8::1]":      false,
		"::1":                false,
		"":                   false,
	} {
		if got := isSingleLabelHost(host); got != want {
			t.Errorf("isSingleLabelHost(%q) = %v, want %v", host, got, want)
		}
	}
}

// TestOnlyViews_noOrphanAttach: OnlyViews narrows the render to the views a
// caller asked for. The ATTACH exists solely to back the events view's hot leg,
// so emitting it for a render that defines no events view leaves a connection
// to another machine — one that can abort the whole script — with nothing
// reading through it, under a preamble introducing a leg that is not there.
func TestOnlyViews_noOrphanAttach(t *testing.T) {
	in := orderedInput()
	in.OnlyViews = ViewSet{"shop.orders": true}
	out := Generate(in)

	if !strings.Contains(out, `CREATE OR REPLACE VIEW "shop"."orders"`) {
		t.Fatalf("the view that WAS asked for is missing, so this proves nothing:\n%s", out)
	}
	if strings.Contains(out, `CREATE OR REPLACE VIEW "events"`) {
		t.Fatalf("OnlyViews did not narrow the render:\n%s", out)
	}
	if strings.Contains(out, "ATTACH ") {
		t.Errorf("an ATTACH is emitted for a render that defines no events view:\n%s", out)
	}
}

// TestStateViewSkip_keepsItsSeparator: the skip line used to be the last thing
// in the file, so nothing followed it. Since the reorder it is followed by the
// live preamble and the events block, and without a blank line it butts
// straight against them and reads as part of the next section.
func TestStateViewSkip_keepsItsSeparator(t *testing.T) {
	in := orderedInput()
	in.Baselines, in.BaselineSource = nil, ""
	out := Generate(in)

	const skip = "-- (skipped: no baseline snapshot was discovered)\n"
	if !strings.Contains(out, skip) {
		t.Fatalf("no skip line to check:\n%s", out)
	}
	if !strings.Contains(out, skip+"\n") {
		t.Errorf("the skip line runs straight into the next section with no blank "+
			"line between them:\n%s", out)
	}
}
