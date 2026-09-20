package views

import (
	"strings"
	"testing"
)

// The events view is the expensive statement in a generated file: union_by_name
// makes DuckDB open one Parquet footer per archived file at CREATE VIEW time,
// before a single row comes back, and the cost is O(archived files) and grows
// forever (#1535). Everyone reading the file paid it, including an operator who
// only wanted their tables. It is opt-in now, and these pin what that means.

func TestOmitEvents_leavesTheViewOutButSaysSo(t *testing.T) {
	in := goldenInput()
	in.OmitEvents = true
	out := Generate(in)

	if strings.Contains(out, `CREATE OR REPLACE VIEW "events"`) {
		t.Fatalf("OmitEvents still defined the events view:\n%s", out)
	}
	// The state views are the whole point of the cheap file, so their absence
	// would make this test pass for the wrong reason.
	if !strings.Contains(out, `CREATE OR REPLACE VIEW "state_shop_orders"`) {
		t.Fatalf("the state views went missing too:\n%s", out)
	}
	// Silence would leave the reader unable to tell "left out" from "your
	// archive registry is broken", which is what the OTHER empty-events branch
	// means and what its wording sends them to check.
	if !strings.Contains(out, "-- events: not included in this file.") {
		t.Errorf("the file never says the events view was left out:\n%s", out)
	}
	if !strings.Contains(out, "--include-events") {
		t.Errorf("the file never says how to get the events view back:\n%s", out)
	}
	for _, wrong := range []string{
		"no archive sources are registered",
		"archive_state could not be read",
	} {
		if strings.Contains(out, wrong) {
			t.Errorf("a deliberate omission borrows the wording of a FAULT (%q), sending "+
				"the reader to check a registry that is fine", wrong)
		}
	}
}

// TestOmitEvents_dropsTheS3SecretWhenOnlyTheArchivesWereOnS3: the secret's
// CREATE SECRET aborts the whole script when no credential resolves. Emitting
// it for a file that reads no S3 path would abort a purely local render over a
// bucket it was never going to touch.
func TestOmitEvents_dropsTheS3SecretWhenOnlyTheArchivesWereOnS3(t *testing.T) {
	in := goldenInput()
	in.ArchiveSources = []string{"s3://my-bucket/archives/bintrail_id=1111"}
	for i := range in.Baselines {
		in.Baselines[i].Path = "/local/baselines/2026-04-30/shop/orders.parquet"
	}
	if !in.NeedsS3() {
		t.Fatal("fixture does not need S3 with events included, so this proves nothing")
	}
	in.OmitEvents = true
	if in.NeedsS3() {
		t.Error("a file that reads no S3 path still emits the S3 secret, so an " +
			"unresolvable credential chain aborts a render that never touches a bucket")
	}
}

// TestOmitEvents_fileClaimsNoSelfFollowing: the events view's globs are the ONE
// self-following part of a generated file — they pick up newly rotated
// partitions with no regeneration. The state views do not. A file with no
// events view therefore follows nothing, and must not say otherwise.
func TestOmitEvents_fileClaimsNoSelfFollowing(t *testing.T) {
	in := goldenInput()
	in.OmitEvents = true
	out := Generate(in)

	for _, wrong := range []string{
		"globs below",
		"keep picking up newly rotated partitions",
	} {
		if strings.Contains(out, wrong) {
			t.Errorf("a file with no events view claims %q, which nothing in it does", wrong)
		}
	}
	// PRESENCE, not only absence. Three absence checks is what this was, and a
	// revert to the pre-flip wording ("live in `events` above") matched none of
	// them and walked through green: a negative assertion fails OPEN against
	// any string it did not think to name.
	for _, want := range []string{
		"are not in this file",
		"--include-events",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the state block never says %q, so a reader is not told where the "+
				"changes went:\n%s", want, out)
		}
	}
	if strings.Contains(out, "live in the `events` view") {
		t.Error("the state block sends the reader to an events view this file does not define")
	}
	// The header's else arm, same reasoning: blanking it left a dangling
	// "THIS FILE IS A SNAPSHOT OF THE LAYOUT, NOT A LIVE BINDING. " with no
	// sentence after it, and every guard was an absence check.
	if !strings.Contains(out, "The baseline state") {
		t.Errorf("the header's snapshot sentence has no follow-on clause:\n%s", out)
	}
}

// TestOmitEvents_withNoArchiveSourceOffersNoUselessRemedy: OmitEvents and "there
// is nothing to define the view from" are different facts. Telling a reader
// with no archived partitions to pass --include-events is advice that
// regenerates the same file, and the state block must not name the flag either.
func TestOmitEvents_withNoArchiveSourceOffersNoUselessRemedy(t *testing.T) {
	in := goldenInput()
	in.ArchiveSources, in.OmitEvents = nil, true
	out := Generate(in)

	if strings.Contains(out, "-- events: not included in this file.") {
		t.Errorf("the file says the view was LEFT OUT when there is nothing to define "+
			"it from:\n%s", out)
	}
	if !strings.Contains(out, "no archive sources are registered") {
		t.Errorf("the file does not state the real reason there is no events view:\n%s", out)
	}
	if strings.Contains(out, "--include-events") {
		t.Errorf("the file offers a flag that would produce this same file again:\n%s", out)
	}
	// And the state block takes its third arm rather than the flag-naming one.
	if !strings.Contains(out, "it defines no `events` view") {
		t.Errorf("the state block does not say plainly that there is no events view:\n%s", out)
	}
}

// TestOmitEvents_namesBothRoutes: the file is written by two producers and the
// console reader has no command line. A remedy naming only the CLI flag is
// unreachable for half the audience.
func TestOmitEvents_namesBothRoutes(t *testing.T) {
	in := goldenInput()
	in.OmitEvents = true
	out := Generate(in)

	for _, want := range []string{"--include-events", "box in the web interface"} {
		if !strings.Contains(out, want) {
			t.Errorf("the state block does not name the %q route:\n%s", want, out)
		}
	}
}

// TestOmitEvents_isIndependentOfOnlyViews: OnlyViews is the SQL panel's knob and
// OmitEvents is the file producers'. They compose — either one alone withholds
// the view — and the zero value of the new field must not change what a caller
// that never heard of it gets.
func TestOmitEvents_isIndependentOfOnlyViews(t *testing.T) {
	in := goldenInput()
	if !strings.Contains(Generate(in), `CREATE OR REPLACE VIEW "events"`) {
		t.Error("the zero value of OmitEvents changed the default render, so every " +
			"existing caller silently lost the events view")
	}
	only := goldenInput()
	only.OnlyViews = ViewSet{eventsViewName: true}
	if !strings.Contains(Generate(only), `CREATE OR REPLACE VIEW "events"`) {
		t.Error("OnlyViews asking for events no longer yields it")
	}
	only.OmitEvents = true
	if strings.Contains(Generate(only), `CREATE OR REPLACE VIEW "events"`) {
		t.Error("OmitEvents does not compose with OnlyViews: a caller that asked for " +
			"events by name got it back despite the omission")
	}
}
