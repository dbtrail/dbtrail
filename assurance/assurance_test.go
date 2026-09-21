package assurance

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// The facade must ALIAS the internal types, never restate them. A separately
// declared struct with identical fields would satisfy every call in this
// package and still give an embedder its own copy to drift with — and these
// types carry verdicts (`bintrail status` renders the same ones). Assignment
// between two distinct named types does not compile, so this pins it.
//
// The named non-struct types use `*new(T)`: `var _ status.BaselineStalenessVerdict
// = BaselineUnknown` would type the CONSTANT, not the type, and compiles
// whether or not the type is an alias. VerifyHistory is pinned in pointer
// form — the value form compiles but trips go vet's copylocks (sync.Mutex).
func TestFacadeTypesAreTheInternalTypes(t *testing.T) {
	var (
		_ status.CoverageSummary          = CoverageSummary{}
		_ CoverageSummary                 = status.CoverageSummary{}
		_ status.DeltaFloor               = DeltaFloor{}
		_ DeltaFloor                      = status.DeltaFloor{}
		_ status.BaselineInfo             = BaselineInfo{}
		_ BaselineInfo                    = status.BaselineInfo{}
		_ status.StreamStateInfo          = StreamStateInfo{}
		_ status.BaselineStalenessVerdict = *new(BaselineStalenessVerdict)
		_ reconstruct.BaselineFile        = BaselineFile{}
		_ BaselineFile                    = reconstruct.BaselineFile{}
		_ *console.VerifyHistory          = (*VerifyHistory)(nil)
		_ console.VerifyRunRecord         = VerifyRunRecord{}
		_ VerifyRunRecord                 = console.VerifyRunRecord{}
		_ console.VerifyStatus            = VerifyStatus{}
		_ console.VerifySummary           = VerifySummary{}
		_ console.VerifyTableResult       = VerifyTableResult{}
		_ console.VerifyMode              = *new(VerifyMode)
	)
}

// Every re-exported constant must carry the core's value. These can only fail
// if someone replaces a `= pkg.X` re-export with a hand-written literal, which
// is exactly how a wire vocabulary drifts.
func TestConstantsMatchTheCore(t *testing.T) {
	pairs := []struct {
		name      string
		got, want string
	}{
		{"ContinuityOK", ContinuityOK, status.ContinuityOK},
		{"ContinuityGapLost", ContinuityGapLost, status.ContinuityGapLost},
		{"ContinuityUnknown", ContinuityUnknown, status.ContinuityUnknown},
		{"ContinuityUnavailable", ContinuityUnavailable, status.ContinuityUnavailable},
		{"ContinuityNone", ContinuityNone, status.ContinuityNone},
		{"VerifyTriggerManual", VerifyTriggerManual, console.VerifyTriggerManual},
		{"VerifyTriggerScheduled", VerifyTriggerScheduled, console.VerifyTriggerScheduled},
		{"VerifyStateSkipped", VerifyStateSkipped, console.VerifyStateSkipped},
		{"VerifyStateIdle", VerifyStateIdle, console.VerifyStateIdle},
		{"VerifyStateRunning", VerifyStateRunning, console.VerifyStateRunning},
		{"VerifyStateSucceeded", VerifyStateSucceeded, console.VerifyStateSucceeded},
		{"VerifyStateFailed", VerifyStateFailed, console.VerifyStateFailed},
		{"VerifyTableMatch", VerifyTableMatch, string(verify.StatusMatch)},
		{"VerifyTableMismatch", VerifyTableMismatch, string(verify.StatusMismatch)},
		{"VerifyTableInconclusive", VerifyTableInconclusive, string(verify.StatusInconclusive)},
		{"VerifyTableError", VerifyTableError, string(verify.StatusError)},
		{"VerifyVerdictVerified", VerifyVerdictVerified, verify.VerdictVerified},
		{"VerifyVerdictMismatch", VerifyVerdictMismatch, verify.VerdictMismatch},
		{"VerifyVerdictError", VerifyVerdictError, verify.VerdictError},
		{"VerifyVerdictUnproven", VerifyVerdictUnproven, verify.VerdictUnproven},
		{"VerifyVerdictNoPredecessor", VerifyVerdictNoPredecessor, verify.VerdictNoPredecessor},
		{"VerifyModeBaselineAnchored", string(VerifyModeBaselineAnchored), string(console.VerifyModeBaselineAnchored)},
		{"VerifyModeLiveSource", string(VerifyModeLiveSource), string(console.VerifyModeLiveSource)},
		{"VerifyModeRecoverInputs", string(VerifyModeRecoverInputs), string(console.VerifyModeRecoverInputs)},
		{"BaselineOK", string(BaselineOK), string(status.BaselineOK)},
		{"BaselineAging", string(BaselineAging), string(status.BaselineAging)},
		{"BaselineBroken", string(BaselineBroken), string(status.BaselineBroken)},
		{"BaselineUnknown", string(BaselineUnknown), string(status.BaselineUnknown)},
	}
	for _, p := range pairs {
		if p.got != p.want {
			t.Errorf("%s = %q, core has %q", p.name, p.got, p.want)
		}
	}
	if VerifyHistoryCap != console.VerifyHistoryCap {
		t.Errorf("VerifyHistoryCap = %d, core has %d", VerifyHistoryCap, console.VerifyHistoryCap)
	}
}

func TestContinuityStatusReachesTheOneRule(t *testing.T) {
	// The two verdicts a facade is most likely to get wrong by re-deriving:
	// no stream row is a genuine no-claim, an unreadable one is not "ok".
	if got := ContinuityStatus(nil, nil); got != ContinuityNone {
		t.Fatalf("no stream row = %q, want %q", got, ContinuityNone)
	}
	if got := ContinuityStatus(nil, errors.New("boom")); got != ContinuityUnavailable {
		t.Fatalf("unreadable stream state = %q, want %q", got, ContinuityUnavailable)
	}
}

// The #1219 demotion has to survive the trip through the facade: below an
// unattributable floor a snapshot grades unknown, not broken. An embedder
// that got "broken" here would page an operator whose archives are fine.
func TestStalenessGradingKeepsTheAmbiguityDemotion(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	below := []BaselineInfo{{Database: "shop", Table: "orders", SnapshotTime: now.Add(-72 * time.Hour)}}

	attributable := DeltaFloor{Hour: now.Add(-24 * time.Hour)}
	AnnotateBaselineStaleness(below, attributable, now)
	if below[0].Staleness != BaselineBroken {
		t.Fatalf("below an attributable floor = %q, want %q", below[0].Staleness, BaselineBroken)
	}

	ambiguous := DeltaFloor{Hour: now.Add(-24 * time.Hour), BelowIsUnknown: true}
	AnnotateBaselineStaleness(below, ambiguous, now)
	if below[0].Staleness != BaselineUnknown {
		t.Fatalf("below an unattributable floor = %q, want %q", below[0].Staleness, BaselineUnknown)
	}
	if got := OverallBaselineStaleness(below); got != BaselineUnknown {
		t.Fatalf("overall = %q, want %q", got, BaselineUnknown)
	}
}

// End-to-end through the facade: absent, existing-but-empty, and populated.
// The populated leg is what pins that OpenVerifyHistory reads the file the
// path names — pointing it at a sibling path (or any wrong file) satisfies
// every "absent" assertion forever, and a consumer wired that way reports
// "never verified" for a deployment that verifies nightly.
func TestVerifyHistoryRoundTripThroughTheFacade(t *testing.T) {
	dir := t.TempDir()
	path := DefaultVerifyHistoryPath(filepath.Join(dir, "console-servers.yaml"))
	if base := filepath.Base(path); base != "console-verify-history.json" {
		t.Fatalf("history path = %q", base)
	}

	absent, err := OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if absent.Found() {
		t.Fatal("Found() true with no history file on disk")
	}
	if ids := absent.ServerIDs(); len(ids) != 0 {
		t.Fatalf("server ids from an absent history: %v", ids)
	}
	if recs := absent.List("srv1"); len(recs) != 0 {
		t.Fatalf("records from an absent history: %v", recs)
	}

	// Written by the daemon side, deliberately not through the facade — the
	// facade's contract is read-only.
	writer, err := console.OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	rec := console.VerifyRunRecord{
		ServerID: "srv1", ServerName: "wp", Trigger: console.VerifyTriggerScheduled,
		VerifyStatus: console.VerifyStatus{
			State: console.VerifyStateSucceeded, Mode: console.VerifyModeRecoverInputs,
			Summary: console.VerifySummary{Match: 3, Total: 3},
		},
	}
	if err := writer.Append(rec); err != nil {
		t.Fatal(err)
	}
	// A run that reached its end and proved no table: "succeeded" says only
	// that it finished, and a consumer branching on the verdict must see it.
	unproven := rec
	unproven.Summary = console.VerifySummary{Inconclusive: 4, Total: 4}
	if err := writer.Append(unproven); err != nil {
		t.Fatal(err)
	}

	h, err := OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if !h.Found() {
		t.Fatal("Found() false after a run was recorded")
	}
	ids := h.ServerIDs()
	if len(ids) != 1 || ids[0] != "srv1" {
		t.Fatalf("ServerIDs() = %v, want [srv1]", ids)
	}
	got := h.List("srv1")
	if len(got) != 2 {
		t.Fatalf("List() returned %d records, want 2", len(got))
	}
	if got[1].State != VerifyStateSucceeded || got[1].Trigger != VerifyTriggerScheduled ||
		got[1].Mode != VerifyModeRecoverInputs || got[1].Summary.Match != 3 {
		t.Fatalf("record did not survive the facade: %+v", got[1])
	}
	if got[0].Verdict != VerifyVerdictUnproven || got[1].Verdict != VerifyVerdictVerified {
		t.Fatalf("verdicts through the facade: %q, %q; want %q, %q (newest first)",
			got[0].Verdict, got[1].Verdict, VerifyVerdictUnproven, VerifyVerdictVerified)
	}
	held := VerifyStatus{State: VerifyStateSucceeded, Summary: VerifySummary{Mismatch: 1, Match: 2, Total: 3}}
	if v := held.WithVerdict().Verdict; v != VerifyVerdictMismatch {
		t.Fatalf("WithVerdict through the facade = %q, want %q", v, VerifyVerdictMismatch)
	}
}

// A corrupt history must not degrade into an empty one: through the facade
// that would be indistinguishable from "this deployment never verified".
func TestOpenVerifyHistoryForwardsTheCorruptFileError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-verify-history.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	h, err := OpenVerifyHistory(path)
	if err == nil {
		t.Fatalf("corrupt history opened cleanly: found=%v", h.Found())
	}
}

// Presence in ServerIDs is not evidence that a server was verified: a
// scheduled cycle that could not run is recorded rather than dropped, so a
// server can appear with nothing but skips behind it.
func TestSkippedOnlyServerIsDistinguishable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-verify-history.json")
	writer, err := console.OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(console.VerifyRunRecord{
		ServerID: "srv1", Trigger: console.VerifyTriggerScheduled,
		SkipReason:   "a manual run was already in flight",
		VerifyStatus: console.VerifyStatus{State: console.VerifyStateSkipped},
	}); err != nil {
		t.Fatal(err)
	}

	h, err := OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if ids := h.ServerIDs(); len(ids) != 1 {
		t.Fatalf("ServerIDs() = %v, want the skipped server listed", ids)
	}
	recs := h.List("srv1")
	if len(recs) != 1 || recs[0].State != VerifyStateSkipped || recs[0].SkipReason == "" {
		t.Fatalf("skip not legible through the facade: %+v", recs)
	}
}

func TestListBaselinesThroughTheFacade(t *testing.T) {
	dir := t.TempDir()
	snap := filepath.Join(dir, "2026-08-03T12-00-00Z", "shop")
	if err := os.MkdirAll(snap, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snap, "orders.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := ListBaselines(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("ListBaselines() returned %d entries, want 1: %+v", len(got), got)
	}
	if got[0].Schema != "shop" || got[0].Table != "orders" {
		t.Fatalf("wrong entry: %+v", got[0])
	}
	if want := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC); !got[0].SnapshotTime.Equal(want) {
		t.Fatalf("SnapshotTime = %s, want %s", got[0].SnapshotTime, want)
	}
}

func TestDefaultRegistryPathIsTheCoreDefault(t *testing.T) {
	// The default location has to be derivable through the facade, or a
	// caller hardcodes it and reads nothing once the registry moves.
	if got, want := DefaultRegistryPath(), console.DefaultRegistryPath(); got != want {
		t.Fatalf("DefaultRegistryPath() = %q, want %q", got, want)
	}
}
