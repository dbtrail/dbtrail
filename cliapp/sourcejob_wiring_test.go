package cliapp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// The extension source-job seam (ext.RegisterSourceJob) exists so an
// embedding distribution can run per-source background work — identity
// capture, sidecar collectors — for as long as a capture daemon lives.
// These tests pin the two properties the wiring must have: every long-lived
// capture command reaches the seam, and it reaches it exactly once.

func TestStreamSourceJobInfoCarriesTheStreamsSource(t *testing.T) {
	// The seam gets what the stream actually runs with, including the values
	// `up` copies in via populateStreamFlags.
	origSource, origIndex, origFlavor := strmSourceDSN, strmIndexDSN, strmFlavor
	t.Cleanup(func() { strmSourceDSN, strmIndexDSN, strmFlavor = origSource, origIndex, origFlavor })

	strmSourceDSN = "u:p@tcp(src:3306)/"
	strmIndexDSN = "u:p@tcp(idx:3306)/bintrail_index"
	strmFlavor = "" // not declared: the flavor comes from what the stream resolved

	got := streamSourceJobInfo("mariadb")
	if got.SourceDSN != strmSourceDSN || got.IndexDSN != strmIndexDSN || got.Flavor != "mariadb" {
		t.Errorf("streamSourceJobInfo() = %+v, want the strm* stream configuration", got)
	}
}

// TestUpReachesTheSeamWithItsOwnConfiguration walks the delegation path `up`
// now depends on: runUpStream copies its flags into the strm* globals, and
// runStream (the only call site) builds SourceJobInfo from those. Flavor is
// the fragile half — populateStreamFlags deliberately does NOT copy it, so it
// carries whatever streamCmd's own default/env binding left behind. An empty
// flavor here would make every flavor-gated source job skip silently: no
// error, no log, capture simply does not happen. That is the exact failure
// class this wiring exists to fix.
func TestUpReachesTheSeamWithItsOwnConfiguration(t *testing.T) {
	origUpSource, origUpIndex := upSourceDSN, upIndexDSN
	origSource, origIndex, origFlavor := strmSourceDSN, strmIndexDSN, strmFlavor
	t.Cleanup(func() {
		upSourceDSN, upIndexDSN = origUpSource, origUpIndex
		strmSourceDSN, strmIndexDSN, strmFlavor = origSource, origIndex, origFlavor
	})

	upSourceDSN = "u:p@tcp(up-src:3306)/"
	upIndexDSN = "u:p@tcp(up-idx:3306)/bintrail_index"
	strmSourceDSN, strmIndexDSN = "", ""

	populateStreamFlags(12345)

	got := streamSourceJobInfo("mariadb")
	if got.SourceDSN != upSourceDSN || got.IndexDSN != upIndexDSN {
		t.Errorf("after populateStreamFlags, streamSourceJobInfo() = %+v, want up's DSNs", got)
	}
	if got.Flavor != "mariadb" {
		t.Errorf("Flavor = %q on the `up` path, want the resolved mariadb", got.Flavor)
	}
}

func TestAgentSourceJobInfoRequiresSourceAndIndex(t *testing.T) {
	origSource, origIndex := agtSourceDSN, agtIndexDSN
	t.Cleanup(func() { agtSourceDSN, agtIndexDSN = origSource, origIndex })

	tests := []struct {
		name             string
		source, index    string
		flavor           string
		wantOK           bool
		wantSrc, wantIdx string
		wantFlavor       string
	}{
		{name: "both set", source: "u:p@tcp(src:3306)/", index: "u:p@tcp(idx:3306)/bintrail_index",
			flavor: "mysql", wantOK: true, wantSrc: "u:p@tcp(src:3306)/", wantIdx: "u:p@tcp(idx:3306)/bintrail_index",
			wantFlavor: "mysql"},
		// The resolved mariadb flavor must reach the source job: a flavor-gated
		// job otherwise observes a MariaDB source believing it is MySQL.
		{name: "mariadb flavor", source: "u:p@tcp(src:3306)/", index: "u:p@tcp(idx:3306)/bintrail_index",
			flavor: "mariadb", wantOK: true, wantSrc: "u:p@tcp(src:3306)/", wantIdx: "u:p@tcp(idx:3306)/bintrail_index",
			wantFlavor: "mariadb"},
		// Stateless BYOS: a live source, but nowhere to persist an
		// observation — jobs must not start and then fail on an empty DSN.
		{name: "no index", source: "u:p@tcp(src:3306)/", index: "", wantOK: false},
		// Query-only agent: no live source to observe at all.
		{name: "no source", source: "", index: "u:p@tcp(idx:3306)/bintrail_index", wantOK: false},
		{name: "neither", source: "", index: "", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agtSourceDSN, agtIndexDSN = tc.source, tc.index
			got, ok := agentSourceJobInfo(tc.flavor)
			if ok != tc.wantOK {
				t.Fatalf("agentSourceJobInfo() ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if got.SourceDSN != tc.wantSrc || got.IndexDSN != tc.wantIdx {
				t.Errorf("agentSourceJobInfo() = %+v, want the agent's DSNs", got)
			}
			if got.Flavor != tc.wantFlavor {
				t.Errorf("agentSourceJobInfo().Flavor = %q, want %q (an empty flavor would make a flavor-gated source job skip with no signal)", got.Flavor, tc.wantFlavor)
			}
		})
	}
}

// TestSourceJobsStartOncePerDaemon is the double-fire guard. `up` delegates to
// runStream, so a second ext.RunSourceJobs call in runUpStream — the shape the
// wiring had before stream/agent adopted the seam — would run every registered
// job TWICE under `up`: two identity pollers on one source, each writing the
// same intervals. Exactly two call sites are correct: runStream (stream + up)
// and runAgent (BYOS).
func TestSourceJobsStartOncePerDaemon(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read cliapp dir: %v", err)
	}
	fset := token.NewFileSet()
	callers := map[string]int{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "RunSourceJobs" {
					return true
				}
				if ident, ok := sel.X.(*ast.Ident); !ok || ident.Name != "ext" {
					return true
				}
				callers[fn.Name.Name]++
				return true
			})
		}
	}
	want := map[string]int{"runStream": 1, "runAgent": 1}
	for fn, n := range want {
		if callers[fn] != n {
			t.Errorf("ext.RunSourceJobs called %d time(s) in %s, want %d", callers[fn], fn, n)
		}
	}
	for fn, n := range callers {
		if _, expected := want[fn]; !expected {
			t.Errorf("unexpected ext.RunSourceJobs call in %s (%d): source jobs share the stream's lifecycle — "+
				"a caller that delegates to runStream must not start them again", fn, n)
		}
	}
}

// The stream's --ssl-* configure its source connection, so a source job must
// get them: a job opening the source from the DSN alone connects in cleartext
// and is refused by a source that only accepts encrypted connections.
// Distinct values per field, so a dropped or swapped field fails.
func TestStreamSourceJobInfoCarriesTheStreamsSourceTLS(t *testing.T) {
	orig := [4]string{strmSSLMode, strmSSLCA, strmSSLCert, strmSSLKey}
	t.Cleanup(func() { strmSSLMode, strmSSLCA, strmSSLCert, strmSSLKey = orig[0], orig[1], orig[2], orig[3] })

	strmSSLMode, strmSSLCA, strmSSLCert, strmSSLKey = "verify-ca", "/s/ca.pem", "/s/cert.pem", "/s/key.pem"
	got := streamSourceJobInfo("mysql").SourceTLS
	want := ext.SourceTLS{Mode: "verify-ca", CA: "/s/ca.pem", Cert: "/s/cert.pem", Key: "/s/key.pem"}
	if got != want {
		t.Fatalf("SourceTLS = %+v, want %+v", got, want)
	}

	strmSSLMode, strmSSLCA, strmSSLCert, strmSSLKey = "disabled", "", "", ""
	if got := streamSourceJobInfo("mysql").SourceTLS; got != (ext.SourceTLS{Mode: "disabled"}) {
		t.Fatalf("disabled: SourceTLS = %+v", got)
	}
}

// The agent has no --ssl-* flags and opens the source from the DSN alone, so
// its jobs get the zero value ("preferred"), never a stale setting.
func TestAgentSourceJobInfoSourceTLSIsZero(t *testing.T) {
	origSource, origIndex := agtSourceDSN, agtIndexDSN
	t.Cleanup(func() { agtSourceDSN, agtIndexDSN = origSource, origIndex })
	agtSourceDSN, agtIndexDSN = "u:p@tcp(src:3306)/", "u:p@tcp(idx:3306)/bintrail_index"
	got, ok := agentSourceJobInfo("mysql")
	if !ok || got.SourceTLS != (ext.SourceTLS{}) {
		t.Fatalf("ok = %v, SourceTLS = %+v, want the zero value", ok, got.SourceTLS)
	}
}
