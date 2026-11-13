package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const startPage2637 = "https://www.dbtrail.com/docs/quickstart/"

// `bintrail agent` connects to an endpoint that does not resolve today, so it
// stays out of --help and out of the command table in docs/install.md. It is
// hidden, not removed: calling it by name still runs it.
func TestAgent_hiddenButStillRegistered(t *testing.T) {
	found, _, err := rootCmd.Find([]string{"agent"})
	if err != nil || found != agentCmd {
		t.Fatalf("`agent` resolves to %v (err %v); the command must stay registered", found, err)
	}
	if !agentCmd.Hidden {
		t.Error("`agent` shows in --help; its endpoint does not resolve, so a new user has nowhere to connect")
	}
	for _, line := range strings.Split(rootCmd.UsageString(), "\n") {
		if f := strings.Fields(line); len(f) > 0 && f[0] == "agent" {
			t.Errorf("`bintrail --help` lists agent: %q", line)
		}
	}
	b, err := os.ReadFile(filepath.Join("..", "docs", "install.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "| `stream` |") {
		t.Fatal("docs/install.md has no command table this check can read; update the check")
	}
	if strings.Contains(string(b), "| `agent` |") {
		t.Error("docs/install.md still lists `agent` in its command table")
	}
}

// The root help says what this binary is, and sends a reader who wanted the
// heavy queries to the start page, instead of opening on the earlier
// "indexer and recovery tool" line alone.
func TestRootHelp_pointsAtTheStartPage(t *testing.T) {
	if strings.Contains(rootCmd.Short, "indexer and recovery tool") {
		t.Errorf("root Short still says the earlier line: %q", rootCmd.Short)
	}
	if !strings.Contains(rootCmd.Long, startPage2637) {
		t.Errorf("root Long does not link the start page %s:\n%s", startPage2637, rootCmd.Long)
	}
	// Builds that embed this command line name their service binary
	// differently, so the help says "the DBTrail service", not a file name.
	if !strings.Contains(rootCmd.Long, "the DBTrail service") || strings.Contains(rootCmd.Long, "bintrail-console") {
		t.Errorf("root Long must point at the DBTrail service without naming a binary:\n%s", rootCmd.Long)
	}
}
