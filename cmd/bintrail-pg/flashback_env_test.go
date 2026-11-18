package main

import (
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// BINTRAIL_PG_FLASHBACK_LISTEN and BINTRAIL_PG_SHIM_CONFIG are flags with a
// default of their own: the variable applies unless the flag was typed.
func TestApplyEnvUnlessGiven(t *testing.T) {
	newCmd := func() (*cobra.Command, *string) {
		c := &cobra.Command{Use: "x"}
		v := new(string)
		c.Flags().StringVar(v, "listen", "127.0.0.1:5433", "")
		return c, v
	}

	t.Setenv("BINTRAIL_PG_FLASHBACK_LISTEN", "0.0.0.0:6543")
	c, v := newCmd()
	if err := c.ParseFlags(nil); err != nil {
		t.Fatal(err)
	}
	applyEnvUnlessGiven(c, "listen", v, "BINTRAIL_PG_FLASHBACK_LISTEN")
	if *v != "0.0.0.0:6543" {
		t.Fatalf("flag not given: %q, want the variable's", *v)
	}

	c, v = newCmd()
	if err := c.ParseFlags([]string{"--listen", "127.0.0.1:7000"}); err != nil {
		t.Fatal(err)
	}
	applyEnvUnlessGiven(c, "listen", v, "BINTRAIL_PG_FLASHBACK_LISTEN")
	if *v != "127.0.0.1:7000" {
		t.Fatalf("flag given: %q, want the flag's", *v)
	}

	t.Setenv("BINTRAIL_PG_FLASHBACK_LISTEN", "")
	c, v = newCmd()
	if err := c.ParseFlags(nil); err != nil {
		t.Fatal(err)
	}
	applyEnvUnlessGiven(c, "listen", v, "BINTRAIL_PG_FLASHBACK_LISTEN")
	if *v != "127.0.0.1:5433" {
		t.Fatalf("no variable: %q, want the default", *v)
	}
}

// runFlashback applies the two variables with applyEnvUnlessGiven, not with
// applyEnvFallback, which never applies them over a non-empty default.
func TestFlashbackEnvDefaultsAreApplied(t *testing.T) {
	b, err := os.ReadFile("flashback.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	for _, want := range []string{
		`applyEnvUnlessGiven(cmd, "listen", &fbListen, "BINTRAIL_PG_FLASHBACK_LISTEN")`,
		`applyEnvUnlessGiven(cmd, "shim-config", &fbShimConfig, "BINTRAIL_PG_SHIM_CONFIG")`,
	} {
		if !strings.Contains(src, want) {
			t.Errorf("flashback.go does not apply %s", want)
		}
	}
}
