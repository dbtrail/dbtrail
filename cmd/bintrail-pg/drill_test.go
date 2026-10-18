package main

import (
	"context"
	"strings"
	"testing"
)

// This binary gives drill its postgres:// target: the run gets as far as
// connecting, instead of the refusal a binary without the loader gives.
func TestDrillHasAPostgresTarget(t *testing.T) {
	rootCmd.SetArgs([]string{"drill", "--index-dsn", "u@tcp(127.0.0.1:1)/x", "--baseline-dir", t.TempDir(),
		"--tables", "public.t", "--target-dsn", "postgres://u@127.0.0.1:1/db?connect_timeout=2"})
	t.Cleanup(func() { rootCmd.SetArgs(nil) })
	err := rootCmd.ExecuteContext(context.Background())
	if err == nil || !strings.Contains(err.Error(), "connect to --target-dsn") {
		t.Fatalf("err = %v", err)
	}
}
