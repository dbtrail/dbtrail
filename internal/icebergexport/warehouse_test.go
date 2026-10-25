package icebergexport

import (
	"context"
	"os"
	"strings"
	"testing"
)

// TestCheckWarehouse pins which --warehouse values are a local directory.
// Before #2552 an object-store URL was not refused: filepath.Abs turned
// "s3://b/p" into the relative directory "./s3:/b/p" and the export wrote
// real tables there.
func TestCheckWarehouse(t *testing.T) {
	refused := []struct {
		in, want string
	}{
		{"s3://b/p", "object-store URL"},
		{"S3://B/p", "object-store URL"},
		{"gs://x", "object-store URL"},
		{"abfs://c@a.dfs.core.windows.net/p", "object-store URL"},
		{"s3a://b/p", "object-store URL"},
		{"  s3://b/p", "object-store URL"},
		{"file:///tmp/x", "file:// URL"},
		{"FILE:///tmp/x", "file:// URL"},
		{"", "required"},
		{"   ", "required"},
		{"\t\n", "required"},
	}
	for _, tc := range refused {
		err := CheckWarehouse(tc.in)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("CheckWarehouse(%q) = %v, want an error containing %q", tc.in, err, tc.want)
		}
	}
	for _, in := range []string{"/tmp/x", "rel/dir", "./w", "a/b://c", "my:dir", "dir with space"} {
		if err := CheckWarehouse(in); err != nil {
			t.Errorf("CheckWarehouse(%q) = %v, want nil (a local path)", in, err)
		}
	}
}

// TestCheckWarehouse_messageNamesTheRace: the refusal says why and what to
// do, and promises no flag that does not exist yet.
func TestCheckWarehouse_messageNamesTheRace(t *testing.T) {
	msg := CheckWarehouse("s3://b/p").Error()
	for _, want := range []string{"s3://b/p", "version-hint", "local directory"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q lacks %q", msg, want)
		}
	}
	if strings.Contains(msg, "--") {
		t.Errorf("message %q names a flag; the refusal must not promise one", msg)
	}
}

// TestRun_refusesURLWarehouseBeforeCreatingAnything: the refusal comes
// before the warehouse directory is created, so nothing lands in the
// working directory.
func TestRun_refusesURLWarehouseBeforeCreatingAnything(t *testing.T) {
	for _, wh := range []string{"s3://b/p", "gs://x", "file:///tmp/x", "   "} {
		t.Run(wh, func(t *testing.T) {
			dir := t.TempDir()
			t.Chdir(dir)
			_, err := Run(context.Background(), Config{
				IndexDSN:    "u:p@tcp(127.0.0.1:1)/db?timeout=200ms",
				BaselineSrc: dir,
				Warehouse:   wh,
				Tables:      []string{"shop.orders"},
			})
			if err == nil || strings.Contains(err.Error(), "connect to index") {
				t.Fatalf("Run(warehouse=%q) err = %v, want a warehouse refusal", wh, err)
			}
			ents, rerr := os.ReadDir(dir)
			if rerr != nil {
				t.Fatal(rerr)
			}
			if len(ents) != 0 {
				t.Fatalf("Run(warehouse=%q) created %d entries in the working directory (first %q)", wh, len(ents), ents[0].Name())
			}
		})
	}
}
