package reconstruct

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
)

// #2517: the carry forward validates a base once per process per identity.
// These two tests pin the WIRING, not the memo: a refresh that carries a table
// forward twice reads it once, and a base replaced under a new inode between
// refreshes is still refused.

// carryTwice builds snapshot s1 holding demo/orders.parquet under a manifest,
// carries it into s2 (writing s2's manifest from s1, as the refresh does), and
// returns the three snapshot directories.
func carryTwiceFixture(t *testing.T) (root, s1, s2, s3 string) {
	t.Helper()
	root = t.TempDir()
	s1, s2, s3 = filepath.Join(root, "s1"), filepath.Join(root, "s2"), filepath.Join(root, "s3")
	src := filepath.Join(s1, "demo", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, bytes.Repeat([]byte("orders bytes "), 500), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baselineintegrity.WriteManifest(s1); err != nil {
		t.Fatal(err)
	}
	// The manifest writer seeds the memo with the files it hashed (round 2,
	// F4). These tests are about the READER's first and second check, so the
	// seed is dropped: s1's file is new to the process when the first carry
	// checks it.
	baselineintegrity.ForgetVerified()
	// Aged past the memo's racy margin: a file modified seconds ago is never
	// remembered (coarse timestamps), and these tests expect a skip after one read.
	ageFile(t, src)
	return root, s1, s2, s3
}

// ageFile sets path's mtime a minute in the past, outside the memo's racy margin.
func ageFile(t *testing.T, path string) {
	t.Helper()
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
}

func TestCarryForward_validatesABaseOncePerProcess_2517(t *testing.T) {
	baselineintegrity.ForgetVerified()
	t.Cleanup(baselineintegrity.ForgetVerified)
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	_, s1, s2, s3 := carryTwiceFixture(t)

	// Refresh 1: s1 -> s2, no changes.
	linked, err := carryForward(context.Background(), filepath.Join(s1, "demo", "orders.parquet"), s2, "demo", "orders")
	if err != nil || !linked {
		t.Fatalf("first carry: linked=%v err=%v", linked, err)
	}
	if _, err := baselineintegrity.WriteManifestFrom(s2, []string{s1}); err != nil {
		t.Fatal(err)
	}
	first := logs.String()
	if n := strings.Count(first, `msg="integrity check read the whole file and verified it"`); n != 1 {
		t.Fatalf("the first carry read the file %d times, want 1:\n%s", n, first)
	}
	logs.Reset()

	// Refresh 2: s2 -> s3, no changes. The file in s2 is the inode verified
	// under s1, with the same digest in s2's manifest; nothing is read.
	src2 := filepath.Join(s2, "demo", "orders.parquet")
	linked, err = carryForward(context.Background(), src2, s3, "demo", "orders")
	if err != nil || !linked {
		t.Fatalf("second carry: linked=%v err=%v", linked, err)
	}
	second := logs.String()
	if strings.Contains(second, "integrity check read the whole file") {
		t.Errorf("the second carry read the file again:\n%s", second)
	}
	if !strings.Contains(second, `msg="integrity check skipped: these bytes were verified earlier by this process"`) ||
		!strings.Contains(second, "path="+src2) {
		t.Errorf("the second carry did not say it skipped the read of %s:\n%s", src2, second)
	}
	assertSharesInode(t, filepath.Join(s1, "demo", "orders.parquet"), filepath.Join(s3, "demo", "orders.parquet"),
		"the file carried twice is not the inode it started as")
}

// The mirror: between two refreshes the base is replaced by a corrupt file
// under a NEW inode (a rewrite by rename). The memo holds the old inode, so the
// next carry reads the new one and refuses it. A regression guard, not a
// discriminator: it passes with ValidateLocalFile as well (which reads every
// time); what it pins is that the memo never lets a new inode through.
func TestCarryForward_aBaseReplacedBetweenRefreshesIsStillRefused_2517(t *testing.T) {
	baselineintegrity.ForgetVerified()
	t.Cleanup(baselineintegrity.ForgetVerified)
	_, s1, s2, s3 := carryTwiceFixture(t)

	if _, err := carryForward(context.Background(), filepath.Join(s1, "demo", "orders.parquet"), s2, "demo", "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := baselineintegrity.WriteManifestFrom(s2, []string{s1}); err != nil {
		t.Fatal(err)
	}
	src2 := filepath.Join(s2, "demo", "orders.parquet")
	tmp := src2 + ".tmp"
	if err := os.WriteFile(tmp, []byte("corrupt bytes under a fresh inode"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, src2); err != nil {
		t.Fatal(err)
	}

	_, err := carryForward(context.Background(), src2, s3, "demo", "orders")
	if !errors.Is(err, baselineintegrity.ErrIntegrity) {
		t.Fatalf("a corrupt base under a new inode was carried forward: err=%v", err)
	}
	if _, statErr := os.Stat(filepath.Join(s3, "demo", "orders.parquet")); statErr == nil {
		t.Errorf("the refused file was still published into s3")
	}
}
