package doctor

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const frGiB = uint64(1) << 30

// stubFullReadDisk replaces the free-space probe and the estimate for one
// test, and records what each was asked.
type fullReadStub struct {
	measured []string
	schemas  [][]string
}

func stubFullReadDisk(t *testing.T, free, total uint64, diskErr error, est DumpEstimate, estErr error) *fullReadStub {
	t.Helper()
	s := &fullReadStub{}
	prevDisk, prevEst := fullReadDiskSpaceFn, fullReadEstimateFn
	fullReadDiskSpaceFn = func(path string) (uint64, uint64, error) {
		s.measured = append(s.measured, path)
		return free, total, diskErr
	}
	fullReadEstimateFn = func(_ context.Context, _ string, schemas []string) (DumpEstimate, error) {
		s.schemas = append(s.schemas, schemas)
		return est, estErr
	}
	t.Cleanup(func() { fullReadDiskSpaceFn, fullReadEstimateFn = prevDisk, prevEst })
	return s
}

// #2259: what doctor says about a working folder, from the same numbers the
// console's check reads. It warns at most: a FAIL would change the exit code
// of a command that cannot know where the snapshot is written.
func TestFullReadDiskResult(t *testing.T) {
	plain := DumpEstimate{DataBytes: int64(10 * frGiB), Bytes: int64(40 * frGiB), Tables: 3}
	cases := []struct {
		name    string
		free    uint64
		est     DumpEstimate
		estErr  error
		status  CheckStatus
		has     []string
		hasNot  []string
		hasFix  bool
		schemas []string
	}{
		{name: "under half the data: what the console refuses", free: 4 * frGiB, est: plain, status: StatusWarn, hasFix: true,
			has: []string{"10.0 GiB", "4.0 GiB", "/stage", "refuses to start a full read"}},
		{name: "exactly half the data is not under it", free: 5 * frGiB, est: plain, status: StatusWarn,
			has: []string{"40.0 GiB", "5.0 GiB", "may not fit"}, hasNot: []string{"refuses"}},
		{name: "over the refusal line, under the tables: may not fit", free: 20 * frGiB, est: plain, status: StatusWarn, hasFix: true,
			has: []string{"40.0 GiB", "20.0 GiB", "may not fit", "upper bound"}},
		{name: "room for the dump, not for dump plus Parquet: passes and says the condition", free: 50 * frGiB, est: plain, status: StatusPass,
			has: []string{"40.0 GiB", "50.0 GiB", "72.0 GiB", "If the snapshot is written to this disk too"}},
		{name: "room for the peak", free: 100 * frGiB, est: plain, status: StatusPass,
			has: []string{"40.0 GiB", "100.0 GiB"}, hasNot: []string{"If the snapshot"}},
		// A user that sees no table reads the same as there being none.
		{name: "no visible tables is nothing sized, not a pass", free: frGiB, est: DumpEstimate{}, status: StatusSkip, schemas: []string{"shop"},
			has: []string{"nothing was sized", "sees no tables", "--schemas shop", "1.0 GiB"}, hasNot: []string{"needs about", "nothing to write"}},
		{name: "sizes could not be read: the free space is still reported", free: 7 * frGiB, estErr: errors.New("Error 1045: denied\nsecond line"), status: StatusSkip,
			has: []string{"7.0 GiB", "Error 1045: denied", "not compared"}, hasNot: []string{"second line"}},
		{name: "compressed tables: never a pass, never fits", free: 100 * frGiB,
			est:    DumpEstimate{DataBytes: int64(10 * frGiB), Bytes: int64(10 * frGiB), Tables: 2, Compressed: 1, CompressedTop: []string{"shop.zipped"}},
			status: StatusSkip, has: []string{"cannot tell", "shop.zipped", "compressed size"}, hasNot: []string{"upper bound", "needs about"}},
		{name: "compressed tables under the refusal line: still what the console refuses", free: 4 * frGiB,
			est:    DumpEstimate{DataBytes: int64(10 * frGiB), Bytes: int64(10 * frGiB), Tables: 2, Compressed: 1, CompressedTop: []string{"shop.zipped"}},
			status: StatusWarn, has: []string{"refuses to start a full read", "shop.zipped"}, hasNot: []string{"upper bound"}},
		{name: "compressed tables under the tables' size: no upper bound claimed", free: 8 * frGiB,
			est:    DumpEstimate{DataBytes: int64(10 * frGiB), Bytes: int64(10 * frGiB), Tables: 2, Compressed: 1, CompressedTop: []string{"shop.zipped"}},
			status: StatusWarn, has: []string{"may not fit", "shop.zipped"}, hasNot: []string{"upper bound"}},
		{name: "a table with no size: room for the sum is not room for the read", free: 100 * frGiB,
			est:    DumpEstimate{DataBytes: int64(frGiB), Bytes: int64(frGiB), Tables: 2, Unsized: 1},
			status: StatusSkip, has: []string{"cannot tell", "no size for 1 table"}, hasNot: []string{"needs about"}},
		{name: "no table has a size: never needs 0 B", free: 100 * frGiB,
			est:    DumpEstimate{Tables: 3, Unsized: 3},
			status: StatusSkip, has: []string{"cannot tell", "no size for 3 table"}, hasNot: []string{"needs about"}},
		{name: "stale sizes are said on a pass", free: 100 * frGiB,
			est:    DumpEstimate{DataBytes: int64(frGiB), Bytes: int64(frGiB), Tables: 2, Stale: true},
			status: StatusPass, has: []string{"up to a day old"}},
		{name: "a source that timed out says how long was waited", free: 7 * frGiB, estErr: context.DeadlineExceeded, status: StatusSkip,
			has: []string{"did not answer within 15s", "7.0 GiB"}, hasNot: []string{"deadline exceeded"}},
		{name: "a filesystem that reports 2^63 bytes free prints no negative size", free: 1 << 63,
			est:    DumpEstimate{DataBytes: int64(frGiB), Bytes: int64(frGiB), Tables: 1},
			status: StatusPass, has: []string{"8.0 EiB free"}, hasNot: []string{"-"}},
		{name: "two sizes that print alike are given in bytes", free: 10*frGiB - 1,
			est:    DumpEstimate{DataBytes: int64(10*frGiB + 2), Bytes: int64(10*frGiB + 2), Tables: 1},
			status: StatusWarn, has: []string{"bytes"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := fullReadDiskResult("/stage", "", c.free, c.schemas, c.est, c.estErr)
			if got.Name != FullReadDiskCheckName || got.Status != c.status {
				t.Fatalf("%s/%s, want %s: %s", got.Name, got.Status, c.status, got.Detail)
			}
			for _, w := range c.has {
				if !strings.Contains(got.Detail, w) {
					t.Errorf("detail lacks %q:\n%s", w, got.Detail)
				}
			}
			for _, w := range c.hasNot {
				if strings.Contains(got.Detail, w) {
					t.Errorf("detail has %q:\n%s", w, got.Detail)
				}
			}
			if c.hasFix && got.Remediation == "" {
				t.Error("a warning with no remediation")
			}
			if got.Status != StatusWarn && got.Remediation != "" {
				t.Errorf("remediation on a %s: %s", got.Status, got.Remediation)
			}
			// Every outcome says whose disk and whose tables it measured.
			if !strings.Contains(got.Detail, "/stage") {
				t.Errorf("detail does not name the folder:\n%s", got.Detail)
			}
			if strings.Contains(got.Detail, "\n") || strings.Contains(got.Detail, "—") {
				t.Errorf("detail is not one plain line:\n%s", got.Detail)
			}
		})
	}
	// The grade follows the same two ratios the console's check uses.
	if DumpRefuseTenths != 5 || DumpPeakTenths != 18 {
		t.Fatalf("ratios changed (%d, %d): the cases above assume 0.5x and 1.8x", DumpRefuseTenths, DumpPeakTenths)
	}
}

// The folder as the operator typed it: not there yet, a file, relative,
// unmeasurable. None of them may report a number for the wrong thing.
func TestCheckFullReadDisk_theFolder(t *testing.T) {
	ctx := context.Background()
	est := DumpEstimate{DataBytes: int64(frGiB), Bytes: int64(frGiB), Tables: 1}

	t.Run("no folder given: the pointer, and nothing is measured or asked", func(t *testing.T) {
		s := stubFullReadDisk(t, 0, 0, nil, est, nil)
		for _, dir := range []string{"", "   "} {
			got := CheckFullReadDisk(ctx, dir, "dsn", nil)
			if got.Status != StatusSkip || !strings.Contains(got.Detail, "--staging-dir") || !strings.Contains(got.Detail, "Snapshots page") {
				t.Fatalf("%q: %s (%s)", dir, got.Status, got.Detail)
			}
		}
		if len(s.measured) != 0 || len(s.schemas) != 0 {
			t.Fatalf("measured %v, asked %v", s.measured, s.schemas)
		}
	})
	t.Run("a folder that does not exist yet: its nearest existing parent is measured, and named", func(t *testing.T) {
		s := stubFullReadDisk(t, 100*frGiB, 200*frGiB, nil, est, nil)
		base := t.TempDir()
		dir := filepath.Join(base, "not", "there")
		got := CheckFullReadDisk(ctx, dir, "dsn", nil)
		if got.Status != StatusPass || len(s.measured) != 1 || s.measured[0] != base {
			t.Fatalf("%s, measured %v, want %s: %s", got.Status, s.measured, base, got.Detail)
		}
		if !strings.Contains(got.Detail, dir) || !strings.Contains(got.Detail, "does not exist yet") || !strings.Contains(got.Detail, base) {
			t.Fatalf("detail = %s", got.Detail)
		}
	})
	t.Run("a file is not a folder", func(t *testing.T) {
		s := stubFullReadDisk(t, 100*frGiB, 200*frGiB, nil, est, nil)
		file := filepath.Join(t.TempDir(), "f")
		if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		got := CheckFullReadDisk(ctx, file, "dsn", nil)
		if got.Status != StatusWarn || !strings.Contains(got.Detail, "is a file") || len(s.measured) != 0 || len(s.schemas) != 0 {
			t.Fatalf("%s (%s), measured %v", got.Status, got.Detail, s.measured)
		}
		// So is a folder under a file.
		got = CheckFullReadDisk(ctx, filepath.Join(file, "sub"), "dsn", nil)
		if got.Status != StatusWarn || !strings.Contains(got.Detail, "is a file") {
			t.Fatalf("under a file: %s (%s)", got.Status, got.Detail)
		}
	})
	t.Run("a link to a folder that is not there is not measured at its parent", func(t *testing.T) {
		s := stubFullReadDisk(t, 100*frGiB, 200*frGiB, nil, est, nil)
		base := t.TempDir()
		link := filepath.Join(base, "stage")
		if err := os.Symlink(filepath.Join(base, "unmounted", "stage"), link); err != nil {
			t.Skip("no symlinks here:", err)
		}
		for _, dir := range []string{link, filepath.Join(link, "sub")} {
			got := CheckFullReadDisk(ctx, dir, "dsn", nil)
			if got.Status != StatusSkip || !strings.Contains(got.Detail, "is a link to something that is not there") || len(s.measured) != 0 {
				t.Fatalf("%s: %s (%s), measured %v", dir, got.Status, got.Detail, s.measured)
			}
		}
		// A link to a folder that is there is that folder.
		real := t.TempDir()
		good := filepath.Join(base, "good")
		if err := os.Symlink(real, good); err != nil {
			t.Fatal(err)
		}
		if got := CheckFullReadDisk(ctx, good, "dsn", nil); got.Status != StatusPass || len(s.measured) != 1 || s.measured[0] != good {
			t.Fatalf("live link: %s (%s), measured %v", got.Status, got.Detail, s.measured)
		}
	})
	t.Run("a parent that cannot be looked into is cannot tell, with no number", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root reads every folder")
		}
		s := stubFullReadDisk(t, 100*frGiB, 200*frGiB, nil, est, nil)
		locked := filepath.Join(t.TempDir(), "locked")
		if err := os.Mkdir(locked, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(locked, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
		got := CheckFullReadDisk(ctx, filepath.Join(locked, "stage"), "dsn", nil)
		if got.Status != StatusSkip || !strings.Contains(got.Detail, "could not be looked at") || strings.Contains(got.Detail, "GiB") || len(s.measured) != 0 {
			t.Fatalf("%s (%s), measured %v", got.Status, got.Detail, s.measured)
		}
	})
	t.Run("a relative folder is measured, and named, by its full path", func(t *testing.T) {
		s := stubFullReadDisk(t, 100*frGiB, 200*frGiB, nil, est, nil)
		t.Chdir(t.TempDir())
		wd, _ := os.Getwd()
		got := CheckFullReadDisk(ctx, ".", "dsn", nil)
		if got.Status != StatusPass || len(s.measured) != 1 || !filepath.IsAbs(s.measured[0]) || !strings.Contains(got.Detail, wd) {
			t.Fatalf("%s, measured %v: %s", got.Status, s.measured, got.Detail)
		}
	})
	t.Run("a disk that cannot answer is cannot tell, never full, and the source is not asked", func(t *testing.T) {
		dir := t.TempDir()
		s := stubFullReadDisk(t, 0, 0, nil, est, nil)
		got := CheckFullReadDisk(ctx, dir, "dsn", nil)
		if got.Status != StatusSkip || !strings.Contains(got.Detail, "reports no size") || len(s.schemas) != 0 {
			t.Fatalf("no size: %s (%s)", got.Status, got.Detail)
		}
		stubFullReadDisk(t, 0, 0, errors.New("statfs: boom\nmore"), est, nil)
		got = CheckFullReadDisk(ctx, dir, "dsn", nil)
		if got.Status != StatusSkip || !strings.Contains(got.Detail, "statfs: boom") || strings.Contains(got.Detail, "more") {
			t.Fatalf("error: %s (%s)", got.Status, got.Detail)
		}
	})
	t.Run("a full disk is zero free with a real total: a warning", func(t *testing.T) {
		stubFullReadDisk(t, 0, 200*frGiB, nil, est, nil)
		got := CheckFullReadDisk(ctx, t.TempDir(), "dsn", nil)
		if got.Status != StatusWarn || !strings.Contains(got.Detail, "0 B") {
			t.Fatalf("%s (%s)", got.Status, got.Detail)
		}
	})
	t.Run("the schema list reaches the estimate as given", func(t *testing.T) {
		s := stubFullReadDisk(t, 100*frGiB, 200*frGiB, nil, est, nil)
		got := CheckFullReadDisk(ctx, t.TempDir(), "dsn", []string{"Shop", "b"})
		if len(s.schemas) != 1 || strings.Join(s.schemas[0], ",") != "Shop,b" || !strings.Contains(got.Detail, "--schemas Shop,b") {
			t.Fatalf("asked %v: %s", s.schemas, got.Detail)
		}
	})
}

// A source that never answers costs the wait and a line that says so, not a
// report that never prints.
func TestCheckFullReadDisk_aSourceThatHangs(t *testing.T) {
	prev := fullReadEstimateWait
	fullReadEstimateWait = 50 * time.Millisecond
	t.Cleanup(func() { fullReadEstimateWait = prev })
	prevDisk, prevEst := fullReadDiskSpaceFn, fullReadEstimateFn
	fullReadDiskSpaceFn = func(string) (uint64, uint64, error) { return 9 * frGiB, 10 * frGiB, nil }
	fullReadEstimateFn = func(ctx context.Context, _ string, _ []string) (DumpEstimate, error) {
		<-ctx.Done()
		return DumpEstimate{}, ctx.Err()
	}
	t.Cleanup(func() { fullReadDiskSpaceFn, fullReadEstimateFn = prevDisk, prevEst })

	done := make(chan CheckResult, 1)
	go func() { done <- CheckFullReadDisk(context.Background(), t.TempDir(), "dsn", nil) }()
	select {
	case got := <-done:
		if got.Status != StatusSkip || !strings.Contains(got.Detail, "9.0 GiB") || !strings.Contains(got.Detail, "did not answer within 50ms") {
			t.Fatalf("%s (%s)", got.Status, got.Detail)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the check did not return")
	}
	if DumpEstimateServerLimit >= fullReadEstimateWaitDefault {
		t.Fatalf("the server-side limit %v is not under doctor's wait %v", DumpEstimateServerLimit, fullReadEstimateWaitDefault)
	}
}

// Both outcomes as the report prints them, in text and in JSON: the pointer is
// one skipped line and not a finding, and a small folder is a warning with
// both numbers.
func TestFullReadDisk_inTheReport(t *testing.T) {
	render := func(c CheckResult, format string) string {
		r := &Report{}
		r.Add(c)
		var buf bytes.Buffer
		if err := r.Write(&buf, format); err != nil {
			t.Fatal(err)
		}
		if err := r.Err(); err != nil {
			t.Fatalf("the check changed the exit code: %v", err)
		}
		return buf.String()
	}
	pointer := CheckFullReadDisk(context.Background(), "", "dsn", nil)
	text := render(pointer, "text")
	if !strings.HasPrefix(text, "- "+FullReadDiskCheckName+" (not checked: ") || !strings.Contains(text, "Warnings: 0  Skipped: 1") {
		t.Fatalf("pointer, text:\n%s", text)
	}
	if js := render(pointer, "json"); !strings.Contains(js, `"status": "skip"`) || !strings.Contains(js, "--staging-dir") {
		t.Fatalf("pointer, json:\n%s", js)
	}

	small := fullReadDiskResult("/var/tmp/stage", "", 4*frGiB, nil, DumpEstimate{DataBytes: int64(10 * frGiB), Bytes: int64(40 * frGiB), Tables: 3}, nil)
	text = render(small, "text")
	if !strings.HasPrefix(text, "! "+FullReadDiskCheckName+" (") || !strings.Contains(text, "10.0 GiB") || !strings.Contains(text, "4.0 GiB") || !strings.Contains(text, "Warnings: 1") {
		t.Fatalf("small folder, text:\n%s", text)
	}
	t.Logf("pointer:\n%s\nsmall folder:\n%s", render(pointer, "text"), text)
}
