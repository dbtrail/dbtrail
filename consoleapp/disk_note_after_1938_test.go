package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
)

// #1938: a low-disk warning is written before the dump, in the future tense
// ("this read may fail with a full disk"), and it was shown as written after
// the read had finished well, in the error style. Once the snapshot exists
// the warning is about the NEXT read, and says so.

func TestDumpDiskOnceItFit_1938(t *testing.T) {
	stage, local := t.TempDir(), t.TempDir()
	plain := dumpEstimate{dataBytes: int64(10 * gib), bytes: int64(10 * gib), tables: 4}
	zipped := dumpEstimate{dataBytes: int64(10 * gib), bytes: int64(10 * gib), tables: 4, compressed: 1, compressedTop: []string{"shop.orders"}}

	type lowCase struct {
		name     string
		localDir string
		sameFS   bool
		free     map[string]uint64
		wantFit  string // the sentence the note carries once the read fit
	}
	cases := []lowCase{
		{"below the sizes", "", true, map[string]uint64{stage: 9 * gib}, diskRiskDumpFit},
		{"below the peak, one disk", "", true, map[string]uint64{stage: 15 * gib}, diskRiskFit},
		{"the snapshot folder's disk is short", local, false, map[string]uint64{stage: 100 * gib, local: gib}, diskRiskFit},
	}
	for _, c := range cases {
		for estName, est := range map[string]dumpEstimate{"": plain, ", compressed tables": zipped} {
			t.Run(c.name+estName, func(t *testing.T) {
				stubSameFS(t, c.sameFS, nil)
				diskByPath(t, c.free)
				check, before, err := dumpDiskVerdict(stage, c.localDir, est, nil)
				if err != nil || check != dumpDiskLow {
					t.Fatalf("check = %q, err = %v: not a low-disk case", check, err)
				}
				gotCheck, after := dumpDiskOnceItFit(check, before)
				t.Logf("before: %s\n after: %s", before, after)
				if gotCheck != dumpDiskTight {
					t.Fatalf("check = %q, want %q", gotCheck, dumpDiskTight)
				}
				// Nothing in the future tense about a read that is over.
				for _, not := range []string{"may fail", "may not fit", "This read may"} {
					if strings.Contains(after, not) {
						t.Errorf("the note still says %q: %s", not, after)
					}
				}
				if strings.Count(after, c.wantFit) != 1 || !strings.Contains(after, "This read fit. The next one may not.") {
					t.Errorf("the note does not say the read fit and the next may not: %s", after)
				}
				// Only that sentence changed: the folder, the numbers and
				// how to get more room are what they were.
				risk := diskRisk
				if c.wantFit == diskRiskDumpFit {
					risk = diskRiskDump
				}
				if strings.Replace(before, risk, c.wantFit, 1) != after {
					t.Errorf("more than the risk sentence changed:\nbefore: %s\n after: %s", before, after)
				}
			})
		}
	}

	t.Run("every other verdict is returned as it came", func(t *testing.T) {
		for _, c := range [][2]string{
			{dumpDiskOK, "Disk check: a full read needs about 10.0 GiB free at /w; 100.0 GiB free."},
			{dumpDiskUnchecked, "Disk check did not run: the table sizes could not be read. The full read went ahead."},
			{dumpDiskTight, "Low disk: /w has 15.0 GiB free. " + diskRiskFit},
			// The sentence alone decides nothing: the verdict has to be "low".
			{dumpDiskUnchecked, "Disk check did not finish. " + diskRisk},
			{dumpDiskOK, "Disk check: room. " + diskRiskDump},
			{"", ""},
		} {
			if check, note := dumpDiskOnceItFit(c[0], c[1]); check != c[0] || note != c[1] {
				t.Errorf("(%q, %q) became (%q, %q)", c[0], c[1], check, note)
			}
		}
	})
	t.Run("a low note in a wording this build did not write is not called a fit", func(t *testing.T) {
		old := "Low disk: the dump may not fit at /w."
		if check, note := dumpDiskOnceItFit(dumpDiskLow, old); check != dumpDiskLow || note != old {
			t.Errorf("(%q, %q)", check, note)
		}
		// Twice in one note is not a note this build writes either.
		twice := "Low disk. " + diskRisk + " " + diskRisk
		if check, note := dumpDiskOnceItFit(dumpDiskLow, twice); check != dumpDiskLow || note != twice {
			t.Errorf("(%q, %q)", check, note)
		}
	})
}

// writeWholeDump is writeHalfBrokenDump with the second table whole.
func writeWholeDump(t *testing.T, dir string) {
	t.Helper()
	writeHalfBrokenDump(t, dir)
	whole := dumpHeader + "INSERT INTO `bad` (`id`,`label`) VALUES(1,\"uno\")\n,(2,\"dos\")\n;\n"
	if err := os.WriteFile(filepath.Join(dir, "shop.bad.00000.sql"), []byte(whole), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Through Trigger, the real execute and the real conversion, on a disk the
// check called low: a read that works ends "tight" in the status and in the
// history, and one that fails keeps the warning it was given.
func TestFullRead_aLowDiskReadThatWorkedSaysItFit_1938(t *testing.T) {
	run := func(t *testing.T, write func(*testing.T, string)) (console.BaselineStatus, []console.BaselineRunRecord) {
		stage, local := t.TempDir(), t.TempDir()
		prev := runMydumperFunc
		runMydumperFunc = func(_ context.Context, _ string, _ config.SSL, _ []string, dir string, _ baseline.LockMode, _ lockModeSource) error {
			write(t, dir)
			return nil
		}
		t.Cleanup(func() { runMydumperFunc = prev })
		prevDDL, prevEv := dumpDDLMarkFunc, dumpEventMarkFunc
		dumpDDLMarkFunc = func(console.BaselineRequest) string { return "" }
		dumpEventMarkFunc = func(console.BaselineRequest) string { return "" }
		t.Cleanup(func() { dumpDDLMarkFunc, dumpEventMarkFunc = prevDDL, prevEv })
		stubEstimate(t, dumpEstimate{bytes: int64(10 * gib), dataBytes: int64(10 * gib), tables: 2}, nil)
		stubSameFS(t, true, nil)
		prevDisk := diskSpaceFn
		diskSpaceFn = func(string) (uint64, uint64, error) { return 15 * gib, 1 << 40, nil }
		t.Cleanup(func() { diskSpaceFn = prevDisk })
		s := supWithHistory(t, stage)
		st := runFullRead(t, s, console.BaselineRequest{ServerID: "s1", ServerName: "shop-db", SourceDSN: "src", LocalDir: local})
		return st, s.history.List("s1")
	}

	t.Run("the read worked", func(t *testing.T) {
		st, runs := run(t, writeWholeDump)
		if st.State != "succeeded" || st.DiskCheck != dumpDiskTight {
			t.Fatalf("state = %q, disk check = %q (%s; last error %q)", st.State, st.DiskCheck, st.DiskNote, st.LastError)
		}
		if !strings.Contains(st.DiskNote, "This read fit. The next one may not.") || strings.Contains(st.DiskNote, "may fail") {
			t.Fatalf("note = %q", st.DiskNote)
		}
		if len(runs) != 1 || runs[0].DiskCheck != dumpDiskTight || runs[0].DiskNote != st.DiskNote {
			t.Fatalf("history = %+v", runs)
		}
	})
	t.Run("the read failed: the warning it was given stays", func(t *testing.T) {
		st, runs := run(t, writeHalfBrokenDump)
		if st.State != "failed" || st.DiskCheck != dumpDiskLow || !strings.Contains(st.DiskNote, diskRisk) {
			t.Fatalf("state = %q, disk check = %q, note = %q", st.State, st.DiskCheck, st.DiskNote)
		}
		if len(runs) != 1 || runs[0].DiskCheck != dumpDiskLow || runs[0].DiskNote != st.DiskNote {
			t.Fatalf("history = %+v", runs)
		}
	})
}

// With a destination to copy to, the snapshot is written before the copy
// starts and the copy can fail. The warning was about the dump and its
// conversion, so it says the read fit from the moment the snapshot exists,
// and still says so when the copy fails afterwards.
func TestFullRead_aLowDiskNoteSaysItFitOnceTheSnapshotIsWritten_1938(t *testing.T) {
	stage, local := t.TempDir(), t.TempDir()
	ds := stubDumpUpload(t, map[string]bool{}, errors.New("upload: access denied"))
	stubDumpThatWrites(t, func(dir string) { writeWholeDump(t, dir) })
	stubEstimate(t, dumpEstimate{bytes: int64(10 * gib), dataBytes: int64(10 * gib), tables: 2}, nil)
	prevDisk := diskSpaceFn
	diskSpaceFn = func(string) (uint64, uint64, error) { return 15 * gib, 1 << 40, nil }
	t.Cleanup(func() { diskSpaceFn = prevDisk })
	s := supWithHistory(t, stage)
	if err := s.Trigger(console.BaselineRequest{ServerID: "s1", ServerName: "shop-db", SourceDSN: "src", LocalDir: local, S3: "s3://b/p"}); err != nil {
		t.Fatal(err)
	}
	<-ds.entered
	during := s.Status("s1")
	if !during.Uploading || !during.Published || during.DiskCheck != dumpDiskTight || !strings.Contains(during.DiskNote, diskRiskFit) {
		t.Fatalf("while the copy runs: uploading=%v published=%v check=%q note=%q", during.Uploading, during.Published, during.DiskCheck, during.DiskNote)
	}
	close(ds.hold)
	// The status reads "succeeded" from the local publish on, so wait for
	// the copy itself to end.
	var st console.BaselineStatus
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		if st = s.Status("s1"); !st.Uploading {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the copy never ended")
		}
	}
	if st.State != "failed" || !st.Published || st.DiskCheck != dumpDiskTight || st.DiskNote != during.DiskNote {
		t.Fatalf("after the copy failed: state=%q published=%v check=%q note=%q", st.State, st.Published, st.DiskCheck, st.DiskNote)
	}
	runs := s.history.List("s1")
	if len(runs) != 1 || runs[0].DiskCheck != dumpDiskTight || runs[0].DiskNote != st.DiskNote {
		t.Fatalf("history = %+v", runs)
	}
}
