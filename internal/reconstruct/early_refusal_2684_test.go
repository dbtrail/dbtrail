package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// A complete dump an earlier run left in the output folder.
func seedCompleteDump(t *testing.T, dir string, files ...string) {
	t.Helper()
	writeFiles(t, dir, append([]string{baseline.SuccessMarker, "metadata", baseline.ChangesOnlyMarkerFile}, files...)...)
}

// unreachableIndex is an index DSN nothing listens on: the run is refused
// when it connects, the first thing it does after the checks that need no
// index.
const unreachableIndex = "root:x@tcp(127.0.0.1:1)/idx"

// #2684. A run refused before any table starts leaves the output folder as it
// found it: the earlier dump's files AND its markers. Before, the markers
// changed first (_SUCCESS and _CHANGES_ONLY removed, _INCOMPLETE written), so
// a refusal turned a complete dump into one `bintrail baseline` refuses; and a
// wrong snapshot location deleted the earlier dump's tables, then failed.
func TestReconstructTables_anEarlyRefusalLeavesTheFolderAsItWas(t *testing.T) {
	cases := []struct {
		name    string
		cfg     func(out, snapshots string) FullTableConfig
		files   []string
		wantErr string
	}{
		{"a bad --tables entry", func(out, snaps string) FullTableConfig {
			return FullTableConfig{IndexDSN: unreachableIndex, BaselineSrc: snaps, OutputDir: out,
				Tables: []string{"shop.t", "shop."}, OutputFormat: OutputFormatMydumper}
		}, []string{"shop.t-schema.sql", "shop.t.00000.sql"}, `invalid --tables entry "shop."`},
		{"psql tables that share a file", func(out, snaps string) FullTableConfig {
			return FullTableConfig{IndexDSN: unreachableIndex, BaselineSrc: snaps, OutputDir: out,
				Tables: []string{"shop.orders", "shop.Orders"}, OutputFormat: OutputFormatPsql}
		}, []string{"shop.orders.sql", "shop.Orders.sql"}, "would be written to one file"},
		{"a snapshot folder that does not exist", func(out, snaps string) FullTableConfig {
			return FullTableConfig{IndexDSN: unreachableIndex, BaselineSrc: filepath.Join(snaps, "typo"), OutputDir: out,
				Tables: []string{"shop.t"}, OutputFormat: OutputFormatMydumper}
		}, []string{"shop.t-schema.sql", "shop.t.00000.sql"}, "cannot be read"},
		{"a snapshot location that is a file", func(out, snaps string) FullTableConfig {
			writeFiles(t, snaps, "afile")
			return FullTableConfig{IndexDSN: unreachableIndex, BaselineSrc: filepath.Join(snaps, "afile"), OutputDir: out,
				Tables: []string{"shop.t"}, OutputFormat: OutputFormatMydumper}
		}, []string{"shop.t-schema.sql", "shop.t.00000.sql"}, "is not a folder"},
		{"an index that cannot be reached", func(out, snaps string) FullTableConfig {
			return FullTableConfig{IndexDSN: unreachableIndex, BaselineSrc: snaps, OutputDir: out,
				Tables: []string{"shop.t"}, OutputFormat: OutputFormatMydumper}
		}, []string{"shop.t-schema.sql", "shop.t.00000.sql"}, "connect to index DB"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, snaps := t.TempDir(), t.TempDir()
			seedCompleteDump(t, out, tc.files...)
			before := dirNames(t, out)
			_, err := ReconstructTables(context.Background(), tc.cfg(out, snaps))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want one saying %q", err, tc.wantErr)
			}
			t.Logf("rendered: %s", err)
			if after := dirNames(t, out); !slices.Equal(after, before) {
				t.Errorf("folder changed by a run refused up front:\nbefore %v\nafter  %v", before, after)
			}
		})
	}
}

// An S3 location is listed once before anything changes: a bucket that does
// not exist, or that this host may not read, refuses the run with the folder
// as it was. A location that answers, even with nothing in it, lets the run go
// on: every table is then refused "no snapshot", which is the case the
// refusal exists for.
func TestReconstructTables_theS3LocationIsReadFirst(t *testing.T) {
	orig := listS3Location
	t.Cleanup(func() { listS3Location = orig })

	listS3Location = func(context.Context, string) error { return errors.New("NoSuchBucket: The specified bucket does not exist") }
	out := t.TempDir()
	seedCompleteDump(t, out, "shop.t-schema.sql", "shop.t.00000.sql")
	before := dirNames(t, out)
	_, err := ReconstructTables(context.Background(), FullTableConfig{IndexDSN: unreachableIndex, BaselineSrc: "s3://typo-bucket/snapshots",
		OutputDir: out, Tables: []string{"shop.t"}, OutputFormat: OutputFormatMydumper})
	if err == nil || !strings.Contains(err.Error(), `the snapshot location "s3://typo-bucket/snapshots" cannot be read`) ||
		!strings.Contains(err.Error(), "NoSuchBucket") {
		t.Fatalf("err = %v", err)
	}
	t.Logf("rendered: %s", err)
	if after := dirNames(t, out); !slices.Equal(after, before) {
		t.Errorf("folder changed: %v", after)
	}

	listS3Location = func(context.Context, string) error { return nil }
	_, err = ReconstructTables(context.Background(), FullTableConfig{IndexDSN: unreachableIndex, BaselineSrc: "s3://empty-bucket/snapshots",
		OutputDir: t.TempDir(), Tables: []string{"shop.t"}, OutputFormat: OutputFormatMydumper})
	if err == nil || !strings.Contains(err.Error(), "connect to index DB") {
		t.Errorf("a location that answers did not let the run go on to the index: %v", err)
	}
}

// An empty local location passes the check: it is a location with no
// snapshot, not a wrong one.
func TestCheckSnapshotLocation(t *testing.T) {
	ctx := context.Background()
	if err := checkSnapshotLocation(ctx, t.TempDir()); err != nil {
		t.Errorf("empty folder: %v", err)
	}
	if err := checkSnapshotLocation(ctx, filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("a folder that does not exist passed")
	}
	if os.Geteuid() != 0 {
		dir := filepath.Join(t.TempDir(), "locked")
		if err := os.Mkdir(dir, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(dir, 0o755) })
		if err := checkSnapshotLocation(ctx, dir); err == nil {
			t.Error("a folder that cannot be read passed")
		}
	}
}
