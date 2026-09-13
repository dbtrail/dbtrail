package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/views"
)

const groupTestID = "aaaa"

// A real directory with real files: ArchiveGroups stats every local path before
// it will put one in a group (a registered file that is gone would make DuckDB
// refuse the whole script).
func groupTestBase(t *testing.T) string {
	t.Helper()
	base := filepath.Join(t.TempDir(), "bintrail_id="+groupTestID)
	for _, hour := range []string{"03", "04"} {
		p := groupTestFile(base, hour)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return base
}

func groupTestFile(base, hour string) string {
	return base + "/event_date=2026-05-01/event_hour=" + hour + "/e.parquet"
}

// The whole point of #1535 reaching the downloadable file: `bintrail views`
// must carry the groups, or the file it writes still binds one footer per
// archived file in the operator's own DuckDB.
func TestResolveArchiveGroupsFrom_carriesTheGroups(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	base := groupTestBase(t)
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "local_path", "s3_key", "column_set"}).
			AddRow(groupTestID, groupTestFile(base, "03"), nil, "event_id,query_text").
			AddRow(groupTestID, groupTestFile(base, "04"), nil, "event_id"))

	in := views.Input{ArchiveSources: []string{base}}
	if err := resolveArchiveGroupsFrom(context.Background(), db, &in); err != nil {
		t.Fatal(err)
	}
	if in.UngroupedPartitions != 0 {
		t.Fatalf("UngroupedPartitions = %d, want 0", in.UngroupedPartitions)
	}
	if len(in.ArchiveGroups) != 2 {
		t.Fatalf("%d group(s) reached the generator, want 2", len(in.ArchiveGroups))
	}
	// Rendered, not just carried: the field is only worth anything if the file
	// it produces actually names the files instead of the glob.
	sql := views.Generate(views.Input{
		ArchiveSources:      in.ArchiveSources,
		ArchiveGroups:       in.ArchiveGroups,
		UngroupedPartitions: in.UngroupedPartitions,
	})
	if strings.Contains(sql, "union_by_name = true") {
		t.Errorf("the generated file still asks DuckDB to unify every footer:\n%s", sql)
	}
	if !strings.Contains(sql, groupTestFile(base, "03")) {
		t.Errorf("the generated file does not name the archived partitions:\n%s", sql)
	}
}

// The all-or-nothing rule. One unrecorded partition and the file keeps the
// globbed leg, because the group list comes from the registry: grouping a
// partial registry would leave the unrecorded partitions out of the view
// entirely, which is a wrong answer rather than a slow one.
func TestResolveArchiveGroupsFrom_partialRegistryKeepsTheGlob(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	base := groupTestBase(t)
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "local_path", "s3_key", "column_set"}).
			AddRow(groupTestID, groupTestFile(base, "03"), nil, "event_id,query_text").
			AddRow(groupTestID, groupTestFile(base, "04"), nil, nil))

	in := views.Input{ArchiveSources: []string{base}}
	if err := resolveArchiveGroupsFrom(context.Background(), db, &in); err != nil {
		t.Fatal(err)
	}
	if len(in.ArchiveGroups) != 0 {
		t.Fatalf("grouping was used over a partial registry: %d group(s)", len(in.ArchiveGroups))
	}
	if in.UngroupedPartitions != 1 {
		t.Fatalf("UngroupedPartitions = %d, want 1", in.UngroupedPartitions)
	}

	// And the file says so, with the command that fixes it. An operator who
	// cannot see why the wait is still there has no way to act on it.
	sql := views.Generate(views.Input{
		ArchiveSources:      in.ArchiveSources,
		UngroupedPartitions: in.UngroupedPartitions,
	})
	if !strings.Contains(sql, "cannot be grouped by schema") {
		t.Errorf("the file does not explain why it still binds every footer:\n%s", sql)
	}
	if !strings.Contains(sql, "bintrail archive reconcile") || !strings.Contains(sql, "--repair") {
		t.Errorf("the file names no way to fix it:\n%s", sql)
	}
}
