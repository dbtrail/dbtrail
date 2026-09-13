package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// unreadable makes dir unreadable for the duration of the test. Root bypasses
// directory permissions, so the test that needs it skips there rather than
// pass on a fixture that did nothing.
func unreadable(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		// Under CI a root runner would turn every #1601 test green by skip
		// with no signal; fail there so the loss of coverage is seen.
		if os.Getenv("CI") != "" {
			t.Fatal("running as root under CI: the mode-000 fixture is a no-op and this coverage would silently vanish")
		}
		t.Skip("root bypasses directory read permissions; the mode-000 fixture is a no-op")
	}
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
}

// TestListBaselinesReport_countsWhatItCouldNotRead (#1601): a snapshot
// directory the walk cannot open is skipped, as before, and now COUNTED, so a
// consumer can tell "listed everything" from "listed what it could". The
// files it did read still come back, and the plain ListBaselines is
// unchanged.
func TestListBaselinesReport_countsWhatItCouldNotRead(t *testing.T) {
	dir := t.TempDir()
	writeListFixture(t, dir, "2026-06-01T00-00-00Z", "shop", "orders.parquet")
	writeListFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	unreadable(t, filepath.Join(dir, "2026-06-10T12-00-00Z"))

	files, skipped, err := ListBaselinesReport(context.Background(), dir)
	if err != nil {
		t.Fatalf("a partial listing is not an error: %v", err)
	}
	if skipped != 1 {
		t.Errorf("skipped = %d, want 1: the unreadable snapshot directory was not counted", skipped)
	}
	if len(files) != 1 || files[0].SnapshotTime.Format("2006-01-02") != "2026-06-01" {
		t.Errorf("the readable snapshot was not listed: %+v", files)
	}
	// The count is a floor that the newest snapshot hides behind: the file
	// list alone says "newest is June 1", which is false.
	plain, perr := ListBaselines(context.Background(), dir)
	if perr != nil || len(plain) != len(files) {
		t.Errorf("ListBaselines diverged from the report: %v, %d files", perr, len(plain))
	}
}

// An unreadable SCHEMA directory inside a readable snapshot is the other
// append site of the same skip; it counts too.
func TestListBaselinesReport_countsAnUnreadableSchemaDirectory(t *testing.T) {
	dir := t.TempDir()
	writeListFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	writeListFixture(t, dir, "2026-06-10T12-00-00Z", "billing", "invoices.parquet")
	unreadable(t, filepath.Join(dir, "2026-06-10T12-00-00Z", "billing"))

	files, skipped, err := ListBaselinesReport(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 1 || len(files) != 1 || files[0].Schema != "shop" {
		t.Errorf("skipped = %d, files = %+v; want the billing directory counted and shop listed", skipped, files)
	}
}

// A fully readable tree reports zero, so the count never turns a healthy
// location into an unknown verdict downstream.
func TestListBaselinesReport_zeroWhenEverythingReads(t *testing.T) {
	dir := t.TempDir()
	writeListFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	_, skipped, err := ListBaselinesReport(context.Background(), dir)
	if err != nil || skipped != 0 {
		t.Errorf("skipped = %d, err = %v; want 0, nil", skipped, err)
	}
}
