package console

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// Several copies S3 lacks and, among them, the last restore's result whose
// question S3 failed to answer (#2691): from the record the real cleanup
// writes, through the settings row the page reads, to the line the page
// draws. The line says which copies S3 lacks and which it could not be
// asked about; it does not say S3 lacks them all.
func TestNotInS3_aRestoreResultNotAskedAmongSeveral(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	srv, _ := newLocalCopyServer(t)
	_, dto := createServer(t, srv, newServerBody+`,"baseline_s3":"s3://bucket/prod/"}`)
	e, _ := srv.cm.reg.Get(dto.ID)
	dir := e.BaselineDir
	now := time.Now().UTC().Truncate(time.Second)
	name := func(d time.Duration) string { return now.Add(-d).Format("2006-01-02T15-04-05") + "Z" }
	a, b, restored, newest := name(3*time.Hour), name(2*time.Hour), name(30*time.Hour), name(time.Hour)
	for _, n := range []string{a, b, restored, newest} {
		pinFixture(t, dir, n)
	}
	if err := baseline.MarkRestored(dir, restored); err != nil {
		t.Fatal(err)
	}
	has := func(_ context.Context, n string) (bool, error) {
		if n == restored {
			return false, errors.New("api error AccessDenied")
		}
		return false, nil
	}
	if _, err := baseline.PruneLocalAsking(context.Background(), baseline.PruneOptions{LocalDir: dir, S3URL: "s3://bucket/prod/", KeepNewest: 1}, has); err != nil {
		t.Fatal(err)
	}
	row := srv.backupSettingsServerDTO(e)
	if row.NotInS3 == nil {
		t.Fatalf("no not_in_s3 on the row: %+v", row)
	}
	raw, _ := json.Marshal(row.NotInS3)
	t.Logf("not_in_s3 on the wire: %s", raw)
	script := renderHarnessJS + `
const rec = ` + string(raw) + `;
console.log(JSON.stringify(vm.runInContext("notInS3Words", ctx)(rec.count, rec)));
`
	path := filepath.Join(t.TempDir(), "notin.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var line string
	if err := json.Unmarshal(out, &line); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	t.Logf("the page says: %s", line)
	label := func(n string) string {
		ts, _ := time.Parse("2006-01-02T15-04-05Z", n)
		return ts.UTC().Format("2006-01-02 15:04:05") + " UTC"
	}
	want := "Kept here: 3 older copies (" + label(a) + ", " + label(b) + ", " + label(restored) + "). S3 does not have 2 older copies (" +
		label(a) + ", " + label(b) + ") complete, and could not be asked about the copy of " + label(restored) +
		"; send them to S3 again, or remove them by hand if not needed."
	if line != want {
		t.Fatalf("the page says\n  %q\nwant\n  %q", line, want)
	}
}

// pinFixture writes a complete snapshot <dir>/<name>/shop/orders.parquet.
func pinFixture(t *testing.T, dir, name string) {
	t.Helper()
	p := filepath.Join(dir, name, "shop", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(filepath.Join(dir, name)); err != nil {
		t.Fatal(err)
	}
}
