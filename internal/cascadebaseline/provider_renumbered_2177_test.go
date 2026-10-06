package cascadebaseline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The #2177 provider halves that need no MySQL. The renumbering verdicts
// themselves run against a real index in
// provider_renumbered_2177_integration_test.go.

func markedChildBaseline(t *testing.T, mark string) string {
	t.Helper()
	dir := t.TempDir()
	md := map[string]string{
		baseline.MetaKeyBinlogFile: "mysql-bin.000009",
		baseline.MetaKeyBinlogPos:  "500",
	}
	if mark != "" {
		md[baseline.MetaKeyEventMark] = mark
	}
	writeChildBaselineParquet(t, dir, "2026-01-01T00-00-00Z", "shop", [][]string{{"10", "1"}}, md)
	return dir
}

var renumberedTestAt = time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)

// No mark: today's behavior, and the index is not read at all (sqlmock has
// no expectations, so any query would fail the lookup).
func TestProvider_noEventMarkReadsNothingFromTheIndex_2177(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir := markedChildBaseline(t, "")
	lookup, ok, err := New(Source(dir), childResolver("shop"), db).
		BaselineChildren(context.Background(), "shop", "child", "pid", "1", renumberedTestAt, 100)
	if err != nil || !ok || len(lookup.Rows) != 1 {
		t.Fatalf("no mark: ok=%v rows=%d err=%v, want the baseline used as before", ok, len(lookup.Rows), err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A marked baseline with no index to check it against is an error, never a
// skipped check.
func TestProvider_markedBaselineWithoutIndexIsAnError_2177(t *testing.T) {
	mark := reconstruct.EventMark{ID: 10, File: "mysql-bin.000009", End: 400}.Encode()
	dir := markedChildBaseline(t, mark)
	_, ok, err := New(Source(dir), childResolver("shop"), nil).
		BaselineChildren(context.Background(), "shop", "child", "pid", "1", renumberedTestAt, 100)
	if err == nil || ok {
		t.Fatalf("ok=%v err=%v, want an error: the check cannot run without the index", ok, err)
	}
	if errors.Is(err, reconstruct.ErrBinlogRenumbered) {
		t.Errorf("a missing index connection is not a renumbering verdict: %v", err)
	}
}

// A check that cannot read the index is an error with its cause, never a
// pass and never a renumbering.
func TestProvider_failedNumberingCheckIsAnError_2177(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("index_state").WillReturnError(errors.New("Error 1146: Table 'index_state' doesn't exist"))
	mark := reconstruct.EventMark{ID: 10, File: "mysql-bin.000009", End: 400}.Encode()
	dir := markedChildBaseline(t, mark)
	_, ok, err := New(Source(dir), childResolver("shop"), db).
		BaselineChildren(context.Background(), "shop", "child", "pid", "1", renumberedTestAt, 100)
	if err == nil || ok {
		t.Fatalf("ok=%v err=%v, want the failed check as an error", ok, err)
	}
	if errors.Is(err, reconstruct.ErrBinlogRenumbered) {
		t.Errorf("a failed check is not a renumbering verdict: %v", err)
	}
	if !strings.Contains(err.Error(), "check the binlog numbering since the snapshot of shop.child") || !strings.Contains(err.Error(), "1146") {
		t.Errorf("error must name the check and its cause: %v", err)
	}
}
