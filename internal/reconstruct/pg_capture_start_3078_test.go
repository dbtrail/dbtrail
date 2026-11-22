package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func TestPGFileBeforeCapture_3078(t *testing.T) {
	for _, tc := range []struct {
		name         string
		floor, start uint64
		known, want  bool
	}{
		{"capture started before the file's floor", 500, 400, true, false},
		{"capture starts at the slot the snapshot created", 500, 500, true, false},
		{"capture started after the file's floor", 500, 501, true, true},
		{"no start on record", 500, 0, false, false},
		{"a start that reads as unknown is not a start", 500, 900, false, false},
		{"a file with no floor", 0, 900, true, false},
	} {
		if got := pgFileBeforeCapture(tc.floor, tc.start, tc.known); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// answer makes pgCaptureStartRead answer lsn, ok, err.
func answer(lsn uint64, ok bool, err error) func(context.Context, *sql.DB) (uint64, bool, error) {
	return func(context.Context, *sql.DB) (uint64, bool, error) { return lsn, ok, err }
}

func TestPGRefuseFileBeforeCapture_3078(t *testing.T) {
	prev := pgCaptureStartRead
	t.Cleanup(func() { pgCaptureStartRead = prev })
	db := &sql.DB{}
	bmeta := baseline.DumpMetadata{LSN: 0x9_B74B_0000}

	pgCaptureStartRead = answer(0xD_3D66_B980, true, nil)
	err := pgRefuseFileBeforeCapture(db, FullTableConfig{}, "public", "big", bmeta)
	if err == nil {
		t.Fatal("a file capture started after was not refused")
	}
	for _, want := range []string{"public.big", "9/B74B0000", "D/3D66B980", "never captured", "Take a new snapshot (`snapshot run`)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q:\n%v", want, err)
		}
	}
	if err := pgRefuseFileBeforeCapture(db, FullTableConfig{AllowGaps: true}, "public", "big", bmeta); err != nil {
		t.Errorf("a run told to go on over missing changes was refused: %v", err)
	}
	if err := pgRefuseFileBeforeCapture(nil, FullTableConfig{}, "public", "big", bmeta); err != nil {
		t.Errorf("no index: %v", err)
	}
	asked := false
	pgCaptureStartRead = func(context.Context, *sql.DB) (uint64, bool, error) { asked = true; return 1, true, nil }
	if err := pgRefuseFileBeforeCapture(db, FullTableConfig{}, "public", "big", baseline.DumpMetadata{}); err != nil || asked {
		t.Errorf("a file with no floor: err %v, index asked %v", err, asked)
	}
	pgCaptureStartRead = answer(0x9_B74B_0000, true, nil)
	if err := pgRefuseFileBeforeCapture(db, FullTableConfig{}, "public", "big", bmeta); err != nil {
		t.Errorf("capture starting at the file's floor was refused: %v", err)
	}
}

// The three answers of the index, in the update (#3078): a start, nothing
// recorded, and a read that failed. A failed read is an error of the update,
// never a table let through: rewritten with a new floor, a gap it carries
// could not be seen again.
func TestPGRefuseFileBeforeCaptureReadFails_3078(t *testing.T) {
	prev := pgCaptureStartRead
	t.Cleanup(func() { pgCaptureStartRead = prev })
	db := &sql.DB{}
	bmeta := baseline.DumpMetadata{LSN: 0x9_B74B_0000}
	denied := errors.New("Error 1142 (42000): SELECT command denied to user 'ro'@'%' for table 'stream_state'")

	pgCaptureStartRead = answer(0, false, nil)
	if err := pgRefuseFileBeforeCapture(db, FullTableConfig{}, "public", "big", bmeta); err != nil {
		t.Errorf("nothing recorded (no row, NULL, an index older than the column): err %v, want the table let through", err)
	}
	pgCaptureStartRead = answer(0xD_3D66_B980, true, nil)
	if err := pgRefuseFileBeforeCapture(db, FullTableConfig{}, "public", "big", bmeta); err == nil {
		t.Error("a start after the file's floor was let through")
	}
	pgCaptureStartRead = answer(0, false, denied)
	err := pgRefuseFileBeforeCapture(db, FullTableConfig{}, "public", "big", bmeta)
	if !errors.Is(err, denied) {
		t.Fatalf("a failed read: err %v, want the update refused with the read's error", err)
	}
	for _, want := range []string{"public.big", "could not be checked"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q:\n%v", want, err)
		}
	}
	if err := pgRefuseFileBeforeCapture(db, FullTableConfig{AllowGaps: true}, "public", "big", bmeta); err != nil {
		t.Errorf("a run told to go on over missing changes was refused over a failed read: %v", err)
	}
}
