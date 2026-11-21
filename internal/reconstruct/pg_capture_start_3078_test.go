package reconstruct

import (
	"database/sql"
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

func TestPGRefuseFileBeforeCapture_3078(t *testing.T) {
	prev := pgCaptureStartLSN
	t.Cleanup(func() { pgCaptureStartLSN = prev })
	db := &sql.DB{}
	bmeta := baseline.DumpMetadata{LSN: 0x9_B74B_0000}

	pgCaptureStartLSN = func(*sql.DB) (uint64, bool) { return 0xD_3D66_B980, true }
	err := pgRefuseFileBeforeCapture(db, FullTableConfig{}, "public", "big", bmeta)
	if err == nil {
		t.Fatal("a file capture started after was not refused")
	}
	for _, want := range []string{"public.big", "9/B74B0000", "D/3D66B980", "never captured", "Take a new snapshot of the table"} {
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
	pgCaptureStartLSN = func(*sql.DB) (uint64, bool) { asked = true; return 1, true }
	if err := pgRefuseFileBeforeCapture(db, FullTableConfig{}, "public", "big", baseline.DumpMetadata{}); err != nil || asked {
		t.Errorf("a file with no floor: err %v, index asked %v", err, asked)
	}
	pgCaptureStartLSN = func(*sql.DB) (uint64, bool) { return 0x9_B74B_0000, true }
	if err := pgRefuseFileBeforeCapture(db, FullTableConfig{}, "public", "big", bmeta); err != nil {
		t.Errorf("capture starting at the file's floor was refused: %v", err)
	}
}
