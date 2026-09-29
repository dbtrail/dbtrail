package console

import (
	"reflect"
	"strings"
	"testing"
)

// TestFullReadDiskNote_wireNamesMatchTheFrontend (#1938): the full read's
// disk check reaches the page under the names the Go structs send. The words
// themselves are made in Go and rendered as text; this guards only that the
// two places that show them still read the fields that exist.
func TestFullReadDiskNote_wireNamesMatchTheFrontend(t *testing.T) {
	tag := func(v any, field string) string {
		f, ok := reflect.TypeOf(v).FieldByName(field)
		if !ok {
			t.Fatalf("%T has no field %s", v, field)
		}
		return strings.Split(f.Tag.Get("json"), ",")[0]
	}
	js := readAsset(t, "app.js")
	toast := jsFunctionBody(t, js, "createBaseline")
	for _, name := range []string{tag(BaselineStatus{}, "DiskCheck"), tag(BaselineStatus{}, "DiskNote")} {
		if !strings.Contains(toast, "done."+name) {
			t.Errorf("createBaseline does not read done.%s, so a low disk never reaches the toast", name)
		}
	}
	card := jsFunctionBody(t, js, "backupScheduleCard")
	for _, name := range []string{tag(backupScheduleRunDTO{}, "DiskCheck"), tag(backupScheduleRunDTO{}, "DiskNote")} {
		if !strings.Contains(card, "run."+name) {
			t.Errorf("backupScheduleCard does not read run.%s, so a scheduled read on a low disk says nothing", name)
		}
	}
	detail := jsFunctionBody(t, js, "loadBackupDetail")
	for _, name := range []string{tag(baselineRunDTO{}, "DiskCheck"), tag(baselineRunDTO{}, "DiskNote")} {
		if !strings.Contains(detail, "d.run."+name) {
			t.Errorf("loadBackupDetail does not read d.run.%s, so the snapshot detail loses the disk check", name)
		}
	}
}

// The schedule card's last run carries the check from both of its sources:
// the history record and the loop's own copy of the status.
func TestScheduleRun_carriesTheDiskCheck(t *testing.T) {
	rec := scheduleRunFromRecord(&BaselineRunRecord{Kind: BaselineRunDump, DiskCheck: "low", DiskNote: "Low disk: x"})
	if rec.DiskCheck != "low" || rec.DiskNote != "Low disk: x" {
		t.Fatalf("from the record: %+v", rec)
	}
	st := scheduleRunFromStatus(BackupScheduleState{LastMethod: BackupMethodFull,
		Last: &BaselineStatus{State: "failed", DiskCheck: "unchecked", DiskNote: "Disk check did not run"}})
	if st.DiskCheck != "unchecked" || st.DiskNote != "Disk check did not run" {
		t.Fatalf("from the status: %+v", st)
	}
}
