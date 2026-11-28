package sqlsandbox

import (
	"strings"
	"testing"
)

// #3321: the value of BINTRAIL_SQL_WORKER_NICE. Unset is the default; a value
// that is not one is said and the default used, never a silent 0.
func TestWorkerNiceFrom(t *testing.T) {
	for _, c := range []struct {
		raw  string
		want int
		warn bool
	}{
		{"", DefaultWorkerNice, false},
		{"   ", DefaultWorkerNice, false},
		{"off", 0, false},
		{"OFF", 0, false},
		{" false ", 0, false},
		{"no", 0, false},
		{"0", 0, false},
		{"1", 1, false},
		{" 5 ", 5, false},
		{"19", 19, false},
		{"20", DefaultWorkerNice, true},
		{"-1", DefaultWorkerNice, true},
		{"-5", DefaultWorkerNice, true},
		{"10.5", DefaultWorkerNice, true},
		{"on", DefaultWorkerNice, false},
		{"Yes", DefaultWorkerNice, false},
		{"true", DefaultWorkerNice, false},
		{"high", DefaultWorkerNice, true},
		{"5\n6", DefaultWorkerNice, true},
	} {
		got, warn := WorkerNiceFrom(c.raw)
		if got != c.want || (warn != "") != c.warn {
			t.Errorf("WorkerNiceFrom(%q) = %d, %q; want %d, warning %v", c.raw, got, warn, c.want, c.warn)
		}
		if warn != "" && (!strings.Contains(warn, WorkerNiceEnv) || !strings.Contains(warn, "default")) {
			t.Errorf("WorkerNiceFrom(%q): the warning %q does not name the setting and what is used", c.raw, warn)
		}
	}
}

// The step is relative and only ever lowers: a process already at nice 15
// gets 19, not 10, which would be a raise that needs a privilege.
func TestNiceTarget(t *testing.T) {
	for _, c := range []struct {
		cur, step, want int
		lower           bool
	}{
		{0, 10, 10, true},
		{0, 0, 0, false},
		{0, -3, 0, false},
		{5, 10, 15, true},
		{15, 10, 19, true},
		{19, 10, 19, false},
		{-5, 10, 5, true},
		{-20, 19, -1, true},
		{0, 19, 19, true},
	} {
		got, lower := niceTarget(c.cur, c.step)
		if got != c.want || lower != c.lower {
			t.Errorf("niceTarget(%d, %d) = %d, %v; want %d, %v", c.cur, c.step, got, lower, c.want, c.lower)
		}
	}
}

// A Config outside 0..19 is brought into it, so no step is taken below the
// bottom or as a raise.
func TestNew_workerNiceIsBounded(t *testing.T) {
	for in, want := range map[int]int{-4: 0, 0: 0, 7: 7, 19: 19, 40: 19} {
		r := New(Config{WorkerNice: in})
		if r.workerNice != want {
			t.Errorf("WorkerNice %d: the runner keeps %d, want %d", in, r.workerNice, want)
		}
		r.Close()
	}
}
