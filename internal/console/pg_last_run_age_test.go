package console

import (
	"testing"
	"time"
)

// LastRunAge tells an answer with tails, one from the snapshot alone, one
// whose files cannot be told, and one that read no table of the copy.
func TestLastRunAge(t *testing.T) {
	ran := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	through := ran.Add(-time.Second)
	files := map[string][]string{"/c/a": {"/c/a/t.parquet"}}
	cases := []struct {
		name    string
		pending *copyAnswer
		want    CopyRunAge
	}{
		{"nothing ran", nil, CopyRunAge{}},
		{"read no table", &copyAnswer{side: "copy", ranAt: ran, ok: false}, CopyRunAge{}},
		{"tails", &copyAnswer{side: "copy", ranAt: ran, readCopy: true, allTailed: true, tailThrough: through, ok: true, files: files}, CopyRunAge{Ran: ran, Through: through, Tailed: true}},
		{"tails with no instant", &copyAnswer{side: "copy", ranAt: ran, readCopy: true, allTailed: true, ok: true, files: files}, CopyRunAge{Ran: ran, Snapshot: true}},
		{"snapshot alone", &copyAnswer{side: "copy", ranAt: ran, readCopy: true, ok: true, files: files}, CopyRunAge{Ran: ran, Snapshot: true}},
		{"files not told", &copyAnswer{side: "copy", ranAt: ran, readCopy: true, ok: false}, CopyRunAge{Ran: ran, Unknown: true}},
		{"files told, none listed", &copyAnswer{side: "copy", ranAt: ran, readCopy: true, ok: true}, CopyRunAge{Ran: ran, Unknown: true}},
	}
	for _, c := range cases {
		q := &SQLOnCopy{}
		q.last.pending = c.pending
		if got := q.LastRunAge(); got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
	if (*SQLOnCopy)(nil).LastRunAge() != (CopyRunAge{}) {
		t.Error("nil connection")
	}
}
