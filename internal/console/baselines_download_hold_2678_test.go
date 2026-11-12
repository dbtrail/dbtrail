package console

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// holdWatcher records, at every write of the archive, which job stops the
// cleanup of the snapshot folder.
type holdWatcher struct {
	*httptest.ResponseRecorder
	root   string
	during map[string]bool
}

func (h *holdWatcher) Write(b []byte) (int, error) {
	h.during[baseline.Reading(h.root)] = true
	return h.ResponseRecorder.Write(b)
}

// A download of a local snapshot stops the cleanup of its folder for as long
// as the archive streams, and no longer (#2678): the files are opened one by
// one, and the count is applied every few minutes.
func TestBaselineDownload_stopsTheCleanupWhileItStreams(t *testing.T) {
	dir := newDetailFixture(t)
	srv := newBaselineServer(t, dir, true)
	w := &holdWatcher{ResponseRecorder: httptest.NewRecorder(), root: dir, during: map[string]bool{}}
	srv.handleBaselineDownload(w, httptest.NewRequest("GET", "/api/baselines/download"+detailQuery(detailSnapAt), nil))
	if w.Code != http.StatusOK || w.Body.Len() == 0 {
		t.Fatalf("download: code %d, %d bytes", w.Code, w.Body.Len())
	}
	if len(w.during) != 1 || !w.during["a snapshot download"] {
		t.Fatalf("while the archive streamed the folder's reader was %v, want only the download", w.during)
	}
	if what := baseline.Reading(dir); what != "" {
		t.Fatalf("the download is over and %q still stops the cleanup", what)
	}
}
