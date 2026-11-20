package console

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// The not-captured refusal is about the data, not a server fault: 422 with the
// refusal's own text, built here from a real generator run over a real row.
func TestWriteRecoverError_notCapturedIs422(t *testing.T) {
	_, err := recovery.NewForDialect(nil, nil, recovery.PostgresDialect).GenerateSQLFromRows([]query.ResultRow{{
		EventID: 1, SchemaName: "public", TableName: "orders", EventType: parser.EventDelete, PKValues: "1",
		RowBefore: map[string]any{"id": "1", "note": map[string]any{event.NotCapturedKey: true}},
	}}, io.Discard)
	var nc *event.NotCapturedError
	if !errors.As(err, &nc) {
		t.Fatalf("premise: want the not-captured refusal, got %v", err)
	}
	rec := httptest.NewRecorder()
	writeRecoverError(rec, err)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
	var body map[string]string
	if jerr := json.Unmarshal(rec.Body.Bytes(), &body); jerr != nil || body["error"] != err.Error() {
		t.Fatalf("body = %s, want the refusal's own text %q", rec.Body.String(), err.Error())
	}
	t.Logf("screen text: %s", body["error"])

	// Any other failure stays a 500.
	rec = httptest.NewRecorder()
	writeRecoverError(rec, errors.New("boom"))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("other error: status = %d, want 500", rec.Code)
	}
}
