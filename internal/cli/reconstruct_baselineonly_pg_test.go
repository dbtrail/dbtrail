package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// `reconstruct --baseline-only` is a recovery path that needs no index: the
// PostgreSQL check refuses a table whose capture dropped changes, and never
// fails the read for the index itself. Whatever keeps the check from running
// is said, and the read goes on.
func TestBaselineOnlyPGCheck(t *testing.T) {
	pg := baseline.DumpMetadata{LSN: 1000}
	refused := fmt.Errorf("a read of public.t at a past moment is refused: x: %w", reconstruct.ErrPGPastReadNotCovered)
	for _, tc := range []struct {
		name     string
		md       baseline.DumpMetadata
		metaErr  error
		dsn      string
		check    error
		wantErr  bool
		notCheck string // "" = no warning
		neutral  bool   // the warning that names nothing of PostgreSQL
	}{
		{name: "a MySQL snapshot is not asked", md: baseline.DumpMetadata{}, dsn: "dsn"},
		{name: "no index given", md: pg, notCheck: "(no --index-dsn)"},
		{name: "the index is down", md: pg, dsn: "dsn", check: errors.New("connect to index: dial tcp: connection refused"), notCheck: "(the index could not be read)"},
		{name: "the record cannot be read", md: pg, dsn: "dsn", check: fmt.Errorf("read the capture's record: %w", reconstruct.ErrCaptureGap), notCheck: "(the index could not be read)"},
		{name: "the index is another source's", md: pg, dsn: "dsn", check: fmt.Errorf("%w: the index given is not this PostgreSQL capture's", errPGIndexNotChecked), notCheck: "(the index given is not this PostgreSQL capture's)"},
		{name: "the index records no lost events", md: pg, dsn: "dsn", check: fmt.Errorf("%w: the index does not record lost events", errPGIndexNotChecked), notCheck: "(the index does not record lost events)"},
		{name: "the footer cannot be read", metaErr: errors.New("not a parquet file"), dsn: "dsn", neutral: true},
		{name: "covered", md: pg, dsn: "dsn"},
		{name: "not covered", md: pg, dsn: "dsn", check: refused, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
			defer slog.SetDefault(prev)
			called := false
			err := baselineOnlyPGCheck(context.Background(), tc.md, tc.metaErr, tc.dsn, "public", "t", "/snap/public/t.parquet",
				func(context.Context, string, string, string, baseline.DumpMetadata) error { called = true; return tc.check })
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, reconstruct.ErrPGPastReadNotCovered) {
				t.Errorf("the refusal must come through as it is: %v", err)
			}
			got := buf.String()
			t.Logf("log: %s", got)
			if tc.neutral {
				if !strings.Contains(got, "the snapshot's footer could not be read, so it was not checked") || strings.Contains(got, "REPLICA IDENTITY") || strings.Contains(got, "PostgreSQL") {
					t.Errorf("want the neutral warning, got: %s", got)
				}
				return
			}
			if tc.notCheck == "" && strings.Contains(got, "was not checked") {
				t.Errorf("unexpected warning: %s", got)
			}
			if tc.notCheck != "" && (!strings.Contains(got, "the snapshot was not checked against the capture's record of dropped changes "+tc.notCheck) || !strings.Contains(got, "REPLICA IDENTITY FULL")) {
				t.Errorf("want the not-checked warning %q, got: %s", tc.notCheck, got)
			}
			if tc.md.LSN == 0 && called {
				t.Errorf("a snapshot with no WAL floor must not reach the index")
			}
		})
	}
}
