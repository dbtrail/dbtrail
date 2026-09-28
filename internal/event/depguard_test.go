package event_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestReadLayerDoesNotLinkGoMySQL is the #528 guard: the read/recover/reconstruct
// value stack consumes the source-agnostic event.Event, so it must NOT link ANY
// capture library — neither the go-mysql binlog library (MySQL/MariaDB) nor the
// PostgreSQL replication libraries (pglogrepl, and pgx once the capturer lands in
// slice 2; both are banned pre-emptively here for internal/pgcapture, #530). A
// future change that re-imports internal/parser or internal/pgcapture into any of
// these packages — directly or transitively — fails here. This keeps the
// index/query/recover side swappable across MySQL and PostgreSQL sources.
//
// internal/shim is deliberately NOT listed: it links go-mysql for its MySQL
// wire-protocol server (go-mysql/server), which is its own dependency, unrelated
// to the Event type. internal/parser, internal/streamrun, and internal/pgcapture
// are the capture layer and legitimately link their source drivers.
func TestReadLayerDoesNotLinkGoMySQL(t *testing.T) {
	// Capture libraries the read side must never link. pgx is a general PostgreSQL
	// driver, but on the read side the index is always MySQL (go-sql-driver) — pgx
	// is exclusively pgcapture's, so banning it on the read side is correct.
	banned := []string{
		"github.com/go-mysql-org/go-mysql",
		"github.com/jackc/pglogrepl",
		"github.com/jackc/pgx",
	}
	// The read/value stack. Add new read-side packages here as they are
	// introduced — the guarantee is only as strong as this enumeration.
	readPkgs := []string{
		"github.com/dbtrail/dbtrail/internal/event",
		"github.com/dbtrail/dbtrail/internal/indexer",
		"github.com/dbtrail/dbtrail/internal/query",
		"github.com/dbtrail/dbtrail/internal/recovery",
		"github.com/dbtrail/dbtrail/internal/reconstruct",
		"github.com/dbtrail/dbtrail/internal/parquetquery",
		"github.com/dbtrail/dbtrail/internal/buffer",
		"github.com/dbtrail/dbtrail/internal/byos",
		"github.com/dbtrail/dbtrail/internal/cliutil",
		"github.com/dbtrail/dbtrail/internal/console",
		"github.com/dbtrail/dbtrail/internal/archive",
		"github.com/dbtrail/dbtrail/internal/agent",
		"github.com/dbtrail/dbtrail/internal/status",
		"github.com/dbtrail/dbtrail/internal/mcptools",
		"github.com/dbtrail/dbtrail/internal/ddltext",
	}
	for _, pkg := range readPkgs {
		out, err := exec.Command("go", "list", "-deps", pkg).CombinedOutput()
		if err != nil {
			t.Fatalf("go list -deps %s: %v\n%s", pkg, err, out)
		}
		for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
			dep := strings.TrimSpace(line)
			for _, b := range banned {
				if dep == b || strings.HasPrefix(dep, b+"/") {
					t.Errorf("%s transitively links %s — the read layer must consume event.Event, not a capture library", pkg, dep)
				}
			}
		}
	}
}
