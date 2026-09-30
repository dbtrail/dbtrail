//go:build integration

package cliapp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestBYOSStream_MariaDBReconnectAfterCommitOnce drives the agent's own
// capture pieces (byosSyncerConfig, startBYOSSyncer, which now starts a
// MariaDB source in GTID mode, and the StreamParser the BYOS stream runs)
// against a real MariaDB: two rows commit, the source's binlog dump thread is
// killed, go-mysql reconnects and the source re-sends the last transaction,
// two more rows commit. Every row must come out exactly once.
func TestBYOSStream_MariaDBReconnectAfterCommitOnce(t *testing.T) {
	sourceDB, schema := testutil.CreateTestMariaDB(t)
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (id INT PRIMARY KEY AUTO_INCREMENT, amount INT NOT NULL)`)

	cfg, err := drivermysql.ParseDSN(testutil.MariaDBBaseDSN() + "/")
	if err != nil {
		t.Fatalf("parse MariaDB DSN: %v", err)
	}
	host, portStr, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		t.Fatalf("split %q: %v", cfg.Addr, err)
	}
	port, _ := strconv.Atoi(portStr)

	resolver, err := buildResolverFromSource(sourceDB, []string{schema})
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}
	syncer := replication.NewBinlogSyncer(byosSyncerConfig(4_000_000_201, "mariadb", host, uint16(port), cfg.User, cfg.Passwd))
	defer syncer.Close()
	streamer, err := startBYOSSyncer(sourceDB, syncer, "mariadb", "")
	if err != nil {
		t.Fatalf("startBYOSSyncer: %v", err)
	}

	var logBuf lockedLog
	sp := parser.NewStreamParser(resolver, cliutil.BuildIndexFilters(schema, ""),
		slog.New(slog.NewTextHandler(io.MultiWriter(&logBuf, os.Stderr), nil)))
	sp.SetFlavor("mariadb")
	out := make(chan parser.Event, 256)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- sp.Run(ctx, streamer, out) }()

	ids := map[any]int{}
	commits := 0
	collectUntil := func(wantRows, wantCommits int) {
		deadline := time.After(60 * time.Second)
		for len(ids) < wantRows || commits < wantCommits {
			select {
			case ev := <-out:
				switch ev.EventType {
				case parser.EventInsert:
					ids[fmt.Sprint(ev.RowAfter["id"])]++
				case parser.EventCommit:
					commits++
				}
			case err := <-runErr:
				t.Fatalf("parser stopped: %v", err)
			case <-deadline:
				t.Fatalf("timed out: rows %v, commits %d", ids, commits)
			}
		}
	}
	insert := func() { testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (1)") }

	insert()
	insert()
	collectUntil(2, 2)

	var killed int
	rows, err := sourceDB.Query("SELECT ID FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'")
	if err != nil {
		t.Fatalf("list dump threads: %v", err)
	}
	var threadIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		threadIDs = append(threadIDs, id)
	}
	rows.Close()
	for _, id := range threadIDs {
		if _, err := sourceDB.Exec(fmt.Sprintf("KILL %d", id)); err == nil {
			killed++
		}
	}
	if killed == 0 {
		t.Fatal("no binlog dump thread was killed")
	}
	// go-mysql waits a second before reconnecting; let the re-send land.
	time.Sleep(3 * time.Second)

	insert()
	insert()
	collectUntil(4, 4)
	// Drain whatever else is queued before judging.
	time.Sleep(500 * time.Millisecond)
	for drained := false; !drained; {
		select {
		case ev := <-out:
			if ev.EventType == parser.EventInsert {
				ids[fmt.Sprint(ev.RowAfter["id"])]++
			}
		default:
			drained = true
		}
	}
	for _, id := range []string{"1", "2", "3", "4"} {
		if ids[id] != 1 {
			t.Errorf("row %s emitted %d times, want exactly once (all: %v)", id, ids[id], ids)
		}
	}
	if len(ids) != 4 {
		t.Errorf("rows emitted = %v, want ids 1-4", ids)
	}
	// The guard must have acted exactly once, or no re-send happened and the
	// test proved nothing.
	if n := strings.Count(logBuf.String(), "dropping a transaction the source re-sent after a reconnect"); n != 1 {
		t.Errorf("the re-send guard acted %d times, want exactly 1", n)
	}
}

// lockedLog is a bytes.Buffer safe for the parser goroutine's log writes.
type lockedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedLog) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
