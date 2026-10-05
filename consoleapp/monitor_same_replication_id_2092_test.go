package consoleapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// sameIDPacket is the error packet a MariaDB source sends the reader it drops
// for another one with the same server id, as go-mysql decodes it.
func sameIDPacket() *gomysql.MyError {
	return &gomysql.MyError{Code: 4052, State: "HY000",
		Message: "A slave with the same server_id is already connected; the first event '.' at 0, the last event read from 'binlog.000955' at 51806, the last byte read from 'binlog.000955' at 51837."}
}

func TestMonitorErrorCode_sameReplicationID(t *testing.T) {
	var typedNil *gomysql.MyError
	for _, c := range []struct {
		name string
		err  error
		want string
	}{
		{"the packet", sameIDPacket(), console.MonitorErrSameReplicationID},
		{"as the stream returns it", parser.WrapReplicationError(sameIDPacket()), console.MonitorErrSameReplicationID},
		{"as a failed start returns it", fmt.Errorf("StartSyncGTID: %w", parser.WrapReplicationError(sameIDPacket())), console.MonitorErrSameReplicationID},
		{"joined with another error", errors.Join(errors.New("flush failed"), sameIDPacket()), console.MonitorErrSameReplicationID},
		{"another replication error", parser.WrapReplicationError(&gomysql.MyError{Code: 1236, State: "HY000", Message: "Could not find first log file name in binary log index file"}), ""},
		// The number from the SQL driver is an index or source statement's
		// error, not a packet on the replication connection.
		{"the same number from the SQL driver", &mysql.MySQLError{Number: 4052, Message: "A slave with the same server_id is already connected"}, ""},
		{"a text that only reads like it", errors.New(sameIDPacket().Error()), ""},
		{"a nil packet in the chain", fmt.Errorf("stream: %w", typedNil), ""},
		{"nil", nil, ""},
	} {
		if got := monitorErrorCode(c.err); got != c.want {
			t.Errorf("%s: code %q, want %q", c.name, got, c.want)
		}
	}
}

// TestMonitorRun_sameReplicationIDCarriesItsCode: a supervised source dropped
// for another reader reports the code with the raw error, retries on its own,
// and drops the code when the state moves on.
func TestMonitorRun_sameReplicationIDCarriesItsCode(t *testing.T) {
	oldBase, oldCap := monitorBackoffBase, monitorBackoffCap
	monitorBackoffBase, monitorBackoffCap = time.Hour, time.Hour
	defer func() { monitorBackoffBase, monitorBackoffCap = oldBase, oldCap }()

	ctx, cancel := context.WithCancel(context.Background())
	m := &monitorSupervisor{baseCtx: ctx, jobs: map[string]*monitorJob{}}
	job := &monitorJob{cancel: cancel, done: make(chan struct{})}
	job.set("pending", "")
	m.wg.Add(1)
	go m.run(ctx, job, console.ServerEntry{ID: "e2092", Name: "shared"}, console.FlavorMariaDB,
		func(context.Context) error { return parser.WrapReplicationError(sameIDPacket()) })

	deadline := time.Now().Add(5 * time.Second)
	for job.snapshot().State != "failed" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	st := job.snapshot()
	if st.State != "failed" || !st.Retrying || st.ErrorCode != console.MonitorErrSameReplicationID {
		t.Errorf("%+v, want failed, retrying, code %q", st, console.MonitorErrSameReplicationID)
	}
	if want := sameIDPacket().Error() + " (retrying)"; st.LastError != want {
		t.Errorf("last error = %q\n      want   %q", st.LastError, want)
	}
	raw, _ := json.Marshal(st)
	if !strings.Contains(string(raw), `"error_code":"same_replication_id"`) {
		t.Errorf("status document = %s", raw)
	}
	cancel()
	<-job.done
	if st := job.snapshot(); st.State != "stopped" || st.ErrorCode != "" {
		t.Errorf("after stop: %+v, want stopped with no code", st)
	}

	// The next run starting clears it too: while the stream is connected the
	// status says nothing about the drop before it.
	job.fail("boom (retrying)", console.MonitorErrSameReplicationID, true)
	job.set("pending", "")
	if st := job.snapshot(); st.ErrorCode != "" || st.LastError != "" {
		t.Errorf("after the next run started: %+v, want no code and no error", st)
	}
}
