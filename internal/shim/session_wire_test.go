package shim

import (
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// Over a real connection: a client prepares, executes, then re-executes
// WITHOUT re-sending its argument types (what Connector/J's server prepares,
// the C API and PHP's mysqlnd do). The second execution must run with its
// own argument, and an EXECUTE error must keep its MySQL code.
func TestSession_overTheWire(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	f := &fakeFreeSQL{res: oneCell("n", "BIGINT", "3")}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := listener.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(30 * time.Second))
		h := NewHandler(nil, nil)
		h.BindFreeSQL(f)
		auth, _ := NewTenantAuth(map[string]string{"u": "p"})
		mc, err := server.NewCustomizedConn(c, server.NewDefaultServer(), auth, h)
		if err != nil {
			return
		}
		session := NewSession(mc, h)
		for session.HandleCommand() == nil {
		}
	}()

	conn, err := client.Connect(listener.Addr().String(), "u", "p", "")
	if err != nil {
		t.Fatal(err)
	}
	st, err := conn.Prepare("SELECT count(*) AS n FROM t WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	if st.ParamNum() != 1 {
		t.Fatalf("params = %d", st.ParamNum())
	}
	res, err := st.Execute(int64(2))
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.GetInt(0, 0); n != 3 {
		t.Errorf("first execution answered %d, want 3", n)
	}
	if !strings.HasSuffix(squash(f.gotStmt), "id = 2") {
		t.Fatalf("first execution ran %q", f.gotStmt)
	}

	// The re-execution, by hand: new-params-bound = 0, the value alone.
	pkt := []byte{0, 0, 0, 0, mysql.COM_STMT_EXECUTE}
	pkt = binary.LittleEndian.AppendUint32(pkt, st.ID)
	pkt = append(pkt, 0, 1, 0, 0, 0) // flags, iteration count
	pkt = append(pkt, 0, 0)          // NULL bitmap, new-params-bound
	pkt = binary.LittleEndian.AppendUint64(pkt, 7)
	conn.ResetSequence()
	if err := conn.WritePacket(pkt); err != nil {
		t.Fatal(err)
	}
	first, err := conn.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if first[0] == mysql.ERR_HEADER {
		t.Fatalf("the re-execution was refused: %q", first[3:])
	}
	if !strings.HasSuffix(squash(f.gotStmt), "id = 7") {
		t.Errorf("the re-execution ran %q, want its own argument (id = 7)", f.gotStmt)
	}
	conn.Close()
	<-done

	// An EXECUTE error keeps its code (go-mysql's own loop turns it into 1105).
	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener2.Close()
	go func() {
		c, err := listener2.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(30 * time.Second))
		h := NewHandler(nil, nil)
		h.BindFreeSQL(f)
		auth, _ := NewTenantAuth(map[string]string{"u": "p"})
		mc, err := server.NewCustomizedConn(c, server.NewDefaultServer(), auth, h)
		if err != nil {
			return
		}
		session := NewSession(mc, h)
		for session.HandleCommand() == nil {
		}
	}()
	conn2, err := client.Connect(listener2.Addr().String(), "u", "p", "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	// No schema selected: the time-travel parser refuses with a typed error.
	del, err := conn2.Prepare("SELECT * FROM _flashback.t AS OF ? WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	_, textErr := conn2.Execute("SELECT * FROM _flashback.t AS OF '2026-01-01 00:00:00' WHERE id = 1")
	_, prepErr := del.Execute("2026-01-01 00:00:00", int64(1))
	if code := mysqlErrCode(prepErr); code == 0 || code == mysql.ER_UNKNOWN_ERROR || code != mysqlErrCode(textErr) {
		t.Errorf("prepared statement error = %v; want the typed error the text statement gets (%v)", prepErr, textErr)
	}
	// The connection is still usable after an error and after a close.
	if err := del.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conn2.Ping(); err != nil {
		t.Errorf("ping after a statement close: %v", err)
	}
}
