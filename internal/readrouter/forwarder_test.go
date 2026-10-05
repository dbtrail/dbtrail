package readrouter

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// A source that cannot be reached, or that drops the connection, is lost for
// the rest of the client connection: every later call fails with
// CodeUpstreamLost without dialling again, so the client learns its session
// is gone instead of getting a fresh one behind its back.
func TestForwarder_lostStaysLost(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var accepted atomic.Int32
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			c.Close() // no handshake: the dial succeeds, the protocol fails
		}
	}()
	f, err := NewForwarder("u:p@tcp("+ln.Addr().String()+")/db?tls=false", config.SSL{Mode: "disabled"}, DefaultPolicy(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	f.connectTimeout = time.Second
	ctx := context.Background()
	if _, err := f.Forward(ctx, "SELECT 1", &BufferSink{}); !IsLost(err) {
		t.Fatalf("first statement: err = %v, want the lost error", err)
	}
	if _, err := f.Decide(ctx, "SELECT 1"); !IsLost(err) {
		t.Errorf("Decide after the loss: err = %v, want the lost error", err)
	}
	if err := f.UseDB(ctx, "x"); !IsLost(err) {
		t.Errorf("UseDB after the loss: err = %v, want the lost error", err)
	}
	if n := accepted.Load(); n != 1 {
		t.Errorf("the source was dialled %d times, want once: a lost connection must not be replaced", n)
	}
	if f.InTransaction() {
		t.Error("a lost connection reports a transaction")
	}
}

func TestNewForwarder_dsn(t *testing.T) {
	f, err := NewForwarder("root:pw@tcp(db.example)/shop?tls=skip-verify&parseTime=true", config.SSL{Mode: "disabled"}, Policy{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if f.addr != "db.example:3306" || f.db != "shop" || f.tls == nil || !f.tls.InsecureSkipVerify {
		t.Errorf("parsed addr=%q db=%q tls=%v, want the default port, the database and the DSN's TLS", f.addr, f.db, f.tls)
	}
	if _, err := NewForwarder("root@unix(/tmp/sock)/x", config.SSL{Mode: "disabled"}, Policy{}, 0); err == nil {
		t.Error("a unix-socket DSN was accepted")
	}
	if _, err := NewForwarder("not a dsn", config.SSL{Mode: "disabled"}, Policy{}, 0); err == nil {
		t.Error("garbage was accepted")
	}
}

// Reading a plan must not hand the EXPLAIN result back to go-mysql's
// resultset pool: the pool keeps column definitions, and the next result
// built in the process would carry the plan's column (its name, its type)
// as its own first column.
func TestPlanFromExplain_doesNotPoisonTheResultsetPool(t *testing.T) {
	const plan = `{"query_block": {"select_id": 1, "cost_info": {"query_cost": "1.00"}, "table": {"table_name": "t", "access_type": "ALL", "rows_examined_per_scan": 3}}}`
	for range 20 {
		rs, err := mysql.BuildSimpleTextResultset([]string{"EXPLAIN"}, [][]any{{plan}})
		if err != nil {
			t.Fatal(err)
		}
		// What the client's text reader fills in.
		rs.Values = [][]mysql.FieldValue{{mysql.NewFieldValue(mysql.FieldValueTypeString, 0, []byte(plan))}}
		p, err := planFromExplain(&mysql.Result{Resultset: rs})
		if err != nil {
			t.Fatal(err)
		}
		if p.FullScans != 1 {
			t.Fatalf("plan = %+v", p)
		}
	}
	for i := range 50 {
		rs, err := mysql.BuildSimpleTextResultset([]string{"n"}, [][]any{{int64(i)}})
		if err != nil {
			t.Fatal(err)
		}
		if got := string(rs.Fields[0].Name); got != "n" || rs.Fields[0].Type != mysql.MYSQL_TYPE_LONGLONG {
			t.Fatalf("result %d built after reading plans has column %q of type %d, want n / BIGINT", i, got, rs.Fields[0].Type)
		}
	}
}
