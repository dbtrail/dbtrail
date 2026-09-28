package telemetry

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// classedErr is a stand-in for a producer package's error: it declares its
// class through the Classed interface and carries a message that must never
// reach the wire.
type classedErr struct{ class, msg string }

func (e *classedErr) Error() string          { return e.msg }
func (e *classedErr) TelemetryClass() string { return e.class }

func TestClassifyErrorClassed(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"declared class wins", &classedErr{class: ClassConfigInvalid}, ClassConfigInvalid},
		{"declared class survives wrapping", fmt.Errorf("preflight failed: %w", &classedErr{class: ClassConfigInvalid}), ClassConfigInvalid},
		{"declared class beats a wrapped MySQL number", &wrapBoth{&classedErr{class: ClassSchemaMismatch}, &mysql.MySQLError{Number: 1045}}, ClassSchemaMismatch},
		// A producer that spells a class wrong must not put free text on the
		// wire: normalizeClass coerces it, and the wiring test in that
		// producer's package is what catches the typo.
		{"typo is coerced to unknown", &classedErr{class: "config-invalid"}, ClassUnknown},
		{"empty class is coerced to unknown", &classedErr{class: ""}, ClassUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClassifyError(c.err); got != c.want {
				t.Errorf("ClassifyError = %q, want %q", got, c.want)
			}
		})
	}
}

// wrapBoth exposes two errors to errors.As at once, the way a chain built
// with fmt.Errorf("%w ... %w") does.
type wrapBoth struct{ a, b error }

func (w *wrapBoth) Error() string   { return w.a.Error() + "; " + w.b.Error() }
func (w *wrapBoth) Unwrap() []error { return []error{w.a, w.b} }

// numberedErr stands in for parser.ReplicationError: a server error number
// arriving from the replication client, which this package must not link.
type numberedErr struct {
	code uint16
	msg  string
}

func (e *numberedErr) Error() string            { return e.msg }
func (e *numberedErr) MySQLErrorNumber() uint16 { return e.code }

// TestClassifyErrorReplicationClient: a binlog-side failure never arrives as
// the driver's *MySQLError — it comes from the replication client through
// MySQLNumbered — and 1236 is the one number a capture daemon actually dies
// on. Before #1503 the classifier only looked at the driver's type, so every
// replication-side server error was unknown.
func TestClassifyErrorReplicationClient(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"binlog purged (1236)", &numberedErr{code: 1236}, ClassBinlogNotFound},
		{"1236 through the driver type", &mysql.MySQLError{Number: 1236}, ClassBinlogNotFound},
		{"host not allowed (1130)", &numberedErr{code: 1130}, ClassDBPermission},
		{"1130 through the driver type", &mysql.MySQLError{Number: 1130}, ClassDBPermission},
		{"access denied via replication client", &numberedErr{code: 1045}, ClassDBPermission},
		{"unbucketed number via replication client", &numberedErr{code: 1064}, ClassUnknown},
		{"wrapped replication error", fmt.Errorf("stream: %w", &numberedErr{code: 1236}), ClassBinlogNotFound},
		{"cancelled context stays unknown", context.Canceled, ClassUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClassifyError(c.err); got != c.want {
				t.Errorf("ClassifyError = %q, want %q", got, c.want)
			}
		})
	}
}

// TestClassifyErrorNeverLeaksClassedOrReplicationMessage extends the
// load-bearing no-leak property to the two new inputs.
func TestClassifyErrorNeverLeaksClassedOrReplicationMessage(t *testing.T) {
	secret := "mysql-bin.000042 on db.internal for customer_orders"
	for name, err := range map[string]error{
		"classed":     &classedErr{class: ClassBinlogNotFound, msg: secret},
		"replication": &numberedErr{code: 1236, msg: secret},
	} {
		got := ClassifyError(fmt.Errorf("gap: %s: %w", secret, err))
		if strings.Contains(got, "000042") || strings.Contains(got, "db.internal") || strings.Contains(got, "customer_orders") {
			t.Fatalf("%s: ClassifyError leaked the error message: %q", name, got)
		}
		if !classes[got] {
			t.Fatalf("%s: ClassifyError returned %q, outside the taxonomy", name, got)
		}
	}
}

// TestDroppedClassesAreGone pins #1503's removal: a class with no producer
// must not be reintroduced without one.
func TestDroppedClassesAreGone(t *testing.T) {
	for _, dropped := range []string{"binlog_parse", "flag_invalid", "network"} {
		if classes[dropped] {
			t.Errorf("class %q is back in the taxonomy; it was removed because nothing can emit it", dropped)
		}
		if got := normalizeClass(dropped); got != ClassUnknown {
			t.Errorf("normalizeClass(%q) = %q, want unknown", dropped, got)
		}
	}
	if _, ok := any(errors.New("x")).(Classed); ok {
		t.Fatal("a plain error must not satisfy Classed")
	}
}

// TestClassifyMySQLNumberPostStart pins the numbers a server that is
// reachable but unwell answers once capture is running (#1630). Both client
// libraries share the mapping, so each number is checked through the driver's
// type and through MySQLNumbered.
func TestClassifyMySQLNumberPostStart(t *testing.T) {
	cases := []struct {
		name   string
		number uint16
		want   string
	}{
		{"disk full", 1021, ClassStorageIO},
		{"table is full", 1114, ClassStorageIO},
		{"too many connections", 1040, ClassDBConnection},
		{"too many user connections", 1203, ClassDBConnection},
		{"lock wait timeout", 1205, ClassDBConnection},
		{"deadlock", 1213, ClassDBConnection},
		{"no partition for value", 1526, ClassNotFound},
		{"packet too large", 1153, ClassConfigInvalid},
		{"server has gone away", 2006, ClassDBConnection},
		{"lost connection during query", 2013, ClassDBConnection},
		{"syntax error has no bucket", 1064, ClassUnknown},
		{"zero has no bucket", 0, ClassUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			drv := fmt.Errorf("batch INSERT of %d events failed: %w", 3, &mysql.MySQLError{Number: c.number, Message: "for customer_orders"})
			if got := ClassifyError(drv); got != c.want {
				t.Errorf("driver: ClassifyError(%d) = %q, want %q", c.number, got, c.want)
			}
			if got := ClassifyError(&numberedErr{code: c.number}); got != c.want {
				t.Errorf("replication client: ClassifyError(%d) = %q, want %q", c.number, got, c.want)
			}
		})
	}
}

// messageOnly carries a message that spells out a classed failure and
// nothing structural. If the classifier ever matched on text, these would
// land in a class.
type messageOnly string

func (m messageOnly) Error() string { return string(m) }

// TestClassifyErrorNeverReadsTheMessage: the class comes from the error's
// type, sentinel or number, never from what its text says.
func TestClassifyErrorNeverReadsTheMessage(t *testing.T) {
	for _, msg := range []string{
		"Error 1114 (HY000): The table 'binlog_events' is full",
		"Error 1040: Too many connections",
		"Error 1213 (40001): Deadlock found when trying to get lock; try restarting transaction",
		"Error 1205 (HY000): Lock wait timeout exceeded; try restarting transaction",
		"index write deadline exceeded",
		"context deadline exceeded",
		"invalid connection",
		"driver: bad connection",
		"dial tcp 10.0.0.1:3306: connect: connection refused",
		"schema drift detected",
		"db_connection",
		"storage_io",
	} {
		if got := ClassifyError(messageOnly(msg)); got != ClassUnknown {
			t.Errorf("ClassifyError(%q) = %q; a message alone must stay unknown", msg, got)
		}
		if got := ClassifyError(fmt.Errorf("stream: %w", messageOnly(msg))); got != ClassUnknown {
			t.Errorf("wrapped ClassifyError(%q) = %q; a message alone must stay unknown", msg, got)
		}
	}
}

// TestClassifyErrorEdges pins the corners: nil, deep wrapping, the driver's
// dead-connection sentinel, and what wins when one error carries two classes.
func TestClassifyErrorEdges(t *testing.T) {
	full := &mysql.MySQLError{Number: 1114}
	busy := &mysql.MySQLError{Number: 1040}
	declared := &classedErr{class: ClassSchemaMismatch}
	other := &classedErr{class: ClassConfigInvalid}

	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil has no class", nil, ""},
		{"wrapped three times", fmt.Errorf("a: %w", fmt.Errorf("b: %w", fmt.Errorf("c: %w", full))), ClassStorageIO},
		{"dead connection from database/sql", fmt.Errorf("batch INSERT of 1 events failed: %w", driver.ErrBadConn), ClassDBConnection},
		// A normal shutdown does not surface as an error at all; if a
		// cancelled context ever does arrive it is not a capture failure and
		// stays out of every bucket.
		{"cancelled context", fmt.Errorf("stream: %w", context.Canceled), ClassUnknown},
		// Two classes in one error. A declared class beats a number whatever
		// the order; between two of the same kind the first one joined wins.
		{"join: declared then number", errors.Join(declared, full), ClassSchemaMismatch},
		{"join: number then declared", errors.Join(full, declared), ClassSchemaMismatch},
		{"join: two declared, first wins", errors.Join(declared, other), ClassSchemaMismatch},
		{"join: two declared, reversed", errors.Join(other, declared), ClassConfigInvalid},
		{"join: two numbers, first wins", errors.Join(full, busy), ClassStorageIO},
		{"join: two numbers, reversed", errors.Join(busy, full), ClassDBConnection},
		{"join: number beats a deadline", errors.Join(context.DeadlineExceeded, full), ClassStorageIO},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClassifyError(c.err); got != c.want {
				t.Errorf("ClassifyError = %q, want %q", got, c.want)
			}
		})
	}
}
