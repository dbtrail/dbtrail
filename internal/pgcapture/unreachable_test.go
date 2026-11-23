package pgcapture

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestSourceUnreachable(t *testing.T) {
	pg := func(code string) error {
		return fmt.Errorf("pgcapture: x: %w", &pgconn.PgError{Severity: "FATAL", Code: code})
	}
	dial := func(e error) error {
		return &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", e)}
	}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"connection refused", dial(syscall.ECONNREFUSED), true},
		{"connection reset", dial(syscall.ECONNRESET), true},
		{"no route to host", dial(syscall.EHOSTUNREACH), true},
		{"network unreachable", dial(syscall.ENETUNREACH), true},
		{"kernel connect timeout", dial(syscall.ETIMEDOUT), true},
		{"name does not resolve", &net.DNSError{Err: "no such host", Name: "pg", IsNotFound: true}, true},
		{"cut mid-stream", fmt.Errorf("pgcapture: receive message: %w", io.ErrUnexpectedEOF), true},
		{"closed by the peer", fmt.Errorf("x: %w", io.EOF), true},
		{"connection already closed", fmt.Errorf("x: %w", pgconn.ErrConnClosed), true},
		{"server stopping", pg("57P01"), true},
		{"server crashed", pg("57P02"), true},
		{"server starting or in recovery", pg("57P03"), true},
		{"connection failure", pg("08006"), true},

		{"wrong password", pg("28P01"), false},
		{"no such role / hba", pg("28000"), false},
		{"no such database", pg("3D000"), false},
		{"no such publication", pg("42704"), false},
		{"no permission", pg("42501"), false},
		{"protocol violation", pg("08P01"), false},
		{"max_wal_senders exhausted", pg("53300"), false},
		{"replication slots exhausted", pg("53400"), false},
		{"slot lost", fmt.Errorf("x: %w", ErrSlotLost), false},
		{"slot missing on resume", fmt.Errorf("x: %w", ErrSlotMissingOnResume), false},
		{"slot still being created", fmt.Errorf("x: %w", ErrSlotNotReady), false},
		{"statement deadline", fmt.Errorf("x: %w", context.DeadlineExceeded), false},
		{"untrusted certificate", fmt.Errorf("tls: %w", x509.UnknownAuthorityError{}), false},
		{"TLS alert from the peer", &net.OpError{Op: "remote error", Err: errors.New("tls: handshake failure")}, false},
		{"unknown error", errors.New("something else"), false},
		{"inside the decoder", &decodeError{dial(syscall.ECONNREFUSED)}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsSourceUnreachable(markSourceUnreachable(c.err, true)); got != c.want {
				t.Fatalf("IsSourceUnreachable(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
	if markSourceUnreachable(nil, true) != nil {
		t.Fatal("nil must stay nil")
	}
}

// The mark changes neither the text nor what the error matches.
func TestSourceUnreachable_KeepsTheError(t *testing.T) {
	inner := fmt.Errorf("pgcapture: receive message: %w", io.ErrUnexpectedEOF)
	got := markSourceUnreachable(inner, false)
	if got.Error() != inner.Error() || !errors.Is(got, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v", got)
	}
}

// Real errors from the driver, with nothing listening.
func TestSourceUnreachable_RealDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = ConnectReplPinned(ctx, "postgres://u:p@"+addr+"/db?sslmode=disable&replication=database")
	if err == nil {
		t.Fatal("connected to a closed port")
	}
	if !sourceUnreachable(err) {
		t.Fatalf("a refused connection is not classified as unreachable: %v", err)
	}
}

// An address that accepts and never answers ends the attempt, as an outage.
func TestCapturerRun_SilentSourceIsUnreachable(t *testing.T) {
	old := sourceConnectTimeout
	sourceConnectTimeout = 200 * time.Millisecond
	t.Cleanup(func() { sourceConnectTimeout = old })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	dsn := "postgres://u:p@" + ln.Addr().String() + "/db?sslmode=disable"
	c := New(Config{ReplDSN: dsn + "&replication=database", QueryDSN: dsn, SlotName: "s", Publication: "p", ExpectExistingSlot: true})
	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background(), nil) }()
	select {
	case err := <-done:
		if !IsSourceUnreachable(err) || !SourceWasCaptured(err) {
			t.Fatalf("got %v; want an unreachable source that was captured before", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run hung on a source that never answers")
	}
}
