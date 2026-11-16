package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
)

// #2645: the router's reads of a source's binary log position.

func TestSourcePositionErrorMarksRefusals(t *testing.T) {
	dsn := "repl:s3cret@tcp(db.prod:3306)/"
	for _, n := range []uint16{1227, 1044, 1045, 1142} {
		err := sourcePositionError(&mysql.MySQLError{Number: n, Message: "denied for repl:s3cret"}, dsn)
		if !errors.Is(err, console.ErrSourcePositionRefused) {
			t.Errorf("error %d is not a refusal: %v", n, err)
		}
	}
	err := sourcePositionError(errors.New("dial tcp: i/o timeout"), dsn)
	if errors.Is(err, console.ErrSourcePositionRefused) {
		t.Errorf("a timeout read as a refusal: %v", err)
	}
	if strings.Contains(sourcePositionError(errors.New("bad "+dsn), dsn).Error(), "s3cret") {
		t.Error("the DSN's password is in the error")
	}
}

// A source that cannot be reached costs one attempt per sourcePositionRetry,
// not one per statement.
func TestSourcePositionsRemembersAFailedConnection(t *testing.T) {
	p := newSourcePositions()
	tries := 0
	p.connect = func(string, config.SSL) (*sql.DB, error) {
		tries++
		return nil, errors.New("connection refused")
	}
	e := console.ServerEntry{ID: "a", SourceDSN: "repl:pw@tcp(db.prod:3306)/"}
	for range 5 {
		if _, err := p.SourcePosition(context.Background(), e); err == nil {
			t.Fatal("a position from a source that cannot be reached")
		}
	}
	if tries != 1 {
		t.Fatalf("%d connection attempts for 5 reads, want 1", tries)
	}
	// Another source for the server (its DSN changed) is tried at once.
	e.SourceDSN = "repl:pw@tcp(db.other:3306)/"
	p.SourcePosition(context.Background(), e)
	if tries != 2 {
		t.Fatalf("a changed source was not tried: %d attempts", tries)
	}
}
