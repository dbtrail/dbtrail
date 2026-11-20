//go:build integration

package pgcapture_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestPgoutputReplicaIdentityWire_Integration pins what pgoutput actually sends
// under each REPLICA IDENTITY setting, read straight off a live PostgreSQL with
// no decoder of ours in between: the per-column key flag in the RelationMessage,
// the old-tuple kind of each UPDATE and DELETE ('K' key only, 'O' whole row,
// none), and which column of each tuple arrives as 'u' (an unchanged value
// stored out of line) or 'n' (null, or not sent). It also pins that PostgreSQL
// itself refuses UPDATE and DELETE on a published table that has no replica
// identity (NOTHING, or DEFAULT with no primary key).
//
// These facts decide how a table that is not at REPLICA IDENTITY FULL can be
// captured, so they are asserted rather than assumed. A failure here means the
// server behaves differently from what the capture code is built on; the log
// shows every message as received.
//
// Signatures: a tuple is one letter per column in table order ('t' text,
// 'n' null or not sent, 'u' unchanged out-of-line value); an old tuple is
// prefixed by its kind ("K:" or "O:"), and "-" means the message has none.
// Relation flags are one digit per column, 1 = marked as part of the key.
func TestPgoutputReplicaIdentityWire_Integration(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const slot = "bintrail_riwire_it"
	const pub = "bintrail_riwire_pub"
	tables := []string{"riwire_def", "riwire_idxpk", "riwire_idxnopk", "riwire_nothing", "riwire_full", "riwire_nopk", "riwire_bigkey"}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+strings.Join(tables, ", "))
	}
	cleanup()
	t.Cleanup(cleanup)
	mustExec := func(q string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	var version string
	if err := conn.QueryRow(ctx, "SHOW server_version").Scan(&version); err != nil {
		t.Fatalf("server_version: %v", err)
	}
	t.Logf("PostgreSQL %s", version)

	// big is stored out of line, uncompressed (STORAGE EXTERNAL and well past the
	// 2 kB TOAST threshold), so an UPDATE that leaves it alone sends it as 'u'.
	// The key column of riwire_bigkey is out of line the same way, kept under the
	// B-tree entry size limit.
	const bigVal = "(SELECT string_agg(md5(i::text), '') FROM generate_series(1, 100) i)"
	const bigKey = "(SELECT string_agg(md5(i::text), '') FROM generate_series(1, 80) i)"
	mustExec("CREATE TABLE riwire_def (id int PRIMARY KEY, v text, big text)")
	mustExec("CREATE TABLE riwire_idxpk (id int PRIMARY KEY, u int NOT NULL, v text, big text)")
	mustExec("CREATE UNIQUE INDEX riwire_idxpk_uk ON riwire_idxpk (id, u)")
	mustExec("ALTER TABLE riwire_idxpk REPLICA IDENTITY USING INDEX riwire_idxpk_uk")
	mustExec("CREATE TABLE riwire_idxnopk (id int PRIMARY KEY, u int NOT NULL, v text, big text)")
	mustExec("CREATE UNIQUE INDEX riwire_idxnopk_uk ON riwire_idxnopk (u)")
	mustExec("ALTER TABLE riwire_idxnopk REPLICA IDENTITY USING INDEX riwire_idxnopk_uk")
	mustExec("CREATE TABLE riwire_nothing (id int PRIMARY KEY, v text)")
	mustExec("ALTER TABLE riwire_nothing REPLICA IDENTITY NOTHING")
	mustExec("CREATE TABLE riwire_full (id int PRIMARY KEY, v text, big text)")
	mustExec("ALTER TABLE riwire_full REPLICA IDENTITY FULL")
	mustExec("CREATE TABLE riwire_nopk (a int, v text)")
	mustExec("CREATE TABLE riwire_bigkey (k text PRIMARY KEY, v text)")
	for _, tb := range []string{"riwire_def", "riwire_idxpk", "riwire_idxnopk", "riwire_full"} {
		mustExec("ALTER TABLE " + tb + " ALTER COLUMN big SET STORAGE EXTERNAL")
	}
	mustExec("ALTER TABLE riwire_bigkey ALTER COLUMN k SET STORAGE EXTERNAL")
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + strings.Join(tables, ", "))
	if _, err := conn.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot); err != nil {
		t.Fatalf("create slot: %v", err)
	}

	flags := map[string]string{}
	sig := func(td *pglogrepl.TupleData) string {
		if td == nil {
			return "<nil>"
		}
		var b strings.Builder
		for _, c := range td.Columns {
			b.WriteByte(c.DataType)
		}
		return b.String()
	}
	oldSig := func(kind uint8, td *pglogrepl.TupleData) string {
		if kind == 0 {
			return "-"
		}
		return string(rune(kind)) + ":" + sig(td)
	}
	// run executes one statement in its own transaction and returns the change
	// messages it produced, consuming them from the slot.
	run := func(label, q string) ([]string, error) {
		t.Helper()
		_, execErr := conn.Exec(ctx, q)
		rows, err := conn.Query(ctx, "SELECT data FROM pg_logical_slot_get_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)", slot, pub)
		if err != nil {
			t.Fatalf("%s: read slot: %v", label, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var data []byte
			if err := rows.Scan(&data); err != nil {
				t.Fatalf("%s: scan: %v", label, err)
			}
			msg, err := pglogrepl.Parse(data)
			if err != nil {
				t.Fatalf("%s: parse %q: %v", label, data[:1], err)
			}
			switch m := msg.(type) {
			case *pglogrepl.RelationMessage:
				var f, names strings.Builder
				for _, c := range m.Columns {
					fmt.Fprintf(&f, "%d", c.Flags)
					names.WriteString(" " + c.Name)
				}
				flags[m.RelationName] = f.String()
				t.Logf("%-22s RELATION %s replica_identity=%q flags=%s columns:%s", label, m.RelationName, m.ReplicaIdentity, f.String(), names.String())
			case *pglogrepl.InsertMessage:
				out = append(out, "INSERT new="+sig(m.Tuple))
			case *pglogrepl.UpdateMessage:
				out = append(out, "UPDATE old="+oldSig(m.OldTupleType, m.OldTuple)+" new="+sig(m.NewTuple))
			case *pglogrepl.DeleteMessage:
				out = append(out, "DELETE old="+oldSig(m.OldTupleType, m.OldTuple))
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: rows: %v", label, err)
		}
		t.Logf("%-22s %s -> %v (exec error: %v)", label, q, out, execErr)
		return out, execErr
	}

	type step struct {
		label, sql string
		want       []string // change messages; nil = none
		refused    bool     // PostgreSQL must refuse the statement (SQLSTATE 55000)
	}
	steps := []step{
		// DEFAULT (primary key is the identity).
		{"def insert", "INSERT INTO riwire_def VALUES (1, 'a', " + bigVal + ")", []string{"INSERT new=ttt"}, false},
		{"def update non-key", "UPDATE riwire_def SET v = 'b' WHERE id = 1", []string{"UPDATE old=- new=ttu"}, false},
		{"def update key", "UPDATE riwire_def SET id = 2 WHERE id = 1", []string{"UPDATE old=K:tnn new=ttu"}, false},
		{"def delete", "DELETE FROM riwire_def WHERE id = 2", []string{"DELETE old=K:tnn"}, false},

		// USING INDEX on (id, u): the index covers the primary key.
		{"idxpk insert", "INSERT INTO riwire_idxpk VALUES (1, 10, 'a', " + bigVal + ")", []string{"INSERT new=tttt"}, false},
		{"idxpk update non-key", "UPDATE riwire_idxpk SET v = 'b' WHERE id = 1", []string{"UPDATE old=- new=tttu"}, false},
		{"idxpk update pk", "UPDATE riwire_idxpk SET id = 2 WHERE id = 1", []string{"UPDATE old=K:ttnn new=tttu"}, false},
		{"idxpk delete", "DELETE FROM riwire_idxpk WHERE id = 2", []string{"DELETE old=K:ttnn"}, false},

		// USING INDEX on (u): the index does NOT cover the primary key. A primary
		// key change sends no old tuple at all, so the old key is lost.
		{"idxnopk insert", "INSERT INTO riwire_idxnopk VALUES (1, 10, 'a', " + bigVal + ")", []string{"INSERT new=tttt"}, false},
		{"idxnopk update non-key", "UPDATE riwire_idxnopk SET v = 'b' WHERE id = 1", []string{"UPDATE old=- new=tttu"}, false},
		{"idxnopk update pk", "UPDATE riwire_idxnopk SET id = 2 WHERE id = 1", []string{"UPDATE old=- new=tttu"}, false},
		{"idxnopk update index", "UPDATE riwire_idxnopk SET u = 20 WHERE id = 2", []string{"UPDATE old=K:ntnn new=tttu"}, false},
		{"idxnopk delete", "DELETE FROM riwire_idxnopk WHERE id = 2", []string{"DELETE old=K:ntnn"}, false},

		// FULL: the whole old row, out-of-line values flattened into it; the new
		// tuple still sends the unchanged out-of-line value as 'u'.
		{"full insert", "INSERT INTO riwire_full VALUES (1, 'a', " + bigVal + ")", []string{"INSERT new=ttt"}, false},
		{"full update non-key", "UPDATE riwire_full SET v = 'b' WHERE id = 1", []string{"UPDATE old=O:ttt new=ttu"}, false},
		{"full update key", "UPDATE riwire_full SET id = 2 WHERE id = 1", []string{"UPDATE old=O:ttt new=ttu"}, false},
		{"full delete", "DELETE FROM riwire_full WHERE id = 2", []string{"DELETE old=O:ttt"}, false},

		// DEFAULT with an out-of-line primary key value: an UPDATE that leaves the
		// key alone still sends a key-only old tuple carrying the key in full,
		// while the new tuple sends that same key as 'u'.
		{"bigkey insert", "INSERT INTO riwire_bigkey VALUES (" + bigKey + ", 'a')", []string{"INSERT new=tt"}, false},
		{"bigkey update non-key", "UPDATE riwire_bigkey SET v = 'b'", []string{"UPDATE old=K:tn new=ut"}, false},
		{"bigkey delete", "DELETE FROM riwire_bigkey", []string{"DELETE old=K:tn"}, false},

		// NOTHING, and DEFAULT without a primary key: PostgreSQL refuses UPDATE
		// and DELETE on the published table; only INSERT reaches the stream.
		{"nothing insert", "INSERT INTO riwire_nothing VALUES (1, 'a')", []string{"INSERT new=tt"}, false},
		{"nothing update", "UPDATE riwire_nothing SET v = 'b' WHERE id = 1", nil, true},
		{"nothing delete", "DELETE FROM riwire_nothing WHERE id = 1", nil, true},
		{"nopk insert", "INSERT INTO riwire_nopk VALUES (1, 'a')", []string{"INSERT new=tt"}, false},
		{"nopk update", "UPDATE riwire_nopk SET v = 'b' WHERE a = 1", nil, true},
		{"nopk delete", "DELETE FROM riwire_nopk WHERE a = 1", nil, true},
	}
	for _, s := range steps {
		got, execErr := run(s.label, s.sql)
		if s.refused {
			var pgErr *pgconn.PgError
			if !errors.As(execErr, &pgErr) || pgErr.Code != "55000" || !strings.Contains(pgErr.Message, "does not have a replica identity") {
				t.Errorf("%s: PostgreSQL must refuse with 55000 \"does not have a replica identity\"; got %v", s.label, execErr)
			} else {
				t.Logf("%-22s refused: %s (hint: %s)", s.label, pgErr.Message, pgErr.Hint)
			}
		} else if execErr != nil {
			t.Fatalf("%s: %v", s.label, execErr)
		}
		if strings.Join(got, "; ") != strings.Join(s.want, "; ") {
			t.Errorf("%s: messages = %q, want %q", s.label, got, s.want)
		}
	}

	// The per-column key flag. Under FULL every column is flagged; under DEFAULT
	// and USING INDEX only the identity's columns; under NOTHING, and DEFAULT
	// without a primary key, none.
	wantFlags := map[string]string{
		"riwire_def":     "100",
		"riwire_idxpk":   "1100",
		"riwire_idxnopk": "0100",
		"riwire_nothing": "00",
		"riwire_full":    "111",
		"riwire_nopk":    "00",
		"riwire_bigkey":  "10",
	}
	for tb, want := range wantFlags {
		if got, ok := flags[tb]; !ok || got != want {
			t.Errorf("%s: relation key flags = %q (seen: %v), want %q", tb, got, ok, want)
		}
	}
}
