//go:build integration

package pgshim

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestScanSQLAgreesWithPostgreSQL holds the port's reader of SQL text to a
// real PostgreSQL: many texts made of the pieces a reader gets wrong
// (comments closed by either line end, nested comments, strings of every
// kind, dollar quotes, names that look like the start of one) are run there,
// in a read-only transaction and made of constants only, and PostgreSQL must
// find in each exactly the statements the port found. A text PostgreSQL runs
// and the port could not read whole is a failure too, and so is one the
// screen would send on with fewer statements than PostgreSQL ran.
func TestScanSQLAgreesWithPostgreSQL(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["standard_conforming_strings"] = "on"
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	pg, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close(ctx)

	lits := []string{
		"1", "1.5", "'a'", "'a;b'", "'a''b'", `'a\'`, `e'a\'b'`, `e'a\\'`, `E'a;\''`, "$$a;b$$", "$t$a;$u$b$t$", "$t$a'$t$", "x'ab'", "b'1'",
		"'a'\n'b;'", "e'a'\n'\\';'", "'a' -- c\n ';b'", "n'a;'", "'--'", "'/*'", `"q"`, "1 + 2", "- 1", "1e3", ".5",
	}
	names := []string{"x", "x$t$", "y$$", "e", "mode", "typee", `"q;q"`, `"a""b"`, `"--"`, "é", "x1$"}
	gaps := []string{" ", " ", "\n", "\r", "\r\n", "\t", " --c\n", " --c\r", " --;\r", " -- ; select 9\r", " -- ; select 9\n", " /* ; */ ", " /* /* ; */ ; */ ", " --'\r", " --$$\n", ""}
	seed := uint64(0x2545F4914F6CDD1D)
	next := func(n int) int {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		return int(seed % uint64(n))
	}
	gap := func() string {
		g := gaps[next(len(gaps))]
		if g == "" {
			return " "
		}
		return g
	}
	ran, compared := 0, 0
	for n := 0; n < 6000; n++ {
		var b strings.Builder
		for k := 1 + next(3); k > 0; k-- {
			switch next(3) {
			case 0:
				b.WriteString("select" + gap() + lits[next(len(lits))])
			case 1:
				b.WriteString("select" + gap() + lits[next(len(lits))] + gap() + "as" + gap() + names[next(len(names))])
			case 2:
				b.WriteString("values" + gap() + "(" + lits[next(len(lits))] + gap() + ")")
			}
			// Sometimes nothing between a statement's end and the next piece,
			// so a name, a string or a comment runs into what follows.
			if next(4) > 0 {
				b.WriteString(gap())
			}
			if next(5) > 0 {
				b.WriteString(";")
			}
			if next(4) > 0 {
				b.WriteString(gap())
			}
		}
		q := b.String()
		toks, why := scanSQL(q)
		port := len(scanStatements(toks))
		res, err := pg.Exec(ctx, q).ReadAll()
		ran++
		if err != nil {
			// PostgreSQL ran none of it (the whole text is parsed first).
			continue
		}
		compared++
		if why != "" {
			t.Fatalf("PostgreSQL ran %d statements of %q, which the port could not read (%s)", len(res), q, why)
		}
		if len(res) != port {
			t.Fatalf("PostgreSQL ran %d statements of %q, the port read %d", len(res), q, port)
		}
		for _, r := range res {
			if tag := r.CommandTag.String(); !strings.HasPrefix(tag, "SELECT") {
				t.Fatalf("PostgreSQL answered %q for a statement of %q", tag, q)
			}
		}
		if forwardRefusal(q) != nil {
			t.Fatalf("the screen refuses %q, a text of plain reads", q)
		}
	}
	t.Logf("%d texts run on PostgreSQL, %d of them valid there and compared statement by statement", ran, compared)
	if compared < ran/10 {
		t.Errorf("only %d of %d texts were valid on PostgreSQL: the generator compares too little", compared, ran)
	}
}
