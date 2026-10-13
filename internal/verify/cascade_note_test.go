package verify

import (
	"bytes"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func TestCascadeNote(t *testing.T) {
	wrap := func(lines ...string) string {
		return "/*!40101 SET NAMES utf8mb4*/;\nCREATE TABLE `t` (\n  `id` int NOT NULL,\n" + strings.Join(lines, "\n") + "\n) ENGINE=InnoDB;\n"
	}
	cases := []struct {
		name, sql string
		want      []string // each must be in the note; none: the note is ""
	}{
		{"empty", "", nil},
		{"not MySQL text", "CREATE TABLE public.t (id integer REFERENCES p(id) ON DELETE CASCADE);", nil},
		{"no foreign key", wrap("  PRIMARY KEY (`id`)"), nil},
		{"restrict", wrap("  CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `customers` (`id`) ON DELETE RESTRICT"), nil},
		{"no action", wrap("  CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `customers` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION"), nil},
		{"no rule at all", wrap("  CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `customers` (`id`)"), nil},
		{"delete cascade", wrap("  PRIMARY KEY (`id`),", "  CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `customers` (`id`) ON DELETE CASCADE"),
			[]string{"customers (ON DELETE CASCADE)"}},
		{"set null, spaced and lower case", wrap("  constraint `fk` foreign key (`a`) references `p` (`id`) on delete set  null"),
			[]string{"p (ON DELETE SET NULL)"}},
		{"both rules", wrap("  CONSTRAINT `fk` FOREIGN KEY (`a`) REFERENCES `p` (`id`) ON DELETE CASCADE ON UPDATE CASCADE"),
			[]string{"p (ON DELETE CASCADE), p (ON UPDATE CASCADE)"}},
		{"one cascades, one restricts", wrap("  CONSTRAINT `a` FOREIGN KEY (`a`) REFERENCES `p` (`id`) ON DELETE RESTRICT,", "  CONSTRAINT `b` FOREIGN KEY (`b`) REFERENCES `q` (`id`) ON UPDATE CASCADE"),
			[]string{"q (ON UPDATE CASCADE)"}},
		{"parent in another schema", wrap("  CONSTRAINT `fk` FOREIGN KEY (`a`) REFERENCES `crm`.`p` (`id`) ON DELETE CASCADE"),
			[]string{"crm.p (ON DELETE CASCADE)"}},
		{"backtick in the parent's name", wrap("  CONSTRAINT `fk` FOREIGN KEY (`a`) REFERENCES `we``ird` (`id`) ON DELETE CASCADE"),
			[]string{"we`ird (ON DELETE CASCADE)"}},
		{"composite key", wrap("  CONSTRAINT `fk` FOREIGN KEY (`a`, `b`) REFERENCES `p` (`x`, `y`) ON DELETE CASCADE"),
			[]string{"p (ON DELETE CASCADE)"}},
		{"the words in a comment are not a rule", wrap("  `note` varchar(64) DEFAULT NULL COMMENT 'REFERENCES `p` (`id`) ON DELETE CASCADE',", "  PRIMARY KEY (`id`)"), nil},
		{"same rule twice is said once", wrap("  CONSTRAINT `a` FOREIGN KEY (`a`) REFERENCES `p` (`id`) ON DELETE CASCADE,", "  CONSTRAINT `b` FOREIGN KEY (`b`) REFERENCES `p` (`id`) ON DELETE CASCADE"),
			[]string{": p (ON DELETE CASCADE). "}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := cascadeNote(c.sql)
			if len(c.want) == 0 {
				if got != "" {
					t.Fatalf("note = %q, want none", got)
				}
				return
			}
			t.Log(got)
			for _, w := range c.want {
				if !strings.Contains(got, w) {
					t.Fatalf("note %q does not say %q", got, w)
				}
			}
		})
	}
}

const cascadeCreateSQL = "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`),\n" +
	"  CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `customers` (`id`) ON DELETE CASCADE\n);\n"

// The pairing reads the note off the read's own footer.
func TestCascadeNote_pairCarriesTheReadsNote(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	md := lrDump(lr1, 200)
	md[baseline.MetaKeyCreateTableSQL] = cascadeCreateSQL
	lrWrite(t, root, lr1, "orders", md)
	lrWrite(t, root, lr1, "items", lrDump(lr1, 200))
	lrWrite(t, root, lr0, "items", lrDump(lr0, 100))
	pairs, _ := lrFind(t, root)
	if n := lrOne(t, pairs, "orders").NewCascadeNote; !strings.Contains(n, "customers (ON DELETE CASCADE)") {
		t.Fatalf("orders: note = %q, want it to name the rule", n)
	}
	if n := lrOne(t, pairs, "items").NewCascadeNote; n != "" {
		t.Fatalf("items: note = %q on a table with no foreign key", n)
	}
}

// Only a mismatch gets the note.
func TestCascadeNote_onlyOnAMismatch(t *testing.T) {
	p := BaselinePair{NewCascadeNote: cascadeNote(cascadeCreateSQL)}
	for _, st := range []Status{StatusMatch, StatusInconclusive, StatusError} {
		res := TableResult{Status: st, Detail: "d"}
		withCascadeNote(&res, p)
		if res.Detail != "d" {
			t.Fatalf("%s: detail = %q, want it unchanged", st, res.Detail)
		}
	}
	res := TableResult{Status: StatusMismatch, Detail: "row count differs: source=1 reconstructed=2"}
	withCascadeNote(&res, p)
	t.Log(res.Detail)
	if !strings.HasPrefix(res.Detail, "row count differs: source=1 reconstructed=2. Likely cause: this table has a foreign key") {
		t.Fatalf("detail = %q", res.Detail)
	}
	plain := TableResult{Status: StatusMismatch, Detail: "d"}
	withCascadeNote(&plain, BaselinePair{})
	if plain.Detail != "d" {
		t.Fatalf("no note on the pair: detail = %q", plain.Detail)
	}
}

// The drill-down prints it, once, before the rows.
func TestCascadeNote_inTheDrillDown(t *testing.T) {
	ex := &MismatchExplanation{Schema: "shop", Table: "orders", Anchor: "binlog.000001:200", CascadeNote: cascadeNote(cascadeCreateSQL)}
	ex.add(RowDiff{PK: "id=1", Kind: "extra"})
	var buf bytes.Buffer
	ex.Write(&buf)
	out := buf.String()
	t.Log(out)
	if strings.Count(out, "customers (ON DELETE CASCADE)") != 1 || strings.Index(out, "foreign key") > strings.Index(out, "id=1") {
		t.Fatalf("drill-down does not say it once, before the rows:\n%s", out)
	}
	var none bytes.Buffer
	(&MismatchExplanation{Schema: "shop", Table: "orders"}).Write(&none)
	if strings.Contains(none.String(), "foreign key") {
		t.Fatalf("a table with no such key is told about one:\n%s", none.String())
	}
}
