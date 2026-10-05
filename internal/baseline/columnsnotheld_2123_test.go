package baseline

import (
	"context"
	"reflect"
	"testing"
)

// #2123: the columns MySQL can NAME and a snapshot file does not hold, by
// name, and whether that list is the whole of them.
func TestColumnsNotHeld(t *testing.T) {
	cases := []struct {
		name, body, tail string
		want             []string
		unread           bool
	}{
		{name: "plain columns", body: "  `id` int NOT NULL,\n  `name` varchar(8) DEFAULT NULL,\n"},
		{name: "stored", body: "  `id` int NOT NULL,\n  `twice` int GENERATED ALWAYS AS ((`id` * 2)) STORED,\n  `a` int,\n", want: []string{"twice"}},
		{name: "virtual", body: "  `id` int NOT NULL,\n  `v` int GENERATED ALWAYS AS (`id` + 1) VIRTUAL,\n", want: []string{"v"}},
		{name: "MariaDB persistent", body: "  `id` int,\n  `b` int AS (2) PERSISTENT,\n", want: []string{"b"}},
		// MySQL's star leaves it out, so starDifference does not name it;
		// a statement can still NAME it, and the file does not hold it.
		{name: "generated and invisible", body: "  `id` int NOT NULL,\n  `g` int GENERATED ALWAYS AS (`id`) VIRTUAL /*!80023 INVISIBLE */,\n", want: []string{"g"}},
		// The file holds an invisible column: nothing is missing.
		{name: "invisible only", body: "  `id` int NOT NULL,\n  `secret` int DEFAULT NULL /*!80023 INVISIBLE */,\n"},
		{name: "explicit period columns", body: "  `id` int NOT NULL,\n  `rs` timestamp(6) GENERATED ALWAYS AS ROW START,\n  `re` timestamp(6) GENERATED ALWAYS AS ROW END,\n",
			want: []string{"rs", "re"}},
		// MariaDB lists no period column for a table versioned without
		// declaring them, and still answers `SELECT row_start FROM t`.
		{name: "implicit system versioning", body: "  `id` int NOT NULL,\n", tail: ") ENGINE=InnoDB WITH SYSTEM VERSIONING;\n",
			want: []string{"row_start", "row_end"}},
		{name: "the words in a comment are not versioning", body: "  `id` int NOT NULL COMMENT 'WITH SYSTEM VERSIONING',\n",
			tail: ") ENGINE=InnoDB COMMENT='with system versioning';\n"},
		{name: "a definition that cannot be read", body: "  `id` int NOT NULL,\n  `we``ird` int AS (1) STORED,\n", unread: true},
		{name: "several, in declared order", body: "  `z` int AS (1) STORED,\n  `id` int,\n  `a` int AS (2) VIRTUAL,\n", want: []string{"z", "a"}},
		{name: "the clause in a comment or a default", body: "  `id` int NOT NULL COMMENT 'x AS (1) STORED',\n  `s` varchar(32) DEFAULT 'AS (1) VIRTUAL',\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tail := c.tail
			if tail == "" {
				tail = ") ENGINE=InnoDB;\n"
			}
			got, unread := columnsNotHeld("CREATE TABLE `t` (\n" + c.body + "  PRIMARY KEY (`id`)\n" + tail)
			if !reflect.DeepEqual(got, c.want) || unread != c.unread {
				t.Errorf("columnsNotHeld = %q, unread %v; want %q, unread %v", got, unread, c.want, c.unread)
			}
		})
	}
}

// The footer read carries the names beside the order.
func TestReadTableFooters_notHeld(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `twice` int GENERATED ALWAYS AS (`id` * 2) STORED,\n" +
		"  `hid` int GENERATED ALWAYS AS (`id` * 3) VIRTUAL /*!80023 INVISIBLE */,\n  `b` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	path := writeOrderFile(t, "t", ddl, ddl)
	footers, err := TableFootersFor(context.Background(), []string{path})
	if err != nil {
		t.Fatal(err)
	}
	f := footers[path]
	if want := []string{"twice", "hid"}; !reflect.DeepEqual(f.NotHeld, want) || f.NotHeldUnread {
		t.Errorf("NotHeld = %q (unread %v), want %q", f.NotHeld, f.NotHeldUnread, want)
	}
	if want := []string{"id", "b"}; !reflect.DeepEqual(f.Columns, want) {
		t.Errorf("Columns = %q, want %q", f.Columns, want)
	}
}

// A generation expression whose string ends in a backslash, as a server under
// NO_BACKSLASH_ESCAPES prints it: the quote after the backslash closes the
// string. Read as an escape it swallowed the rest of the line, the STORED
// with it, and the column was taken for a plain one.
func TestGeneratedColumnLine_stringEndingInABackslash(t *testing.T) {
	for _, c := range []struct {
		line string
		want bool
	}{
		{"  `p` varchar(9) GENERATED ALWAYS AS (concat(`a`,'\\')) STORED,", true},
		{"  `p` varchar(9) GENERATED ALWAYS AS (concat(`a`,_utf8mb4'\\')) VIRTUAL,", true},
		// Escaped, as a server prints it by default: two backslashes.
		{"  `p` varchar(9) GENERATED ALWAYS AS (concat(`a`,'\\\\')) STORED,", true},
		{"  `p` varchar(9) GENERATED ALWAYS AS (concat(`a`,'it\\'s')) STORED,", true},
		// The clause inside a string is still not the clause.
		{"  `p` varchar(40) DEFAULT 'x\\' AS (1) STORED',", false},
		{"  `p` varchar(40) DEFAULT 'AS (1) STORED' COMMENT 'c\\\\',", false},
		{"  `p` varchar(40) DEFAULT 'a\\',", false},
	} {
		if got := generatedColumnLine(c.line); got != c.want {
			t.Errorf("generatedColumnLine(%q) = %v, want %v", c.line, got, c.want)
		}
	}
}
