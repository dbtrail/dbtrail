package baseline

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/consistency"
)

func latin1Of(t *testing.T, createSQL string) []string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "db.t-schema.sql")
	if err := os.WriteFile(p, []byte(createSQL), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := ParseSchema(p)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for i, is := range Latin1Columns(createSQL, cols) {
		if is {
			out = append(out, cols[i].Name)
		}
	}
	return out
}

func TestLatin1Columns(t *testing.T) {
	for name, c := range map[string]struct {
		sql  string
		want []string
	}{
		"table latin1, columns inherit": {
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` varchar(20) DEFAULT NULL,\n  `x` text,\n  `e` enum('a','é') DEFAULT NULL,\n  `b` varbinary(10) DEFAULT NULL,\n  `n` decimal(5,2) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n",
			[]string{"v", "x", "e"},
		},
		"table latin1, one column utf8mb4": {
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` varchar(20) DEFAULT NULL,\n  `u` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n",
			[]string{"v"},
		},
		"table utf8mb4, one column latin1 by charset": {
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` varchar(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,\n  `x` mediumtext CHARACTER SET latin1,\n  `u` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n",
			[]string{"v", "x"},
		},
		"column names only a latin1 collation": {
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` varchar(20) COLLATE latin1_bin DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n",
			[]string{"v"},
		},
		"table names only a latin1 collation": {
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` char(3) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;\n",
			[]string{"v"},
		},
		"upper case and spacing": {
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `V` VARCHAR(20) CHARACTER SET LATIN1 DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n",
			[]string{"V"},
		},
		"utf8mb4 table: nothing": {
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n",
			nil,
		},
		"no charset written anywhere: not assumed": {
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n",
			nil,
		},
		"latin2 is not latin1": {
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin2;\n",
			nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := latin1Of(t, c.sql); !slices.Equal(got, c.want) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestDumpIsBinary(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for name, c := range map[string]struct {
		body string
		want bool
	}{
		"mydumper default":       {"/*!40101 SET NAMES binary*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\nINSERT INTO `t` VALUES(1,\"caf\xe9\");\n", true},
		"set names utf8mb4":      {"/*!40101 SET NAMES utf8mb4*/;\nINSERT INTO `t` VALUES(1,\"café\");\n", false},
		"lower case, spaces":     {"/*!40101   set names   'BINARY' */;\n", true},
		"no SET NAMES: mydumper": {"INSERT INTO `t` VALUES(1,\"caf\xe9\");\n", true},
	} {
		t.Run(name, func(t *testing.T) {
			p := write(name+".sql", c.body)
			if got := dumpIsBinary(TableFiles{Format: "sql", DataFiles: []string{p}}); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
	if !dumpIsBinary(TableFiles{Format: "sql"}) {
		t.Fatal("a table with no data file has nothing to decide; binary")
	}
	// A tab dump's first lines are rows: a cell reading SET NAMES utf8 is
	// data, not the dump's character set.
	tab := write("rows.dat", "1\tset names utf8\n2\tcaf\xe9\n")
	if !dumpIsBinary(TableFiles{Format: "tab", DataFiles: []string{tab}}) {
		t.Fatal("a tab dump is stored bytes; its rows are not a header")
	}
}

// TestProcessTable_decodesLatin1 is the conversion of a real-shaped mydumper
// dump of a latin1 table (bytes as stored, SET NAMES binary; the shape mydumper
// 1.0.5 wrote against MySQL 8.0.46 for #2634). DuckDB must read the file, the
// text must be UTF-8, the utf8mb4 column must be untouched, and the content
// digest must stay over the stored bytes, where the live checksum reads them.
func TestProcessTable_decodesLatin1(t *testing.T) {
	const schema = "/*!40101 SET NAMES binary*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n\n" +
		"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` varchar(20) DEFAULT NULL,\n  `x` text,\n" +
		"  `u` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=latin1;\n"
	const data = "/*!40101 SET NAMES binary*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"INSERT INTO `t` (`id`,`v`,`x`,`u`) VALUES(1,\"caf\xe9\",\"na\xefve \x80\",\"caf\xc3\xa9\"),\n(2,\"\xc3\xa9\",\"plain\",NULL);\n"
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "l1.t-schema.sql")
	dataPath := filepath.Join(dir, "l1.t.00000.sql")
	if err := os.WriteFile(schemaPath, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	tf := TableFiles{Database: "l1", Table: "t", SchemaFile: schemaPath, DataFiles: []string{dataPath}, Format: "sql"}
	outPath := filepath.Join(dir, "t.parquet")
	if _, err := processTable(context.Background(), tf, outPath, WriterConfig{Compression: "none", RowGroupSize: 100}); err != nil {
		t.Fatalf("processTable: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(context.Background(),
		"SELECT id, v, x, u FROM parquet_scan('"+strings.ReplaceAll(outPath, "'", "''")+"') ORDER BY id")
	if err != nil {
		t.Fatalf("DuckDB must read a latin1 table's file: %v", err)
	}
	defer rows.Close()
	type row struct {
		id      int
		v, x, u sql.NullString
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.v, &r.x, &r.u); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].v.String != "café" || got[0].x.String != "naïve €" || got[0].u.String != "café" ||
		got[1].v.String != "Ã©" || got[1].x.String != "plain" || got[1].u.Valid {
		t.Fatalf("rows = %+v", got)
	}

	md, err := ReadParquetMetadata(outPath)
	if err != nil {
		t.Fatal(err)
	}
	h := consistency.NewHasher()
	h.AddStrings([]string{"1", "caf\xe9", "na\xefve \x80", "caf\xc3\xa9"}, []bool{false, false, false, false})
	h.AddStrings([]string{"2", "\xc3\xa9", "plain", ""}, []bool{false, false, false, true})
	if md.ContentDigest != h.Digest() {
		t.Fatalf("the content digest must be over the stored bytes: got %s, want %s", md.ContentDigest, h.Digest())
	}
}
