package sysversion

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// The row_end spellings below are copied verbatim from binlog_events.row_after
// as a MariaDB 11.8 capture stored them (the parser renders TIMESTAMP in UTC,
// #757), plus the 2038 marker MariaDB 11.4 and older write.
func TestRowEnd(t *testing.T) {
	evAt := time.Date(2026, 10, 2, 6, 13, 43, 0, time.UTC)
	for _, c := range []struct {
		name    string
		v       any
		want    State
		wantErr string
	}{
		{"11.5+ current marker", "2106-02-07 06:28:15.999999", Current, ""},
		{"11.4 and older current marker", "2038-01-19 03:14:07.999999", Current, ""},
		{"history: the moment the version ended", "2026-10-02 06:13:43.628276", History, ""},
		{"history: same second as the event, later fraction", "2026-10-02 06:13:43.999999", History, ""},
		{"history: ended long ago", "2001-01-01 00:00:00.000000", History, ""},
		{"a far-future value that is no known marker", "2090-01-01 00:00:00.000000", 0, "neither"},
		{"one microsecond short of the marker", "2106-02-07 06:28:15.999998", 0, "neither"},
		{"not a timestamp", "abc", 0, "not a TIMESTAMP"},
		{"empty string", "", 0, "not a TIMESTAMP"},
		{"missing (NULL)", nil, 0, "no value"},
		{"a number (transaction-precise BIGINT shape)", float64(1.8446744073709552e19), 0, "not a TIMESTAMP"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := RowEnd(c.v, evAt)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("RowEnd(%v) = %v, %v; want an error containing %q", c.v, got, err, c.wantErr)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("RowEnd(%v) = %v, %v; want %v", c.v, got, err, c.want)
			}
		})
	}
}

// Every CREATE TABLE below is what SHOW CREATE TABLE / mydumper wrote on
// MariaDB 11.8, copied from a live server, except the last two MySQL shapes.
func TestFromCreateTable(t *testing.T) {
	for _, c := range []struct {
		name               string
		sql                string
		versioned          bool
		wantStart, wantEnd string
	}{
		{"implicit hidden period columns", "CREATE TABLE `prices` (\n" +
			"  `id` int(11) NOT NULL,\n  `sku` varchar(20) DEFAULT NULL,\n  `price` decimal(10,2) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci WITH SYSTEM VERSIONING;",
			true, "row_start", "row_end"},
		{"explicit INVISIBLE period columns", "CREATE TABLE `exp` (\n  `id` int(11) NOT NULL,\n  `v` int(11) DEFAULT NULL,\n" +
			"  `rs` timestamp(6) GENERATED ALWAYS AS ROW START INVISIBLE,\n  `re` timestamp(6) GENERATED ALWAYS AS ROW END INVISIBLE,\n" +
			"  PRIMARY KEY (`id`,`re`),\n  PERIOD FOR SYSTEM_TIME (`rs`, `re`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 WITH SYSTEM VERSIONING;",
			true, "rs", "re"},
		{"period names that need quoting", "CREATE TABLE `q` (\n  `id` int NOT NULL,\n" +
			"  `valid from` timestamp(6) GENERATED ALWAYS AS ROW START,\n  `valid``to` timestamp(6) GENERATED ALWAYS AS ROW END,\n" +
			"  PRIMARY KEY (`id`,`valid``to`),\n  PERIOD FOR SYSTEM_TIME (`valid from`, `valid``to`)\n) ENGINE=InnoDB WITH SYSTEM VERSIONING",
			true, "valid from", "valid`to"},
		{"PARTITION BY SYSTEM_TIME after the options", "CREATE TABLE `parted` (\n  `id` int(11) NOT NULL,\n  `v` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 WITH SYSTEM VERSIONING\n PARTITION BY SYSTEM_TIME \n" +
			"(PARTITION `p_hist` HISTORY ENGINE = InnoDB,\n PARTITION `p_cur` CURRENT ENGINE = InnoDB);",
			true, "row_start", "row_end"},
		{"a column WITHOUT SYSTEM VERSIONING in a versioned table", "CREATE TABLE `colv` (\n  `id` int(11) NOT NULL,\n" +
			"  `b` int(11) DEFAULT NULL WITHOUT SYSTEM VERSIONING,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB WITH SYSTEM VERSIONING;",
			true, "row_start", "row_end"},
		{"lower-case spelling", "create table t (\n id int primary key\n) engine=innodb with system versioning",
			true, "row_start", "row_end"},
		{"a plain MySQL table", "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			false, "", ""},
		{"only a column-level WITHOUT, table not versioned", "CREATE TABLE `t` (\n  `id` int NOT NULL,\n" +
			"  `b` int DEFAULT NULL WITHOUT SYSTEM VERSIONING,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;",
			false, "", ""},
		{"the phrase inside a column comment only", "CREATE TABLE `t` (\n  `id` int NOT NULL COMMENT 'copied ) WITH SYSTEM VERSIONING',\n" +
			"  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;",
			false, "", ""},
		{"empty", "", false, "", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			p, ok := FromCreateTable(c.sql)
			if ok != c.versioned {
				t.Fatalf("FromCreateTable versioned = %v, want %v (period %+v)", ok, c.versioned, p)
			}
			if ok && (p.Start != c.wantStart || p.End != c.wantEnd) {
				t.Fatalf("period = (%q, %q), want (%q, %q)", p.Start, p.End, c.wantStart, c.wantEnd)
			}
		})
	}
}

func TestFromSnapshot(t *testing.T) {
	gen := func(name string, pk bool, typ string) metadata.ColumnMeta {
		return metadata.ColumnMeta{Name: name, IsPK: pk, DataType: typ, IsGenerated: true}
	}
	id := metadata.ColumnMeta{Name: "id", IsPK: true, DataType: "int"}
	for _, c := range []struct {
		name string
		cols []metadata.ColumnMeta
		want Period
		ok   bool
	}{
		{"implicit (synthesized #1272)", []metadata.ColumnMeta{id, gen("row_start", false, "timestamp"), gen("row_end", true, "timestamp")},
			Period{"row_start", "row_end"}, true},
		{"explicit", []metadata.ColumnMeta{id, gen("rs", false, "timestamp"), gen("re", true, "timestamp")}, Period{"rs", "re"}, true},
		{"MySQL: one generated TIMESTAMP in the key", []metadata.ColumnMeta{id, gen("ts", true, "timestamp")}, Period{}, false},
		{"MySQL: one generated TIMESTAMP outside the key", []metadata.ColumnMeta{id, gen("ts", false, "timestamp")}, Period{}, false},
		{"transaction-precise (BIGINT)", []metadata.ColumnMeta{id, gen("rs", false, "bigint"), gen("re", true, "bigint")}, Period{}, false},
		{"two generated TIMESTAMPs in the key", []metadata.ColumnMeta{id, gen("a", true, "timestamp"), gen("b", true, "timestamp"), gen("s", false, "timestamp")}, Period{}, false},
		{"plain table", []metadata.ColumnMeta{id}, Period{}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			p, ok := FromSnapshot(c.cols)
			if ok != c.ok || p != c.want {
				t.Fatalf("FromSnapshot = %+v, %v; want %+v, %v", p, ok, c.want, c.ok)
			}
		})
	}
}
