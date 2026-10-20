package baseline

import (
	"slices"
	"testing"
)

func TestPrimaryKeyColumns(t *testing.T) {
	for _, c := range []struct {
		name, sql string
		want      []string
	}{
		{"one column", "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `n` varchar(10),\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB", []string{"id"}},
		{"composite, key order", "CREATE TABLE `t` (\n  `a` int,\n  `b` int,\n  PRIMARY KEY (`b`,`a`),\n  KEY `k` (`a`)\n)", []string{"b", "a"}},
		{"using btree", "CREATE TABLE `t` (\n  `a` int,\n  PRIMARY KEY (`a`) USING BTREE\n)", []string{"a"}},
		{"backquote in name", "CREATE TABLE `t` (\n  `a``b` int,\n  PRIMARY KEY (`a``b`)\n)", []string{"a`b"}},
		{"none", "CREATE TABLE `t` (\n  `a` int,\n  KEY `k` (`a`)\n)", nil},
		{"prefix length", "CREATE TABLE `t` (\n  `a` varchar(100),\n  PRIMARY KEY (`a`(10))\n)", nil},
		{"in a comment only", "CREATE TABLE `t` (\n  `a` int COMMENT 'PRIMARY KEY (`a`)'\n)", nil},
		{"comment before the real key", "CREATE TABLE `t` (\n  `a` int COMMENT 'PRIMARY KEY (`b`)',\n  `b` int,\n  PRIMARY KEY (`a`)\n)", []string{"a"}},
		{"empty", "", nil},
		{"unclosed", "CREATE TABLE `t` (\n  PRIMARY KEY (`a`", nil},
		{"spaces in list", "CREATE TABLE `t` (\n  PRIMARY KEY (`a`, `b`)\n)", nil},
		{"two key lines", "CREATE TABLE `t` (\n  PRIMARY KEY (`a`),\n  PRIMARY KEY (`b`)\n)", nil},
		{"lower case keyword", "CREATE TABLE `t` (\n  primary key (`a`)\n)", nil},
	} {
		if got := PrimaryKeyColumns(c.sql); !slices.Equal(got, c.want) {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}
