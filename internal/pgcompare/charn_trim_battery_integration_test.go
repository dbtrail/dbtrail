//go:build integration

package pgcompare

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A battery for reading char(n) columns on the copy without their trailing
// padding. It judges the behavior from the outside: the real daemon captures
// a real PostgreSQL, the real router serves the port, and every statement runs
// on PostgreSQL and through the port, in both protocols, with and without a
// forwarding user, on a database with a non-C collation.
//
// The verdict, per statement:
//
//   - Answered by the source, or refused: always acceptable.
//   - Answered by the copy: it must give PostgreSQL's rows, in PostgreSQL's
//     order when the statement has an ORDER BY at its top.
//   - The one difference tolerated is trailing spaces in a char(n) column
//     shown directly in the output: PostgreSQL says so in the row description
//     (the column's type is bpchar and it names its table and attribute), and
//     a case marks the few outputs that are the column's own value without
//     that origin (min, max and a set operation of the column).
//   - A value computed from the column must match exactly.
//
// The report says how many statements reached the copy, by group, so that a
// change that passes by refusing everything is visible.
//
// CHARN_BATTERY_OUT, when set, is a file that receives every failure.

const (
	cnToken      = "charn-battery-token"
	cnServer     = "charn"
	cnFwdPass    = "charn-fwd-pw"
	cnPub        = "charn_pub"
	cnSlot       = "charn_slot"
	cnWait       = 3 * time.Minute
	cnStatement  = 20 * time.Second
	cnFailLogged = 25
)

// cnCase is one statement. args, when set, makes it extended-only, with oids
// the declared types (0 is unknown). asColumn: an output of type char(n) with
// no origin is still the column's own value (min, max, a set operation).
type cnCase struct {
	group    string
	sql      string
	args     [][]byte
	oids     []uint32
	asColumn bool
	// noCopy: the copy must not answer it at all (a domain over char(n) is
	// outside this change and keeps going to the source). wantCopy: it should
	// reach the copy, and the report says how many did not.
	noCopy, wantCopy bool
}

func (c cnCase) key() string {
	if len(c.args) == 0 {
		return c.sql
	}
	return fmt.Sprintf("%s  -- args %q oids %v", c.sql, c.args, c.oids)
}

// cnValues are the char(n) values of the seed: short, empty, all blanks,
// explicit trailing blanks, leading blanks, inner blanks, non-ASCII, case and
// digits, and a NULL.
var cnValues = []string{"a", "a  ", "", "   ", "ab", "AB", "abcde", "é", "ñu", "ß", " l", "a b", "日本", "10", "9", "z", "Z", "b ", "ab%", "a%b", "a_b", "a\t", "a\u00a0", "a\u3000"}

func cnLit(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

func cnSeed() []string {
	var a, b, p, m, x, f, cs, alt1 []string
	for i, v := range cnValues {
		l := cnLit(v)
		id := i + 1
		a = append(a, fmt.Sprintf("(%d, %s, %s, %s, %d, %s::char(3), %s)", id, l, l, l, id%4, l, l))
		b = append(b, fmt.Sprintf("(%d, %s::char(3), %s, %s)", id, l, l, l))
		p = append(p, fmt.Sprintf("(%d, 'p%d', %s)", id, id, l))
		m = append(m, fmt.Sprintf("(%d, %s::char(4), %d)", id, l, id%3))
		cs = append(cs, fmt.Sprintf("(%d, %s, %s)", id, l, l))
		alt1 = append(alt1, fmt.Sprintf("(%d, %s)", id, l))
		x = append(x, fmt.Sprintf("(%d, %s::char(4), %s::char(4), ARRAY[%s::char(3), 'x'], %s::char(4))", id, l, l, l, l))
		if v != "a  " && v != "   " && v != "b " {
			f = append(f, "("+l+")")
		}
	}
	n := len(cnValues) + 1
	cs = append(cs, fmt.Sprintf("(%d, NULL, NULL)", n))
	alt1 = append(alt1, fmt.Sprintf("(%d, NULL)", n))
	a = append(a, fmt.Sprintf("(%d, NULL, NULL, NULL, 0, NULL, NULL)", n))
	b = append(b, fmt.Sprintf("(%d, NULL, NULL, NULL)", n))
	p = append(p, fmt.Sprintf("(%d, 'p%d', NULL)", n, n))
	m = append(m, fmt.Sprintf("(%d, NULL, 0)", n))
	x = append(x, fmt.Sprintf("(%d, NULL, NULL, NULL, NULL)", n))
	out := []string{
		`CREATE DOMAIN cn_dom AS char(4)`,
		`CREATE DOMAIN cn_dom2 AS cn_dom`,
		`CREATE TYPE cn_mood AS ENUM ('sad', 'ok', 'happy', 'Abc')`,
		`CREATE TABLE cn_a (id int PRIMARY KEY, c char(5), v varchar(10), t text, k int, m char(3), u bpchar)`,
		`INSERT INTO cn_a VALUES ` + strings.Join(a, ","),
		`CREATE TABLE cn_b (id int PRIMARY KEY, c char(3), v varchar(5), t text)`,
		`INSERT INTO cn_b VALUES ` + strings.Join(b, ","),
		`CREATE TABLE cn_p (id int PRIMARY KEY, name text, code text)`,
		`INSERT INTO cn_p VALUES ` + strings.Join(p, ","),
		`CREATE TABLE cn_f (code char(5))`,
		`INSERT INTO cn_f VALUES ` + strings.Join(f, ","),
		`CREATE TABLE "Cn_Mixed" (id int PRIMARY KEY, "Code" char(4), n int)`,
		`INSERT INTO "Cn_Mixed" VALUES ` + strings.Join(m, ","),
		`CREATE TABLE cn_dm (id int PRIMARY KEY, d1 cn_dom, d2 cn_dom2, arr char(3)[], c char(4))`,
		`INSERT INTO cn_dm VALUES ` + strings.Join(x, ","),
		`CREATE TABLE cn_ty (id int PRIMARY KEY, e cn_mood, mn money, ip inet, big numeric(45,0), c char(4))`,
		`INSERT INTO cn_ty VALUES
			(1, 'sad', '-5.00', '10.0.0.2', 9, 'a'), (2, 'ok', '10.00', '9.0.0.1', 100000000000000000000000000000000000000001, 'ab'),
			(3, 'happy', '9.50', '192.168.0.1', 99999999999999999999999999999999999999999, ''), (4, 'Abc', '100.00', '10.0.0.10', -7, 'é'),
			(5, 'sad', '-20.00', '::1', 20, 'a  '), (6, NULL, NULL, NULL, NULL, NULL)`,
		`CREATE TABLE cn_pk (k char(5) PRIMARY KEY, n int, note text)`,
		`INSERT INTO cn_pk VALUES ('ab', 1, 'one'), ('cd', 2, 'two'), ('ef', 3, 'three'), ('gh', 4, 'four'), ('ij', 5, 'five'),
			('é', 6, 'six'), ('x y', 7, 'seven'), ('', 8, 'eight'), ('st  ', 9, 'nine')`,
		`CREATE TABLE cn_mark (id int PRIMARY KEY)`,
		`CREATE TABLE cn_case (id int PRIMARY KEY, c char(5), "C" text)`,
		`INSERT INTO cn_case VALUES ` + strings.Join(cs, ","),
		`CREATE TABLE cn_alt1 (id int PRIMARY KEY, c char(5))`,
		`INSERT INTO cn_alt1 VALUES ` + strings.Join(alt1, ","),
		`CREATE TABLE cn_alt2 (id int PRIMARY KEY, c text)`,
		`INSERT INTO cn_alt2 VALUES ` + strings.Join(alt1, ","),
		`CREATE TABLE cn_noshape (id int PRIMARY KEY, c char(5))`,
		`INSERT INTO cn_noshape VALUES ` + strings.Join(alt1, ","),
		`CREATE TABLE cn_cp (id int PRIMARY KEY, c char(5))`,
		`INSERT INTO cn_cp VALUES (1, 'a'), (2, ''), (3, 'é'), (4, NULL)`,
	}
	stored, _ := cnStoredSeed()
	return append(append(out, cnHammerSeed()...), stored...)
}

// cnChanges are made after the snapshot, so they are answered from the
// recent changes laid over it (fresh mode): by key, with keys that have
// trailing blanks in their stored form and keys written with them.
func cnChanges() []string {
	return []string{
		`UPDATE cn_pk SET n = 11 WHERE k = 'ab  '`,
		`UPDATE cn_pk SET k = 'cz' WHERE k = 'cd'`,
		`DELETE FROM cn_pk WHERE k = 'ef'`,
		`DELETE FROM cn_pk WHERE k = 'gh'`,
		`INSERT INTO cn_pk VALUES ('gh', 44, 'four again')`,
		`DELETE FROM cn_pk WHERE k = 'ij'`,
		`INSERT INTO cn_pk VALUES ('ij   ', 55, 'five, written with blanks')`,
		`INSERT INTO cn_pk VALUES ('new  ', 12, 'written with blanks')`,
		`INSERT INTO cn_pk VALUES ('ñu', 13, 'new')`,
		`UPDATE cn_pk SET n = 88 WHERE k = '   '`,
		`UPDATE cn_pk SET n = 99, note = 'nine again' WHERE k = 'st'`,
		`UPDATE cn_pk SET n = 77 WHERE k = 'x y  '`,
		`UPDATE cn_pk SET n = 78 WHERE k = 'x y'`,
		`UPDATE cn_a SET c = 'ch  ' WHERE id = 5`,
		`UPDATE cn_b SET c = 'q' WHERE id = 5`,
	}
}

// cnHammerSeed is the TPROC-C schema as HammerDB builds it on PostgreSQL,
// with a few rows.
func cnHammerSeed() []string {
	return []string{
		`CREATE TABLE warehouse (w_id numeric(4,0) PRIMARY KEY, w_ytd numeric(12,2), w_tax numeric(4,4), w_name varchar(10), w_street_1 varchar(20), w_street_2 varchar(20), w_city varchar(20), w_state char(2), w_zip char(9))`,
		`CREATE TABLE district (d_id numeric(2,0), d_w_id numeric(4,0), d_ytd numeric(12,2), d_tax numeric(4,4), d_next_o_id numeric, d_name varchar(10), d_street_1 varchar(20), d_street_2 varchar(20), d_city varchar(20), d_state char(2), d_zip char(9), PRIMARY KEY (d_w_id, d_id))`,
		`CREATE TABLE customer (c_id numeric(5,0), c_d_id numeric(2,0), c_w_id numeric(4,0), c_first varchar(16), c_middle char(2), c_last varchar(16), c_street_1 varchar(20), c_street_2 varchar(20), c_city varchar(20), c_state char(2), c_zip char(9), c_phone char(16), c_since timestamp, c_credit char(2), c_credit_lim numeric(12,2), c_discount numeric(4,4), c_balance numeric(12,2), c_ytd_payment numeric(12,2), c_payment_cnt numeric(8,0), c_delivery_cnt numeric(8,0), c_data varchar(500), PRIMARY KEY (c_w_id, c_d_id, c_id))`,
		`CREATE TABLE history (h_c_id numeric, h_c_d_id numeric, h_c_w_id numeric, h_d_id numeric, h_w_id numeric, h_date timestamp, h_amount numeric(6,2), h_data varchar(24))`,
		`CREATE TABLE orders (o_id numeric, o_w_id numeric(4,0), o_d_id numeric(2,0), o_c_id numeric(5,0), o_carrier_id numeric(2,0), o_ol_cnt numeric(2,0), o_all_local numeric(1,0), o_entry_d timestamp, PRIMARY KEY (o_w_id, o_d_id, o_id))`,
		`CREATE TABLE new_order (no_w_id numeric, no_d_id numeric, no_o_id numeric, PRIMARY KEY (no_w_id, no_d_id, no_o_id))`,
		`CREATE TABLE order_line (ol_delivery_d timestamp, ol_o_id numeric, ol_w_id numeric, ol_i_id numeric(6,0), ol_supply_w_id numeric(4,0), ol_d_id numeric(2,0), ol_number numeric(2,0), ol_quantity numeric(2,0), ol_amount numeric(6,2), ol_dist_info char(24), PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))`,
		`CREATE TABLE item (i_id numeric(6,0) PRIMARY KEY, i_im_id numeric, i_name varchar(24), i_price numeric(5,2), i_data varchar(50))`,
		`CREATE TABLE stock (s_i_id numeric(6,0), s_w_id numeric(4,0), s_quantity numeric(6,0), s_dist_01 char(24), s_dist_02 char(24), s_dist_03 char(24), s_dist_04 char(24), s_dist_05 char(24), s_dist_06 char(24), s_dist_07 char(24), s_dist_08 char(24), s_dist_09 char(24), s_dist_10 char(24), s_ytd numeric(8,0), s_order_cnt numeric(6,0), s_remote_cnt numeric(6,0), s_data varchar(50), PRIMARY KEY (s_w_id, s_i_id))`,
		`INSERT INTO warehouse SELECT w, 1000, 0.1, 'w' || w, 's', 's2', 'city', 'NY', '12345' FROM generate_series(1, 3) w`,
		`INSERT INTO district SELECT d, w, 100, 0.05, 100, 'd' || d, 's', 's2', 'city', 'CA', '54321' FROM generate_series(1, 3) w, generate_series(1, 2) d`,
		`INSERT INTO customer SELECT c, d, w, 'first' || c, (ARRAY['OE', 'O', ''])[1 + c % 3], 'last' || (c % 4), 's', 's2', 'city', (ARRAY['NY', 'C', ''])[1 + c % 3], '123', '555', TIMESTAMP '2026-01-01' + c * interval '1 day',
			(ARRAY['GC', 'BC', 'G', ''])[1 + c % 4], 5000, 0.1, c * 1.25, 10, 1, 0, 'data' FROM generate_series(1, 3) w, generate_series(1, 2) d, generate_series(1, 8) c`,
		`INSERT INTO item SELECT i, i, 'item' || (i % 7), 1 + i % 50, CASE WHEN i % 3 = 0 THEN 'xORIGINALx' ELSE 'plain' END FROM generate_series(1, 30) i`,
		`INSERT INTO stock SELECT i, w, 10 + (i * w) % 40, 'd1', 'd2', 'd3', 'd4', 'd5', 'd6', 'd7', 'd8', 'd9', 'd10', i % 9, 1, 0, CASE WHEN i % 4 = 0 THEN 'aORIGINAL' ELSE 'none' END FROM generate_series(1, 3) w, generate_series(1, 30) i`,
		`INSERT INTO orders SELECT o, w, d, 1 + (o * d + w) % 8, 1, 1 + o % 3, o % 2, TIMESTAMP '2026-03-01' + ((o * 37 + d * 11 + w * 5) % 480) * interval '1 hour' FROM generate_series(1, 3) w, generate_series(1, 2) d, generate_series(1, 40) o`,
		`INSERT INTO order_line SELECT CASE WHEN o % 3 = 0 THEN NULL ELSE TIMESTAMP '2026-03-25' END, o, w, 1 + (o * n + w) % 30, w, d, n, 1 + (o + n) % 9, ((o * n + d * 13 + w * 7) % 97) + 0.25, (ARRAY['x', '', '  ', 'ñandú'])[1 + (o + n) % 4]
			FROM generate_series(1, 3) w, generate_series(1, 2) d, generate_series(1, 40) o, generate_series(1, 3) n WHERE n <= 1 + o % 3`,
	}
}

func cnTables() []string {
	_, stored := cnStoredSeed()
	return append([]string{"cn_a", "cn_b", "cn_p", "cn_f", `"Cn_Mixed"`, "cn_dm", "cn_ty", "cn_pk", "cn_mark", "cn_case", "cn_alt1", "cn_alt2", "cn_noshape", "cn_cp",
		"warehouse", "district", "customer", "history", "orders", "new_order", "order_line", "item", "stock"}, stored...)
}

// cnCases is the battery.
func cnCases() []cnCase {
	seen := map[string]bool{}
	var out []cnCase
	add := func(c cnCase) {
		if k := c.key(); !seen[k] {
			seen[k] = true
			out = append(out, c)
		}
	}
	q := func(group, sql string) { add(cnCase{group: group, sql: sql}) }
	col := func(group, sql string) { add(cnCase{group: group, sql: sql, asColumn: true}) }

	// The column as a table holds it, and carried outward.
	type src struct{ with, from, c string }
	carriers := []src{
		{"", "cn_a s", "c"}, {"", "public.cn_a s", "s.c"},
		{"", "(SELECT * FROM cn_a) s", "c"}, {"", "(SELECT id, c, k FROM cn_a) s", "c"}, {"", "(SELECT id, c AS r, k FROM cn_a) s", "r"},
		{"", "(SELECT id, r AS c2, k FROM (SELECT id, c AS r, k FROM cn_a) i) s", "c2"},
		{"WITH w AS (SELECT * FROM cn_a) ", "w s", "c"}, {"WITH w AS (SELECT id, c AS r, k FROM cn_a), x AS (SELECT id, r AS z, k FROM w) ", "x s", "z"},
	}
	lits := []string{"a", "a  ", "a     ", "", "   ", "ab", "AB", "é", "é  ", "ñu", "abcde", "abcdef", "z", "10", " l"}
	ops := []string{"=", "<>", "<", "<=", ">", ">=", "IS DISTINCT FROM", "IS NOT DISTINCT FROM"}
	for _, f := range carriers {
		for _, l := range lits {
			for _, op := range ops {
				q("compare literal", fmt.Sprintf("%sSELECT id FROM %s WHERE %s %s %s ORDER BY id", f.with, f.from, f.c, op, cnLit(l)))
			}
		}
		for _, w := range []string{
			"%C IN ('a', 'é')", "%C IN ('a  ', 'ab   ')", "%C NOT IN ('a', 'ab')", "%C NOT IN ('a  ', '')", "%C IN ('', 'z')", "%C = ANY (ARRAY['a', 'ab'])", "%C = ANY (ARRAY['a  ', 'é  '])", "%C <> ALL (ARRAY['a  '])",
			"%C BETWEEN 'a' AND 'b'", "%C BETWEEN 'a  ' AND 'ab  '", "%C NOT BETWEEN 'a' AND 'b'", "%C BETWEEN '' AND 'a'", "%C BETWEEN SYMMETRIC 'b' AND 'a'",
			"%C LIKE '%a'", "%C LIKE 'a%'", "%C LIKE 'a____'", "%C LIKE 'a'", "%C LIKE 'a  '", "%C LIKE '_'", "%C LIKE '%'", "%C LIKE '% '", "%C NOT LIKE '%a'", "%C ILIKE 'A%'", "%C ILIKE '%A'", "%C ILIKE 'a____'",
			"%C ~ 'a$'", "%C ~ '^a'", "%C ~ '^a *$'", "%C ~ ' $'", "%C !~ 'a$'", "%C ~* 'A$'", "%C SIMILAR TO 'a'", "%C SIMILAR TO 'a%'", "%C SIMILAR TO 'a *'", "%C SIMILAR TO '%a'",
			"%C IS NULL", "%C IS NOT NULL", "%C::text = 'a'", "%C::text = 'a    '", "%C::varchar = 'a'", "%C::char(2) = 'a'", "%C::char(8) = 'a'", "length(%C) = 1", "octet_length(%C) = 5", "%C || 'x' = 'ax'", "upper(%C) = 'A'", "lower(%C) = 'ab'", "trim(%C) = 'a'", "rtrim(%C) = 'a'",
			"CASE WHEN %C = 'a' THEN true ELSE false END", "COALESCE(%C, 'n') = 'a'", "COALESCE(%C, 'n') = 'n'", "NULLIF(%C, 'a') IS NULL", "%C = (SELECT c FROM cn_a WHERE id = 1)", "%C IN (SELECT c FROM cn_b)", "%C IN (SELECT v FROM cn_b)", "%C IN (SELECT t FROM cn_b)", "EXISTS (SELECT 1 FROM cn_b b WHERE b.c = %C)", "EXISTS (SELECT 1 FROM cn_b b WHERE b.t = %C)", "%C::text IN (SELECT t FROM cn_b)",
		} {
			q("predicate", fmt.Sprintf("%sSELECT id FROM %s WHERE %s ORDER BY id", f.with, f.from, strings.ReplaceAll(w, "%C", f.c)))
		}
		for _, e := range []string{
			"%C", "%C AS shown", "concat(%C, '|')", "concat('|', %C)", "concat_ws('-', %C, 'x')", "format('%s|', %C)", "format('%I', %C)", "format('%L', %C)", "to_json(%C)", "to_jsonb(%C)", "json_build_array(%C)", "json_build_object('k', %C)",
			"octet_length(%C)", "bit_length(%C)", "length(%C)", "char_length(%C)", "character_length(%C)", "%C || '|'", "'|' || %C", "%C || %C", "substr(%C, 1, 2)", "substr(%C, 2)", "substring(%C from 1 for 3)", "left(%C, 2)", "left(%C, 7)", "right(%C, 1)", "right(%C, 2)", "position('a' in %C)", "position(' ' in %C)", "strpos(%C, ' ')", "md5(%C)", "md5(%C::text)", "ascii(%C)", "reverse(%C)", "repeat(%C, 2)", "replace(%C, ' ', '_')", "translate(%C, ' ', '_')", "split_part(%C, ' ', 1)", "lpad(%C, 7, '*')", "rpad(%C, 7, '*')", "upper(%C)", "lower(%C)", "initcap(%C)", "trim(%C)", "ltrim(%C)", "rtrim(%C)", "btrim(%C)", "hashtext(%C)", "encode(%C::bytea, 'hex')", "convert_to(%C, 'UTF8')",
			"%C::text", "%C::varchar", "%C::varchar(3)", "%C::char(2)", "%C::char(8)", "%C::bpchar", "%C::text || '|'", "%C::varchar || '|'", "%C::char(8) || '|'", "length(%C::text)", "length(%C::varchar)", "length(%C::char(8))", "octet_length(%C::text)", "CAST(%C AS text)", "CAST(%C AS varchar(10))", "CAST(%C AS char(7))",
			"CASE WHEN id > 0 THEN %C END", "CASE WHEN %C = 'a' THEN 'yes' ELSE 'no' END", "CASE %C WHEN 'a' THEN 1 ELSE 0 END", "COALESCE(%C, 'none')", "COALESCE(%C, '')", "NULLIF(%C, 'a')", "NULLIF(%C, '')", "GREATEST(%C, 'b')", "LEAST(%C, 'b')", "(SELECT %C)", "ARRAY[%C]", "ROW(%C)",
			"%C = 'a'", "%C = 'a  '", "%C < 'b'", "%C LIKE 'a%'", "%C ~ 'a$'",
		} {
			q("output expression", fmt.Sprintf("%sSELECT id, %s FROM %s ORDER BY id", f.with, strings.ReplaceAll(e, "%C", f.c), f.from))
		}
		for _, tm := range []string{
			"SELECT %C, count(*) FROM %F GROUP BY 1 ORDER BY 2, 1", "SELECT %C, count(*) FROM %F GROUP BY %C ORDER BY %C", "SELECT count(*) FROM %F GROUP BY %C ORDER BY 1", "SELECT DISTINCT %C FROM %F ORDER BY 1", "SELECT DISTINCT %C FROM %F", "SELECT count(DISTINCT %C) FROM %F", "SELECT id, %C FROM %F ORDER BY %C, id", "SELECT id, %C FROM %F ORDER BY %C DESC, id", "SELECT id, %C FROM %F ORDER BY 2, 1", "SELECT id FROM %F ORDER BY %C NULLS FIRST, id", "SELECT id FROM %F ORDER BY lower(%C), id",
			"SELECT id, row_number() OVER (ORDER BY %C, id) FROM %F ORDER BY id", "SELECT id, rank() OVER (ORDER BY %C) FROM %F ORDER BY id", "SELECT id, count(*) OVER (PARTITION BY %C) FROM %F ORDER BY id", "SELECT DISTINCT ON (%C) id FROM %F ORDER BY %C, id", "SELECT string_agg(%C, ',' ORDER BY id) FROM %F", "SELECT string_agg(%C::text, ',' ORDER BY id) FROM %F", "SELECT array_agg(%C ORDER BY id) FROM %F", "SELECT json_agg(%C ORDER BY id) FROM %F", "SELECT count(%C) FROM %F", "SELECT k, count(DISTINCT %C) FROM %F GROUP BY k ORDER BY k", "SELECT k, sum(length(%C)) FROM %F GROUP BY k ORDER BY k",
		} {
			q("group and sort", f.with+strings.NewReplacer("%F", f.from, "%C", f.c).Replace(tm))
		}
		for _, tm := range []string{"SELECT min(%C) FROM %F", "SELECT max(%C) FROM %F", "SELECT k, min(%C) FROM %F GROUP BY k ORDER BY k", "SELECT k, max(%C) FROM %F GROUP BY k ORDER BY k"} {
			col("min and max", f.with+strings.NewReplacer("%F", f.from, "%C", f.c).Replace(tm))
		}
		for _, tm := range []string{"SELECT %C FROM %F UNION SELECT c FROM cn_a", "SELECT %C FROM %F UNION SELECT c FROM cn_b", "SELECT %C FROM %F UNION ALL SELECT c FROM cn_a", "SELECT %C FROM %F INTERSECT SELECT c FROM cn_b", "SELECT %C FROM %F EXCEPT SELECT c FROM cn_b", "SELECT %C FROM %F UNION SELECT c FROM cn_b ORDER BY 1"} {
			col("set operation", f.with+strings.NewReplacer("%F", f.from, "%C", f.c).Replace(tm))
		}
		for _, tm := range []string{"SELECT %C FROM %F UNION SELECT t FROM cn_b", "SELECT %C FROM %F UNION SELECT v FROM cn_b", "SELECT %C::text FROM %F UNION SELECT t FROM cn_b", "SELECT %C FROM %F INTERSECT SELECT t FROM cn_b", "SELECT %C FROM %F EXCEPT SELECT t FROM cn_b", "SELECT count(*) FROM (SELECT %C FROM %F UNION SELECT c FROM cn_b) u", "SELECT count(*) FROM (SELECT %C FROM %F UNION SELECT t FROM cn_b) u"} {
			q("set operation, text", f.with+strings.NewReplacer("%F", f.from, "%C", f.c).Replace(tm))
		}
	}

	// Joins: char to char of another length, to varchar and to text.
	for _, on := range []string{"a.c = b.c", "b.c = a.c", "a.c = b.v", "a.c = b.t", "a.c::text = b.t", "a.c = b.c::char(5)", "a.m = b.c", "a.c = a.m", "a.v = b.c", "a.t = b.c", "a.c < b.c", "a.c <> b.c AND a.id = b.id", "a.c = trim(b.t)", "a.u = b.c", "a.u = b.t"} {
		for _, j := range []string{"JOIN", "LEFT JOIN", "FULL JOIN"} {
			if strings.Contains(on, "<") && j == "FULL JOIN" {
				continue
			}
			q("join", fmt.Sprintf("SELECT a.id, b.id FROM cn_a a %s cn_b b ON %s ORDER BY 1 NULLS FIRST, 2 NULLS FIRST", j, on))
			q("join", fmt.Sprintf("SELECT count(*) FROM (SELECT * FROM cn_a) a %s (SELECT * FROM cn_b) b ON %s", j, on))
		}
		q("join", fmt.Sprintf("WITH x AS (SELECT * FROM cn_a), y AS (SELECT * FROM cn_b) SELECT a.id, b.id FROM x a JOIN y b ON %s ORDER BY 1, 2", on))
	}
	for _, s := range []string{
		"SELECT a.id, b.id FROM cn_a a JOIN cn_b b USING (c) ORDER BY 1, 2", "SELECT count(*) FROM cn_a a NATURAL JOIN cn_b b", "SELECT a.id, p.id FROM cn_a a JOIN cn_p p ON a.c = p.code ORDER BY 1, 2", "SELECT a.id FROM cn_a a WHERE a.c IN (SELECT code FROM cn_p) ORDER BY 1", "SELECT p.id FROM cn_p p WHERE p.code IN (SELECT c FROM cn_a) ORDER BY 1", "SELECT p.id FROM cn_p p WHERE p.code NOT IN (SELECT c FROM cn_a WHERE c IS NOT NULL) ORDER BY 1",
		`SELECT m.id, a.id FROM "Cn_Mixed" m JOIN cn_a a ON m."Code" = a.c ORDER BY 1, 2`, `SELECT m.id FROM "Cn_Mixed" m WHERE m."Code" = 'a' ORDER BY 1`, `SELECT m.id FROM "Cn_Mixed" m WHERE "Code" = 'a  ' ORDER BY 1`, `SELECT "Code", count(*) FROM "Cn_Mixed" GROUP BY 1 ORDER BY 2, 1`, `SELECT id, "Code" || '|' FROM public."Cn_Mixed" ORDER BY id`, `SELECT id, octet_length("Code") FROM "Cn_Mixed" ORDER BY id`,
		"SELECT * FROM cn_a ORDER BY id", "SELECT a.* FROM cn_a a ORDER BY a.id", "SELECT * FROM cn_f ORDER BY 1", "SELECT count(*) FROM (SELECT * FROM cn_a) s", "SELECT id, s FROM (SELECT id, c FROM cn_a) s ORDER BY id", "SELECT id, s::text FROM cn_a s ORDER BY id", "SELECT to_json(s) FROM cn_a s ORDER BY id", "SELECT row_to_json(s) FROM cn_a s ORDER BY id", "SELECT row_to_json(s) FROM (SELECT id, c FROM cn_a) s ORDER BY id", "SELECT hashtext(s::text) FROM cn_a s ORDER BY id", "SELECT md5(s::text) FROM cn_a s ORDER BY id", "SELECT count(DISTINCT s) FROM (SELECT c FROM cn_a) s", "SELECT count(*) FROM cn_a x JOIN cn_a y ON x = y", "SELECT id FROM cn_a s WHERE ROW(s.c) = ROW('a') ORDER BY id", "SELECT id FROM cn_a s WHERE (s.c, s.k) = ('a', 1) ORDER BY id", "SELECT id FROM cn_a s WHERE (s.c, s.k) IN (('a', 1), ('ab', 1)) ORDER BY id",
		"SELECT 'a  ' IN (SELECT * FROM cn_f)", "SELECT 'a  ' = ANY (SELECT * FROM cn_f)", "SELECT 'a  ' NOT IN (SELECT * FROM cn_f)", "SELECT 'a  ' IS DISTINCT FROM (SELECT * FROM cn_f WHERE code = 'a')", "SELECT EXISTS (SELECT * FROM cn_f WHERE code = 'a  ')", "SELECT id FROM cn_a s, LATERAL (SELECT s.c AS x) l WHERE l.x = 'a' ORDER BY id", "WITH RECURSIVE r AS (SELECT c FROM cn_f UNION SELECT c FROM cn_a) SELECT count(*) FROM r",
	} {
		q("whole rows and shapes", s)
	}

	// Arguments: declared bpchar, text and unknown, with trailing blanks.
	for _, oid := range []uint32{1042, 25, 1043, 0} {
		for _, v := range []string{"a", "a  ", "", "   ", "ab   ", "é  "} {
			arg := [][]byte{[]byte(v)}
			for _, op := range []string{"=", "<>", "<", ">=", "IS DISTINCT FROM"} {
				add(cnCase{group: "parameter", sql: "SELECT id FROM cn_a WHERE c " + op + " $1 ORDER BY id", args: arg, oids: []uint32{oid}})
				add(cnCase{group: "parameter", sql: "SELECT id FROM (SELECT * FROM cn_a) s WHERE s.c " + op + " $1 ORDER BY id", args: arg, oids: []uint32{oid}})
			}
			add(cnCase{group: "parameter", sql: "SELECT id FROM cn_a WHERE c IN ($1, 'z') ORDER BY id", args: arg, oids: []uint32{oid}})
			add(cnCase{group: "parameter", sql: "SELECT id FROM cn_a WHERE c BETWEEN $1 AND 'b' ORDER BY id", args: arg, oids: []uint32{oid}})
			add(cnCase{group: "parameter", sql: "SELECT id FROM cn_a WHERE c = ANY (ARRAY[$1]) ORDER BY id", args: arg, oids: []uint32{oid}})
			add(cnCase{group: "parameter", sql: "SELECT $1 = 'a'", args: arg, oids: []uint32{oid}})
			add(cnCase{group: "parameter", sql: "SELECT k, n FROM cn_pk WHERE k = $1 ORDER BY k, n", args: arg, oids: []uint32{oid}})
		}
	}

	// Domains over char(n), a domain over a domain, arrays of char(n).
	for _, c := range []string{"d1", "d2"} {
		for _, l := range []string{"a", "a  ", "", "   ", "ab", "é"} {
			for _, op := range []string{"=", "<>", "<"} {
				q("domain", fmt.Sprintf("SELECT id FROM cn_dm WHERE %s %s %s ORDER BY id", c, op, cnLit(l)))
			}
		}
		for _, e := range []string{"%C", "%C || '|'", "length(%C)", "octet_length(%C)", "concat(%C, '|')", "format('%s|', %C)", "to_json(%C)", "upper(%C)", "%C::text", "%C::varchar", "%C::char(2)", "right(%C, 1)", "md5(%C)", "%C LIKE 'a___'", "%C ~ 'a$'"} {
			q("domain", fmt.Sprintf("SELECT id, %s FROM cn_dm ORDER BY id", strings.ReplaceAll(e, "%C", c)))
			q("domain", fmt.Sprintf("SELECT id, %s FROM (SELECT * FROM cn_dm) s ORDER BY id", strings.ReplaceAll(e, "%C", c)))
		}
		for _, tm := range []string{"SELECT %C, count(*) FROM cn_dm GROUP BY 1 ORDER BY 2, 1", "SELECT DISTINCT %C FROM cn_dm ORDER BY 1", "SELECT id FROM cn_dm ORDER BY %C, id", "SELECT a.id, b.id FROM cn_dm a JOIN cn_a b ON a.%C = b.c ORDER BY 1, 2", "SELECT a.id, b.id FROM cn_dm a JOIN cn_a b ON a.%C = b.t ORDER BY 1, 2", "SELECT count(DISTINCT %C) FROM cn_dm"} {
			q("domain", strings.ReplaceAll(tm, "%C", c))
		}
		col("domain", fmt.Sprintf("SELECT min(%s) FROM cn_dm", c))
		col("domain", fmt.Sprintf("SELECT max(%s) FROM cn_dm", c))
	}
	for _, s := range []string{
		"SELECT id, arr FROM cn_dm ORDER BY id", "SELECT id, arr[1] FROM cn_dm ORDER BY id", "SELECT id FROM cn_dm WHERE arr[1] = 'a' ORDER BY id", "SELECT id FROM cn_dm WHERE arr[1] = 'a  ' ORDER BY id", "SELECT id, arr[1] || '|' FROM cn_dm ORDER BY id", "SELECT id, length(arr[1]) FROM cn_dm ORDER BY id", "SELECT id, octet_length(arr[1]) FROM cn_dm ORDER BY id", "SELECT id, array_length(arr, 1) FROM cn_dm ORDER BY id", "SELECT id, arr::text FROM cn_dm ORDER BY id", "SELECT id, to_json(arr) FROM cn_dm ORDER BY id", "SELECT id, array_to_string(arr, ',') FROM cn_dm ORDER BY id", "SELECT id FROM cn_dm WHERE 'x' = ANY (arr) ORDER BY id", "SELECT id FROM cn_dm WHERE 'x  ' = ANY (arr) ORDER BY id", "SELECT id FROM cn_dm WHERE arr @> ARRAY['x'::char(3)] ORDER BY id", "SELECT arr, count(*) FROM cn_dm GROUP BY 1 ORDER BY 2, 1", "SELECT DISTINCT arr FROM cn_dm ORDER BY 1", "SELECT id FROM cn_dm ORDER BY arr, id", "SELECT a.id, b.id FROM cn_dm a JOIN cn_dm b ON a.arr = b.arr ORDER BY 1, 2", "SELECT id, unnest(arr) FROM cn_dm ORDER BY 1, 2",
	} {
		q("array", s)
	}

	// Types that are only text on the copy, alone and beside a char(n).
	for _, c := range []string{"e", "mn", "ip", "big"} {
		for _, tm := range []string{
			"SELECT id, %C FROM cn_ty ORDER BY %C NULLS FIRST, id", "SELECT id, %C FROM cn_ty ORDER BY %C DESC NULLS LAST, id", "SELECT id FROM cn_ty ORDER BY 1", "SELECT %C, count(*) FROM cn_ty GROUP BY 1 ORDER BY 1 NULLS FIRST", "SELECT DISTINCT %C FROM cn_ty ORDER BY 1 NULLS FIRST", "SELECT count(DISTINCT %C) FROM cn_ty", "SELECT id, rank() OVER (ORDER BY %C) FROM cn_ty ORDER BY id",
			"SELECT id, c FROM cn_ty ORDER BY %C NULLS FIRST, c, id", "SELECT id FROM cn_ty WHERE c = 'a' ORDER BY %C NULLS FIRST, id", "SELECT c, min(%C) FROM cn_ty GROUP BY c ORDER BY c NULLS FIRST", "SELECT c, max(%C) FROM cn_ty GROUP BY c ORDER BY c NULLS FIRST", "SELECT %C, c, count(*) FROM cn_ty GROUP BY 1, 2 ORDER BY 1 NULLS FIRST, 2 NULLS FIRST", "SELECT id FROM (SELECT * FROM cn_ty) s ORDER BY %C NULLS FIRST, id", "WITH w AS (SELECT * FROM cn_ty) SELECT id, c FROM w ORDER BY %C NULLS FIRST, id",
		} {
			q("text-on-copy types", strings.ReplaceAll(tm, "%C", c))
		}
		q("text-on-copy types", fmt.Sprintf("SELECT min(%s), max(%s) FROM cn_ty", c, c))
	}
	for _, s := range []string{
		"SELECT id FROM cn_ty WHERE e < 'happy' ORDER BY id", "SELECT id FROM cn_ty WHERE e > 'ok' ORDER BY id", "SELECT id FROM cn_ty WHERE e = 'sad' ORDER BY id", "SELECT id FROM cn_ty WHERE e BETWEEN 'ok' AND 'Abc' ORDER BY id",
		"SELECT id FROM cn_ty WHERE mn > '9.00' ORDER BY id", "SELECT id FROM cn_ty WHERE mn < '0' ORDER BY id", "SELECT id FROM cn_ty WHERE mn = '10.00' ORDER BY id", "SELECT id FROM cn_ty WHERE mn BETWEEN '-6' AND '10' ORDER BY id",
		"SELECT id FROM cn_ty WHERE ip > '10.0.0.2' ORDER BY id", "SELECT id FROM cn_ty WHERE ip < '10.0.0.10' ORDER BY id", "SELECT id FROM cn_ty WHERE ip = '9.0.0.1' ORDER BY id", "SELECT id FROM cn_ty WHERE ip << '10.0.0.0/8' ORDER BY id",
		"SELECT id FROM cn_ty WHERE big > 100 ORDER BY id", "SELECT id FROM cn_ty WHERE big < 0 ORDER BY id", "SELECT id FROM cn_ty WHERE big = 99999999999999999999999999999999999999999 ORDER BY id", "SELECT id FROM cn_ty WHERE big > 99999999999999999999999999999999999999998 ORDER BY id",
		"SELECT id FROM cn_ty WHERE e > 'ok' AND c = 'a' ORDER BY id", "SELECT id FROM cn_ty WHERE mn > '0' AND c = 'a  ' ORDER BY id", "SELECT id FROM cn_ty WHERE big > 10 AND c <> 'ab' ORDER BY id", "SELECT id FROM cn_ty WHERE ip > '10.0.0.1' AND c IN ('a', 'ab') ORDER BY id",
	} {
		q("text-on-copy types", s)
	}

	// The char(n) key after update, delete and reinsert.
	for _, s := range []string{
		"SELECT k, n, note FROM cn_pk ORDER BY k, n", "SELECT count(*), count(DISTINCT k) FROM cn_pk", "SELECT k, count(*) FROM cn_pk GROUP BY k HAVING count(*) > 1 ORDER BY k", "SELECT n FROM cn_pk ORDER BY n", "SELECT sum(n) FROM cn_pk", "SELECT count(*) FROM (SELECT * FROM cn_pk) s", "WITH w AS (SELECT * FROM cn_pk) SELECT n FROM w ORDER BY n",
	} {
		q("key after changes", s)
	}
	for _, k := range []string{"ab", "ab  ", "ab   ", "cd", "cz", "cz  ", "ef", "gh", "gh ", "ij", "ij   ", "new", "new  ", "ñu", "ñu  ", "é", "x y", "x y  ", "", "   ", "st", "st  "} {
		l := cnLit(k)
		q("key after changes", "SELECT n, note FROM cn_pk WHERE k = "+l+" ORDER BY n")
		q("key after changes", "SELECT count(*) FROM cn_pk WHERE k = "+l)
		q("key after changes", "SELECT n FROM cn_pk WHERE k IN ("+l+", 'zz') ORDER BY n")
		q("key after changes", "SELECT n FROM (SELECT * FROM cn_pk) s WHERE s.k = "+l+" ORDER BY n")
		add(cnCase{group: "key after changes", sql: "SELECT n FROM cn_pk WHERE k = $1 ORDER BY n", args: [][]byte{[]byte(k)}, oids: []uint32{1042}})
	}

	// The four reports, on the schema HammerDB builds.
	for _, r := range cnReports() {
		q("reports", r)
	}

	// Measured on PostgreSQL 16 as giving other rows once the padding is gone.
	for _, w := range []string{
		`c LIKE 'ab\%'`, `c LIKE 'ab#%' ESCAPE '#'`, `c ILIKE 'AB\%'`, `c NOT LIKE 'ab\%'`, `c LIKE 'a%b'`, `c LIKE 'a_%'`,
		`c = E'ab\x20'`, `c = U&'ab\0020'`, `c = $x$ab  $x$`, `c IN ('x', E'ab\x20')`,
		`(c, 1) = ('ab  ', 1)`, `(c, 1) IN (('ab  ', 1))`, `c = ANY('{"ab  "}')`, `NULLIF(c, 'ab  ') IS NULL`, `CASE c WHEN 'ab  ' THEN true ELSE false END`, `COALESCE(c, 'x') = 'ab  '`,
		`c = v`, `c IN (SELECT v FROM cn_b)`, `c = COALESCE(v, '')`, `c = (SELECT 'ab  '::varchar)`,
	} {
		q("padding-sensitive", "SELECT id FROM cn_a WHERE "+w+" ORDER BY id")
		q("padding-sensitive", "SELECT id FROM (SELECT * FROM cn_a) s WHERE "+w+" ORDER BY id")
	}
	// These must stay equal, and should reach the copy.
	for _, w := range []string{"c LIKE 'ab%'", "c LIKE 'a%b%'", "c LIKE '%'", "c = v::text", "position('a' in c) = 1", "substring(c from 'a.') = 'ab'"} {
		add(cnCase{group: "should reach the copy", sql: "SELECT id FROM cn_a WHERE " + w + " ORDER BY id", wantCopy: true})
	}
	for _, e := range []string{"c || 1", "1 || c", "c || c", "length(c)", "char_length(c)", "position('a' in c)", "substring(c from 'a.')"} {
		add(cnCase{group: "should reach the copy", sql: "SELECT id, " + e + " FROM cn_a ORDER BY id", wantCopy: true})
	}

	// Where the screen and the view can disagree on what a column is: two
	// names that differ only in case, a type changed after the snapshot, a
	// table whose snapshot records no shape.
	for _, tb := range []string{"cn_case", "cn_alt1", "cn_alt2", "cn_noshape"} {
		for _, tm := range []string{
			"SELECT id FROM %T WHERE c = 'a' ORDER BY id", "SELECT id FROM %T WHERE c = 'a  ' ORDER BY id", "SELECT id FROM %T WHERE c <> 'a' ORDER BY id", "SELECT id FROM %T WHERE c IN ('a', 'ab') ORDER BY id", "SELECT id FROM %T WHERE c LIKE 'a%' ORDER BY id", "SELECT id, c FROM %T ORDER BY id", "SELECT c, count(*) FROM %T GROUP BY 1 ORDER BY 2, 1", "SELECT id, c || '|' FROM %T ORDER BY id", "SELECT id, length(c) FROM %T ORDER BY id", "SELECT id FROM %T ORDER BY c, id", "SELECT a.id, b.id FROM %T a JOIN cn_a b ON a.c = b.c ORDER BY 1, 2", "SELECT id FROM (SELECT * FROM %T) s WHERE c = 'a' ORDER BY id",
		} {
			q("screen against view", strings.ReplaceAll(tm, "%T", tb))
		}
	}
	for _, s := range []string{`SELECT id FROM cn_case WHERE "C" = 'a' ORDER BY id`, `SELECT id FROM cn_case WHERE "C" = 'a  ' ORDER BY id`, `SELECT id FROM cn_case WHERE "c" = 'a  ' ORDER BY id`, `SELECT id, "C", c FROM cn_case ORDER BY id`, `SELECT id, "C" || '|', c || '|' FROM cn_case ORDER BY id`, `SELECT id FROM cn_case WHERE "C" = c ORDER BY id`, `SELECT * FROM cn_case ORDER BY id`} {
		q("screen against view", s)
	}
	// A domain beside a char(n): the domain alone must keep it off the copy.
	for _, w := range []string{"d1 = 'a' AND c = 'a'", "d2 = 'a' AND c = 'a'", "c = 'a' AND d1 = 'a  '", "d1 = c", "d2 = c", "d1 IN ('a') OR c = 'z'"} {
		q("domain", "SELECT id FROM cn_dm WHERE "+w+" ORDER BY id")
		q("domain", "SELECT id FROM (SELECT * FROM cn_dm) s WHERE "+w+" ORDER BY id")
	}
	// A pattern or literal PostgreSQL joins before it compares.
	for _, w := range []string{"c LIKE 'a%' || 'b'", "c LIKE 'a' || '%'", "c = 'ab' || '  '", "c = 'ab'\n'  '", "c LIKE 'a%'\n'b'", "c = 'a'\n  \n'  '", "c IN ('x', 'ab'\n'  ')", "c LIKE 'ab' \n '%'"} {
		q("padding-sensitive", "SELECT id FROM cn_a WHERE "+w+" ORDER BY id")
		q("padding-sensitive", "SELECT id FROM (SELECT * FROM cn_a) s WHERE "+w+" ORDER BY id")
	}
	// Only ASCII spaces are padding: a tab and Unicode spaces are data.
	for _, l := range []string{"a\t", "a\u00a0", "a\u3000", "a \t", "a\t ", "a\u00a0 ", "a\u3000  "} {
		for _, op := range []string{"=", "<>", "<", ">="} {
			q("other blanks", "SELECT id FROM cn_a WHERE c "+op+" "+cnLit(l)+" ORDER BY id")
		}
		q("other blanks", "SELECT id FROM cn_a WHERE c IN ("+cnLit(l)+", 'z') ORDER BY id")
		q("other blanks", "SELECT id FROM cn_a WHERE c LIKE "+cnLit(l+"%")+" ORDER BY id")
		add(cnCase{group: "other blanks", sql: "SELECT id FROM cn_a WHERE c = $1 ORDER BY id", args: [][]byte{[]byte(l)}, oids: []uint32{1042}})
	}
	for _, s := range []string{"SELECT id, c FROM cn_a WHERE c LIKE 'a_' ORDER BY id", "SELECT c, count(*) FROM cn_a GROUP BY 1 ORDER BY 2, 1", "SELECT id, length(c), octet_length(c), c || '|' FROM cn_a ORDER BY id", "SELECT a.id, b.id FROM cn_a a JOIN cn_b b ON a.c = b.t ORDER BY 1, 2", "SELECT DISTINCT c FROM cn_a ORDER BY 1"} {
		q("other blanks", s)
	}
	// Which rows a sort chooses, on this collation.
	for _, n := range []string{"1", "3", "7", "12"} {
		for _, tm := range []string{"SELECT id FROM (SELECT id FROM cn_a ORDER BY c, id LIMIT %N) s ORDER BY id", "SELECT id FROM (SELECT id FROM cn_a ORDER BY c DESC, id LIMIT %N) s ORDER BY id", "SELECT id FROM (SELECT * FROM cn_a ORDER BY c, id LIMIT %N) s ORDER BY id", "SELECT id FROM (SELECT id, row_number() OVER (ORDER BY c, id) AS r FROM cn_a) s WHERE r <= %N ORDER BY id", "SELECT id FROM (SELECT id, rank() OVER (ORDER BY c) AS r FROM cn_a) s WHERE r <= %N ORDER BY id", "WITH w AS (SELECT id, c AS x FROM cn_a ORDER BY c, id LIMIT %N) SELECT id FROM w ORDER BY id", "SELECT id FROM cn_a ORDER BY c, id LIMIT %N", "SELECT id FROM cn_a ORDER BY c, id LIMIT %N OFFSET 2"} {
			q("rows chosen by a sort", strings.ReplaceAll(tm, "%N", n))
		}
	}
	// char(n) to varchar through a subquery alias.
	for _, s := range []string{"SELECT s.id, b.id FROM (SELECT id, c AS x FROM cn_a) s JOIN cn_b b ON s.x = b.v ORDER BY 1, 2", "SELECT s.id, b.id FROM (SELECT id, c AS x FROM cn_a) s JOIN cn_b b ON b.v = s.x ORDER BY 1, 2", "SELECT s.id, b.id FROM (SELECT id, c AS x FROM cn_a) s LEFT JOIN cn_b b ON s.x = b.v ORDER BY 1, 2 NULLS FIRST", "SELECT s.id, b.id FROM (SELECT id, c AS x FROM cn_a) s JOIN (SELECT id, v AS y FROM cn_b) b ON s.x = b.y ORDER BY 1, 2", "WITH s AS (SELECT id, c AS x FROM cn_a) SELECT s.id, b.id FROM s JOIN cn_b b ON s.x = b.v ORDER BY 1, 2", "SELECT s.id, b.id FROM (SELECT id, y AS x FROM (SELECT id, c AS y FROM cn_a) i) s JOIN cn_b b ON s.x = b.t ORDER BY 1, 2"} {
		q("join", s)
	}
	for _, c := range cnStoredCases() {
		add(c)
	}
	for i := range out {
		if out[i].group == "domain" {
			out[i].noCopy = true
		}
	}
	return out
}

// cnReports are the reports over the TPROC-C schema that read char(n) tables
// (top customers per district, churn, order value deciles, item demand vs
// stock), and variants of the first that use its char(n) columns.
func cnReports() []string {
	const win = "o_entry_d >= TIMESTAMP '2026-03-01' AND o_entry_d < TIMESTAMP '2026-03-21'"
	q07 := `WITH spend AS (
  SELECT o.o_w_id AS w_id, o.o_d_id AS d_id, o.o_c_id AS c_id,
         SUM(CASE WHEN ol.ol_number = 1 THEN 1 ELSE 0 END) AS orders, SUM(ol.ol_amount) AS amount
  FROM orders o JOIN order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id
  WHERE o.o_w_id BETWEEN 1 AND 3 AND o.` + win + `
  GROUP BY o.o_w_id, o.o_d_id, o.o_c_id
), ranked AS (
  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.w_id, s.d_id ORDER BY s.amount DESC, s.c_id) AS rnk,
         SUM(s.amount) OVER (PARTITION BY s.w_id, s.d_id) AS district_amount
  FROM spend s
)
SELECT r.w_id, r.d_id, r.rnk, r.c_id, c.c_first, c.c_last, c.c_credit, r.orders, r.amount,
       ROUND(100.0 * r.amount / NULLIF(r.district_amount, 0), 2) AS pct_of_district
FROM ranked r JOIN customer c ON c.c_w_id = r.w_id AND c.c_d_id = r.d_id AND c.c_id = r.c_id
WHERE r.rnk <= 3 ORDER BY r.w_id, r.d_id, r.rnk LIMIT 500`
	q08 := `WITH early AS (
  SELECT o_w_id AS w_id, o_d_id AS d_id, o_c_id AS c_id, COUNT(*) AS orders
  FROM orders WHERE o_w_id BETWEEN 1 AND 3 AND o_entry_d >= TIMESTAMP '2026-03-01' AND o_entry_d < TIMESTAMP '2026-03-10'
  GROUP BY o_w_id, o_d_id, o_c_id
), flagged AS (
  SELECT e.*, CASE WHEN EXISTS (SELECT 1 FROM orders o2 WHERE o2.o_w_id = e.w_id AND o2.o_d_id = e.d_id AND o2.o_c_id = e.c_id
      AND o2.o_entry_d >= TIMESTAMP '2026-03-10' AND o2.o_entry_d < TIMESTAMP '2026-03-21') THEN 0 ELSE 1 END AS churned
  FROM early e
)
SELECT f.w_id, COUNT(*) AS early_customers, SUM(f.churned) AS churned, ROUND(100.0 * SUM(f.churned) / COUNT(*), 2) AS churn_pct,
       ROUND(AVG(CASE WHEN f.churned = 1 THEN f.orders END), 2) AS early_orders_churned,
       ROUND(AVG(CASE WHEN f.churned = 0 THEN f.orders END), 2) AS early_orders_retained,
       ROUND(AVG(CASE WHEN f.churned = 1 THEN c.c_balance END), 2) AS avg_balance_churned,
       ROUND(AVG(CASE WHEN f.churned = 0 THEN c.c_balance END), 2) AS avg_balance_retained
FROM flagged f JOIN customer c ON c.c_w_id = f.w_id AND c.c_d_id = f.d_id AND c.c_id = f.c_id
GROUP BY f.w_id ORDER BY churn_pct DESC, f.w_id LIMIT 20`
	q09 := `WITH order_totals AS (
  SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, o.o_all_local, SUM(ol.ol_amount) AS total
  FROM orders o JOIN order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id
  WHERE o.o_w_id BETWEEN 1 AND 3 AND o.` + win + `
  GROUP BY o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, o.o_all_local
), deciles AS (
  SELECT t.*, NTILE(10) OVER (ORDER BY t.total, t.o_w_id, t.o_d_id, t.o_id) AS decile FROM order_totals t
)
SELECT decile, COUNT(*) AS orders, MIN(total) AS min_value, MAX(total) AS max_value, ROUND(AVG(total), 2) AS avg_value,
       ROUND(AVG(o_ol_cnt), 2) AS avg_lines, ROUND(100.0 * SUM(total) / NULLIF(SUM(SUM(total)) OVER (), 0), 2) AS pct_of_revenue,
       ROUND(100.0 * SUM(CASE WHEN o_all_local = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_remote
FROM deciles GROUP BY decile ORDER BY decile`
	q14 := `WITH demand AS (
  SELECT ol_i_id AS i_id, SUM(ol_quantity) AS units, COUNT(*) AS order_lines,
         SUM(CASE WHEN ol_delivery_d IS NULL THEN ol_quantity ELSE 0 END) AS pending_units
  FROM order_line WHERE ol_quantity >= 1 GROUP BY ol_i_id
), supply AS (
  SELECT s_i_id AS i_id, SUM(s_quantity) AS on_hand, SUM(s_ytd) AS shipped_ytd,
         SUM(CASE WHEN s_data LIKE '%ORIGINAL%' THEN 1 ELSE 0 END) AS brand_warehouses
  FROM stock GROUP BY s_i_id
)
SELECT i.i_id, i.i_name, i.i_price, d.units, d.order_lines, d.pending_units, s.on_hand, s.shipped_ytd, s.brand_warehouses,
       ROUND(1.0 * s.on_hand / d.units, 3) AS cover_ratio
FROM demand d JOIN supply s ON s.i_id = d.i_id JOIN item i ON i.i_id = d.i_id
ORDER BY cover_ratio, d.units DESC, i.i_id LIMIT 50`
	return []string{q07, q08, q09, q14,
		`SELECT c_credit, count(*), sum(c_balance) FROM customer GROUP BY c_credit ORDER BY c_credit`,
		`SELECT c_id FROM customer WHERE c_credit = 'G' AND c_w_id = 1 AND c_d_id = 1 ORDER BY c_id`,
		`SELECT c_id FROM customer WHERE c_credit = 'G ' AND c_w_id = 1 AND c_d_id = 1 ORDER BY c_id`,
		`SELECT c_state, count(*) FROM customer GROUP BY 1 ORDER BY 1`,
		`SELECT c_w_id, c_d_id, c_id, c_middle || '.' FROM customer ORDER BY 1, 2, 3`,
		`SELECT ol_dist_info, count(*) FROM order_line GROUP BY 1 ORDER BY 1`,
		`SELECT count(*) FROM order_line WHERE ol_dist_info = 'x'`,
		`SELECT count(*) FROM order_line WHERE ol_dist_info LIKE '%x'`,
		`SELECT w.w_id, d.d_id FROM warehouse w JOIN district d ON d.d_w_id = w.w_id AND d.d_zip = w.w_zip ORDER BY 1, 2`,
		`SELECT count(*) FROM customer c JOIN warehouse w ON c.c_state = w.w_state`,
	}
}

// cnAnswer is what one side gave.
type cnAnswer struct {
	err    string
	fields []pgconn.FieldDescription
	rows   [][]*string
}

func cnRead(ctx context.Context, c *pgconn.PgConn, cs cnCase, extended bool) cnAnswer {
	ctx, cancel := context.WithTimeout(ctx, cnStatement)
	defer cancel()
	var fields []pgconn.FieldDescription
	var raw [][][]byte
	var err error
	if extended {
		r := c.ExecParams(ctx, cs.sql, cs.args, cs.oids, nil, nil).Read()
		fields, raw, err = r.FieldDescriptions, r.Rows, r.Err
	} else {
		var res []*pgconn.Result
		res, err = c.Exec(ctx, cs.sql).ReadAll()
		if err == nil && len(res) > 0 {
			l := res[len(res)-1]
			fields, raw = l.FieldDescriptions, l.Rows
		}
	}
	if err != nil {
		return cnAnswer{err: cnCode(err)}
	}
	a := cnAnswer{fields: fields}
	for _, r := range raw {
		row := make([]*string, len(r))
		for i, v := range r {
			if v != nil {
				s := string(v)
				row[i] = &s
			}
		}
		a.rows = append(a.rows, row)
	}
	return a
}

func cnCode(err error) string {
	if pe, ok := err.(*pgconn.PgError); ok {
		return pe.Code + " " + pe.Message
	}
	return err.Error()
}

// cnTolerated says, per output column, whether trailing spaces may differ.
func cnTolerated(src cnAnswer, asColumn bool) []bool {
	out := make([]bool, len(src.fields))
	const bpchar = 1042
	for i, f := range src.fields {
		out[i] = f.DataTypeOID == bpchar && (asColumn || f.TableOID != 0 && f.TableAttributeNumber > 0)
	}
	return out
}

func cnRowText(row []*string, tolerated []bool) string {
	var b strings.Builder
	for i, v := range row {
		if v == nil {
			b.WriteString("\x00NULL\x01")
			continue
		}
		s := *v
		if i < len(tolerated) && tolerated[i] {
			s = strings.TrimRight(s, " ")
		}
		b.WriteString(s)
		b.WriteByte(1)
	}
	return b.String()
}

// cnJudge compares the copy's answer with PostgreSQL's: "" when it is
// acceptable, what differs otherwise.
func cnJudge(cs cnCase, src, cp cnAnswer) string {
	if src.err != "" {
		return "PostgreSQL refuses it (" + cnClip(src.err, 70) + ") and the copy answered"
	}
	// The client library fills the columns only from a result with rows.
	if len(src.rows) > 0 && len(cp.rows) > 0 && len(src.fields) != len(cp.fields) {
		return fmt.Sprintf("%d columns on PostgreSQL, %d on the copy", len(src.fields), len(cp.fields))
	}
	tol := cnTolerated(src, cs.asColumn)
	text := func(rows [][]*string) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = cnRowText(r, tol)
		}
		return out
	}
	a, b := text(src.rows), text(cp.rows)
	ordered := OrderedAtTop(cs.sql)
	if !ordered {
		sort.Strings(a)
		sort.Strings(b)
	}
	if len(a) != len(b) {
		return fmt.Sprintf("%d rows on PostgreSQL, %d on the copy; PostgreSQL %s; copy %s", len(a), len(b), cnShow(src.rows), cnShow(cp.rows))
	}
	for i := range a {
		if a[i] != b[i] {
			what := "row"
			if ordered {
				sa, sb := append([]string{}, a...), append([]string{}, b...)
				sort.Strings(sa)
				sort.Strings(sb)
				if strings.Join(sa, "\x02") == strings.Join(sb, "\x02") {
					what = "order"
				}
			}
			return fmt.Sprintf("%s differs at %d; PostgreSQL %s; copy %s", what, i, cnShow(src.rows), cnShow(cp.rows))
		}
	}
	return ""
}

func cnShow(rows [][]*string) string {
	var b strings.Builder
	for i, r := range rows {
		if i == 4 {
			fmt.Fprintf(&b, " ... (%d rows)", len(rows))
			break
		}
		b.WriteByte('[')
		for j, v := range r {
			if j > 0 {
				b.WriteByte('|')
			}
			if v == nil {
				b.WriteString("NULL")
			} else {
				fmt.Fprintf(&b, "%q", cnClip(*v, 24))
			}
		}
		b.WriteByte(']')
	}
	return b.String()
}

func cnClip(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n]) + "..."
	}
	return s
}

// cnSide is the side the port says answered the connection's last statement.
func cnSide(ctx context.Context, c *pgconn.PgConn) string {
	res, err := c.Exec(ctx, "SHOW dbtrail.router_status").ReadAll()
	if err != nil || len(res) == 0 {
		return "unknown"
	}
	for _, r := range res[0].Rows {
		if len(r) == 2 && string(r[0]) == "last_answer" {
			return string(r[1])
		}
	}
	return "unknown"
}

// cnStack is the real daemon and router over a database of their own.
type cnStack struct {
	t       *testing.T
	bin     string
	dir     string
	env     []string
	admin   *pgconn.PgConn
	source  *pgconn.PgConn
	db      string
	dsn     string
	host    string
	port    string
	user    string
	pass    string
	fwd     string
	portDSN string
	procs   []*exec.Cmd
}

func cnFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// cli runs one command of the real binary against the running daemon.
func (s *cnStack) cli(ctx context.Context, stdin string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, s.bin, args...)
	cmd.Dir, cmd.Env = s.dir, s.env
	cmd.Stdin = strings.NewReader(stdin)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

// start runs a long-lived command, with its log in the stack's directory.
func (s *cnStack) start(name string, args ...string) {
	s.t.Helper()
	log, err := os.Create(filepath.Join(s.dir, name+".log"))
	if err != nil {
		s.t.Fatal(err)
	}
	cmd := exec.Command(s.bin, args...)
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = s.dir, s.env, log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		s.t.Fatalf("start %s: %v", name, err)
	}
	s.procs = append(s.procs, cmd)
	s.t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
		_ = log.Close()
	})
}

func (s *cnStack) fullLog(name string) string {
	b, _ := os.ReadFile(filepath.Join(s.dir, name+".log"))
	return string(b)
}

func (s *cnStack) logTail(name string) string {
	b, _ := os.ReadFile(filepath.Join(s.dir, name+".log"))
	if len(b) > 1800 {
		b = b[len(b)-1800:]
	}
	return string(b)
}

func cnExec(t *testing.T, ctx context.Context, c *pgconn.PgConn, stmt string) {
	t.Helper()
	if _, err := c.Exec(ctx, stmt).ReadAll(); err != nil {
		t.Fatalf("source: %s: %v", cnClip(stmt, 120), err)
	}
}

func cnWaitFor(t *testing.T, what string, d time.Duration, ok func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(d)
	last := ""
	for time.Now().Before(deadline) {
		var done bool
		if done, last = ok(); done {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("%s: not after %v (last: %s)", what, d, cnClip(last, 600))
}

// cnStart makes the database, seeds it, and starts the daemon and the router.
func cnStart(t *testing.T, ctx context.Context) *cnStack {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	baseDSN := testutil.SkipIfNoPostgres(t)
	cfg, err := pgconn.ParseConfig(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close(context.Background()) })
	id := fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano()%1000000)
	s := &cnStack{t: t, dir: t.TempDir(), admin: admin, db: "charn_bat_" + id, fwd: "charn_fwd_" + id,
		host: cfg.Host, port: fmt.Sprint(cfg.Port), user: cfg.User, pass: cfg.Password}

	// The cleanups run last to first: this one after the processes stopped.
	t.Cleanup(func() {
		bg := context.Background()
		for range 40 {
			r, err := admin.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = '"+s.db+"' AND NOT active").ReadAll()
			_ = r
			left, _ := admin.Exec(bg, "SELECT 1 FROM pg_replication_slots WHERE database = '"+s.db+"'").ReadAll()
			if err == nil && (len(left) == 0 || len(left[0].Rows) == 0) {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		_, _ = admin.Exec(bg, "DROP DATABASE IF EXISTS "+s.db+" WITH (FORCE)").ReadAll()
		_, _ = admin.Exec(bg, "DROP ROLE IF EXISTS "+s.fwd).ReadAll()
	})
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+s.db+" TEMPLATE template0 ENCODING 'UTF8' LOCALE 'en_US.utf8'").ReadAll(); err != nil {
		t.Fatalf("a database with a non-C collation: %v", err)
	}
	cnExec(t, ctx, admin, "CREATE ROLE "+s.fwd+" LOGIN PASSWORD '"+cnFwdPass+"'")

	u := url.URL{Scheme: "postgres", User: url.UserPassword(cfg.User, cfg.Password), Host: net.JoinHostPort(cfg.Host, s.port), Path: "/" + s.db, RawQuery: "sslmode=disable"}
	s.dsn = u.String()
	if s.source, err = pgconn.Connect(ctx, s.dsn); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.source.Close(context.Background()) })
	cnExec(t, ctx, s.source, "SET TimeZone = 'UTC'")
	for _, stmt := range cnSeed() {
		cnExec(t, ctx, s.source, stmt)
	}
	tables := cnTables()
	for _, tb := range tables {
		cnExec(t, ctx, s.source, "ALTER TABLE "+tb+" REPLICA IDENTITY FULL")
	}
	cnExec(t, ctx, s.source, "CREATE PUBLICATION "+cnPub+" FOR TABLE "+strings.Join(tables, ", "))
	cnExec(t, ctx, s.source, "GRANT SELECT ON ALL TABLES IN SCHEMA public TO "+s.fwd)
	cnExec(t, ctx, s.source, "ANALYZE")

	// The real binary, built from this tree.
	s.bin = filepath.Join(s.dir, "bintrail-console")
	build := exec.CommandContext(ctx, "go", "build", "-o", s.bin, "github.com/dbtrail/dbtrail/cmd/bintrail-console")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the console: %v\n%s", err, out)
	}

	_, indexDB := testutil.CreateTestDB(t)
	servers, auth, mcp := filepath.Join(s.dir, "console-servers.yaml"), filepath.Join(s.dir, "console-auth.yaml"), filepath.Join(s.dir, "console-mcp-token.yaml")
	base := []string{}
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); strings.HasPrefix(k, "BINTRAIL_") || k == "HOME" {
			continue
		}
		base = append(base, kv)
	}
	s.env = append(base, "HOME="+s.dir, "BINTRAIL_CONSOLE_AUTH="+auth, "BINTRAIL_CONSOLE_SERVERS="+servers, "BINTRAIL_CONSOLE_MCP_TOKEN_FILE="+mcp, "BINTRAIL_CONSOLE_BASELINE_TRIGGER=1")
	portFile := "version: 1\nenabled: true\npassword: " + cnToken + "\n"
	if err := os.WriteFile(filepath.Join(s.dir, "console-mysql-port.yaml"), []byte(portFile), 0o600); err != nil {
		t.Fatal(err)
	}
	// The index database the daemon made for this source, once it has stopped.
	t.Cleanup(func() {
		m := regexp.MustCompile(`index_database=(bintrail_idx_[0-9a-f]+)`).FindStringSubmatch(s.fullLog("watch"))
		if m == nil {
			return
		}
		if db, err := sql.Open("mysql", testutil.IntegrationDSN(indexDB)); err == nil {
			_, _ = db.Exec("DROP DATABASE IF EXISTS " + m[1])
			_ = db.Close()
		}
	})
	s.start("watch", "watch", "--index-dsn", testutil.IntegrationDSN(indexDB), "--console-listen", cnFreeAddr(t),
		"--console-servers-file", servers, "--console-auth-file", auth, "--console-mcp-token-file", mcp,
		"--baseline-dir", filepath.Join(s.dir, "baselines"), "--skip-doctor", "--ssl-mode", "disabled")
	cnWaitFor(t, "the daemon", time.Minute, func() (bool, string) {
		out, err := s.cli(ctx, "", "server", "list")
		return err == nil, out + s.logTail("watch")
	})
	if out, err := s.cli(ctx, s.pass+"\n", "server", "add", "--name", cnServer, "--flavor", "postgres", "-h", s.host, "-P", s.port, "-u", s.user,
		"--password-stdin", "-d", s.db, "--publication", cnPub, "--slot", cnSlot, "--schemas", "public", "--no-forward"); err != nil {
		t.Fatalf("server add: %v\n%s\n%s", err, out, s.logTail("watch"))
	}
	if out, err := s.cli(ctx, "", "snapshot", "run", cnServer); err != nil {
		t.Fatalf("snapshot run: %v\n%s\n%s", err, out, s.logTail("watch"))
	}
	cnStripShape(t, ctx, s.dir, "cn_noshape")
	if out, err := s.cli(ctx, "", "port", "mode", cnServer, "fresh"); err != nil {
		t.Fatalf("port mode: %v\n%s", err, out)
	}

	addr := cnFreeAddr(t)
	s.start("router", "pg-router", "--listen", addr, "--servers-file", servers, "--auth-file", auth, "--mcp-token-file", mcp,
		"--workers", "2", "--threads", "2", "--cost-threshold", "0.01", "--scan-rows", "1")
	host, port, _ := net.SplitHostPort(addr)
	s.portDSN = fmt.Sprintf("host=%s port=%s sslmode=disable user=anyone password=%s dbname=%s", host, port, cnToken, cnServer)
	cnWaitFor(t, "the port", time.Minute, func() (bool, string) {
		c, err := pgconn.Connect(ctx, s.portDSN)
		if err != nil {
			return false, err.Error() + s.logTail("router")
		}
		defer c.Close(ctx)
		_, err = c.Exec(ctx, "SELECT count(*) FROM cn_a").ReadAll()
		return err == nil, fmt.Sprint(err)
	})

	// After the snapshot: answered from the recent changes laid over it.
	for _, stmt := range cnChanges() {
		cnExec(t, ctx, s.source, stmt)
	}
	cnExec(t, ctx, s.source, "INSERT INTO cn_mark VALUES (1)")
	c, err := pgconn.Connect(ctx, s.portDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	cnWaitFor(t, "the recent changes on the copy", cnWait, func() (bool, string) {
		res, err := c.Exec(ctx, "SELECT count(*) FROM cn_mark").ReadAll()
		if err != nil || len(res) == 0 || len(res[0].Rows) != 1 {
			return false, fmt.Sprint(err)
		}
		return string(res[0].Rows[0][0]) == "1", string(res[0].Rows[0][0])
	})
	// A column type changed after the snapshot, both ways.
	cnExec(t, ctx, s.source, "ALTER TABLE cn_alt1 ALTER c TYPE text")
	cnExec(t, ctx, s.source, "ALTER TABLE cn_alt2 ALTER c TYPE char(5)")
	return s
}

// cnTally is what one run of the battery found.
type cnTally struct {
	total, copy, source, refused int
	wanted, wantedOn             int
	byGroup                      map[string]*[3]int // statements, on the copy, wrong
}

type cnFailure struct {
	phase, proto, group, key, why string
	// class: A a wrong answer with no notice, A+ one with a notice beside it,
	// B a visible error, or a right answer that should have gone to the source.
	class, notice string
}

func cnRun(t *testing.T, ctx context.Context, s *cnStack, phase string, cases []cnCase, extended bool, fails *[]cnFailure) cnTally {
	t.Helper()
	proto := "simple"
	if extended {
		proto = "extended"
	}
	tl := cnTally{byGroup: map[string]*[3]int{}}
	var notices []string
	connect := func() (*pgconn.PgConn, error) {
		cfg, err := pgconn.ParseConfig(s.portDSN)
		if err != nil {
			return nil, err
		}
		cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n.Message) }
		return pgconn.ConnectConfig(ctx, cfg)
	}
	port, err := connect()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { port.Close(context.Background()) }()
	for _, cs := range cases {
		notices = nil
		if len(cs.args) > 0 && !extended {
			continue
		}
		g := tl.byGroup[cs.group]
		if g == nil {
			g = &[3]int{}
			tl.byGroup[cs.group] = g
		}
		tl.total++
		g[0]++
		src := cnRead(ctx, s.source, cs, extended)
		if s.source.IsClosed() {
			if s.source, err = pgconn.Connect(ctx, s.dsn); err != nil {
				t.Fatal(err)
			}
			cnExec(t, ctx, s.source, "SET TimeZone = 'UTC'")
		}
		got := cnRead(ctx, port, cs, extended)
		if port.IsClosed() {
			if port, err = connect(); err != nil {
				t.Fatal(err)
			}
			tl.refused++
			continue
		}
		side := cnSide(ctx, port)
		switch {
		case got.err != "" || side == "refused":
			tl.refused++
		case side == "source":
			tl.source++
		case side == "copy":
			tl.copy++
			g[1]++
			why := cnJudge(cs, src, got)
			class, said := "A", ""
			if src.err != "" {
				class = "A, PostgreSQL refuses it"
			}
			if len(notices) > 0 {
				class, said = class+", with a notice", cnClip(notices[0], 120)
			}
			if why == "" && cs.noCopy {
				why, class = "a domain over char(n) was answered by the copy, with the right rows; it must go to the source", "B"
			}
			if why != "" {
				g[2]++
				*fails = append(*fails, cnFailure{phase: phase, proto: proto, group: cs.group, key: cs.key(), why: why, class: class, notice: said})
			}
		default:
			tl.refused++
		}
		if cs.wantCopy {
			tl.wanted++
			if got.err == "" && side == "copy" {
				tl.wantedOn++
			}
		}
	}
	return tl
}

// cnStripShape rewrites a table's snapshot files without their metadata, as
// a snapshot that records no shape for the table.
func cnStripShape(t *testing.T, ctx context.Context, dir, table string) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	n := 0
	err = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") || !strings.Contains(p, table) {
			return err
		}
		tmp := p + ".strip"
		q := strings.ReplaceAll(p, "'", "''")
		if _, err := db.ExecContext(ctx, fmt.Sprintf("COPY (SELECT * FROM read_parquet('%s')) TO '%s' (FORMAT parquet)", q, strings.ReplaceAll(tmp, "'", "''"))); err != nil {
			return err
		}
		n++
		return os.Rename(tmp, p)
	})
	if err != nil || n == 0 {
		t.Fatalf("strip the shape of %s: %d files, %v", table, n, err)
	}
}

// cnDescribeThenExecute prepares and describes a statement while the copy
// can answer it, stops capture so that the copy cannot reach what the source
// showed this connection, and binds and executes it twice, in text and in
// binary results: it is described on one side and executed on the other.
// PostgreSQL's rows or a clean refusal are acceptable; a 0A000 for a statement
// that works directly on PostgreSQL is not.
func cnDescribeThenExecute(t *testing.T, ctx context.Context, s *cnStack, fails *[]cnFailure) (runs, moved, refused int) {
	t.Helper()
	watch := s.procs[0].Process.Pid
	defer func() { _ = syscall.Kill(-watch, syscall.SIGCONT) }()
	next := 100
	for _, st := range []struct {
		sql  string
		args [][]byte
	}{
		{"SELECT id, c FROM cn_cp WHERE id <= 4 ORDER BY id", nil},
		{"SELECT id, c FROM cn_cp WHERE id <= $1::int ORDER BY id", [][]byte{[]byte("4")}},
		{"SELECT c, id FROM (SELECT * FROM cn_cp) s WHERE id <= 4 ORDER BY id", nil},
	} {
		for _, format := range []int16{0, 1} {
			var argFmt []int16
			if len(st.args) > 0 {
				argFmt = []int16{0}
			}
			port, err := pgconn.Connect(ctx, s.portDSN)
			if err != nil {
				t.Fatal(err)
			}
			key := fmt.Sprintf("%s  -- result format %d", st.sql, format)
			desc, err := port.Prepare(ctx, "described", st.sql, nil)
			if err != nil {
				port.Close(ctx)
				t.Fatalf("prepare %s: %v", st.sql, err)
			}
			// The side that described it shows in the type it announced for c:
			// bpchar is PostgreSQL's, text the copy's.
			describedOn := "source"
			for _, f := range desc.Fields {
				if f.Name == "c" && f.DataTypeOID != 1042 {
					describedOn = "copy"
				}
			}
			if err := syscall.Kill(-watch, syscall.SIGSTOP); err != nil {
				t.Fatal(err)
			}
			next++
			cnExec(t, ctx, s.source, fmt.Sprintf("INSERT INTO cn_cp VALUES (%d, 'n')", next))
			// A read the source answers: the copy must now reach it.
			_, _ = port.Exec(ctx, "SELECT now(), count(*) FROM cn_cp").ReadAll()
			want := s.source.ExecParams(ctx, st.sql, st.args, nil, argFmt, []int16{format}).Read()
			wrong := ""
			for range 2 {
				runs++
				run, stop := context.WithTimeout(ctx, cnStatement)
				got := port.ExecPrepared(run, "described", st.args, argFmt, []int16{format}).Read()
				stop()
				side := cnSide(ctx, port)
				if side == "source" && describedOn == "copy" {
					moved++
				}
				switch {
				case want.Err != nil:
				case got.Err != nil:
					code := cnCode(got.Err)
					if strings.HasPrefix(code, "0A000") {
						wrong = "described on the " + describedOn + ", executed on the " + side + ": " + cnClip(code, 120)
					} else {
						refused++
					}
				default:
					if d := cnSameBytes(want, got); d != "" {
						wrong = "described on the " + describedOn + ", executed on the " + side + ": " + d
					}
				}
			}
			_ = syscall.Kill(-watch, syscall.SIGCONT)
			port.Close(ctx)
			if wrong != "" {
				*fails = append(*fails, cnFailure{phase: "forwarding user", proto: "extended", group: "describe then execute", key: key, why: wrong, class: cnClassOf(wrong)})
			}
			want2 := next
			cnWaitFor(t, "capture back after the pause", cnWait, func() (bool, string) {
				c, err := pgconn.Connect(ctx, s.portDSN)
				if err != nil {
					return false, err.Error()
				}
				defer c.Close(ctx)
				res, err := c.Exec(ctx, fmt.Sprintf("SELECT count(*) FROM cn_cp WHERE id = %d", want2)).ReadAll()
				if err != nil || len(res) == 0 || len(res[0].Rows) != 1 {
					return false, fmt.Sprint(err)
				}
				return string(res[0].Rows[0][0]) == "1", "not yet"
			})
		}
	}
	return runs, moved, refused
}

// cnSameBytes compares two results cell by cell, trailing spaces aside in a
// char(n) column shown directly.
func cnSameBytes(want, got *pgconn.Result) string {
	if len(want.Rows) != len(got.Rows) {
		return fmt.Sprintf("%d rows on PostgreSQL, %d here", len(want.Rows), len(got.Rows))
	}
	for i := range want.Rows {
		if len(want.Rows[i]) != len(got.Rows[i]) {
			return "another number of columns"
		}
		for j := range want.Rows[i] {
			a, b := want.Rows[i][j], got.Rows[i][j]
			if j < len(want.FieldDescriptions) && want.FieldDescriptions[j].DataTypeOID == 1042 {
				a, b = bytes.TrimRight(a, " "), bytes.TrimRight(b, " ")
			}
			if (want.Rows[i][j] == nil) != (got.Rows[i][j] == nil) || !bytes.Equal(a, b) {
				return fmt.Sprintf("row %d column %d: %q on PostgreSQL, %q here", i, j, want.Rows[i][j], got.Rows[i][j])
			}
		}
	}
	return ""
}

// cnKnown says which failures main already had when this battery was
// written, and why, so that they are told from new ones without being hidden.
func cnKnown(f cnFailure) string {
	// Right after an ALTER, whether these reach the copy depends on timing.
	if !cnBaseline[cnHash(f)] && !strings.Contains(f.key, "cn_alt") {
		return ""
	}
	switch f.group {
	case "output expression":
		return "a cut of a char(n) (substr, substring) keeps its padding"
	case "domain":
		return "#3233, a domain over char(n)"
	case "text-on-copy types":
		return "a type the copy holds as text, sorted or compared as letters"
	case "screen against view":
		return "a column type changed after the snapshot"
	case "parameter":
		return "an argument declared bpchar, compared with no table in the statement"
	case "describe then execute", "prepare in a transaction":
		return "described on one side and run on the other"
	}
	if strings.HasPrefix(f.group, "stored as text: ") {
		return "a built-in type the copy holds as text"
	}
	return "text sorted by bytes, with no forwarding user"
}

// cnClassOf classes a failure of a prepared statement: a 0A000 is an error
// the client sees, anything else a wrong answer.
func cnClassOf(why string) string {
	if strings.Contains(why, "0A000") {
		return "B"
	}
	return "A"
}

// cnHash identifies one failure exactly: a new failure in a known category
// is still new.
func cnHash(f cnFailure) string {
	h := fnv.New64a()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s", f.phase, f.proto, f.group, f.key)
	return fmt.Sprintf("%012x", h.Sum64()>>16)
}

// cnBaseline is what failed on main when the battery was written.
var cnBaseline = func() map[string]bool {
	m := map[string]bool{}
	for _, h := range strings.Fields(cnBaselineList) {
		m[h] = true
	}
	return m
}()

const cnBaselineList = `
00235c557f6a 003c2f481d61 004c789e51b0 00a3ebf2e644 00af923e2607 012bb6ed2cee 017d8ff6cc26 01ab79408e37
02069fbb38aa 0275ef2a2d5c 02df56b73e09 031e329d4c0d 0327b9337e35 032bddc8c7cc 034f1d919600 038647cb028d
03bdca45c045 03c2b8d8bdd5 049dd9331ea0 04ab2edb2ce5 04fe5497dcb0 051a260ac6b0 052c91f665c1 05bae07db478
05c7d576c3e3 0618b0689889 06b2bfa3afdf 06b797dbc869 06ca5ed95a39 076a3652cee5 07a3baed228d 07d8f8100322
07e263d43f3d 0867d3dedcf6 087b1abc2f92 088cbfd88676 08c41b7a7456 0935aecc9044 098c8572b9d4 09adf423cc62
09bf75d9a7bc 09cfcb824ab6 09d650e5c146 0a0b91c6a05f 0a21805c8fc7 0a9ca1b016d3 0aa9ffe48e6d 0b109e21d25e
0b7e50ecf2e7 0ba2a9323eaa 0ba758833ca7 0bf2054f1a42 0c04928dc4e5 0c456e4b96be 0c7812825333 0cb247a910c8
0cc104f26edc 0ccca3c97ad9 0d8e091cb331 0d8e82aedc63 0e1ed5bf82f3 0e5ac13ea6aa 0e8a4477addd 0f0cb534db1a
0fb272701ba3 0fceca7c61b1 0fd0d665d8cc 0fe3cffcfd7c 100b47165988 1066165ccde2 10b17a38cbc1 10c5238d162b
114a19d36e14 115bd5213e92 1177d646c76c 11d6845ba28e 120e06be8e9b 12246d7bd8df 12318ec71471 125c8b7d47a3
12bb5034df70 12dc9dd8b2cc 12f046ca8f0a 14368af033f8 144605619b7f 1453ccd56425 1500ce7981d0 1515cc8bd256
159b886e1abe 15c3bcb06d4c 162b2bfb78f9 162f3742a14e 1644d92880cf 1694f68a3356 169ad21e17b8 16a0a2da4d46
170d373d9c6e 173963bc4a87 1754b7de2420 17915b0cc663 17c7efd5b2fb 17ccb78e4513 17edcba84d76 18af39fc88d9
18b2b9f61cd6 18e0502f9f07 19013cc69ea5 1947da21ad91 195d6c459591 19852fda3dbc 19b37d3e5a77 1a21fe3a9d89
1a59e484c520 1a7496ce77f6 1a767563862f 1aa4a1c0ba73 1acdc5426286 1ad5c4a2fb7e 1b7a18008270 1bca00e759d4
1c061ff70e8c 1c4088b85132 1c83af0096ae 1cab1b8bc53a 1cd0b804b337 1ce9f9739d7b 1d01a4dbbad1 1d136b1e7d68
1d5076c0c4eb 1d8fd291a9a6 1dd6b0840701 1e0e5238b529 1e33fc551393 1e57ecea0a01 1e6fd30e8618 1eaa7db62b64
1f1978b6b768 1f2a8407f1de 1f44c6cf0969 1f5fafdf8e48 1f851acdc414 1f882a59dc35 1f8980e2f971 1f95b626c955
1fb77865bac8 200421875947 200fa7c3ed6e 2049cccab66b 20538aff137a 2073d1b8631f 214b636b42ae 21a8ac15c4d9
21a94c15862d 21ce23da33b1 220c4142795f 2210c2c65cb6 222e59e97924 2277f5e3c4d6 22a544819f7a 22b2f6dffeb9
22c4c1a6b60f 22c88b450b84 236ce0796024 23bafbeadb5b 23ed0a0de991 243988708cfa 243eb328741b 2454b5a63638
250c29eb385a 2518095e924a 251a13e3499c 254a3197c549 25708b4a1623 25812af50cdb 25db9f99b40e 26905ae0cc9b
26a065154fae 26a978f4ee20 27053562175c 271e3b6ef531 272a8dd08d5b 27405a1a3f7c 276a344cbc45 280612f2fda8
2840a1c80d71 28ab7fd3742a 28c0624451e7 28c413aa79bd 28d6b3c79bd8 29004b75a35e 2904756267ae 295930a15fb4
29b2e652066d 29e368f9dccd 29f905ec7210 2a737d896371 2ad595d69e51 2ade22215f48 2ae553421cc3 2af308937874
2b32c3590e13 2b3b3bb48715 2b517da5095f 2b67f91ef76a 2b812d07fe9f 2be989e99585 2c1a75c87bfe 2c2f7ceed0cd
2d357399c883 2d515bc9de69 2e09a7be5b01 2e6461169f7e 2f4ba783e8f2 2f790a809d6a 2f7e7daed3a9 2fad34936385
2fb6166f5239 307759d5c62b 30c020b488be 3101e458b189 3121e9bf680b 3125c4eb920b 31789966f9e3 32207408fb39
3278f3f4cf1f 329fc380b89b 32c691d70e75 335ee4a8501c 33ce513166f0 33e65b3cde93 343e00c1e740 349b6ddfa1d5
349f55c683ed 34cafbca5525 34ce2788d9a3 355373988e01 35d2ad07b944 3696dcb52e9b 36b39360faa8 37722f3cbb15
37d846bd3334 380ca6503de0 387fdbdf79b0 38a7706487e9 3901be1bc065 399452c2053f 39aafec90550 39ac07d1ab90
39e40a98422d 39e719d42219 3a694fae19a8 3a751ab337e1 3a80ac4b757c 3a913ed52fcd 3a91fc0ae62d 3a941356c7ea
3a95256b73c5 3aeff8897f61 3b590608ba86 3b8e2dbd2f36 3b9e6fa404ff 3baf4ef4c59c 3bb0f89f5314 3c41942750f4
3c4f97d31741 3c5c7dc5e6a0 3c8a7ab618f8 3c8ed1df6b51 3d5c677f3665 3d8e32dfd1b5 3da109455d4a 3e19cd29471d
3e94cbd9a194 3ebfce4f11d2 3ed0b56f59b8 3ee7d1635e55 3f12cc2d9e3a 3fb09f0c6cf6 3febda322b0c 3ff22b8fc5b5
4048ebff9bb2 407d27468a3b 4083261f3861 40ad24582dc2 40b36d5b94f3 418b59bfe36a 41a28f335c5d 420258ac8ebc
42b45600f412 43145ba9a992 433cb38abc3f 439a15c62d27 43bfc729a90f 43eb98220956 44165a0a265b 4427d55eb6f5
444cf37d0e6c 44815d560632 44d5cce34941 44de9d80f1e9 44f2ec394ff3 44f9c6a2742e 450a2b2fb63f 4532a4d0a555
45474bf80c6f 45564ee3da5a 456053f5ec85 45c26480f857 460efef79971 4700a1c9071f 470998aab7fa 470fb39c0638
471306caea48 473443384049 476ad94355ad 4777e32d0c53 478f2c950f26 47bfacc16bc3 47de23892496 48033cf21333
4807522d07c7 4810327bfbf0 487a090c4f2f 487ba06ebebe 4925b9385c31 49289681a2d3 49c52078efa4 4a1cdd588059
4aacf190c568 4aaf402b4fbf 4b2e928cafd7 4b9b6b75d37d 4bf29d52e561 4c0199cf968e 4c16c96ddea6 4c5d3bdd5194
4c6bbad9c398 4ccdec53ec96 4d2a7001cfee 4d4c22a8bb4b 4d863c82a591 4df409c27563 4dff09926eee 4e1b48e7979f
4e88bd8bfecf 4edef248365d 4f3d6e3176db 4f89594c30af 5016c9a3827f 504f0def5487 506a9a7ff3e8 50829f00e848
50cd8b6f332a 5127531ff49f 515de861b371 5164cf0c3429 5183b93acb7f 51a05c9ddbc5 51c023ace5be 51ea212f5b9d
5201264b50a3 521d05c722f5 5234477ac6aa 529dd51d1fd0 52a718bcf4fc 52cfa18b3a26 530705fde6cc 53251533f35c
532a07e3615e 5342fe1ff542 5380e96962d0 53c85c3cc40f 53fcd632f612 5404d907e45e 541815753d98 547dd41a78df
5544d6083008 55856849ab2c 558afbef68e5 5594e8ae5eae 55985e30dea1 55e77f549640 560441248052 56a0c6995762
56ab6cb946d8 56aeda8bc5b6 56bd0734921c 57372fe718db 5769df1c8d6b 57939bed16c4 57d90185231f 57f5c1d8aecc
58147fdea314 581c52551722 58262d68a5a6 58373b165a21 586c438c28f6 588ef492de53 58b5f87fe4a3 58d6a210f987
58e9015c4a1a 5903af1d8259 5917260b0e7a 591ad52a9dd4 592acfca0f9c 5931fe29dc69 59730f5a52af 59df73e046e2
5a7cf17aa1cb 5a83904fb77b 5af2560ff4ca 5b1b1d4941ab 5b1eb98f9c90 5b5f8ddf3ad0 5b7aeba04fc2 5c658e196b0a
5cb1bbf6d8af 5cc0b95d1431 5cdf778ecf27 5dbee6b7370b 5e5ef0e59c8b 5e9e7ba67c70 5ec3d8e61fb8 5fa5293a0f7c
5fbb96efed29 6016c842c688 606f01438642 60800d55a6eb 613ac6e85cad 628ac678ed6a 62980d3c1957 62b1f866eb63
639c65548f9a 63b9fa67b2d1 63be9d743ff2 63c4a82cfbe4 63fd2a62b70c 64190b4e3153 645c74fba6e7 648e5932043d
64b791ff421a 64c06a521dc8 64e6a6b181a6 65069457afe3 656931b15a20 6598a2b4a1c3 65cff32af9f7 6637327966c2
67073f5fe9c8 670906ace4bf 6730df064480 67483e01bd87 6765b5ed9636 6786c2dfbf8a 68bad5d6e613 68cca0dfe91c
68e8aee98c6b 6945e7650d8d 6969b3130578 697b6c82f792 69aac233975f 6a039e20d1e9 6a35169d676a 6a637dbdf6bc
6a6dd1b6f4c4 6a766879b7be 6b5537121566 6b568282276d 6bbedd551a75 6c6e41eb3917 6c7d3b78202d 6c8795f6f38f
6cf2dceaa50e 6cfff7deeeee 6e01838a049f 6e0780136eff 6e3b2ac390d9 6eaa80daa4a7 6ec1c2c993fa 6edbac0f8c88
6f785e2955de 6ff23e7295f5 6ffe3b7be1a9 700d5164d80c 7033b2295e51 706294c7a3cb 7089fc1bfd76 709269d11baa
7096091441d6 711093b55589 7194b7e2157b 724d56114a2f 7265ade0f2dd 7298646a8055 729f02774319 73795f7d46c1
738f0b6382d9 73c6ba7445cb 73ce2f8c8323 73e076d2bb79 7482a4b2f8a0 748d86a224b5 74b8c2f0a545 74ffc24ad1be
751dfb2376d4 754846c6b9fa 7573865acb1c 759eb4c1b876 75b5a5615d66 75c02a9263b8 75eded58b363 7624de6937fd
7637b391dcaa 7676f30ff1f9 769715489945 76ebbafa933a 76fe8bc44c10 770695a9f5d8 776582904398 7767062657e0
778884b4bf19 7792c7b65fe7 77a14c3af737 77afb8c37adb 77eeffb9d0dc 77f246d80b10 7853d1c71df0 78bc5fb61844
78cd705588b9 78d0bf108dde 78d219e8f69a 78d701cd371d 78e0c7a42431 78e2e1a79eab 78e584043802 795d6d339b7c
7985d19d7e41 799bd00d1f0f 79bdb06693d3 79c8e680cfe9 7a13ce1864e6 7a4992c35bba 7a60a6d36f8c 7a9541f7c0c2
7ac9c786248a 7b367252b111 7b3b0e03e5bb 7b90a491346b 7b9c2c86fa65 7bac03e52f90 7bc45785854f 7bd388ce7e46
7be4916da8de 7c0b6d8fc5c9 7c0d76ee8f2a 7cc6433edc0b 7ccc88c5edbe 7d086c56f309 7d0a6c8db230 7d52c368eece
7d5b99b604f6 7d788bfca61c 7da9c0260e21 7db2e4fb4d1b 7dcf6eedc0e9 7e22b914849e 7ed5b71eba4d 7f3a27bf29aa
7f45b92b8d72 7f541d2fb59e 7f866096e482 7fdc6a36698f 7fe852eacf66 7ffd12582621 801bf4c0de96 80b285879387
80da9cc8342d 80f4151ae60c 819af6b8bf61 81a26cdcb8b1 81db228cb161 82311ed298ed 824389f4990f 82c59344b0c8
82ce6a90c5e4 8316392896c3 83acd89e8993 83cdb01d393a 83e6e9f2562c 840493fd5631 8444ecf24714 84e97315f4cd
855116bf00a5 85819bf92c7d 85cc99264a89 863637552ccd 8649d31846e2 8685702d42ea 8694c7842792 86a5212aa6f6
86aa5fc1b4ed 86c83f5eabb3 86f39e9b2923 87058ae6b1af 87ebdf5ff64f 888cb4328ca6 8909b1098412 89800cabc9fc
899b091d7bc6 89e8fcff5564 89fd82acaa43 8a3fab00633c 8a607dbeb7c9 8a94b0db332b 8a9e47a8fbfe 8b42e7adb0c0
8b5e8da65bf6 8c1d2a31be06 8c63fd55535a 8ce5d1116fcf 8d5f369a30ed 8d80a3cb2ce5 8d84d3085f40 8da7922a57a5
8e031acedea3 8e4f378a6de5 8e6f6bd8adc3 8e911e6fe263 8ebdea828e3a 8eeaea8cdc18 8eece0f63dd9 8eed81bbbaf4
8ef175cd96e6 8f63b2b37404 8f8b25d32e70 8fa5547b5c22 900c3587b0a7 905e61c9c598 90fd4b5d78d9 9140158c875c
917400540012 918d38bd3f2d 91b1a00fc593 91cfbe341c1f 921df2677a0e 923291707831 9236802b9430 928c6466bce4
92910b1c0eae 9293d1c55209 92a37481c300 92d35619c93b 92f99f9d36ee 933c89251a31 93600523ab3e 93741237a6f1
93c808ff38e2 93d63258c1bc 9404681b2827 9430afbc2f53 944d97064e1b 94a9aa49b60e 94c41f3cb0d7 95232f70aaf0
959aa8f33f31 959b2c02317d 95a4ea4a05e6 95b71098e3af 9658424dd177 9664ce918a86 96e5bb91aa75 96eff85bac49
9724f98921b2 9747f5410f66 976611613848 97b94c712246 98dca3a2aea2 98e624c51d25 996ac3588618 998569f70520
99c7bab10cc8 9a0b890b8f94 9a546a759487 9a7657dbdecf 9aeb18188b62 9bb0235e2def 9bccfdcbfa8f 9bcdf028c26a
9be0b0e2042f 9bf2e2dbb625 9c01b1a75631 9c8df7b44271 9c91df15bce0 9ca8e2608372 9cbffc49596b 9cf399eef698
9cf83c02e9e3 9d0878bb0bce 9d22e419432f 9d3c352fe97e 9d427e0d8dde 9d4d5dc0aeee 9d4fb6d37ad2 9d63d886ecdc
9d6bc830ebbc 9d966fdd4ebe 9db1017fa763 9db4255ef4dd 9db6041ab93e 9db6ae29f4bb 9e0230ec26fd 9e2b084747ff
9e6b1959c913 9e7556dc2fa1 9eaf75b0bd76 9eb7be614f7c 9ef88849b7aa 9f131ec1728a 9f808874dc5f a0dbf6c2d6f6
a0f2bfcd6084 a1088e98ca3f a198e6267363 a26a864eeeeb a2c31d57fda0 a327ffbc8c3f a34fbc719d7f a364dbf21b1b
a3690ea9cf52 a38392ac7b43 a386fe6f00e2 a3e3c1923dd1 a40f293862d6 a42f10f0ecbf a431eaf38c82 a489cf0dfe4a
a49c19b7f628 a4a1f89f31d2 a500add2bb72 a507c6517a2f a534f559181f a56d4a86a51b a5921d54bcb8 a5ef13f9e4dd
a5f45c1ee182 a60748cf0cf1 a64d8ac67fcc a6676c0911ce a67e99bf16a3 a6962b278763 a6b0b91df17c a6bc74dac038
a7066969fd96 a7dbc77c3915 a7ec01876c12 a82b5773820f a8669069bda2 a8e186895ee1 a8e3c2537399 a90dae844455
a90eae9a7de2 a91f5b5e30dc a92864052e29 a9f8f6270fc0 aa56b3985d72 aa84a052d146 aa8c6c4a5473 aa8e1b62a2ab
aaa9e590f9eb aace9e1e5fce ab0ce5372e65 ab41b16fb7bd ab86da909d8d aba251843c25 aba92311dea1 abe2ca09b723
abf2c490c3b8 ac25d8a0afc1 ac47338ba30e ac5ebb0b868a ac61dec75895 ac66039865ea aca69a6c21df acd4a571076b
ad5cc0d87522 ad95e89620d2 ad9b35d986c8 ae6797ecac1c aec39546b965 afb242d66209 aff6f2473ac9 b02d04c72a59
b0c45766f1da b16fbffd3d53 b180a004ae2d b1972ccfcd7c b1bbe7d255a0 b20433ed2404 b2090c099cd3 b21014a18158
b2117153032c b21fb1c822f9 b27516769217 b29a6a581e05 b36ebba93f40 b3724a64f50c b3b6c5125d9d b3e8dbac8b38
b44113bf653b b45361d56dfd b4dc44951bfe b4f19c9168ba b4f785f59084 b51250aaa718 b512ac49eed4 b57a0b39c21f
b64f14390770 b698b3c5b1b6 b6f0acc24f40 b71ab99c1d1b b7a821802dd9 b7adf5c3819f b7ce1e9c739a b82f33f276c1
b838e489d545 b88ee172f952 b8b589f0fb95 b8ef79f3b54a b8f982a804af b9086cf1301f b97225e8474b b9e22697e14e
b9ef38428c94 ba3e812c7c2e ba3f8f5f5b10 ba44e542968b bae392d75de9 bb6969159ef8 bb77c932f135 bb9fece745a2
bba6eb838457 bbc937cd44e8 bbcf68807c7c bc4143bdc2ca bc5f00c64132 bcc360db3671 bcc43cacc73f bccfd154a78b
bcfc0c088fab bd70a9c10e58 bd7ece175874 bd95d2728293 bdc565f1d2fa bddf1d39268c bde747a28a46 be4cd8d00be2
be65e80d5398 be6842edf08d be6a3c10b5ed be8dc616bc74 bebc5da55c64 bf11441eeff2 bf3d7fc886c8 bf54ff63ee27
bf8d2f388003 c000fab73d6e c063adc5020a c1074b5a0f9e c10f8543b2ae c14300e5bd6a c170c7ea6625 c1902df6cfeb
c1caeb0b5989 c1cb9448d6e0 c1f624d35941 c21ddd480d7b c296e0bea108 c34df24fac67 c3f6e77b2698 c456d8ccc436
c457dad4db10 c463bc768cc2 c47325fa1c8f c4808139d563 c4dd7b1b4c77 c56272730e9c c592dc348ea6 c5bd68c01e5a
c5d8440ad803 c5dd57428d88 c5e3663ff612 c5faff7e2762 c62c54402b3c c6a07bfa6527 c6ae1aa2831a c70c8624aba6
c74203ca816b c7818da0c98d c7855c54a5ed c794683f985f c80ed7ed501b c8195c9c1984 c977687f40f0 ca1c58264ea7
ca7e2c934e8d ca8bdbd3d3d2 caac468c2b9a cad079c35cdd caf08cfc6ef7 cb3f03542472 cb90e3b9513e cbb916c2be2e
cbc92ebcb32c cbee3a2b9916 cbf879544d09 cc3826971f4b cc4baba051fd cc5765f8f153 cc5d40fabc6d cc6f03e3c705
ccb89be95dcb ccf5cb08b81f ccfb9c3acfeb cd07fe57c52d cd47f49de67e cd7e4858bf8f cdbd4cbc303c cdc4bab9dbdb
cdf8db1ebfcc ce3175af0b0f ce62f09adfc3 ce827ba527a2 cec86fc73081 cefc5b1a78c8 cf4fda95294e cf75825be7aa
cf880caa5055 cfe30ae72ffa cff88ec459ce d0c2985ef902 d0f89ad4ed16 d10d0ea30922 d13ddc960216 d143b4afab0e
d14e2457e4af d14e4115b78e d1829c3c3fa0 d21cb6754565 d2b31c7ca222 d2cc9d08e328 d3287065c51d d365475609f5
d3ac2c1b4d48 d425920e10e0 d42b59ef483e d4433b0d4cc6 d45a3f3c237c d471f468f2f2 d4bb4c5d6a6f d5196872d8c5
d54d04252e23 d55e80b98555 d574b0fb73bc d5f293e8dc5c d682044e6469 d6872ba64a1d d6a57ed5e64f d6bb43fbd17a
d6e6a897291f d719ea54e449 d77d3224b0d3 d78fdae2ff2f d83aca306bda d8c2aac66b6a d90678b89476 d9303f5e99cd
d94cf9e8a9c1 d96395ebda10 d977faaef181 d97878fac9b6 da450aa33108 daaa76901083 db41432eb9e8 db56a3aad49f
db610fcfd8c5 db74d233222e dbc7c3328f3f dbde26ac0b66 dc2d5b4dfbb2 dc94fa724140 dca2291496f3 dcc286d08249
dce5745fe70d dcf31dce43f0 dd036f958525 dd65801d379f dd74bf90c0de dd935e3ea8c1 dda2acf05581 ddf86d0604f1
ddfe5cb893ca de7e71f8a0f7 de8336c23344 dea529a7b6cf def43b51321f df1e7dc3d1f9 df52be2ded67 df5c6cdf3bd9
dfa4f504eccb dfbb5c3c357d e0219c386099 e02623ac5a6d e02a294615a9 e06958f2aa38 e142bc0e9cfd e1698fff6e09
e18b55535673 e1a74d6116a0 e1aa83876d20 e1db796d56a4 e2104a3aef25 e26ffa207548 e2fbf578b752 e349fd3b8123
e3756bc0cabf e3ca17d3e23e e3da58e43409 e3f557a2b86b e3f578deae15 e3f6eab988eb e45c15bf6c63 e4e89a8d622c
e4fb85dbe070 e58f55ed0664 e5d0cb5e0e99 e60bfa672c04 e62204a17893 e65047008a19 e671e9bc3172 e6c18ae937a2
e6c39a245087 e6d0d788946e e6ff262df5d4 e75f27d17464 e7ef6382ae63 e8293a7e3cac e82cd316b426 e83a01f6f40c
e882d78bb03b e8834991c0a5 e89200ad963f e8a03ed81bd3 e8acc307e378 e917ffca7994 e92110b99a62 e92a252a520b
e941ee033cbd e948aaad4a90 e9dc29193413 e9f68a383985 ea0d30e47c45 ea164afeebb4 ea3283028f20 ea9d8c8a6b33
eb1d90ebe6d2 eb216d83e2be eb878f487a95 ebd29d587348 ebf9d30ca664 ec12ac35acfc ec22f814558d ec2631435438
ec6e14af9045 ec8f17a171a4 ec94ba788953 eceb92adfa4b ed0b09c9e516 ed79ebb2c8e4 edb65a464b32 edca838278f2
edcc5e90acd5 ee230f678589 ee3cacdb52e2 ee3d3521187a ee418f926fe1 ee7ac2b88da4 ee8bb23e25cc eeea3cbd3664
ef1d0a07f5f2 ef1fe99fd176 ef42ae5d492b ef6e711e00e0 efa31925e8a8 efe42d10f35f f00e6778e835 f0d3f2a8bcb4
f0da6a658280 f131d8dfe788 f138c1481453 f13d1dd3990d f153884cd731 f17e75f8fc2a f18dd042c9f8 f1c01ebe1040
f21b9bd1eb6f f246eae0e310 f25f6a4a8a8a f3200a395d3d f34c9369ceec f3549df806a8 f35596f2c408 f3579cd92705
f373f4e2903f f37a5b3c3182 f419cd775caf f41c94616b58 f42156006ca5 f45a6fed00f5 f4bbb8e297ae f50ab0768a80
f50bdd941f2e f56c30a39789 f56e5ccf1ebb f5ba7b84caed f5d5b55414be f5dd4b08e3ac f629417d0574 f74081a01f88
f78f4a157cdb f796a6305b0d f8511bfcf2fa f8a65d6d06b5 f8ec0b39c942 f8fb4d7d52dd f99a5f36c298 f9b817b1ff05
f9d894026c02 f9ead90b8aae fa1ca1292081 fa93b4f21017 fac117916bef faef076f29b7 faf41198325c fb01cea7acda
fb401a1b37fa fb8714963ecd fb9ca85bc229 fbd53a2cb07f fbed23d77453 fbfe2a0306a4 fc75a1a5b052 fc97e34f6ddf
fca4abf87667 fca9655007c8 fcb1297006c0 fceefa05ef7c fd1e80141f8f fd5f2d004016 fe16d1d06870 fe1fd97c0ea8
fe815f497206 fede90b672a4 fedf89ccd6bd ff3030cdf11e ff45eee78153 ff5605a95075 ff7c32fd9048 ff7efb709a25
fff58b3b4fe0
`

func TestIntegrationPGCharNTrimBattery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Minute)
	defer cancel()
	s := cnStart(t, ctx)
	cases := cnCases()
	var fails []cnFailure
	report := func(phase string) {
		for _, extended := range []bool{false, true} {
			proto := "simple"
			if extended {
				proto = "extended"
			}
			tl := cnRun(t, ctx, s, phase, cases, extended, &fails)
			t.Logf("%s, %s: %d statements: %d answered by the copy, %d by the source, %d refused; of the %d that should reach the copy, %d did", phase, proto, tl.total, tl.copy, tl.source, tl.refused, tl.wanted, tl.wantedOn)
			names := make([]string, 0, len(tl.byGroup))
			for g := range tl.byGroup {
				names = append(names, g)
			}
			sort.Strings(names)
			for _, g := range names {
				n := tl.byGroup[g]
				t.Logf("    %-24s %5d statements, %5d on the copy, %5d wrong", g, n[0], n[1], n[2])
			}
		}
	}
	report("no forwarding user")
	if out, err := s.cli(ctx, cnFwdPass+"\n", "server", "forward-user", cnServer, "-u", s.fwd, "--password-stdin"); err != nil {
		t.Fatalf("forward-user: %v\n%s", err, out)
	}
	cnWaitFor(t, "the router to see the forwarding user", time.Minute, func() (bool, string) {
		c, err := pgconn.Connect(ctx, s.portDSN)
		if err != nil {
			return false, err.Error()
		}
		defer c.Close(ctx)
		if _, err := c.Exec(ctx, "SELECT now() FROM cn_mark").ReadAll(); err != nil {
			return false, err.Error()
		}
		side := cnSide(ctx, c)
		return side == "source", side
	})
	report("forwarding user")
	txRuns, txOnCopy := cnPreparedInTx(t, ctx, s, &fails)
	t.Logf("prepared in a transaction and run outside it, and the reverse, with a statement cache: %d executions, %d of those outside the block answered by the copy", txRuns, txOnCopy)
	runs, moved, refusedRuns := cnDescribeThenExecute(t, ctx, s, &fails)
	t.Logf("described, then executed twice, text and binary: %d executions, %d described on the copy and executed on the source, %d refused cleanly", runs, moved, refusedRuns)

	var out *os.File
	if p := os.Getenv("CHARN_BATTERY_OUT"); p != "" {
		var err error
		if out, err = os.Create(p); err != nil {
			t.Fatal(err)
		}
		defer out.Close()
	}
	known, fresh := 0, 0
	byReason := map[string]int{}
	example := map[string]string{}
	for _, f := range fails {
		why := cnKnown(f)
		if out != nil {
			fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%q\t%q\t%s\t%s\t%q\n", cnOr(why, "NEW"), f.phase, f.proto, f.group, f.key, f.why, cnHash(f), f.class, f.notice)
		}
		if why != "" {
			known++
			k := f.class + " | " + why
			byReason[k]++
			if _, ok := example[k]; !ok {
				example[k] = f.key
			}
			continue
		}
		fresh++
		if fresh <= cnFailLogged {
			t.Errorf("[%s, %s, %s] %s\n    %s", f.phase, f.proto, f.group, cnClip(f.key, 220), cnClip(f.why, 320))
		}
	}
	reasons := make([]string, 0, len(byReason))
	for r := range byReason {
		reasons = append(reasons, r)
	}
	sort.Strings(reasons)
	for _, r := range reasons {
		t.Logf("already failing on main: %5d  %s\n      e.g. %s", byReason[r], r, cnClip(example[r], 150))
	}
	t.Logf("wrong answers from the copy: %d new, %d already failing on main", fresh, known)
	if fresh > cnFailLogged {
		t.Errorf("%d more new wrong answers (set CHARN_BATTERY_OUT to list them all)", fresh-cnFailLogged)
	}
}

func cnOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// cnPreparedInTx uses a client that caches its prepared statements: one
// prepared inside a transaction block (which the source answers) and run
// outside it (which the copy may answer), and the reverse. PostgreSQL's rows
// or a clean refusal are acceptable; "cached plan must not change result
// type", or any 0A000, for a statement that works on PostgreSQL is not.
func cnPreparedInTx(t *testing.T, ctx context.Context, s *cnStack, fails *[]cnFailure) (runs, onCopy int) {
	t.Helper()
	open := func(dsn string) *pgx.Conn {
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
		c, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	read := func(c *pgx.Conn, q string) ([]string, error) {
		run, stop := context.WithTimeout(ctx, cnStatement)
		defer stop()
		rows, err := c.Query(run, q)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id int32
			var v *string
			dest := []any{&id, &v}
			if len(rows.FieldDescriptions()) == 1 {
				dest = []any{&v}
			}
			if err := rows.Scan(dest...); err != nil {
				return nil, err
			}
			cell := "NULL"
			if v != nil {
				cell = strings.TrimRight(*v, " ")
			}
			out = append(out, fmt.Sprintf("%d|%q", id, cell))
		}
		return out, rows.Err()
	}
	direct := open(s.dsn)
	defer direct.Close(ctx)
	for _, q := range []string{
		"SELECT c FROM cn_cp WHERE id = 1",
		"SELECT c FROM cn_cp WHERE id <= 4 ORDER BY id",
		"SELECT id, c FROM cn_cp WHERE id <= 4 ORDER BY id",
		"SELECT id, c FROM (SELECT * FROM cn_cp) s WHERE id <= 4 ORDER BY id",
		"SELECT id, c FROM cn_a WHERE id <= 6 ORDER BY id",
	} {
		want, err := read(direct, q)
		if err != nil {
			t.Fatalf("on PostgreSQL: %s: %v", q, err)
		}
		for _, insideFirst := range []bool{true, false} {
			c := open(s.portDSN)
			how := "prepared inside a transaction, then run outside"
			if !insideFirst {
				how = "prepared outside, then run inside a transaction"
			}
			judge := func(where string, got []string, err error) {
				runs++
				var pe *pgconn.PgError
				switch {
				case err != nil && errors.As(err, &pe) && pe.Code != "0A000":
					// A clean refusal the client can act on.
				case err != nil:
					*fails = append(*fails, cnFailure{phase: "forwarding user", proto: "extended", group: "prepare in a transaction", key: q + "  -- " + how, why: where + ": " + cnClip(err.Error(), 160), class: "B"})
				case strings.Join(got, "\n") != strings.Join(want, "\n"):
					*fails = append(*fails, cnFailure{phase: "forwarding user", proto: "extended", group: "prepare in a transaction", key: q + "  -- " + how, why: where + fmt.Sprintf(": %v on PostgreSQL, %v here", want, got), class: "A"})
				}
			}
			inTx := func() {
				tx, err := c.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				got, err := read(c, q)
				judge("inside the transaction", got, err)
				_ = tx.Rollback(ctx)
			}
			outside := func() {
				got, err := read(c, q)
				judge("outside the transaction", got, err)
				if cnSide(ctx, c.PgConn()) == "copy" {
					onCopy++
				}
			}
			if insideFirst {
				inTx()
				outside()
				outside()
			} else {
				outside()
				inTx()
				outside()
			}
			c.Close(ctx)
		}
	}
	return runs, onCopy
}

// cnStored is a type PostgreSQL compares by value and the copy may hold as
// text: vals are stored, lits are other spellings of some of those values.
type cnStored struct{ name, decl string; vals, lits []string }

func cnStoredTypes() []cnStored {
	return []cnStored{
		{"inet", "inet", []string{"10.0.0.1", "10.0.0.2", "9.0.0.1", "10.0.0.10", "192.168.0.1/24", "::1"}, []string{"10.0.0.1/32", "10.0.0.1", "010.0.0.2"[1:]}},
		{"cidr", "cidr", []string{"10.0.0.0/8", "10.1.0.0/16", "9.0.0.0/8", "192.168.0.0/24"}, []string{"10/8", "10.0.0.0/8", "10.1/16"}},
		{"macaddr", "macaddr", []string{"08:00:2b:01:02:03", "08:00:2b:01:02:04", "a8:00:2b:01:02:03"}, []string{"08-00-2B-01-02-03", "0800.2b01.0203", "08002b010204"}},
		{"macaddr8", "macaddr8", []string{"08:00:2b:01:02:03:04:05", "08:00:2b:01:02:03:04:06"}, []string{"08002B0102030405", "08-00-2B-01-02-03-04-06"}},
		{"json", "json", []string{`{"a":1,"b":2}`, `{"b": 2, "a": 1}`, `[1,2]`}, []string{`{"a": 1, "b": 2}`, `{"b":2,"a":1}`}},
		{"jsonb", "jsonb", []string{`{"a":1,"b":2}`, `{"c":3}`, `[1,2]`, `{"a":10}`}, []string{`{"b":2,"a":1}`, `{ "a" : 1, "b" : 2 }`, `[1, 2]`}},
		{"money", "money", []string{"-5.00", "10.00", "9.50", "100.00", "-20.00"}, []string{"10", "$10.00", "9.5"}},
		{"numeric50", "numeric(50,2)", []string{"10.00", "9.50", "100.00", "-7.25", "99999999999999999999999999999999999999999.00"}, []string{"10", "10.0", "9.5"}},
		{"uuid", "uuid", []string{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12"}, []string{"A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", "a0eebc999c0b4ef8bb6d6bb9bd380a11"}},
		{"interval", "interval", []string{"1 day", "2 days", "36 hours", "90 minutes"}, []string{"24 hours", "1 day", "48:00:00"}},
		{"time", "time", []string{"10:00:00", "09:30:00", "23:59:59"}, []string{"10:00", "9:30", "10:00:00.000"}},
		{"timetz", "timetz", []string{"10:00:00+00", "12:00:00+02", "09:00:00-01"}, []string{"10:00+00", "12:00:00+02:00"}},
		{"bit", "bit(4)", []string{"1010", "0101", "1111"}, []string{"1010", "0101"}},
		{"varbit", "varbit", []string{"1010", "10", "101"}, []string{"1010", "10"}},
		{"bytea", "bytea", []string{`\x6162`, `\x00ff`, `\x61`}, []string{"ab", `\x6162`, `\x00FF`}},
		{"xml", "xml", []string{"<a/>", "<b>1</b>"}, []string{"<a></a>", "<b>1</b>"}},
		{"tsvector", "tsvector", []string{"a b", "c d", "b c"}, []string{"b a", "d  c"}},
		{"tsquery", "tsquery", []string{"a & b", "c | d"}, []string{"a&b", "c|d"}},
		{"point", "point", []string{"(1,2)", "(3,4)"}, []string{"1,2", "(1.0,2.0)"}},
		{"line", "line", []string{"{1,2,3}", "{1,1,1}"}, []string{"{1.0,2.0,3.0}"}},
		{"lseg", "lseg", []string{"[(0,0),(1,1)]", "[(1,1),(2,2)]"}, []string{"((0,0),(1,1))", "0,0,1,1"}},
		{"box", "box", []string{"(1,1),(0,0)", "(3,3),(2,2)"}, []string{"(0,0),(1,1)", "((1,1),(0,0))"}},
		{"path", "path", []string{"((0,0),(1,1),(2,0))", "[(0,0),(1,1)]"}, []string{"((1,1),(2,0),(0,0))"}},
		{"polygon", "polygon", []string{"((0,0),(1,1),(2,0))", "((0,0),(2,2),(4,0))"}, []string{"(0,0),(1,1),(2,0)"}},
		{"circle", "circle", []string{"<(0,0),1>", "<(1,1),2>"}, []string{"((0,0),1)", "0,0,1"}},
		{"pg_lsn", "pg_lsn", []string{"0/16", "0/9", "1/0"}, []string{"0/016", "00/16"}},
		{"int4range", "int4range", []string{"[1,3)", "[5,7)", "empty"}, []string{"[1,2]", "(0,3)"}},
		{"int8range", "int8range", []string{"[1,3)", "[5,7)"}, []string{"[1,2]", "(0,3)"}},
		{"numrange", "numrange", []string{"[1.0,2.0)", "[3,4)"}, []string{"[1,2)", "[1.00,2.00)"}},
		{"daterange", "daterange", []string{"[2026-01-01,2026-01-03)", "[2026-02-01,2026-02-03)"}, []string{"[2026-01-01,2026-01-02]", "(2025-12-31,2026-01-03)"}},
		{"tsrange", "tsrange", []string{"[2026-01-01 00:00,2026-01-03 00:00)", "[2026-02-01 00:00,2026-02-03 00:00)"}, []string{"[2026-01-01,2026-01-03)"}},
		{"tstzrange", "tstzrange", []string{"[2026-01-01 00:00+00,2026-01-03 00:00+00)", "[2026-02-01 00:00+00,2026-02-03 00:00+00)"}, []string{"[2026-01-01 02:00+02,2026-01-03 02:00+02)"}},
		{"oid", "oid", []string{"10", "9", "100"}, []string{"10", "0010"}},
		{"qchar", `"char"`, []string{"a", "b", "Z"}, []string{"a", "abc"}},
	}
}

func cnStoredSeed() (stmts, tables []string) {
	for _, ty := range cnStoredTypes() {
		a, b := "cnt_"+ty.name, "cnu_"+ty.name
		var av, bv []string
		for i, v := range ty.vals {
			av = append(av, fmt.Sprintf("(%d, %s)", i+1, cnLit(v)))
		}
		av = append(av, fmt.Sprintf("(%d, NULL)", len(ty.vals)+1))
		for i, v := range ty.lits {
			bv = append(bv, fmt.Sprintf("(%d, %s)", i+1, cnLit(v)))
		}
		stmts = append(stmts,
			"CREATE TABLE "+a+" (id int PRIMARY KEY, v "+ty.decl+")", "INSERT INTO "+a+" VALUES "+strings.Join(av, ","),
			"CREATE TABLE "+b+" (id int PRIMARY KEY, v "+ty.decl+")", "INSERT INTO "+b+" VALUES "+strings.Join(bv, ","))
		tables = append(tables, a, b)
	}
	return stmts, tables
}

func cnStoredCases() []cnCase {
	var out []cnCase
	for _, ty := range cnStoredTypes() {
		a, b := "cnt_"+ty.name, "cnu_"+ty.name
		g := "stored as text: " + ty.name
		add := func(s string) { out = append(out, cnCase{group: g, sql: s}) }
		for _, l := range append(append([]string{}, ty.lits...), ty.vals[0]) {
			lit := cnLit(l)
			for _, op := range []string{"=", "<>", "<", ">="} {
				add("SELECT id FROM " + a + " WHERE v " + op + " " + lit + " ORDER BY id")
			}
			add("SELECT id FROM " + a + " WHERE v IN (" + lit + ") ORDER BY id")
		}
		add("SELECT id FROM " + a + " WHERE v BETWEEN " + cnLit(ty.lits[0]) + " AND " + cnLit(ty.vals[len(ty.vals)-1]) + " ORDER BY id")
		for _, s := range []string{
			"SELECT min(v), max(v) FROM %A", "SELECT count(DISTINCT v) FROM %A", "SELECT v, count(*) FROM %A GROUP BY v", "SELECT DISTINCT v FROM %A", "SELECT sum(v) FROM %A",
			"SELECT count(*) FROM (SELECT v FROM %A UNION SELECT v FROM %B) u", "SELECT a.id, b.id FROM %A a JOIN %B b ON a.v = b.v ORDER BY 1, 2", "SELECT a.id FROM %A a WHERE a.v IN (SELECT v FROM %B) ORDER BY 1", "SELECT count(DISTINCT v) FROM %B", "SELECT v, count(*) FROM %B GROUP BY v",
		} {
			add(strings.NewReplacer("%A", a, "%B", b).Replace(s))
		}
	}
	return out
}
