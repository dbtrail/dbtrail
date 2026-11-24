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
	for _, c := range cnASCIICases() {
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
		"--password-stdin", "-d", s.db, "--publication", cnPub, "--slot", cnSlot + "_" + id, "--schemas", "public", "--no-forward"); err != nil {
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
// A failure is known when its hash, which includes how it fails (cnKind), is
// in the list. The one exception: on the tables altered after their snapshot
// (cn_alt, the group "screen against view"), whether a statement reaches the
// copy right after the ALTER depends on timing, so those are not listed and
// are known by their group (#3251); nowhere else does cn_alt excuse anything.
func cnKnown(f cnFailure) string {
	altered := f.group == "screen against view" && strings.Contains(f.key, "cn_alt")
	if !cnBaseline[cnHash(f)] && !altered {
		return ""
	}
	switch f.group {
	case "ascii":
		return "ascii() of a char(n) or of a cut of it"
	case "domain":
		return "#3233, a domain over char(n)"
	case "text-on-copy types":
		return "a type the copy holds as text, sorted or compared as letters"
	case "screen against view":
		return "a column type changed after the snapshot (#3251)"
	case "parameter":
		return "an argument declared bpchar, compared with no table in the statement"
	case "describe then execute", "prepare in a transaction":
		return "described on one side and run on the other"
	}
	if strings.HasPrefix(f.group, "stored as text: ") {
		return "a built-in type the copy holds as text"
	}
	if f.phase == "no forwarding user" && cnKind(f) == "order" {
		return "text sorted by bytes, with no forwarding user"
	}
	return "listed on main, group " + f.group + ", " + cnKind(f)
}

// cnKind is how a failure fails: other rows, another order, an error the
// client sees or a statement on the wrong side, or an answer to what
// PostgreSQL refuses. A known failure that starts failing another way is new.
func cnKind(f cnFailure) string {
	switch {
	case strings.HasPrefix(f.class, "B"):
		return "error or wrong side"
	case strings.HasPrefix(f.why, "order differs"):
		return "order"
	case strings.HasPrefix(f.why, "PostgreSQL refuses it"):
		return "answered what PostgreSQL refuses"
	}
	return "rows"
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
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s", f.phase, f.proto, f.group, f.key, cnKind(f))
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
00c5f5576173 0221cce0b3e2 02a5e7ebf199 1e53e6764221 20bd4466d264 250b601fa1dc 252970817959 2ccb2740a8d4
2d8c6787c45e 2f7d37be6f85 339432e478a7 390cc6ace6ab 46b8a5d9d6c3 489b6d28ed8f 4ce38fb70692 4db661d577d9
5395ea082085 54aedff045a8 5a6fb7147f9f 5eb0e65aecf3 6039f7484f1d 67dc3ba3b959 67f54ed753d5 69d6d23a79b0
6f951269e53d 6fba56791169 724498701571 73b98eb2782a 78486743df83 81ad3f69d0d6 863f98d2b053 96fc7e18c0d7
9a3977590e9c 9b3ad49d9748 9d53309d8d54 a60c711b3220 a8308dae410b a8d682396b25 b1aa547af55e b34669505a99
b440bf4dce8f bd7e9896561c c136da616bd9 c6f9759b2cba ca545980546d cb7f0481bd72 cd7a208b45c1 d2e67d0da1ec
d3e2d712f27d d67e3cbff662 d7fc747b8a8d dd3a1afb4277 de65cb4d0b8e e1c8b1d1c4d4 e26b2013f075 ea0be441c06c
ed458d5131e9 edc7773bc61d f109684da0aa f19358ff3916 f7b4177d4693 f98921365dbe fe209b9c985b
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
			var v any
			dest := []any{&id, &v}
			if len(rows.FieldDescriptions()) == 1 {
				dest = []any{&v}
			}
			if err := rows.Scan(dest...); err != nil {
				return nil, err
			}
			// A text is compared without its padding; an array of char(n)
			// (not trimmed on the copy) by its elements as they are.
			cell := "NULL"
			switch x := v.(type) {
			case nil:
			case string:
				cell = strings.TrimRight(x, " ")
			default:
				cell = fmt.Sprint(x)
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
		// An array of char(n): the source describes it as 1014.
		"SELECT id, arr FROM cn_dm WHERE id <= 4 ORDER BY id",
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

// cnASCIICases is ascii(c) and ascii(substr(c, i, 1)) on char(n), the shape
// the CH-benCHmark reports join and group by: in the select list, in WHERE,
// in a join and in GROUP BY, with cuts past the stored value and inside the
// padding (substr(c, 4, 1) of 'ab' in a char(5)).
func cnASCIICases() []cnCase {
	exprs := []string{"ascii(%C)"}
	for i := 1; i <= 7; i++ {
		exprs = append(exprs, fmt.Sprintf("ascii(substr(%%C, %d, 1))", i))
	}
	exprs = append(exprs, "ascii(substr(%C, 1, 2))", "ascii(substr(%C, 4))", "ascii(substring(%C from 4 for 1))", "ascii(left(%C, 1))", "ascii(right(%C, 1))")
	froms := []struct{ with, from, c string }{
		{"", "cn_a s", "c"}, {"", "cn_a s", "s.m"}, {"", "(SELECT * FROM cn_a) s", "c"}, {"", "(SELECT id, c AS r, k FROM cn_a) s", "r"}, {"WITH w AS (SELECT * FROM cn_a) ", "w s", "c"},
	}
	var out []cnCase
	add := func(s string) { out = append(out, cnCase{group: "ascii", sql: s}) }
	for _, f := range froms {
		for _, e := range exprs {
			x := strings.ReplaceAll(e, "%C", f.c)
			add(f.with + "SELECT id, " + x + " FROM " + f.from + " ORDER BY id")
			for _, w := range []string{"= 97", "= 32", "= 0", "<> 32", "> 90", "BETWEEN 65 AND 122", "IN (32, 97, 195)", "IS NULL"} {
				add(f.with + "SELECT id FROM " + f.from + " WHERE " + x + " " + w + " ORDER BY id")
			}
			add(f.with + "SELECT " + x + ", count(*) FROM " + f.from + " GROUP BY 1 ORDER BY 1 NULLS FIRST")
			add(f.with + "SELECT k, sum(" + x + ") FROM " + f.from + " GROUP BY k ORDER BY k")
			add(f.with + "SELECT s.id, b.id FROM " + f.from + " JOIN cn_b b ON " + x + " = ascii(substr(b.c, 1, 1)) ORDER BY 1, 2")
			add(f.with + "SELECT s.id, b.id FROM " + f.from + " JOIN cn_b b ON b.id = " + x + " - 90 ORDER BY 1, 2")
		}
	}
	for _, i := range []string{"1", "2", "3"} {
		sub := "ascii(substr(%s, " + i + ", 1))"
		a, b := fmt.Sprintf(sub, "c.c_state"), fmt.Sprintf(sub, "w.w_state")
		add("SELECT c.c_w_id, c.c_d_id, c.c_id, " + a + " FROM customer c ORDER BY 1, 2, 3")
		add("SELECT " + a + ", count(*) FROM customer c GROUP BY 1 ORDER BY 1 NULLS FIRST")
		add("SELECT count(*) FROM customer c JOIN warehouse w ON " + a + " = " + b)
		add("SELECT c.c_id, d.d_id FROM customer c JOIN district d ON d.d_w_id = c.c_w_id AND d.d_id = " + a + " % 2 + 1 WHERE c.c_w_id = 1 AND c.c_d_id = 1 ORDER BY 1, 2")
		add("SELECT count(*) FROM customer c JOIN item i ON i.i_id = " + a + " - 60")
		add("SELECT count(*) FROM customer c WHERE " + a + " BETWEEN 65 AND 90")
	}
	return out
}
