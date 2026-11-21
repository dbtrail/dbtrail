package pgshim

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// The catalog statements this port recognizes (catalog.go), as the clients
// send them: psql 13 to 18 (to a server that says it is 14), JDBC 42.7,
// SQLAlchemy 1.4 with psycopg2, and Grafana's PostgreSQL data source.
//
// psql writes \l, \dt and the statements of \d its own way in each major
// version (#3059): one more column, one more relation kind, another ORDER BY.
// Each way is a statement of its own here, with the versions seen sending it
// (catalogStmt.psql), and the forms of one command share the code that
// answers them, so every psql is told the same of the copy. The texts are
// the ones collected from each version's psql in testdata/catalog_3059, never
// written from memory, and a text no collected version sends is not here: a
// psql that writes a statement yet another way is refused by name until its
// text is collected.

// The psql majors whose statements were collected (testdata/catalog_3059).
const (
	psqlOldest = 13
	psqlNewest = 18
)

var catalogStmts []*catalogStmt

func init() {
	for _, st := range catalogStmtList() {
		compileCatalogStmt(st)
		catalogStmts = append(catalogStmts, st)
	}
}

// relCols are the columns of psql's relation list.
var relListCols = []catCol{{"Schema", oidName}, {"Name", oidName}, {"Type", oidText}, {"Owner", oidName}}

// psqlRelations is psql's relation list (\dt, \d, \di ...) as the majors
// from..to write it: kinds are the relation kinds its CASE names, which
// changed twice (13 and 14 name 's', a kind PostgreSQL no longer has; 13
// does not name a TOAST table), and amJoin the join 14 and later add. The
// copy has tables and their primary keys' indexes, which every version
// names alike, so the answer is one.
func psqlRelations(from, to int, kinds, amJoin string) *catalogStmt {
	return &catalogStmt{name: `psql \dt`, psql: [2]int{from, to}, cols: relListCols,
		text: `SELECT n.nspname as "Schema", c.relname as "Name", CASE c.relkind ` + kinds + ` END as "Type", pg_catalog.pg_get_userbyid(c.relowner) as "Owner" FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace` + amJoin + ` WHERE $w ORDER BY 1,2`,
		rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
			f, perr := relFilterOf(m[0])
			if perr != nil {
				return nil, perr
			}
			var out [][]any
			for i := range v.rels {
				r := &v.rels[i]
				if f.keep(v, r) {
					kind := "table"
					if r.kind == 'i' {
						kind = "index"
					}
					out = append(out, []any{r.schema, r.name, kind, v.s.user})
				}
			}
			return out, nil
		}}
}

const psqlAmJoin = ` LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam`

// psqlDatabases is \l as the majors from..to write it. Each asks for the
// columns its version prints, and each column is answered by its name: the
// one database is the connection's, in UTF8 with collation C; it has no ICU
// locale or rules and no access privileges to list.
func psqlDatabases(from, to int, text string, cols ...string) *catalogStmt {
	st := &catalogStmt{name: `psql \l`, psql: [2]int{from, to}, text: text}
	for _, c := range cols {
		oid := uint32(oidText)
		switch c {
		case "Name", "Owner", "Encoding":
			oid = oidName
		}
		st.cols = append(st.cols, catCol{c, oid})
	}
	st.rows = func(v *catalogView, _ []string) ([][]any, *pgErr) {
		row := make([]any, len(cols))
		for i, c := range cols {
			switch c {
			case "Name":
				row[i] = v.database
			case "Owner":
				row[i] = v.s.user
			case "Encoding":
				row[i] = "UTF8"
			case "Locale Provider":
				row[i] = "libc"
			case "Collate", "Ctype":
				row[i] = "C"
			}
		}
		return [][]any{row}, nil
	}
	return st
}

// sentBy says which psql majors were seen sending a statement.
func (st *catalogStmt) sentBy(from, to int) *catalogStmt {
	st.psql = [2]int{from, to}
	return st
}

const jdbcTableType = `CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema' WHEN true THEN CASE WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind WHEN 'r' THEN 'SYSTEM TABLE' WHEN 'v' THEN 'SYSTEM VIEW' WHEN 'i' THEN 'SYSTEM INDEX' ELSE NULL END WHEN n.nspname = 'pg_toast' THEN CASE c.relkind WHEN 'r' THEN 'SYSTEM TOAST TABLE' WHEN 'i' THEN 'SYSTEM TOAST INDEX' ELSE NULL END ELSE CASE c.relkind WHEN 'r' THEN 'TEMPORARY TABLE' WHEN 'p' THEN 'TEMPORARY TABLE' WHEN 'i' THEN 'TEMPORARY INDEX' WHEN 'S' THEN 'TEMPORARY SEQUENCE' WHEN 'v' THEN 'TEMPORARY VIEW' ELSE NULL END END WHEN false THEN CASE c.relkind WHEN 'r' THEN 'TABLE' WHEN 'p' THEN 'PARTITIONED TABLE' WHEN 'i' THEN 'INDEX' WHEN 'P' then 'PARTITIONED INDEX' WHEN 'S' THEN 'SEQUENCE' WHEN 'v' THEN 'VIEW' WHEN 'c' THEN 'TYPE' WHEN 'f' THEN 'FOREIGN TABLE' WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE NULL END ELSE NULL END AS TABLE_TYPE`

const grafanaSearchPath = `quote_ident(table_schema) IN ( SELECT CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END FROM generate_series( array_lower(string_to_array(current_setting('search_path'),','),1), array_upper(string_to_array(current_setting('search_path'),','),1) ) as i, string_to_array(current_setting('search_path'),',') s )`

func catalogStmtList() []*catalogStmt {
	return []*catalogStmt{
		// ---- psql ----
		psqlDatabases(13, 14, `SELECT d.datname as "Name", pg_catalog.pg_get_userbyid(d.datdba) as "Owner", pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding", d.datcollate as "Collate", d.datctype as "Ctype", pg_catalog.array_to_string(d.datacl, E'\n') AS "Access privileges" FROM pg_catalog.pg_database d ORDER BY 1`,
			"Name", "Owner", "Encoding", "Collate", "Ctype", "Access privileges"),
		psqlDatabases(15, 15, `SELECT d.datname as "Name", pg_catalog.pg_get_userbyid(d.datdba) as "Owner", pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding", d.datcollate as "Collate", d.datctype as "Ctype", NULL as "ICU Locale", 'libc' AS "Locale Provider", pg_catalog.array_to_string(d.datacl, E'\n') AS "Access privileges" FROM pg_catalog.pg_database d ORDER BY 1`,
			"Name", "Owner", "Encoding", "Collate", "Ctype", "ICU Locale", "Locale Provider", "Access privileges"),
		psqlDatabases(16, 16, `SELECT d.datname as "Name", pg_catalog.pg_get_userbyid(d.datdba) as "Owner", pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding", 'libc' AS "Locale Provider", d.datcollate as "Collate", d.datctype as "Ctype", NULL as "ICU Locale", NULL as "ICU Rules", pg_catalog.array_to_string(d.datacl, E'\n') AS "Access privileges" FROM pg_catalog.pg_database d ORDER BY 1`,
			"Name", "Owner", "Encoding", "Locale Provider", "Collate", "Ctype", "ICU Locale", "ICU Rules", "Access privileges"),
		// 17 and later print "(none)" for an empty list of privileges; the
		// database has no list at all, which they print as nothing.
		psqlDatabases(17, psqlNewest, `SELECT d.datname as "Name", pg_catalog.pg_get_userbyid(d.datdba) as "Owner", pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding", 'libc' AS "Locale Provider", d.datcollate as "Collate", d.datctype as "Ctype", NULL as "Locale", NULL as "ICU Rules", CASE WHEN pg_catalog.array_length(d.datacl, 1) = 0 THEN '(none)' ELSE pg_catalog.array_to_string(d.datacl, E'\n') END AS "Access privileges" FROM pg_catalog.pg_database d ORDER BY 1`,
			"Name", "Owner", "Encoding", "Locale Provider", "Collate", "Ctype", "Locale", "ICU Rules", "Access privileges"),
		{name: "psql \\dn", psql: [2]int{psqlOldest, psqlNewest},
			text: `SELECT n.nspname AS "Name", pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner" FROM pg_catalog.pg_namespace n WHERE $w ORDER BY 1`,
			cols: []catCol{{"Name", oidName}, {"Owner", oidName}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				f, perr := relFilterOf(m[0])
				if perr != nil {
					return nil, perr
				}
				var out [][]any
				for _, sc := range v.schemas {
					if matchAll(f.nsps, sc) {
						out = append(out, []any{sc, v.s.user})
					}
				}
				return out, nil
			}},
		psqlRelations(13, 13, `WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index'`, ""),
		psqlRelations(14, 14, `WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index'`, psqlAmJoin),
		psqlRelations(15, psqlNewest, `WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index'`, psqlAmJoin),
		{name: "psql \\d lookup", psql: [2]int{psqlOldest, psqlNewest},
			text: `SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE $w ORDER BY 2, 3`,
			cols: []catCol{{"oid", oidOID}, {"nspname", oidName}, {"relname", oidName}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				f, perr := relFilterOf(m[0])
				if perr != nil {
					return nil, perr
				}
				// Tables only: what psql then asks of an index is not
				// answered here, so a name that matches only an index is
				// refused rather than answered "not found".
				var out [][]any
				index := false
				for i := range v.rels {
					if r := &v.rels[i]; f.keep(v, r) {
						if r.kind != 'r' {
							index = true
							continue
						}
						out = append(out, []any{r.oid, r.schema, r.name})
					}
				}
				if len(out) == 0 && index {
					return nil, &pgErr{"0A000", "\\d of an index is not answered on this port: the copy has no index; \\d <table> shows a table's primary key"}
				}
				return out, nil
			}},
		psqlTableInfo(`''`, "?column?"),
		psqlTableInfo(`pg_catalog.array_to_string(c.reloptions || array(select 'toast.' || x from pg_catalog.unnest(tc.reloptions) x), ', ')`, "array_to_string"),
		{name: "psql \\d columns", psql: [2]int{psqlOldest, psqlNewest},
			text: `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), a.attnotnull, (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation, a.attidentity, a.attgenerated FROM pg_catalog.pg_attribute a WHERE a.attrelid = $n AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`,
			cols: []catCol{{"attname", oidName}, {"format_type", oidText}, {"pg_get_expr", oidText}, {"attnotnull", oidBool}, {"attcollation", oidName}, {"attidentity", oidChar}, {"attgenerated", oidChar}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				return v.psqlColumns(m[0], false, false), nil
			}},
		// 13 does not ask for a column's compression, which came with 14.
		{name: "psql \\d+ columns", psql: [2]int{13, 13},
			text: `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), a.attnotnull, (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation, a.attidentity, a.attgenerated, a.attstorage, CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget, pg_catalog.col_description(a.attrelid, a.attnum) FROM pg_catalog.pg_attribute a WHERE a.attrelid = $n AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`,
			cols: []catCol{{"attname", oidName}, {"format_type", oidText}, {"pg_get_expr", oidText}, {"attnotnull", oidBool}, {"attcollation", oidName}, {"attidentity", oidChar}, {"attgenerated", oidChar},
				{"attstorage", oidChar}, {"attstattarget", oidInt2}, {"col_description", oidText}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				return v.psqlColumns(m[0], true, false), nil
			}},
		{name: "psql \\d+ columns", psql: [2]int{14, psqlNewest},
			text: `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), a.attnotnull, (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation, a.attidentity, a.attgenerated, a.attstorage, a.attcompression AS attcompression, CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget, pg_catalog.col_description(a.attrelid, a.attnum) FROM pg_catalog.pg_attribute a WHERE a.attrelid = $n AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`,
			cols: []catCol{{"attname", oidName}, {"format_type", oidText}, {"pg_get_expr", oidText}, {"attnotnull", oidBool}, {"attcollation", oidName}, {"attidentity", oidChar}, {"attgenerated", oidChar},
				{"attstorage", oidChar}, {"attcompression", oidChar}, {"attstattarget", oidInt2}, {"col_description", oidText}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				return v.psqlColumns(m[0], true, true), nil
			}},
		{name: "psql \\d indexes", psql: [2]int{13, 17},
			text: `SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x')) WHERE c.oid = $n AND c.oid = i.indrelid AND i.indexrelid = c2.oid ORDER BY i.indisprimary DESC, c2.relname`,
			cols: []catCol{{"relname", oidName}, {"indisprimary", oidBool}, {"indisunique", oidBool}, {"indisclustered", oidBool}, {"indisvalid", oidBool}, {"pg_get_indexdef", oidText},
				{"pg_get_constraintdef", oidText}, {"contype", oidChar}, {"condeferrable", oidBool}, {"condeferred", oidBool}, {"indisreplident", oidBool}, {"reltablespace", oidOID}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				return v.psqlIndexes(m[0]), nil
			}},
		// 18 asks whether the key is a temporal one (WITHOUT OVERLAPS, which
		// a 14 does not have: psql writes the answer into the statement).
		{name: "psql \\d indexes", psql: [2]int{18, psqlNewest},
			text: `SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace, false AS conperiod FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x')) WHERE c.oid = $n AND c.oid = i.indrelid AND i.indexrelid = c2.oid ORDER BY i.indisprimary DESC, c2.relname`,
			cols: []catCol{{"relname", oidName}, {"indisprimary", oidBool}, {"indisunique", oidBool}, {"indisclustered", oidBool}, {"indisvalid", oidBool}, {"pg_get_indexdef", oidText},
				{"pg_get_constraintdef", oidText}, {"contype", oidChar}, {"condeferrable", oidBool}, {"condeferred", oidBool}, {"indisreplident", oidBool}, {"reltablespace", oidOID}, {"conperiod", oidBool}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				rows := v.psqlIndexes(m[0])
				for i := range rows {
					rows[i] = append(rows[i], false)
				}
				return rows, nil
			}},
		// 18 asks for a table's foreign keys, the ones it has and the ones
		// that point at it, whether or not it has any, and \d+ for its
		// named NOT NULL constraints, which a 14 does not keep.
		none("psql \\d foreign keys", `SELECT true as sametable, conname, pg_catalog.pg_get_constraintdef(r.oid, true) as condef, conrelid::pg_catalog.regclass AS ontable FROM pg_catalog.pg_constraint r WHERE r.conrelid = $n AND r.contype = 'f' AND conparentid = 0 ORDER BY conname`,
			catCol{"sametable", oidBool}, catCol{"conname", oidName}, catCol{"condef", oidText}, catCol{"ontable", oidRegclass}).sentBy(18, psqlNewest),
		none("psql \\d referenced by", `SELECT conname, conrelid::pg_catalog.regclass AS ontable, pg_catalog.pg_get_constraintdef(oid, true) AS condef FROM pg_catalog.pg_constraint c WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors($n) UNION ALL VALUES ($n::pg_catalog.regclass)) AND contype = 'f' AND conparentid = 0 ORDER BY conname`,
			catCol{"conname", oidName}, catCol{"ontable", oidRegclass}, catCol{"condef", oidText}).sentBy(18, psqlNewest),
		none("psql \\d+ not-null constraints", `SELECT c.conname, a.attname, c.connoinherit, c.conislocal, c.coninhcount <> 0, c.convalidated FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.conrelid AND a.attnum = c.conkey[1]) WHERE c.contype = 'n' AND c.conrelid = $n::pg_catalog.regclass ORDER BY a.attnum`,
			catCol{"conname", oidName}, catCol{"attname", oidName}, catCol{"connoinherit", oidBool}, catCol{"conislocal", oidBool}, catCol{"?column?", oidBool}, catCol{"convalidated", oidBool}).sentBy(18, psqlNewest),
		none("psql \\d policies", `SELECT pol.polname, pol.polpermissive, CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END, pg_catalog.pg_get_expr(pol.polqual, pol.polrelid), pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid), CASE pol.polcmd WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT' WHEN 'w' THEN 'UPDATE' WHEN 'd' THEN 'DELETE' END AS cmd FROM pg_catalog.pg_policy pol WHERE pol.polrelid = $n ORDER BY 1`,
			catCol{"polname", oidName}, catCol{"polpermissive", oidBool}, catCol{"array_to_string", oidText}, catCol{"pg_get_expr", oidText}, catCol{"pg_get_expr", oidText}, catCol{"cmd", oidText}).sentBy(psqlOldest, psqlNewest),
		// 13 builds the list of a statistics object's columns itself.
		none("psql \\d statistics", `SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace AS nsp, stxname, (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(attname),', ') FROM pg_catalog.unnest(stxkeys) s(attnum) JOIN pg_catalog.pg_attribute a ON (stxrelid = a.attrelid AND a.attnum = s.attnum AND NOT attisdropped)) AS columns, 'd' = any(stxkind) AS ndist_enabled, 'f' = any(stxkind) AS deps_enabled, 'm' = any(stxkind) AS mcv_enabled, stxstattarget FROM pg_catalog.pg_statistic_ext stat WHERE stxrelid = $n ORDER BY 1`,
			catCol{"oid", oidOID}, catCol{"stxrelid", oidRegclass}, catCol{"nsp", oidText}, catCol{"stxname", oidName}, catCol{"columns", oidText}, catCol{"ndist_enabled", oidBool}, catCol{"deps_enabled", oidBool}, catCol{"mcv_enabled", oidBool}, catCol{"stxstattarget", oidInt4}).sentBy(13, 13),
		none("psql \\d statistics", `SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, stxname, pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns, 'd' = any(stxkind) AS ndist_enabled, 'f' = any(stxkind) AS deps_enabled, 'm' = any(stxkind) AS mcv_enabled, stxstattarget FROM pg_catalog.pg_statistic_ext WHERE stxrelid = $n ORDER BY nsp, stxname`,
			catCol{"oid", oidOID}, catCol{"stxrelid", oidRegclass}, catCol{"nsp", oidText}, catCol{"stxname", oidName}, catCol{"columns", oidText}, catCol{"ndist_enabled", oidBool}, catCol{"deps_enabled", oidBool}, catCol{"mcv_enabled", oidBool}, catCol{"stxstattarget", oidInt4}).sentBy(14, psqlNewest),
		// 13 and 14 ask for the publications' names alone.
		none("psql \\d publications", `SELECT pubname FROM pg_catalog.pg_publication p JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid WHERE pr.prrelid = $n UNION ALL SELECT pubname FROM pg_catalog.pg_publication p WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable($n) ORDER BY 1`,
			catCol{"pubname", oidName}).sentBy(13, 14),
		none("psql \\d publications", `SELECT pubname , NULL , NULL FROM pg_catalog.pg_publication p JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid WHERE pr.prrelid = $n UNION ALL SELECT pubname , NULL , NULL FROM pg_catalog.pg_publication p WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable($n) ORDER BY 1`,
			catCol{"pubname", oidName}, catCol{"?column?", oidText}, catCol{"?column?", oidText}).sentBy(15, psqlNewest),
		none("psql \\d parents", `SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid = i.inhparent AND i.inhrelid = $n AND c.relkind != 'p' AND c.relkind != 'I' ORDER BY inhseqno`,
			catCol{"oid", oidRegclass}).sentBy(psqlOldest, psqlNewest),
		// 13 does not ask whether a partition is being detached.
		none("psql \\d partitions", `SELECT c.oid::pg_catalog.regclass, c.relkind, pg_catalog.pg_get_expr(c.relpartbound, c.oid) FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid = i.inhrelid AND i.inhparent = $n ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text`,
			catCol{"oid", oidRegclass}, catCol{"relkind", oidChar}, catCol{"pg_get_expr", oidText}).sentBy(13, 13),
		none("psql \\d partitions", `SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid) FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid = i.inhrelid AND i.inhparent = $n ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text`,
			catCol{"oid", oidRegclass}, catCol{"relkind", oidChar}, catCol{"inhdetachpending", oidBool}, catCol{"pg_get_expr", oidText}).sentBy(14, psqlNewest),

		// ---- JDBC (DatabaseMetaData) ----
		{name: "JDBC getSchemas",
			text: `SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG FROM pg_catalog.pg_namespace WHERE nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_' OR nspname = (pg_catalog.current_schemas(true))[1]) AND (nspname !~ '^pg_toast_temp_' OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))$w ORDER BY TABLE_SCHEM`,
			cols: []catCol{{"table_schem", oidName}, {"table_catalog", oidText}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				re, perr := optionalLike(m[0], "nspname")
				if perr != nil {
					return nil, perr
				}
				var out [][]any
				for _, sc := range v.schemas {
					if re == nil || re.MatchString(sc) {
						out = append(out, []any{sc, nil})
					}
				}
				return out, nil
			}},
		{name: "JDBC getCatalogs",
			text: `SELECT datname AS TABLE_CAT FROM pg_catalog.pg_database WHERE datallowconn = true ORDER BY datname`,
			cols: []catCol{{"table_cat", oidName}},
			rows: func(v *catalogView, _ []string) ([][]any, *pgErr) { return [][]any{{v.database}}, nil }},
		{name: "JDBC getTables",
			text: `SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME, ` + jdbcTableType + `, d.description AS REMARKS, '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0 and d.classoid = 'pg_class'::regclass) WHERE c.relnamespace = n.oid AND $w ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME`,
			cols: []catCol{{"table_cat", oidText}, {"table_schem", oidName}, {"table_name", oidName}, {"table_type", oidText}, {"remarks", oidText}, {"type_cat", oidText}, {"type_schem", oidText}, {"type_name", oidText}, {"self_referencing_col_name", oidText}, {"ref_generation", oidText}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				f, perr := relFilterOf(m[0])
				if perr != nil {
					return nil, perr
				}
				var out [][]any
				for i := range v.rels {
					r := &v.rels[i]
					if !f.keep(v, r) {
						continue
					}
					typ := "TABLE"
					if r.kind == 'i' {
						typ = "INDEX"
					}
					out = append(out, []any{nil, r.schema, r.name, typ, nil, "", "", "", "", ""})
				}
				sort.SliceStable(out, func(i, j int) bool { return out[i][3].(string) < out[j][3].(string) })
				return out, nil
			}},
		{name: "JDBC getColumns",
			text: `SELECT * FROM (SELECT n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,t.typtypmod,row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, nullif(a.attidentity, '') as attidentity,nullif(a.attgenerated, '') as attgenerated,pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') WHERE c.relkind in ('r','p','v','f','m') and a.attnum > 0 AND NOT a.attisdropped$w) c WHERE true$w ORDER BY nspname,c.relname,attnum`,
			cols: []catCol{{"nspname", oidName}, {"relname", oidName}, {"attname", oidName}, {"atttypid", oidOID}, {"attnotnull", oidBool}, {"atttypmod", oidInt4}, {"attlen", oidInt2}, {"typtypmod", oidInt4},
				{"attnum", oidInt8}, {"attidentity", oidChar}, {"attgenerated", oidChar}, {"adsrc", oidText}, {"description", oidText}, {"typbasetype", oidOID}, {"typtype", oidChar}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				f, perr := relFilterOf(strings.TrimPrefix(strings.TrimSpace(m[0]), "and "))
				if perr != nil {
					return nil, perr
				}
				colRe, perr := optionalLike(m[1], "attname")
				if perr != nil {
					return nil, perr
				}
				var out [][]any
				for i := range v.rels {
					r := &v.rels[i]
					if r.kind != 'r' || !f.keep(v, r) {
						continue
					}
					for n, c := range r.table.Columns {
						if colRe != nil && !colRe.MatchString(c.Name) {
							continue
						}
						t, info, _ := typeOf(c)
						out = append(out, []any{r.schema, r.name, c.Name, t.oid, c.NotNull, int(t.typmod), info.length, -1, n + 1,
							nullIfEmpty(c.Identity), nil, nullIfEmpty(c.Default), nil, uint32(0), byte('b')})
					}
				}
				return out, nil
			}},
		{name: "JDBC getPrimaryKeys",
			text: `SELECT result.TABLE_CAT, result.TABLE_SCHEM, result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ, result.PK_NAME FROM (SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME, (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME, information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM FROM pg_catalog.pg_class ct JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid) JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE true$w AND i.indisprimary ) result where result.A_ATTNUM = (result.KEYS).x ORDER BY result.table_name, result.pk_name, result.key_seq`,
			cols: []catCol{{"table_cat", oidText}, {"table_schem", oidName}, {"table_name", oidName}, {"column_name", oidName}, {"key_seq", oidInt4}, {"pk_name", oidName}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				f, perr := relFilterOf(strings.TrimPrefix(strings.TrimSpace(m[0]), "and "))
				if perr != nil {
					return nil, perr
				}
				var out [][]any
				for i := range v.rels {
					r := &v.rels[i]
					if r.kind != 'r' || !f.keep(v, r) {
						continue
					}
					for k, c := range r.table.PrimaryKey {
						out = append(out, []any{nil, r.schema, r.name, c, k + 1, pkeyName(r.table)})
					}
				}
				sort.SliceStable(out, func(i, j int) bool { return out[i][2].(string) < out[j][2].(string) })
				return out, nil
			}},
		{name: "JDBC type is array",
			text: `SELECT typinput='pg_catalog.array_in'::regproc as is_array, typtype, typname, pg_type.oid FROM pg_catalog.pg_type LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r from pg_namespace as ns join ( select s.r, (current_schemas(false))[s.r] as nspname from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r using ( nspname ) ) as sp ON sp.nspoid = typnamespace WHERE pg_type.oid = $n ORDER BY sp.r, pg_type.oid DESC`,
			cols: []catCol{{"is_array", oidBool}, {"typtype", oidChar}, {"typname", oidName}, {"oid", oidOID}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				oid, ok := oidArg(m[0])
				info, known := typeByOID(oid)
				if !ok || !known {
					return nil, nil
				}
				_, isArray := arrayElement[oid]
				return [][]any{{isArray, byte('b'), info.name, oid}}, nil
			}},
		{name: "JDBC type namespace",
			text: `SELECT n.nspname = ANY(current_schemas(true)), n.nspname, t.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = $n`,
			cols: []catCol{{"?column?", oidBool}, {"nspname", oidName}, {"typname", oidName}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				oid, ok := oidArg(m[0])
				info, known := typeByOID(oid)
				if !ok || !known {
					return nil, nil
				}
				return [][]any{{true, "pg_catalog", info.name}}, nil
			}},

		// ---- SQLAlchemy 1.4 (psycopg2) ----
		none("SQLAlchemy hstore", `SELECT t.oid, typarray FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid WHERE typname = 'hstore'`,
			catCol{"oid", oidOID}, catCol{"typarray", oidOID}),
		{name: "SQLAlchemy get_schema_names",
			text: `SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' ORDER BY nspname`,
			cols: []catCol{{"nspname", oidName}},
			rows: func(v *catalogView, _ []string) ([][]any, *pgErr) {
				var out [][]any
				for _, sc := range v.schemas {
					out = append(out, []any{sc})
				}
				return out, nil
			}},
		{name: "SQLAlchemy get_table_names",
			text: `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $s AND $w`,
			cols: []catCol{{"relname", oidName}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				f, perr := relFilterOf(m[1])
				if perr != nil {
					return nil, perr
				}
				var out [][]any
				for i := range v.rels {
					if r := &v.rels[i]; r.schema == unquote(m[0]) && f.keep(v, r) {
						out = append(out, []any{r.name})
					}
				}
				return out, nil
			}},
		{name: "SQLAlchemy table oid",
			text: `SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE $w`,
			cols: []catCol{{"oid", oidOID}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				f, perr := relFilterOf(m[0])
				if perr != nil {
					return nil, perr
				}
				var out [][]any
				for i := range v.rels {
					if r := &v.rels[i]; f.keep(v, r) {
						out = append(out, []any{r.oid})
					}
				}
				return out, nil
			}},
		{name: "SQLAlchemy get_columns",
			text: `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), ( SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef ) AS DEFAULT, a.attnotnull, a.attrelid as table_oid, pgd.description as comment, a.attgenerated as generated, (SELECT json_build_object( 'always', a.attidentity = 'a', 'start', s.seqstart, 'increment', s.seqincrement, 'minvalue', s.seqmin, 'maxvalue', s.seqmax, 'cache', s.seqcache, 'cycle', s.seqcycle) FROM pg_catalog.pg_sequence s JOIN pg_catalog.pg_class c on s.seqrelid = c."oid" WHERE c.relkind = 'S' AND a.attidentity != '' AND s.seqrelid = pg_catalog.pg_get_serial_sequence( a.attrelid::regclass::text, a.attname )::regclass::oid ) as identity_options FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_description pgd ON ( pgd.objoid = a.attrelid AND pgd.objsubid = a.attnum) WHERE a.attrelid = $n AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`,
			cols: []catCol{{"attname", oidName}, {"format_type", oidText}, {"default", oidText}, {"attnotnull", oidBool}, {"table_oid", oidOID}, {"comment", oidText}, {"generated", oidChar}, {"identity_options", oidJSON}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				r := v.relByOID(m[0])
				if r == nil || r.kind != 'r' {
					return nil, nil
				}
				var out [][]any
				for _, c := range r.table.Columns {
					t, info, _ := typeOf(c)
					out = append(out, []any{c.Name, formatType(t, info), nullIfEmpty(c.Default), c.NotNull, r.oid, nil, "", nil})
				}
				return out, nil
			}},
		none("SQLAlchemy domains", `SELECT t.typname as "name", pg_catalog.format_type(t.typbasetype, t.typtypmod) as "attype", not t.typnotnull as "nullable", t.typdefault as "default", pg_catalog.pg_type_is_visible(t.oid) as "visible", n.nspname as "schema" FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE t.typtype = 'd'`,
			catCol{"name", oidName}, catCol{"attype", oidText}, catCol{"nullable", oidBool}, catCol{"default", oidText}, catCol{"visible", oidBool}, catCol{"schema", oidName}),
		none("SQLAlchemy enums", `SELECT t.typname as "name", -- no enum defaults in 8.4 at least
-- t.typdefault as "default",
pg_catalog.pg_type_is_visible(t.oid) as "visible", n.nspname as "schema", e.enumlabel as "label" FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace LEFT JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid WHERE t.typtype = 'e' ORDER BY "schema", "name", e.oid`,
			catCol{"name", oidName}, catCol{"visible", oidBool}, catCol{"schema", oidName}, catCol{"label", oidName}),
		{name: "SQLAlchemy primary key columns",
			text: `SELECT a.attname FROM pg_attribute a JOIN ( SELECT unnest(ix.indkey) attnum, generate_subscripts(ix.indkey, 1) ord FROM pg_index ix WHERE ix.indrelid = $n AND ix.indisprimary ) k ON a.attnum=k.attnum WHERE a.attrelid = $n ORDER BY k.ord`,
			cols: []catCol{{"attname", oidName}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				if unquote(m[0]) != unquote(m[1]) {
					return nil, catalogRefusal
				}
				r := v.relByOID(m[0])
				if r == nil || r.kind != 'r' {
					return nil, nil
				}
				var out [][]any
				for _, c := range r.table.PrimaryKey {
					out = append(out, []any{c})
				}
				return out, nil
			}},
		{name: "SQLAlchemy primary key name",
			text: `SELECT conname FROM pg_catalog.pg_constraint r WHERE r.conrelid = $n AND r.contype = 'p' ORDER BY 1`,
			cols: []catCol{{"conname", oidName}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				r := v.relByOID(m[0])
				if r == nil || r.kind != 'r' || len(r.table.PrimaryKey) == 0 {
					return nil, nil
				}
				return [][]any{{pkeyName(r.table)}}, nil
			}},
		{name: "SQLAlchemy has_table",
			text: `select relname from pg_class c join pg_namespace n on n.oid=c.relnamespace where $w`,
			cols: []catCol{{"relname", oidName}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				w := strings.ReplaceAll(strings.ReplaceAll(m[0], "relname=", "c.relname = "), "n.nspname=", "n.nspname = ")
				f, perr := relFilterOf(w)
				if perr != nil {
					return nil, perr
				}
				var out [][]any
				for i := range v.rels {
					if r := &v.rels[i]; r.kind == 'r' && f.keep(v, r) {
						out = append(out, []any{r.name})
					}
				}
				return out, nil
			}},
		none("SQLAlchemy foreign keys", `SELECT r.conname, pg_catalog.pg_get_constraintdef(r.oid, true) as condef, n.nspname as conschema FROM pg_catalog.pg_constraint r, pg_namespace n, pg_class c WHERE r.conrelid = $n AND r.contype = 'f' AND c.oid = confrelid AND n.oid = c.relnamespace ORDER BY 1`,
			catCol{"conname", oidName}, catCol{"condef", oidText}, catCol{"conschema", oidName}),
		none("SQLAlchemy indexes", `SELECT i.relname as relname, ix.indisunique, ix.indexprs, a.attname, a.attnum, c.conrelid, ix.indkey::varchar, ix.indoption::varchar, i.reloptions, am.amname, pg_get_expr(ix.indpred, ix.indrelid), ix.indnkeyatts as indnkeyatts FROM pg_class t join pg_index ix on t.oid = ix.indrelid join pg_class i on i.oid = ix.indexrelid left outer join pg_attribute a on t.oid = a.attrelid and a.attnum = ANY(ix.indkey) left outer join pg_constraint c on (ix.indrelid = c.conrelid and ix.indexrelid = c.conindid and c.contype in ('p', 'u', 'x')) left outer join pg_am am on i.relam = am.oid WHERE t.relkind IN ('r', 'v', 'f', 'm', 'p') and t.oid = $n and ix.indisprimary = 'f' ORDER BY t.relname, i.relname`,
			catCol{"relname", oidName}, catCol{"indisunique", oidBool}, catCol{"indexprs", oidNodeTree}, catCol{"attname", oidName}, catCol{"attnum", oidInt2}, catCol{"conrelid", oidOID},
			catCol{"indkey", oidVarchar}, catCol{"indoption", oidVarchar}, catCol{"reloptions", oidTextArray}, catCol{"amname", oidName}, catCol{"pg_get_expr", oidText}, catCol{"indnkeyatts", oidInt2}),

		// ---- Grafana ----
		{name: "Grafana version",
			text: `SELECT current_setting('server_version_num')::int/100 as version`,
			cols: []catCol{{"version", oidInt4}},
			rows: func(*catalogView, []string) ([][]any, *pgErr) { return [][]any{{1400}}, nil }},
		{name: "Grafana tables",
			text: `SELECT CASE WHEN ` + grafanaSearchPath + ` THEN quote_ident(table_name) ELSE quote_ident(table_schema) || '.' || quote_ident(table_name) END AS "table" FROM information_schema.tables WHERE quote_ident(table_schema) NOT IN ('information_schema', 'pg_catalog', '_timescaledb_cache', '_timescaledb_catalog', '_timescaledb_internal', '_timescaledb_config', 'timescaledb_information', 'timescaledb_experimental') ORDER BY CASE WHEN ` + grafanaSearchPath + ` THEN 0 ELSE 1 END, 1`,
			cols: []catCol{{"table", oidText}},
			rows: func(v *catalogView, _ []string) ([][]any, *pgErr) {
				var in, out []string
				for i := range v.rels {
					r := &v.rels[i]
					if r.kind != 'r' {
						continue
					}
					if v.visible(r) {
						in = append(in, quoteName(r.name))
					} else {
						out = append(out, quoteName(r.schema)+"."+quoteName(r.name))
					}
				}
				sort.Strings(in)
				sort.Strings(out)
				var rows [][]any
				for _, t := range append(in, out...) {
					rows = append(rows, []any{t})
				}
				return rows, nil
			}},
		{name: "Grafana columns",
			text: `SELECT quote_ident(column_name) AS "column", data_type AS "type" FROM information_schema.columns WHERE CASE WHEN array_length(parse_ident($s),1) = 2 THEN quote_ident(table_schema) = (parse_ident($s))[1] AND quote_ident(table_name) = (parse_ident($s))[2] ELSE quote_ident(table_name) = $s AND ` + grafanaSearchPath + ` END`,
			cols: []catCol{{"column", oidText}, {"type", oidVarchar}},
			rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
				name := unquote(m[0])
				for _, x := range m[1:] {
					if unquote(x) != name {
						return nil, catalogRefusal
					}
				}
				parts, ok := parseIdent(name)
				if !ok {
					return nil, &pgErr{"42601", fmt.Sprintf("string is not a valid identifier: %q", name)}
				}
				var out [][]any
				for i := range v.rels {
					r := &v.rels[i]
					if r.kind != 'r' {
						continue
					}
					switch {
					case len(parts) == 2 && parts[0] == r.schema && parts[1] == r.name:
					case len(parts) == 1 && parts[0] == r.name && v.visible(r):
					default:
						continue
					}
					for _, c := range r.table.Columns {
						_, info, _ := typeOf(c)
						out = append(out, []any{quoteName(c.Name), info.infoTyp})
					}
				}
				return out, nil
			}},
	}
}

// psqlTableInfo is psql's question about one table before it describes it,
// with the column \d asks (detail) and \d+ another.
func psqlTableInfo(detail, detailName string) *catalogStmt {
	return &catalogStmt{name: "psql \\d table " + detailName, psql: [2]int{psqlOldest, psqlNewest},
		text: `SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, ` + detail + `, c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid) LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid) WHERE c.oid = $n`,
		cols: []catCol{{"relchecks", oidInt2}, {"relkind", oidChar}, {"relhasindex", oidBool}, {"relhasrules", oidBool}, {"relhastriggers", oidBool}, {"relrowsecurity", oidBool}, {"relforcerowsecurity", oidBool}, {"relhasoids", oidBool}, {"relispartition", oidBool},
			{detailName, oidText}, {"reltablespace", oidOID}, {"reloftype", oidText}, {"relpersistence", oidChar}, {"relreplident", oidChar}, {"amname", oidName}},
		rows: func(v *catalogView, m []string) ([][]any, *pgErr) {
			r := v.relByOID(m[0])
			if r == nil || r.kind != 'r' {
				return nil, nil
			}
			// No access method: the copy's tables are not heap tables.
			return [][]any{{0, byte('r'), len(r.table.PrimaryKey) > 0, false, false, false, false, false, false, "", uint32(0), "", byte('p'), byte('d'), nil}}, nil
		}}
}

// psqlColumns are \d's rows for the table an OID names, with what \d+ adds
// (verbose) and, there, the column's compression where psql asks for it.
func (v *catalogView) psqlColumns(oid string, verbose, compression bool) [][]any {
	r := v.relByOID(oid)
	if r == nil || r.kind != 'r' {
		return nil
	}
	var out [][]any
	for _, c := range r.table.Columns {
		t, info, _ := typeOf(c)
		row := []any{c.Name, formatType(t, info), nullIfEmpty(c.Default), c.NotNull, nil, c.Identity, ""}
		if verbose {
			row = append(row, info.storage)
			if compression {
				row = append(row, "")
			}
			row = append(row, nil, nil)
		}
		out = append(out, row)
	}
	return out
}

// psqlIndexes are \d's rows for the indexes of the table an OID names: its
// primary key's, the one index the catalog has.
func (v *catalogView) psqlIndexes(oid string) [][]any {
	r := v.relByOID(oid)
	if r == nil || r.kind != 'r' || len(r.table.PrimaryKey) == 0 {
		return nil
	}
	t := r.table
	cols := columnsList(t.PrimaryKey)
	def := "CREATE UNIQUE INDEX " + quoteName(pkeyName(t)) + " ON " + quoteName(t.Schema) + "." + quoteName(t.Name) + " USING btree (" + cols + ")"
	return [][]any{{pkeyName(t), true, true, false, true, def, "PRIMARY KEY (" + cols + ")", byte('p'), false, false, false, uint32(0)}}
}

// none is a recognized statement whose answer is no rows: what it asks
// about (a policy, an inherited table, a foreign key, an enum) the copy has
// none of.
func none(name, text string, cols ...catCol) *catalogStmt {
	return &catalogStmt{name: name, text: text, cols: cols, rows: func(*catalogView, []string) ([][]any, *pgErr) { return nil, nil }}
}

// optionalLike reads " AND <col> LIKE '<pattern>'" when it is there.
func optionalLike(w, col string) (*regexp.Regexp, *pgErr) {
	w = strings.TrimSpace(w)
	if w == "" {
		return nil, nil
	}
	m := regexp.MustCompile(`^and ` + regexp.QuoteMeta(col) + ` like ` + litRe + `$`).FindStringSubmatch(w)
	if m == nil {
		return nil, catalogRefusal
	}
	return likeRegexp(unquote(m[1]))
}

// oidArg reads an OID argument: a number, a quoted one, or a negative one
// in parentheses (never an OID).
func oidArg(s string) (uint32, bool) {
	s = strings.Trim(s, "'")
	var n uint64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint64(c-'0')
		if n > 1<<32-1 {
			return 0, false
		}
	}
	return uint32(n), s != ""
}

// parseIdent is PostgreSQL's parse_ident: names separated by dots, a
// quoted one as written and an unquoted one lowercased.
func parseIdent(s string) ([]string, bool) {
	var parts []string
	for i := 0; ; {
		var name string
		if i < len(s) && s[i] == '"' {
			q, n, ok := readQuoted(s[i:], '"', false)
			if !ok {
				return nil, false
			}
			name, i = q, i+n
		} else {
			j := i
			for j < len(s) && s[j] != '.' {
				j++
			}
			name, i = strings.ToLower(strings.TrimSpace(s[i:j])), j
			if name == "" {
				return nil, false
			}
		}
		parts = append(parts, name)
		if i == len(s) {
			return parts, true
		}
		if s[i] != '.' {
			return nil, false
		}
		i++
	}
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
