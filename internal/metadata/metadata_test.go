package metadata

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestDetectFlavor verifies VERSION()-string flavor detection: any string
// containing "MariaDB" (case-insensitive) is mariadb, everything else (incl.
// Percona) is mysql, and a query error returns "" (unknown) rather than a
// fabricated flavor.
func TestDetectFlavor(t *testing.T) {
	cases := []struct {
		name     string
		version  string
		queryErr bool
		want     string
	}{
		{"mysql 8.0", "8.0.36", false, "mysql"},
		{"percona", "8.0.36-28", false, "mysql"},
		{"mariadb 11.4", "11.4.10-MariaDB-1:11.4.10+maria~ubu2404", false, "mariadb"},
		{"mariadb 10.6", "10.6.18-MariaDB", false, "mariadb"},
		{"query error returns unknown (empty)", "", true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			exp := mock.ExpectQuery("SELECT VERSION()")
			if tc.queryErr {
				exp.WillReturnError(errors.New("connection refused"))
			} else {
				exp.WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow(tc.version))
			}

			if got := DetectFlavor(db); got != tc.want {
				t.Errorf("DetectFlavor(%q) = %q, want %q", tc.version, got, tc.want)
			}
		})
	}
}

// buildTestResolver constructs a Resolver directly without a database,
// allowing MapRow and Resolve to be tested without a MySQL connection.
func buildTestResolver(tables map[string]*TableMeta) *Resolver {
	return &Resolver{snapshotID: 1, tables: tables}
}

func TestResolve_found(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{
		"mydb.orders": {
			Schema: "mydb", Table: "orders",
			Columns: []ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
				{Name: "status", OrdinalPosition: 2, DataType: "varchar"},
			},
			PKColumns: []string{"id"},
		},
	})

	tm, err := r.Resolve("mydb", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tm.Table != "orders" {
		t.Errorf("expected table=orders, got %q", tm.Table)
	}
	if len(tm.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(tm.Columns))
	}
}

func TestResolve_notFound(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{})

	_, err := r.Resolve("mydb", "missing")
	if err == nil {
		t.Fatal("expected error for unknown table, got nil")
	}
}

func TestMapRow_success(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{
		"mydb.orders": {
			Schema: "mydb", Table: "orders",
			Columns: []ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
				{Name: "status", OrdinalPosition: 2, DataType: "varchar"},
				{Name: "amount", OrdinalPosition: 3, DataType: "decimal"},
			},
			PKColumns: []string{"id"},
		},
	})

	row := []any{int64(42), "shipped", 99.95}
	named, err := r.MapRow("mydb", "orders", row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if named["id"] != int64(42) {
		t.Errorf("id: want 42, got %v", named["id"])
	}
	if named["status"] != "shipped" {
		t.Errorf("status: want shipped, got %v", named["status"])
	}
	if named["amount"] != 99.95 {
		t.Errorf("amount: want 99.95, got %v", named["amount"])
	}
}

// TestMapRow_binaryToBytes verifies the #756 fix: go-mysql hands BINARY/
// VARBINARY values back as a raw Go string with no charset applied, and a
// value with the high bit set (an MD5 digest, a binary UUID) is frequently
// invalid UTF-8. Before the fix, that string reached json.Marshal unchanged
// and every invalid byte was silently replaced with U+FFFD. MapRow must now
// reinterpret it as []byte instead, which routes it through marshalRow's
// existing []byte-to-base64 path (byte-perfect, no corruption).
func TestMapRow_binaryToBytes(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{
		"mydb.users": {
			Schema: "mydb", Table: "users",
			Columns: []ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "binary", ColumnType: "binary(16)"},
				{Name: "token", OrdinalPosition: 2, DataType: "varbinary", ColumnType: "varbinary(16)"},
			},
			PKColumns: []string{"id"},
		},
	})

	// An MD5-digest-shaped value with the high bit set on several bytes —
	// invalid UTF-8 (a lone 0xFF is never a valid UTF-8 sequence).
	rawID := string([]byte{0x00, 0x01, 0xFF, 0xFE, 0x7F, 0x80, 0x81, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})
	rawToken := string([]byte{0xDE, 0xAD, 0xBE, 0xEF})

	named, err := r.MapRow("mydb", "users", []any{rawID, rawToken})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	gotID, ok := named["id"].([]byte)
	if !ok {
		t.Fatalf("id: want []byte, got %T", named["id"])
	}
	if string(gotID) != rawID {
		t.Errorf("id: bytes not preserved: got %x, want %x", gotID, rawID)
	}
	gotToken, ok := named["token"].([]byte)
	if !ok {
		t.Fatalf("token: want []byte, got %T", named["token"])
	}
	if string(gotToken) != rawToken {
		t.Errorf("token: bytes not preserved: got %x, want %x", gotToken, rawToken)
	}
}

// TestMapRow_latin1Transcoding verifies scenario 1 from #756: a legacy latin1
// CHAR/VARCHAR value ("José", stored as cp1252 bytes — MySQL's "latin1" is
// actually Windows-1252, not ISO-8859-1) is transcoded to valid UTF-8 instead
// of being corrupted by json.Marshal's silent U+FFFD replacement.
func TestMapRow_latin1Transcoding(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{
		"mydb.customers": {
			Schema: "mydb", Table: "customers",
			Columns: []ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
				{Name: "name", OrdinalPosition: 2, DataType: "varchar", CharacterSet: "latin1"},
			},
			PKColumns: []string{"id"},
		},
	})

	// "José" encoded as cp1252/latin1: J, o, s, 0xE9 ('é'), unlike its UTF-8
	// encoding (0xC3 0xA9), so this string is invalid UTF-8 as-is.
	rawName := "Jos" + string([]byte{0xE9})

	named, err := r.MapRow("mydb", "customers", []any{int64(1), rawName})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if named["name"] != "José" {
		t.Errorf("name: want %q, got %q", "José", named["name"])
	}
}

// TestMapRow_invalidUTF8UnknownCharset_failsLoud verifies the "at minimum"
// fallback from #756: a CHAR/VARCHAR value that is not valid UTF-8 and whose
// column has no captured (or unsupported) character set is rejected with an
// error rather than silently corrupted by json.Marshal. Callers (parser.go)
// already warn-and-skip a MapRow error, turning this into a loud, actionable
// log line instead of silent at-rest data loss.
func TestMapRow_invalidUTF8UnknownCharset_failsLoud(t *testing.T) {
	invalid := string([]byte{0xFF, 0xFE})

	cases := []struct {
		name         string
		characterSet string
	}{
		{"pre-#756 snapshot: no character set captured", ""},
		{"unsupported charset", "cp1251"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := buildTestResolver(map[string]*TableMeta{
				"mydb.legacy": {
					Schema: "mydb", Table: "legacy",
					Columns: []ColumnMeta{
						{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
						{Name: "name", OrdinalPosition: 2, DataType: "varchar", CharacterSet: tc.characterSet},
					},
					PKColumns: []string{"id"},
				},
			})
			_, err := r.MapRow("mydb", "legacy", []any{int64(1), invalid})
			if err == nil {
				t.Fatal("expected error for invalid UTF-8 with no safe transcoding, got nil")
			}
		})
	}
}

func TestCoerceTextEncoding(t *testing.T) {
	valid := "shipped"
	invalid := string([]byte{0xFF})

	cases := []struct {
		name    string
		v       any
		col     ColumnMeta
		want    any
		wantErr bool
	}{
		{"binary string reinterpreted as bytes", "\x00\xff", ColumnMeta{DataType: "binary"}, []byte("\x00\xff"), false},
		{"varbinary string reinterpreted as bytes", "\xde\xad", ColumnMeta{DataType: "varbinary"}, []byte("\xde\xad"), false},
		{"binary nil passthrough", nil, ColumnMeta{DataType: "binary"}, nil, false},
		{"valid utf8 varchar unchanged", valid, ColumnMeta{DataType: "varchar", CharacterSet: "utf8mb4"}, valid, false},
		{"valid utf8 varchar unchanged even with empty charset", valid, ColumnMeta{DataType: "varchar"}, valid, false},
		{"invalid utf8 latin1 transcoded", "Jos" + string([]byte{0xE9}), ColumnMeta{DataType: "varchar", CharacterSet: "latin1"}, "José", false},
		{"invalid utf8 empty charset fails", invalid, ColumnMeta{DataType: "varchar"}, nil, true},
		{"invalid utf8 unsupported charset fails", invalid, ColumnMeta{DataType: "char", CharacterSet: "koi8r"}, nil, true},
		// charmap.Windows1252's decoder is total (never returns an error), so
		// the 5 cp1252-undefined byte positions (0x81, 0x8D, 0x8F, 0x90, 0x9D)
		// would otherwise decode "successfully" straight to U+FFFD — the exact
		// silent-corruption class #756 exists to close. coerceTextEncoding
		// must catch this itself and fail loud instead.
		{"latin1 undefined cp1252 byte 0x81 fails loud", string([]byte{0x81}), ColumnMeta{DataType: "varchar", CharacterSet: "latin1"}, nil, true},
		{"latin1 undefined cp1252 byte 0x9d fails loud", string([]byte{0x9D}), ColumnMeta{DataType: "char", CharacterSet: "latin1"}, nil, true},
		// The reverse of the above: genuine cp1252-only printable characters
		// (0x80-0x9F range that diverges from plain ISO-8859-1, e.g. the euro
		// sign and a left double quotation mark) must transcode cleanly, not
		// be mistaken for the undefined-byte case.
		{"latin1 cp1252 euro sign transcoded", string([]byte{0x80}), ColumnMeta{DataType: "varchar", CharacterSet: "latin1"}, "€", false},
		{"latin1 cp1252 left double quote transcoded", string([]byte{0x93}), ColumnMeta{DataType: "varchar", CharacterSet: "latin1"}, "“", false},
		{"non-char/binary type untouched", int64(42), ColumnMeta{DataType: "int"}, int64(42), false},
		{"non-string value on varchar passes through", int64(42), ColumnMeta{DataType: "varchar"}, int64(42), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := coerceTextEncoding(tc.v, tc.col)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (result %#v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			switch want := tc.want.(type) {
			case []byte:
				gb, ok := got.([]byte)
				if !ok || string(gb) != string(want) {
					t.Errorf("got %#v, want %#v", got, want)
				}
			default:
				if got != tc.want {
					t.Errorf("got %#v, want %#v", got, tc.want)
				}
			}
		})
	}
}

func TestCoerceUnsigned(t *testing.T) {
	const maxU64 = uint64(18446744073709551615) // 2^64-1; int64(-1) bit pattern

	cases := []struct {
		name string
		v    any
		col  ColumnMeta
		want any
	}{
		// Unsigned columns with the high bit set: reinterpret to the correct width.
		{"bigint unsigned max", int64(-1), ColumnMeta{DataType: "bigint", ColumnType: "bigint unsigned"}, maxU64},
		{"int unsigned max", int32(-1), ColumnMeta{DataType: "int", ColumnType: "int unsigned"}, uint32(4294967295)},
		{"mediumint unsigned max (24-bit mask)", int32(-1), ColumnMeta{DataType: "mediumint", ColumnType: "mediumint unsigned"}, uint32(16777215)},
		{"smallint unsigned max", int16(-1), ColumnMeta{DataType: "smallint", ColumnType: "smallint unsigned"}, uint16(65535)},
		{"tinyint unsigned max", int8(-1), ColumnMeta{DataType: "tinyint", ColumnType: "tinyint unsigned"}, uint8(255)},
		// Unsigned but value below the threshold: numerically unchanged (still converted type).
		{"int unsigned small positive", int32(1000000), ColumnMeta{DataType: "int", ColumnType: "int unsigned"}, uint32(1000000)},
		{"tinyint unsigned 200", int8(-56), ColumnMeta{DataType: "tinyint", ColumnType: "tinyint unsigned"}, uint8(200)},
		// Signed column: never touched.
		{"signed bigint negative", int64(-1), ColumnMeta{DataType: "bigint", ColumnType: "bigint(20)"}, int64(-1)},
		// Pre-#212 snapshot: ColumnType empty → cannot know signedness → no-op.
		{"empty column type", int64(-1), ColumnMeta{DataType: "bigint", ColumnType: ""}, int64(-1)},
		// NULL and non-integer values on an unsigned column are returned unchanged.
		{"null on unsigned", nil, ColumnMeta{DataType: "bigint", ColumnType: "bigint unsigned"}, nil},
		{"string on unsigned", "x", ColumnMeta{DataType: "bigint", ColumnType: "bigint unsigned"}, "x"},
		// ENUM whose value list literally contains "unsigned" matches the substring
		// gate, but go-mysql decodes ENUM as int64, so it reaches the DataType switch
		// (DataType "enum" ∉ integer widths) — which must leave it untouched. This
		// locks that load-bearing default so a future refactor can't coerce it.
		{"enum value list contains 'unsigned'", int64(2), ColumnMeta{DataType: "enum", ColumnType: "enum('signed','unsigned')"}, int64(2)},
		// BIT: go-mysql decodes it as int64, so BIT(64) with the high bit set is
		// negative; reinterpret as uint64 (#497). BIT(<64) values are already
		// positive (identity); NULL passes through.
		{"bit(64) all bits set", int64(-1), ColumnMeta{DataType: "bit", ColumnType: "bit(64)"}, maxU64},
		{"bit(64) high bit only", int64(-9223372036854775808), ColumnMeta{DataType: "bit", ColumnType: "bit(64)"}, uint64(9223372036854775808)},
		{"bit(8) positive unchanged", int64(200), ColumnMeta{DataType: "bit", ColumnType: "bit(8)"}, uint64(200)},
		{"bit null passthrough", nil, ColumnMeta{DataType: "bit", ColumnType: "bit(64)"}, nil},
		// SET: go-mysql decodes the member bitmask as int64, so a 64-member SET
		// with member 64 active comes back negative — same class as BIT(64),
		// reinterpret as uint64 (#846). Smaller bitmasks are already positive
		// (identity); NULL passes through.
		{"set 64 members, member 64 active", int64(-9223372036854775808), ColumnMeta{DataType: "set", ColumnType: "set('m1','m64')"}, uint64(9223372036854775808)},
		{"set 64 members, all active", int64(-1), ColumnMeta{DataType: "set", ColumnType: "set('m1','m64')"}, maxU64},
		{"set small bitmask unchanged", int64(5), ColumnMeta{DataType: "set", ColumnType: "set('a','b','c')"}, uint64(5)},
		{"set null passthrough", nil, ColumnMeta{DataType: "set", ColumnType: "set('a','b')"}, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := coerceUnsigned(tc.v, tc.col)
			if got != tc.want {
				t.Errorf("coerceUnsigned(%#v, %q) = %#v (%T); want %#v (%T)",
					tc.v, tc.col.ColumnType, got, got, tc.want, tc.want)
			}
		})
	}
}

func TestMapRow_unsignedCoercion(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{
		"mydb.counters": {
			Schema: "mydb", Table: "counters",
			Columns: []ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "bigint", ColumnType: "bigint unsigned"},
				{Name: "n", OrdinalPosition: 2, DataType: "int", ColumnType: "int unsigned"},
			},
			PKColumns: []string{"id"},
		},
	})

	// go-mysql would hand us these signed values for an unsigned PK and column.
	row := []any{int64(-1), int32(-1)}
	named, err := r.MapRow("mydb", "counters", row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if named["id"] != uint64(18446744073709551615) {
		t.Errorf("id: want uint64 max, got %#v (%T)", named["id"], named["id"])
	}
	if named["n"] != uint32(4294967295) {
		t.Errorf("n: want uint32 max, got %#v (%T)", named["n"], named["n"])
	}
}

func TestMapRow_columnCountMismatch(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{
		"mydb.orders": {
			Schema: "mydb", Table: "orders",
			Columns: []ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
				{Name: "status", OrdinalPosition: 2, DataType: "varchar"},
			},
			PKColumns: []string{"id"},
		},
	})

	// Row has 3 values but snapshot only has 2 columns — should error.
	row := []any{int64(42), "shipped", "extra"}
	_, err := r.MapRow("mydb", "orders", row)
	if err == nil {
		t.Fatal("expected error for column count mismatch, got nil")
	}
}

func TestPKColumnMetas_ordering(t *testing.T) {
	tm := &TableMeta{
		Columns: []ColumnMeta{
			{Name: "seq", OrdinalPosition: 2, IsPK: true},
			{Name: "id", OrdinalPosition: 1, IsPK: true},
			{Name: "note", OrdinalPosition: 3, IsPK: false},
		},
	}

	pks := tm.PKColumnMetas()
	if len(pks) != 2 {
		t.Fatalf("expected 2 PK columns, got %d", len(pks))
	}
	// PKColumnMetas preserves Columns slice order (ordinal order, as loaded from DB).
	// The slice above is already in ordinal order — verify both columns are included.
	names := map[string]bool{}
	for _, c := range pks {
		names[c.Name] = true
	}
	if !names["seq"] || !names["id"] {
		t.Errorf("expected both id and seq in PK columns, got %v", pks)
	}
}

// TestResolverTables pins the behaviour of Resolver.Tables added for
// #315: returns every TableMeta whose Schema matches, sorted by Table
// name, and excludes tables from other schemas in the same snapshot.
// An unknown schema returns an empty (non-nil) slice.
func TestResolverTables(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{
		"appdb.users":    {Schema: "appdb", Table: "users"},
		"appdb.orders":   {Schema: "appdb", Table: "orders"},
		"appdb.products": {Schema: "appdb", Table: "products"},
		"otherdb.audits": {Schema: "otherdb", Table: "audits"},
		"otherdb.events": {Schema: "otherdb", Table: "events"},
	})

	t.Run("appdb_returns_three_tables_sorted", func(t *testing.T) {
		got := r.Tables("appdb")
		if len(got) != 3 {
			t.Fatalf("len = %d, want 3", len(got))
		}
		want := []string{"orders", "products", "users"}
		for i, tm := range got {
			if tm.Table != want[i] {
				t.Errorf("got[%d] = %q, want %q (sort regression)", i, tm.Table, want[i])
			}
		}
	})

	t.Run("otherdb_returns_two_tables_sorted", func(t *testing.T) {
		got := r.Tables("otherdb")
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
		if got[0].Table != "audits" || got[1].Table != "events" {
			t.Errorf("got %v, want [audits events]", got)
		}
	})

	t.Run("unknown_schema_returns_empty_non_nil_slice", func(t *testing.T) {
		got := r.Tables("nope")
		if got == nil {
			t.Error("Tables(unknown) returned nil; want empty (non-nil) slice")
		}
		if len(got) != 0 {
			t.Errorf("len = %d, want 0", len(got))
		}
	})
}

// When schemas are named explicitly, the scan is scoped to exactly those
// schemas via a parameterized IN list — and the bintrail-internal exclusion is
// NOT added (an explicitly named internal schema is still policed).
func TestBuildFKCascadeQuery_withSchemas(t *testing.T) {
	query, args := buildFKCascadeQuery([]string{"iotcore", "billing"})

	if !strings.Contains(query, "CONSTRAINT_SCHEMA IN (?,?)") {
		t.Errorf("expected parameterized IN list, got query:\n%s", query)
	}
	if strings.Contains(query, "NOT IN") || strings.Contains(query, "information_schema.TABLES") {
		t.Errorf("explicit --schemas must not add the internal-schema exclusion, got query:\n%s", query)
	}
	if len(args) != 2 || args[0] != "iotcore" || args[1] != "billing" {
		t.Errorf("expected args [iotcore billing], got %v", args)
	}
}

// The pre-flight must match every cascading referential action recover-cascade
// synthesizes — SET NULL included, on BOTH rules (#1125). Matching CASCADE only
// silently under-reports schemas whose FKs use ON DELETE / ON UPDATE SET NULL.
func TestBuildFKCascadeQuery_matchesSetNull(t *testing.T) {
	query, _ := buildFKCascadeQuery(nil)

	for _, want := range []string{
		"DELETE_RULE IN ('CASCADE', 'SET NULL')",
		"UPDATE_RULE IN ('CASCADE', 'SET NULL')",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("expected query to match %s, got query:\n%s", want, query)
		}
	}
}

// With no schemas filter the scan excludes MySQL system schemas AND bintrail's
// own index schemas. The latter are recognised structurally — a schema is
// bintrail-internal only if it holds all of binlog_events, schema_snapshots and
// stream_state — not by name, so an agent does not fatal-fail on bintrail's own
// index FK cascades regardless of how the index DB is named (#347/#365).
func TestBuildFKCascadeQuery_noSchemasExcludesInternal(t *testing.T) {
	query, args := buildFKCascadeQuery(nil)

	if len(args) != 0 {
		t.Errorf("expected no args for unscoped query, got %v", args)
	}
	for _, want := range []string{"'mysql'", "'information_schema'", "'performance_schema'", "'sys'"} {
		if !strings.Contains(query, want) {
			t.Errorf("expected unscoped query to exclude system schema %s, got query:\n%s", want, query)
		}
	}
	// Structural detection: subquery over information_schema.TABLES requiring all
	// three signature tables (HAVING COUNT(DISTINCT TABLE_NAME) = 3).
	for _, want := range []string{
		"information_schema.TABLES",
		"'binlog_events'", "'schema_snapshots'", "'stream_state'",
		"GROUP BY TABLE_SCHEMA HAVING COUNT(DISTINCT TABLE_NAME) = 3",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("expected unscoped query to contain %q, got query:\n%s", want, query)
		}
	}
}

func TestHasReplPrivileges(t *testing.T) {
	tests := []struct {
		name       string
		grants     []string
		wantSlave  bool
		wantClient bool
	}{
		{
			name:       "both privileges",
			grants:     []string{"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%'"},
			wantSlave:  true,
			wantClient: true,
		},
		{
			name:       "all privileges",
			grants:     []string{"GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost'"},
			wantSlave:  true,
			wantClient: true,
		},
		{
			name:       "only slave",
			grants:     []string{"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'"},
			wantSlave:  true,
			wantClient: false,
		},
		{
			name:       "only client",
			grants:     []string{"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'"},
			wantSlave:  false,
			wantClient: true,
		},
		{
			name:       "no replication privileges",
			grants:     []string{"GRANT SELECT ON mydb.* TO 'reader'@'%'"},
			wantSlave:  false,
			wantClient: false,
		},
		{
			name: "across multiple grant lines",
			grants: []string{
				"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'",
				"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'",
			},
			wantSlave:  true,
			wantClient: true,
		},
		{
			name: "mixed with other privileges",
			grants: []string{
				"GRANT SELECT, INSERT ON mydb.* TO 'user'@'%'",
				"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%'",
			},
			wantSlave:  true,
			wantClient: true,
		},
		{
			name:       "empty grants",
			grants:     nil,
			wantSlave:  false,
			wantClient: false,
		},
		{
			name:       "case insensitive",
			grants:     []string{"GRANT replication slave, replication client ON *.* TO 'user'@'%'"},
			wantSlave:  true,
			wantClient: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSlave, gotClient := HasReplPrivileges(tt.grants)
			if gotSlave != tt.wantSlave {
				t.Errorf("slave = %v, want %v", gotSlave, tt.wantSlave)
			}
			if gotClient != tt.wantClient {
				t.Errorf("client = %v, want %v", gotClient, tt.wantClient)
			}
		})
	}
}

// ─── #1033: corrupt snapshot with duplicated column rows ─────────────────────

// snapshotCols is the column set of NewResolver's schema_snapshots SELECT.
var snapshotCols = []string{
	"schema_name", "table_name", "column_name", "ordinal_position",
	"column_key", "data_type", "column_type",
	"is_generated", "is_identity_always", "character_set_name", "is_nullable",
}

func TestNewResolver_dedupesIdenticalDuplicateRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Pre-#844 concurrent writers: every column double-inserted verbatim.
	rows := sqlmock.NewRows(snapshotCols)
	for range 2 {
		rows.AddRow("mydb", "wp_options", "option_id", 1, "PRI", "bigint", "bigint unsigned", false, false, "", "YES")
	}
	for range 2 {
		rows.AddRow("mydb", "wp_options", "option_name", 2, "", "varchar", "varchar(191)", false, false, "utf8mb4", "YES")
	}
	for range 2 {
		rows.AddRow("mydb", "wp_options", "option_value", 3, "", "longtext", "longtext", false, false, "utf8mb4", "YES")
	}
	for range 2 {
		rows.AddRow("mydb", "wp_options", "autoload", 4, "", "varchar", "varchar(20)", false, false, "utf8mb4", "YES")
	}
	mock.ExpectQuery("SELECT schema_name, table_name").WithArgs(12).WillReturnRows(rows)
	mock.ExpectQuery(`SELECT MIN\(snapshot_time\)`).WithArgs(12).
		WillReturnRows(sqlmock.NewRows([]string{"MIN(snapshot_time)"}).AddRow(time.Date(2026, 7, 4, 15, 2, 39, 0, time.UTC)))

	r, err := NewResolver(db, 12)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}
	tm, err := r.Resolve("mydb", "wp_options")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(tm.Columns) != 4 {
		t.Errorf("expected 4 deduplicated columns, got %d", len(tm.Columns))
	}
	if len(tm.PKColumns) != 1 || tm.PKColumns[0] != "option_id" {
		t.Errorf("expected PKColumns [option_id], got %v", tm.PKColumns)
	}
}

func TestNewResolver_conflictingDuplicateRowsFailLoud(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows(snapshotCols).
		AddRow("mydb", "orders", "id", 1, "PRI", "int", "int", false, false, "", "YES").
		AddRow("mydb", "orders", "name", 2, "", "varchar", "varchar(50)", false, false, "utf8mb4", "YES").
		AddRow("mydb", "orders", "renamed", 2, "", "varchar", "varchar(50)", false, false, "utf8mb4", "YES")
	mock.ExpectQuery("SELECT schema_name, table_name").WithArgs(13).WillReturnRows(rows)
	mock.ExpectQuery(`SELECT MIN\(snapshot_time\)`).WithArgs(13).
		WillReturnRows(sqlmock.NewRows([]string{"MIN(snapshot_time)"}).AddRow(time.Date(2026, 7, 4, 15, 2, 39, 0, time.UTC)))

	_, err = NewResolver(db, 13)
	if err == nil {
		t.Fatal("expected error for conflicting duplicate ordinal rows, got nil")
	}
	if !strings.Contains(err.Error(), "corrupt") || !strings.Contains(err.Error(), "mydb.orders") {
		t.Errorf("error should name the corruption and table, got: %v", err)
	}
}

func TestNewResolver_sameNameDifferentTypeDuplicateFailsLoud(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// The realistic pre-#844 conflict: two writers straddling a DDL capture
	// the SAME column name at the same ordinal with different type metadata.
	// Pins the full-struct comparison — a name-only dedupe would silently
	// load the wrong signedness/charset.
	rows := sqlmock.NewRows(snapshotCols).
		AddRow("mydb", "orders", "id", 1, "PRI", "int", "int", false, false, "", "YES").
		AddRow("mydb", "orders", "qty", 2, "", "int", "int", false, false, "", "YES").
		AddRow("mydb", "orders", "qty", 2, "", "int", "int unsigned", false, false, "", "YES")
	mock.ExpectQuery("SELECT schema_name, table_name").WithArgs(14).WillReturnRows(rows)
	mock.ExpectQuery(`SELECT MIN\(snapshot_time\)`).WithArgs(14).
		WillReturnRows(sqlmock.NewRows([]string{"MIN(snapshot_time)"}).AddRow(time.Date(2026, 7, 4, 15, 2, 39, 0, time.UTC)))

	_, err = NewResolver(db, 14)
	if err == nil {
		t.Fatal("expected error for same-name different-type duplicate, got nil")
	}
	if !strings.Contains(err.Error(), "corrupt") || !strings.Contains(err.Error(), "int unsigned") {
		t.Errorf("error should name the corruption and the diverging column types, got: %v", err)
	}
}
