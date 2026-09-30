package reconstruct

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The key from the RDS for MariaDB test: a UUID primary key the index stores
// as its bytes, spelled in hex because they are not valid UTF-8.
const (
	uuidText   = "cc5c4e6e-bc5a-11f1-9a0c-0affd251cac9"
	uuidStored = "0xCC5C4E6EBC5A11F19A0C0AFFD251CAC9"
)

func TestSpellPKFilter(t *testing.T) {
	u := []metadata.ColumnMeta{colMeta("sid", "uuid", "uuid")}

	for _, typed := range []string{uuidText, strings.ToUpper(uuidText), "cc5c4e6ebc5a11f19a0c0affd251cac9", "0xcc5c4e6ebc5a11f19a0c0affd251cac9"} {
		o := query.Options{PKValues: typed}
		spellPKFilter(&o, u)
		if o.PKValues != uuidStored || o.PKValuesAlt != typed {
			t.Errorf("typed %q: PKValues %q alt %q, want the stored %q first and the typed form as the alternate", typed, o.PKValues, o.PKValuesAlt, uuidStored)
		}
	}

	// Already the stored spelling: nothing changes, and an alternate a caller
	// set (the #957 escape form) is kept.
	o := query.Options{PKValues: uuidStored, PKValuesAlt: "keep"}
	spellPKFilter(&o, u)
	if o.PKValues != uuidStored || o.PKValuesAlt != "keep" {
		t.Errorf("stored spelling was touched: %q / %q", o.PKValues, o.PKValuesAlt)
	}

	// A CHAR(36) key holding UUID text is stored as that text: untouched.
	o = query.Options{PKValues: uuidText}
	spellPKFilter(&o, []metadata.ColumnMeta{colMeta("sid", "char", "char(36)")})
	if o.PKValues != uuidText || o.PKValuesAlt != "" {
		t.Errorf("a CHAR(36) key was re-spelled: %q / %q", o.PKValues, o.PKValuesAlt)
	}

	// Composite: only the UUID component changes.
	comp := []metadata.ColumnMeta{colMeta("sid", "uuid", "uuid"), colMeta("n", "int", "int")}
	o = query.Options{PKValues: uuidText + "|7", PKValuesAlt: event0Escaped(uuidText + "|7")}
	spellPKFilter(&o, comp)
	if o.PKValues != uuidStored+"|7" || o.PKValuesAlt != uuidText+"|7" {
		t.Errorf("composite: %q / %q", o.PKValues, o.PKValuesAlt)
	}

	// --pks: the stored spellings are added beside the typed ones, and the map
	// lets grouped output find a typed label's rows.
	o = query.Options{PKValuesIn: []string{uuidText, uuidStored, "not-a-uuid"}}
	spelled := spellPKFilter(&o, u)
	if want := []string{uuidText, uuidStored, "not-a-uuid"}; !slices.Equal(o.PKValuesIn, want) {
		t.Errorf("PKValuesIn = %q, want %q (the stored spelling listed once)", o.PKValuesIn, want)
	}
	if spelled[uuidText] != uuidStored || len(spelled) != 1 {
		t.Errorf("spelled = %v, want only %s -> %s", spelled, uuidText, uuidStored)
	}
	// Only the text form typed: the stored spelling must be added, or the
	// fetch looks for the text alone and finds nothing.
	o = query.Options{PKValuesIn: []string{uuidText}}
	spellPKFilter(&o, u)
	if want := []string{uuidText, uuidStored}; !slices.Equal(o.PKValuesIn, want) {
		t.Errorf("PKValuesIn = %q, want %q", o.PKValuesIn, want)
	}
}

func event0Escaped(s string) string { return strings.ReplaceAll(s, "|", `\|`) }

// snapshotRowCols is the column list of NewResolver's schema_snapshots SELECT.
var snapshotRowCols = []string{"schema_name", "table_name", "column_name", "ordinal_position", "column_key",
	"data_type", "column_type", "is_generated", "is_identity_always", "character_set_name", "is_nullable"}

func TestSpellIndexPKFilter_readsTheSnapshot(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT snapshot_id, schema_name, table_name FROM schema_snapshots").
		WithArgs("shop", "sessions").
		WillReturnRows(sqlmock.NewRows([]string{"snapshot_id", "schema_name", "table_name"}).AddRow(3, "shop", "sessions"))
	mock.ExpectQuery("FROM schema_snapshots").WithArgs(3).
		WillReturnRows(sqlmock.NewRows(snapshotRowCols).
			AddRow("shop", "sessions", "sid", 1, "PRI", "uuid", "uuid", 0, 0, "", "NO").
			AddRow("shop", "sessions", "payload", 2, "", "varchar", "varchar(64)", 0, 0, "utf8mb4", "YES"))
	mock.ExpectQuery("MIN\\(snapshot_time\\)").WithArgs(3).
		WillReturnRows(sqlmock.NewRows([]string{"t"}).AddRow(time.Now()))
	mock.ExpectQuery("snapshot_exclusions").WillReturnRows(sqlmock.NewRows([]string{"schema_name", "table_name", "reason"}))

	o := query.Options{Schema: "shop", Table: "sessions", PKValues: uuidText}
	if _, err := SpellIndexPKFilter(context.Background(), db, &o); err != nil {
		t.Fatal(err)
	}
	if o.PKValues != uuidStored || o.PKValuesAlt != uuidText {
		t.Errorf("PKValues %q alt %q, want %q and %q", o.PKValues, o.PKValuesAlt, uuidStored, uuidText)
	}
}

// Keys that cannot be one of these types never touch the snapshot: the
// sqlmock has no expectations, so any query fails the call.
func TestSpellIndexPKFilter_plainKeysReadNothing(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, o := range []query.Options{
		{Schema: "shop", Table: "t", PKValues: "42"},
		{Schema: "shop", Table: "t", PKValues: "12345|2"},
		{Schema: "shop", Table: "t", PKValues: "alice"},
		{Schema: "shop", Table: "t", PKValuesIn: []string{"1", "2"}},
		{Schema: "shop", Table: "t"},
		{Schema: "", Table: "t", PKValues: uuidText},
		{Schema: "shop", Table: "t", PKValues: uuidText, DenyTables: []query.SchemaTable{{Schema: "SHOP", Table: "T"}}},
		{Schema: "shop", Table: "t", PKValues: uuidText, AllowTables: []query.SchemaTable{{Schema: "shop", Table: "other"}}},
	} {
		before := o
		if _, err := SpellIndexPKFilter(context.Background(), db, &o); err != nil {
			t.Errorf("%+v: %v", before, err)
		}
		if o.PKValues != before.PKValues || o.PKValuesAlt != before.PKValuesAlt {
			t.Errorf("%+v was re-spelled to %q / %q", before, o.PKValues, o.PKValuesAlt)
		}
	}
}

// A key that looks like a UUID/INET value, no snapshot to type it, and no
// event indexed under it as typed: refused, never answered "no history".
func TestSpellIndexPKFilter_unknownTypeRefuses(t *testing.T) {
	for _, key := range []string{uuidText, uuidStored, "192.168.1.10", "::ffff:1.2.3.4"} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("SELECT snapshot_id").WillReturnRows(sqlmock.NewRows([]string{"snapshot_id", "schema_name", "table_name"}))
		mock.ExpectQuery("SELECT 1 FROM binlog_events").WithArgs("shop", "sessions", key, key).
			WillReturnRows(sqlmock.NewRows([]string{"1"}))
		o := query.Options{Schema: "shop", Table: "sessions", PKValues: key}
		_, err = SpellIndexPKFilter(context.Background(), db, &o)
		db.Close()
		if !errors.Is(err, ErrPKTypeUnknown) {
			t.Errorf("key %q: err = %v, want ErrPKTypeUnknown", key, err)
			continue
		}
		if strings.Contains(err.Error(), "--") {
			t.Errorf("the refusal names a CLI flag, which an MCP client cannot use: %v", err)
		}
		t.Logf("%v", err)
	}
}

// No snapshot, but the live index holds an event under the key exactly as
// typed: a MySQL CHAR(36) UUID, a VARCHAR IP, or a hex BINARY(16) copied from
// query output. That lookup worked before and must keep working.
func TestSpellIndexPKFilter_noSnapshotButTypedKeyIndexedPasses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT snapshot_id").WillReturnRows(sqlmock.NewRows([]string{"snapshot_id", "schema_name", "table_name"}))
	mock.ExpectQuery("SELECT 1 FROM binlog_events").WithArgs("shop", "t", "a", "a").WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock.ExpectQuery("SELECT 1 FROM binlog_events").WithArgs("shop", "t", uuidText, uuidText).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	o := query.Options{Schema: "shop", Table: "t", PKValuesIn: []string{"a", uuidText}}
	if _, err := SpellIndexPKFilter(context.Background(), db, &o); err != nil {
		t.Fatalf("refused a key the index holds as typed: %v", err)
	}
	if !slices.Equal(o.PKValuesIn, []string{"a", uuidText}) {
		t.Errorf("PKValuesIn changed to %q", o.PKValuesIn)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A database fault is reported as one, not as an unknown key type (which the
// console turns into a client error).
func TestSpellIndexPKFilter_readErrorIsNotTheSentinel(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT snapshot_id").WillReturnError(errors.New("Error 2013: Lost connection"))
	o := query.Options{Schema: "shop", Table: "sessions", PKValues: uuidText}
	_, err = SpellIndexPKFilter(context.Background(), db, &o)
	if err == nil || errors.Is(err, ErrPKTypeUnknown) {
		t.Errorf("err = %v, want a plain error", err)
	}
}

// A snapshot that describes the table but gives it no primary key: nothing
// to spell, and no refusal.
func TestSpellIndexPKFilter_describedWithoutKeyPasses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT snapshot_id, schema_name, table_name FROM schema_snapshots").
		WillReturnRows(sqlmock.NewRows([]string{"snapshot_id", "schema_name", "table_name"}).AddRow(3, "shop", "logs"))
	mock.ExpectQuery("FROM schema_snapshots").WithArgs(3).
		WillReturnRows(sqlmock.NewRows(snapshotRowCols).
			AddRow("shop", "logs", "ip", 1, "", "varchar", "varchar(64)", 0, 0, "utf8mb4", "YES"))
	o := query.Options{Schema: "shop", Table: "logs", PKValues: "10.0.0.1"}
	if _, err := SpellIndexPKFilter(context.Background(), db, &o); err != nil || o.PKValues != "10.0.0.1" {
		t.Errorf("err %v, PKValues %q", err, o.PKValues)
	}
}
