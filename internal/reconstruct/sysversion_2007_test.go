package reconstruct

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2007: a refresh of a MariaDB system-versioned table folds its captured
// changes into the current rows, keyed on the declared primary key.

const (
	svImplicitCreate = "CREATE TABLE `prices` (\n  `id` int(11) NOT NULL,\n  `sku` varchar(20) DEFAULT NULL,\n" +
		"  `price` decimal(10,2) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 WITH SYSTEM VERSIONING;"
	svExplicitCreate = "CREATE TABLE `exp` (\n  `id` int(11) NOT NULL,\n  `v` int(11) DEFAULT NULL,\n" +
		"  `rs` timestamp(6) GENERATED ALWAYS AS ROW START INVISIBLE,\n  `re` timestamp(6) GENERATED ALWAYS AS ROW END INVISIBLE,\n" +
		"  PRIMARY KEY (`id`,`re`),\n  PERIOD FOR SYSTEM_TIME (`rs`, `re`)\n) ENGINE=InnoDB WITH SYSTEM VERSIONING;"
	svPlainCreate = "CREATE TABLE `prices` (\n  `id` int(11) NOT NULL,\n  `sku` varchar(20) DEFAULT NULL,\n" +
		"  `price` decimal(10,2) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

	svCur  = "2106-02-07 06:28:15.999999"
	svCur4 = "2038-01-19 03:14:07.999999"
)

// The PK the schema snapshot records for each shape: MariaDB appends the ROW
// END column, which the snapshot marks generated (synthesized for the hidden
// form, #1272).
func svPK(end, dataType string) []metadata.ColumnMeta {
	return []metadata.ColumnMeta{
		{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
		{Name: end, OrdinalPosition: 5, IsPK: true, DataType: dataType, ColumnType: dataType + "(6)", IsGenerated: true},
	}
}

func TestSysVersioningFor_2007(t *testing.T) {
	for _, c := range []struct {
		name      string
		create    string
		pk        []metadata.ColumnMeta
		wantKey   string // the reduced key's column names, comma-joined; "" = refusal
		wantErr   string
		wantNilSV bool
	}{
		{name: "implicit hidden period columns", create: svImplicitCreate, pk: svPK("row_end", "timestamp"), wantKey: "id"},
		{name: "explicit declared period columns", create: svExplicitCreate, pk: svPK("re", "timestamp"), wantKey: "id"},
		{name: "explicit, name differs only in case", create: svExplicitCreate, pk: svPK("RE", "timestamp"), wantKey: "id"},
		{name: "plain key on a plain table", create: svPlainCreate,
			pk: []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"}}, wantKey: "id", wantNilSV: true},
		// Versioning added by ALTER after the snapshot this refresh starts
		// from: the old CREATE says nothing about it. A full read cures it.
		{name: "snapshot CREATE predates the versioning", create: svPlainCreate, pk: svPK("row_end", "timestamp"),
			wantErr: "generated column"},
		{name: "an ordinary generated column in the key", create: svPlainCreate, pk: svPK("g", "int"),
			wantErr: "generated column"},
		{name: "versioned, but the generated key member is not the period end", create: svImplicitCreate,
			pk: svPK("other", "timestamp"), wantErr: "generated column"},
		// DROP SYSTEM VERSIONING after the snapshot (or a snapshot that
		// lacks the synthesized row_end): the window's events still carry
		// the extended key, so folding as a plain table would emit history
		// rows as live ones.
		{name: "CREATE versioned, snapshot key plain", create: svImplicitCreate,
			pk: []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"}}, wantErr: "WITH SYSTEM VERSIONING"},
		{name: "transaction-precise versioning", create: svExplicitCreate, pk: svPK("re", "bigint"),
			wantErr: "transaction-precise"},
		{name: "two generated key members", create: svImplicitCreate,
			pk:      append(svPK("row_end", "timestamp"), metadata.ColumnMeta{Name: "g", IsPK: true, DataType: "int", IsGenerated: true}),
			wantErr: "generated column"},
	} {
		t.Run(c.name, func(t *testing.T) {
			sv, key, err := sysVersioningFor("shop", "prices", c.create, c.pk)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, c.wantErr)
				}
				if c.wantErr == "generated column" && !errors.Is(err, ErrGeneratedPK) {
					t.Fatalf("err = %v, want it classified ErrGeneratedPK", err)
				}
				if c.wantErr == "WITH SYSTEM VERSIONING" && !errors.Is(err, ErrSchemaChanged) {
					t.Fatalf("err = %v, want it classified ErrSchemaChanged (a full read cures it)", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected refusal: %v", err)
			}
			var names []string
			for _, k := range key {
				names = append(names, k.Name)
			}
			if got := strings.Join(names, ","); got != c.wantKey {
				t.Fatalf("key = %q, want %q", got, c.wantKey)
			}
			if (sv == nil) != c.wantNilSV {
				t.Fatalf("sysVersioned = %+v, want nil=%v", sv, c.wantNilSV)
			}
		})
	}
}

var svAt = time.Date(2026, 10, 2, 6, 13, 43, 0, time.UTC)

func svEv(id uint64, typ event.EventType, pk string, before, after map[string]any) query.ResultRow {
	return query.ResultRow{EventID: id, EventTimestamp: svAt, SchemaName: "shop", TableName: "prices",
		EventType: typ, PKValues: pk, RowBefore: before, RowAfter: after}
}

func svRow(id float64, price, start, end string) map[string]any {
	return map[string]any{"id": id, "price": price, "row_start": start, "row_end": end}
}

func implicitSV(t *testing.T) (*sysVersioned, []metadata.ColumnMeta) {
	t.Helper()
	sv, key, err := sysVersioningFor("shop", "prices", svImplicitCreate, svPK("row_end", "timestamp"))
	if err != nil || sv == nil {
		t.Fatalf("sysVersioningFor: %v, %v", sv, err)
	}
	return sv, key
}

// What each captured shape means for the current view, one event at a time.
func TestSysVersionedNormalize_2007(t *testing.T) {
	sv, _ := implicitSV(t)
	t0, t1 := "2026-10-02 06:13:40.000001", "2026-10-02 06:13:43.628276"
	for _, c := range []struct {
		name     string
		ev       query.ResultRow
		wantKeep bool
		wantType event.EventType
		wantKey  string
		wantErr  string
	}{
		{name: "insert of a current row", wantKeep: true, wantType: event.EventInsert, wantKey: "1",
			ev: svEv(1, event.EventInsert, "1|"+svCur, nil, svRow(1, "1.00", t0, svCur))},
		{name: "insert on 11.4 (2038 marker)", wantKeep: true, wantType: event.EventInsert, wantKey: "1",
			ev: svEv(1, event.EventInsert, "1|"+svCur4, nil, svRow(1, "1.00", t0, svCur4))},
		{name: "insert of a history row (the old version an UPDATE keeps)", wantKeep: false,
			ev: svEv(2, event.EventInsert, "1|"+t1, nil, svRow(1, "1.00", t0, t1))},
		{name: "update of the current row", wantKeep: true, wantType: event.EventUpdate, wantKey: "1",
			ev: svEv(3, event.EventUpdate, "1|"+svCur, svRow(1, "1.00", t0, svCur), svRow(1, "1.50", t1, svCur))},
		{name: "versioned delete: row_end moves to the past", wantKeep: true, wantType: event.EventDelete, wantKey: "2",
			ev: svEv(4, event.EventUpdate, "2|"+svCur, svRow(2, "2.00", t0, svCur), svRow(2, "2.00", t0, t1))},
		{name: "update of a history row", wantKeep: false,
			ev: svEv(5, event.EventUpdate, "1|"+t0, svRow(1, "1.00", t0, t0), svRow(1, "1.10", t0, t0))},
		{name: "real delete of a history row (DELETE HISTORY)", wantKeep: false,
			ev: svEv(6, event.EventDelete, "1|"+t1, svRow(1, "1.00", t0, t1), nil)},
		{name: "real delete of a current row", wantKeep: true, wantType: event.EventDelete, wantKey: "3",
			ev: svEv(7, event.EventDelete, "3|"+svCur, svRow(3, "3.00", t0, svCur), nil)},
		{name: "a history row turned current", wantErr: "history version",
			ev: svEv(8, event.EventUpdate, "1|"+t1, svRow(1, "1.00", t0, t1), svRow(1, "1.00", t0, svCur))},
		{name: "an end value no marker explains", wantErr: "neither",
			ev: svEv(9, event.EventInsert, "1|2090-01-01 00:00:00", nil, svRow(1, "1.00", t0, "2090-01-01 00:00:00"))},
		{name: "a stored key without the period component", wantErr: "pk_values",
			ev: svEv(10, event.EventInsert, "1", nil, svRow(1, "1.00", t0, svCur))},
		{name: "an update missing its after image", wantErr: "image",
			ev: svEv(11, event.EventUpdate, "1|"+svCur, svRow(1, "1.00", t0, svCur), nil)},
	} {
		t.Run(c.name, func(t *testing.T) {
			ev := c.ev
			keep, err := sv.normalize(&ev)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("normalize err = %v, want one containing %q", err, c.wantErr)
				}
				if !strings.Contains(err.Error(), "shop.prices") {
					t.Fatalf("the refusal must name the table: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			if keep != c.wantKeep {
				t.Fatalf("keep = %v, want %v", keep, c.wantKeep)
			}
			if !keep {
				return
			}
			if ev.EventType != c.wantType || ev.PKValues != c.wantKey {
				t.Fatalf("normalized to type %v key %q, want %v %q", ev.EventType, ev.PKValues, c.wantType, c.wantKey)
			}
			for _, img := range []map[string]any{ev.RowBefore, ev.RowAfter} {
				for _, col := range []string{"row_start", "row_end"} {
					if _, ok := img[col]; ok {
						t.Fatalf("period column %s survived into the image %v", col, img)
					}
				}
			}
			if ev.EventType == event.EventDelete && (ev.RowAfter != nil || ev.RowBefore == nil) {
				t.Fatalf("a delete keeps its before image only: before=%v after=%v", ev.RowBefore, ev.RowAfter)
			}
			if c.ev.RowAfter != nil && c.ev.RowAfter["row_end"] == nil {
				t.Fatal("normalize changed the caller's image map instead of a copy")
			}
		})
	}
}

// The sequences a refresh window holds, folded the way the refresh folds them.
func TestFoldPage_systemVersioned_2007(t *testing.T) {
	t0 := "2026-10-02 06:13:40.000001"
	ta, tb, tc := "2026-10-02 06:13:41.000001", "2026-10-02 06:13:42.000001", "2026-10-02 06:13:43.000001"
	for _, c := range []struct {
		name string
		page []query.ResultRow
		want map[string]string // reduced key → "type:price" of the change kept
	}{
		{name: "several updates of one row in the window: the last current image wins", page: []query.ResultRow{
			svEv(1, event.EventUpdate, "1|"+svCur, svRow(1, "1.00", t0, svCur), svRow(1, "1.10", ta, svCur)),
			svEv(2, event.EventInsert, "1|"+ta, nil, svRow(1, "1.00", t0, ta)),
			svEv(3, event.EventUpdate, "1|"+svCur, svRow(1, "1.10", ta, svCur), svRow(1, "1.20", tb, svCur)),
			svEv(4, event.EventInsert, "1|"+tb, nil, svRow(1, "1.10", ta, tb)),
		}, want: map[string]string{"1": "update:1.20"}},
		{name: "delete, then insert of the same id: the new row is current", page: []query.ResultRow{
			svEv(1, event.EventUpdate, "3|"+svCur, svRow(3, "3.00", t0, svCur), svRow(3, "3.00", t0, ta)),
			svEv(2, event.EventInsert, "3|"+svCur, nil, svRow(3, "3.30", ta, svCur)),
		}, want: map[string]string{"3": "insert:3.30"}},
		{name: "insert, then delete inside the window", page: []query.ResultRow{
			svEv(1, event.EventInsert, "4|"+svCur, nil, svRow(4, "4.00", ta, svCur)),
			svEv(2, event.EventUpdate, "4|"+svCur, svRow(4, "4.00", ta, svCur), svRow(4, "4.00", ta, tb)),
		}, want: map[string]string{"4": "delete:"}},
		{name: "a history purge leaves the current row alone", page: []query.ResultRow{
			svEv(1, event.EventUpdate, "5|"+svCur, svRow(5, "5.00", t0, svCur), svRow(5, "5.50", tc, svCur)),
			svEv(2, event.EventInsert, "5|"+tc, nil, svRow(5, "5.00", t0, tc)),
			svEv(3, event.EventDelete, "5|"+tc, svRow(5, "5.00", t0, tc), nil),
		}, want: map[string]string{"5": "update:5.50"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			sv, key := implicitSV(t)
			res := &foldResult{Changes: map[string]*query.ResultRow{}}
			if err := foldPage(c.page, "shop", "prices", key, sv, res); err != nil {
				t.Fatalf("foldPage: %v", err)
			}
			got := map[string]string{}
			for k, ev := range res.Changes {
				typ := map[event.EventType]string{event.EventInsert: "insert", event.EventUpdate: "update", event.EventDelete: "delete"}[ev.EventType]
				price := ""
				if ev.RowAfter != nil {
					price, _ = ev.RowAfter["price"].(string)
				}
				got[k] = typ + ":" + price
			}
			if len(got) != len(c.want) {
				t.Fatalf("changes = %v, want %v", got, c.want)
			}
			for k, w := range c.want {
				if got[k] != w {
					t.Fatalf("changes = %v, want %v", got, c.want)
				}
			}
			if _, ok := res.ImageColumns["row_end"]; ok {
				t.Fatal("the period columns reached the image column set; the post-baseline column check would refuse them")
			}
		})
	}
}

// A real change of the declared key on a versioned table is still a
// PK-changing UPDATE (#782), judged on the declared key.
func TestFoldPage_systemVersioned_pkChangeStillRefused_2007(t *testing.T) {
	sv, key := implicitSV(t)
	t0 := "2026-10-02 06:13:40.000001"
	page := []query.ResultRow{
		svEv(1, event.EventUpdate, "1|"+svCur, svRow(1, "1.00", t0, svCur), svRow(9, "1.00", t0, svCur)),
	}
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	err := foldPage(page, "shop", "prices", key, sv, res)
	if err == nil || !strings.Contains(err.Error(), "PK-changing UPDATE") || !strings.Contains(err.Error(), `"1" → "9"`) {
		t.Fatalf("foldPage = %v, want the PK-changing UPDATE refusal on the declared key (1 → 9)", err)
	}
}

// Single-row reconstruct of a versioned table (#2007): the declared-key value
// is looked up under both current markers and the events are read for what
// they mean to the current row, or the lookup refuses when it cannot be
// sure. Never the snapshot row as if nothing had happened.
func TestSingleRowSysVersioning_2007(t *testing.T) {
	plainPK := []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"}}
	for _, c := range []struct {
		name      string
		create    string
		pk        []metadata.ColumnMeta
		cols      []string
		flavor    string
		versioned bool
		wantErr   string
	}{
		{"implicit", svImplicitCreate, svPK("row_end", "timestamp"), []string{"id"}, "mariadb", true, ""},
		{"explicit", svExplicitCreate, svPK("re", "timestamp"), []string{"id"}, "mariadb", true, ""},
		{"period end named in the key columns", svImplicitCreate, svPK("row_end", "timestamp"), []string{"id", "row_end"}, "mariadb", false, "declared"},
		// Metadata unreadable or older than the embedded CREATE: the
		// snapshot key alone says the lookup would miss.
		{"no CREATE, generated key member", "", svPK("row_end", "timestamp"), []string{"id"}, "mariadb", false, "system-versioned"},
		{"transaction-precise", svExplicitCreate, svPK("re", "bigint"), []string{"id"}, "mariadb", false, "transaction-precise"},
		{"plain table", svPlainCreate, plainPK, []string{"id"}, "mariadb", false, ""},
		{"nothing known, MariaDB source", "", nil, []string{"id"}, "mariadb", false, "schema snapshot"},
		{"nothing known, source not recorded: warns only", "", nil, []string{"id"}, "", false, ""},
		{"nothing known, MySQL source", "", nil, []string{"id"}, "mysql", false, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, err := SingleRowSysVersioning("shop", "prices", c.create, c.pk, c.cols, c.flavor)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) || !strings.Contains(err.Error(), "shop.prices") {
					t.Fatalf("err = %v, want a refusal containing %q and naming the table", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected refusal: %v", err)
			}
			if (h != nil) != c.versioned {
				t.Fatalf("handle = %v, want versioned=%v", h, c.versioned)
			}
		})
	}

	h, err := SingleRowSysVersioning("shop", "prices", svImplicitCreate, svPK("row_end", "timestamp"), []string{"id"}, "mariadb")
	if err != nil || h == nil {
		t.Fatalf("SingleRowSysVersioning: %v %v", h, err)
	}
	opts := query.Options{PKValues: "2"}
	h.ExpandKey(&opts)
	if opts.PKValues != "" || len(opts.PKValuesIn) != 3 {
		t.Fatalf("ExpandKey: PKValues=%q In=%q, want the value plus both markers", opts.PKValues, opts.PKValuesIn)
	}
	t0, t1 := "2026-10-02 06:13:40.000001", "2026-10-02 06:13:43.628276"
	evs, err := h.Normalize([]query.ResultRow{
		svEv(1, event.EventUpdate, "2|"+svCur, svRow(2, "2.00", t0, svCur), svRow(2, "2.50", t1, svCur)),
		svEv(2, event.EventInsert, "2|"+t1, nil, svRow(2, "2.00", t0, t1)),
		svEv(3, event.EventUpdate, "2|"+svCur, svRow(2, "2.50", t1, svCur), svRow(2, "2.50", t1, "2026-10-02 06:13:44.000000")),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 || evs[0].EventType != event.EventUpdate || evs[1].EventType != event.EventDelete {
		t.Fatalf("Normalize = %+v, want the update then the delete, the history row dropped", evs)
	}
	state, err := ApplyAt(map[string]any{"id": 2.0, "price": "2.00"}, evs, svAt.Add(time.Hour))
	if err != nil || state != nil {
		t.Fatalf("ApplyAt over the normalized events = %v, %v; want the row deleted (nil)", state, err)
	}
}

// Versioning added and dropped again inside the window leaves the period
// columns in some events' images; that is not a column added after the
// baseline, and the refusal must say what happened.
func TestCheckPostBaselineColumns_periodColumnsWording_2007(t *testing.T) {
	changes := map[string]*query.ResultRow{"1": {EventType: event.EventUpdate,
		RowAfter: map[string]any{"id": 1.0, "price": "1.00", "row_start": "x", "row_end": svCur}}}
	in := mergeInput{Schema: "shop", Table: "prices", CreateTableSQL: svPlainCreate}
	err := checkPostBaselineColumns(in, changes, []string{"id", "price", "sku"})
	if err == nil || !errors.Is(err, ErrSchemaChanged) || !strings.Contains(err.Error(), "system-versioned during part of this window") ||
		strings.Contains(err.Error(), "added after the baseline") {
		t.Fatalf("err = %v, want the versioning wording, classified ErrSchemaChanged", err)
	}
	changes["1"].RowAfter["note"] = "new column"
	if err := checkPostBaselineColumns(in, changes, []string{"id", "price", "sku"}); err == nil ||
		!strings.Contains(err.Error(), "added after the baseline") {
		t.Fatalf("a real added column keeps the original wording: %v", err)
	}
}
