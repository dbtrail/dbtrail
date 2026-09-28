package baseline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// tableWithPairs writes <root>/<snapshot>/<schema>/<table>.parquet and the
// pairs 0..last of a chain beside it, every pair recording chainStart.
func tableWithPairs(t *testing.T, root, snapshot, schema, table string, last int, chainStart string) string {
	t.Helper()
	dir := filepath.Join(root, snapshot, schema)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, table+".parquet")
	cols := []Column{{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)}}
	w, err := NewWriter(base, cols, WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
		MetaKeyBinlogFile: "binlog.000042", MetaKeyBinlogPos: "12345",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for seq := 0; seq <= last; seq++ {
		md := map[string]string{
			MetaKeyBinlogFile: "binlog.000042", MetaKeyBinlogPos: strconv.Itoa(20000 + seq),
			MetaKeyDeltaBaseAnchor: "binlog.000042:12345", MetaKeyDeltaBaseSize: "1",
		}
		if chainStart != "" {
			md[MetaKeyDeltaChainStart] = chainStart
		}
		if err := WriteTableDeltaPair(base, seq, cols, WithDeltaSeq(md, seq), nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	return base
}

// TestDiscoverBaselines_chainStart (#1707): `bintrail status` grades on what
// this walk reports. Each file carries the start of ITS chain, a file with
// none carries nothing, and a chain that cannot say where it starts is an
// error on that file alone.
func TestDiscoverBaselines_chainStart(t *testing.T) {
	root := t.TempDir()
	const snap = "2026-09-27T12-00-00Z"
	tableWithPairs(t, root, snap, "shop", "orders", 3, "2026-09-27T02:00:00Z")
	tableWithPairs(t, root, snap, "shop", "users", 0, "2026-09-27T13:45:00+02:00") // 11:45 UTC
	tableWithPairs(t, root, snap, "shop", "plain", -1, "")
	tableWithPairs(t, root, snap, "shop", "orders_2", -1, "")
	tableWithPairs(t, root, snap, "shop", "nostart", 0, "")
	half := tableWithPairs(t, root, snap, "shop", "half", 1, "2026-09-27T02:00:00Z")
	posdel, _ := TableDeltaPaths(half, 1)
	if err := os.Remove(posdel); err != nil {
		t.Fatal(err)
	}
	// The same table name in another schema, with another start.
	tableWithPairs(t, root, snap, "other", "orders", 0, "2026-09-27T09:00:00Z")
	// An older snapshot of orders, with the chain it had then.
	tableWithPairs(t, root, "2026-09-26T12-00-00Z", "shop", "orders", 0, "2026-09-26T12:00:00Z")

	infos, unreadable, err := DiscoverBaselinesReport(root)
	if err != nil || len(unreadable) != 0 {
		t.Fatalf("err = %v, unreadable = %v", err, unreadable)
	}
	type row struct {
		start string
		err   bool
	}
	got := map[string]row{}
	for _, b := range infos {
		r := row{err: b.ChainStartErr != nil}
		if !b.ChainStart.IsZero() {
			r.start = b.ChainStart.UTC().Format(time.RFC3339)
		}
		got[b.SnapshotTime.Format("02T15")+" "+b.Database+"."+b.Table] = r
		if b.BinlogFile != "binlog.000042" {
			t.Errorf("%s.%s: the file's own footer was not read: %+v", b.Database, b.Table, b)
		}
	}
	want := map[string]row{
		"27T12 shop.orders":   {start: "2026-09-27T02:00:00Z"},
		"27T12 shop.users":    {start: "2026-09-27T11:45:00Z"},
		"27T12 shop.plain":    {},
		"27T12 shop.orders_2": {},
		"27T12 shop.nostart":  {err: true},
		"27T12 shop.half":     {err: true},
		"27T12 other.orders":  {start: "2026-09-27T09:00:00Z"},
		"26T12 shop.orders":   {start: "2026-09-26T12:00:00Z"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: %+v, want %+v", k, got[k], w)
		}
	}
	for _, b := range infos {
		if b.Table == "half" && !errors.Is(b.ChainStartErr, ErrHalfTableDelta) {
			t.Errorf("half: err = %v, want the half pair named", b.ChainStartErr)
		}
	}
}

// NewestTableDeltaUpserts answers for every table of a folder what
// TableDeltaChainIn answers for one, damage included: the two are what the
// listings and FindBaseline's lookup use, and they must not part.
func TestNewestTableDeltaUpserts_isTableDeltaChainInPerTable(t *testing.T) {
	names := []string{
		"orders.parquet", "orders.000000.posdel", "orders.000000.upserts", "orders.000001.posdel", "orders.000001.upserts",
		"orders_2.parquet",
		"orders.000001x.parquet", "orders.000001x.000000.posdel", "orders.000001x.000000.upserts",
		"ranged.parquet", "ranged.000000-000005.posdel", "ranged.000000-000005.upserts", "ranged.000006.posdel", "ranged.000006.upserts",
		"old.parquet", "old.posdel", "old.upserts",
		"half.parquet", "half.000000.posdel",
		"halfup.parquet", "halfup.000000.upserts",
		"gap.parquet", "gap.000000.posdel", "gap.000000.upserts", "gap.000002.posdel", "gap.000002.upserts",
		"both.parquet", "both.posdel", "both.upserts", "both.000000.posdel", "both.000000.upserts",
		"a[b].parquet", "a[b].000000.posdel", "a[b].000000.upserts",
		"views.sql", "_MANIFEST", ".000000.upserts",
		// Delta files with no table file beside them.
		"ghost.000000.posdel", "ghost.000000.upserts",
	}
	for _, dir := range []string{"/snap/shop", "s3://b/base/2026-09-27T12-00-00Z/shop"} {
		newest, damaged := NewestTableDeltaUpserts(dir, names)
		var stems []string
		for _, n := range names {
			if s, _, _, ok := ParseTableDeltaName(n); ok {
				stems = append(stems, s)
			}
		}
		stems = append(stems, "orders_2", "absent")
		sort.Strings(stems)
		for _, stem := range stems {
			chain, err := TableDeltaChainIn(dir, names, stem)
			switch {
			case err != nil:
				if damaged[stem] == nil || newest[stem] != "" || damaged[stem].Error() != err.Error() {
					t.Errorf("%s %s: damaged %v, newest %q; TableDeltaChainIn says %v", dir, stem, damaged[stem], newest[stem], err)
				}
			case chain == nil:
				if damaged[stem] != nil || newest[stem] != "" {
					t.Errorf("%s %s: damaged %v, newest %q; TableDeltaChainIn says no chain", dir, stem, damaged[stem], newest[stem])
				}
			default:
				if damaged[stem] != nil || newest[stem] != chain.LastFileUpserts() {
					t.Errorf("%s %s: damaged %v, newest %q; TableDeltaChainIn says %q", dir, stem, damaged[stem], newest[stem], chain.LastFileUpserts())
				}
			}
		}
		for stem, want := range map[string]string{
			"orders": "orders.000001.upserts", "orders.000001x": "orders.000001x.000000.upserts",
			"ranged": "ranged.000006.upserts", "old": "old.upserts", "a[b]": "a[b].000000.upserts",
		} {
			if got := newest[stem]; got != dirJoin(dir, want) {
				t.Errorf("%s %s: %q, want %q", dir, stem, got, want)
			}
		}
		for _, stem := range []string{"half", "halfup", "gap", "both"} {
			if !errors.Is(damaged[stem], ErrHalfTableDelta) {
				t.Errorf("%s %s: %v, want damaged", dir, stem, damaged[stem])
			}
		}
	}
	if newest, damaged := NewestTableDeltaUpserts("/snap/shop", nil); len(newest) != 0 || len(damaged) != 0 {
		t.Errorf("an empty folder: %v, %v", newest, damaged)
	}
}

func TestTableDeltaStart_noChain(t *testing.T) {
	if at, err := TableDeltaStart(context.Background(), nil); err != nil || !at.IsZero() {
		t.Errorf("no chain: %s, %v", at, err)
	}
}
