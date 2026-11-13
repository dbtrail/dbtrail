package reconstruct

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func writeFiles(t *testing.T, dir string, names ...string) {
	t.Helper()
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	slices.Sort(out)
	return out
}

// #2684 point 1. A reused output folder keeps the files an earlier run wrote
// for a table. The run removes, before writing anything, exactly the files it
// writes for the tables it names, and nothing that only looks like them.
func TestRemoveTableLeftovers_mydumper(t *testing.T) {
	dir := t.TempDir()
	leftovers := []string{
		"shop.orders-schema.sql",
		"shop.orders.00000.sql",
		"shop.orders.00007.sql",  // a higher chunk than the next run writes
		"shop.orders.100000.sql", // past five digits
		"shop.order.items-schema.sql",
		"shop.order.items.00000.sql",
	}
	kept := []string{
		"metadata",
		"notes.txt",
		"shop-schema-create.sql",
		// the same prefix as another table
		"shop.orders_archive-schema.sql",
		"shop.orders_archive.00000.sql",
		"shop.order-schema.sql",
		"shop.order.00000.sql",
		// another letter case is another table on Linux
		"shop.Orders-schema.sql",
		"shop.Orders.00000.sql",
		"Shop.orders.00000.sql",
		// the table `orders.00001`
		"shop.orders.00001-schema.sql",
		"shop.orders.00001.00000.sql",
		// files a loader reads beside a table, which this package never writes
		"shop.orders-schema-triggers.sql",
		"shop.orders-schema-view.sql",
		"shop.orders.sql",
		"shop.orders.00000.sql.gz",
		"shop.orders.00000.dat",
		"shop.orders.00000.sql.bak",
		"shop.orders.0001.sql",
		"shop.orders.00x01.sql",
		"shop.orders.-schema.sql",
		" shop.orders.00000.sql",
		"shop.orders.00000.sql ",
		"other.orders-schema.sql",
	}
	writeFiles(t, dir, append(append([]string{}, leftovers...), kept...)...)
	// Not regular files: never removed.
	if err := os.Mkdir(filepath.Join(dir, "shop.orders.00002.sql"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "notes.txt"), filepath.Join(dir, "shop.orders.00003.sql")); err != nil {
		t.Fatal(err)
	}
	kept = append(kept, "shop.orders.00002.sql", "shop.orders.00003.sql")

	left, err := removeTableLeftovers(dir, OutputFormatMydumper, []string{"shop.orders", "shop.order.items", "not-a-table"})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(leftovers)
	if !slices.Equal(left.Removed, leftovers) {
		t.Errorf("removed %v\nwant    %v", left.Removed, leftovers)
	}
	slices.Sort(kept)
	if got := dirNames(t, dir); !slices.Equal(got, kept) {
		t.Errorf("left in the folder %v\nwant                %v", got, kept)
	}
	wantOthers := []string{
		"other.orders-schema.sql", "shop.Orders-schema.sql", "shop.order-schema.sql",
		"shop.orders.-schema.sql", "shop.orders.00001-schema.sql", "shop.orders_archive-schema.sql",
	}
	if !slices.Equal(left.Others, wantOthers) {
		t.Errorf("others %v\nwant   %v", left.Others, wantOthers)
	}
}

// An unrelated file named like nothing this package writes stays, and a
// folder with nothing to remove reports nothing.
func TestRemoveTableLeftovers_nothingToRemove(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, "notes.txt", "shop.items-schema.sql")
	left, err := removeTableLeftovers(dir, OutputFormatMydumper, []string{"shop.orders"})
	if err != nil {
		t.Fatal(err)
	}
	if len(left.Removed) != 0 || !slices.Equal(left.Others, []string{"shop.items-schema.sql"}) {
		t.Errorf("left = %+v", left)
	}
	if got := dirNames(t, dir); !slices.Equal(got, []string{"notes.txt", "shop.items-schema.sql"}) {
		t.Errorf("folder = %v", got)
	}
	empty := t.TempDir()
	if left, err := removeTableLeftovers(empty, OutputFormatMydumper, nil); err != nil || len(left.Removed)+len(left.Others) != 0 {
		t.Errorf("empty folder: %+v, %v", left, err)
	}
}

// psql output writes one file per table, <schema>.<table>.sql with the dot of
// a name escaped. Parquet output writes into a snapshot folder of its own and
// is never touched here.
func TestRemoveTableLeftovers_psqlAndParquet(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, "shop.orders.sql", "shop.order%2Eitems.sql", "shop.orders.00000.sql", "shop.orders-schema.sql", "shop.ordersx.sql")
	left, err := removeTableLeftovers(dir, OutputFormatPsql, []string{"shop.orders", "shop.order.items"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(left.Removed, []string{"shop.order%2Eitems.sql", "shop.orders.sql"}) || len(left.Others) != 0 {
		t.Errorf("left = %+v", left)
	}
	if got := dirNames(t, dir); !slices.Equal(got, []string{"shop.orders-schema.sql", "shop.orders.00000.sql", "shop.ordersx.sql"}) {
		t.Errorf("folder = %v", got)
	}

	pq := t.TempDir()
	writeFiles(t, pq, "shop.orders.00000.sql")
	if left, err := removeTableLeftovers(pq, OutputFormatParquet, []string{"shop.orders"}); err != nil || len(left.Removed) != 0 {
		t.Errorf("parquet: %+v, %v", left, err)
	}
	if got := dirNames(t, pq); len(got) != 1 {
		t.Errorf("parquet folder = %v", got)
	}
}

// A leftover that cannot be removed stops the run: going on would leave an
// earlier run's rows beside this one's.
func TestRemoveTableLeftovers_cannotRemove(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root removes files from a read-only folder")
	}
	dir := t.TempDir()
	writeFiles(t, dir, "shop.orders.00004.sql")
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })
	_, err := removeTableLeftovers(dir, OutputFormatMydumper, []string{"shop.orders"})
	if err == nil {
		t.Fatal("a leftover that could not be removed did not stop the run")
	}
	t.Logf("rendered: %s", err)
	if !strings.Contains(err.Error(), "shop.orders.00004.sql") || !strings.Contains(err.Error(), "an earlier run left") {
		t.Errorf("err = %v", err)
	}
}

// The writer and the cleanup share the names: what the writer writes is what
// the cleanup removes.
func TestMydumperNames_writerAndCleanupAgree(t *testing.T) {
	for _, idx := range []int{0, 1, 99999, 100000} {
		if n := mydumperChunkFile("shop", "orders", idx); !isMydumperTableFile(n, "shop", "orders") {
			t.Errorf("chunk %d (%s) is not recognized", idx, n)
		}
	}
	if !isMydumperTableFile(mydumperSchemaFile("shop", "order.items"), "shop", "order.items") {
		t.Error("schema file of a table with a dot is not recognized")
	}
	t.Logf("rendered: %s | %s", leftoversRemovedMsg, leftoversOtherTablesMsg)
}
