//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// MariaDB's ALTER IGNORE TABLE ... ADD UNIQUE deletes the duplicates with no
// row events (#2640). Through the real refresh path and the real
// schema_changes query, whose LIKE prefilter must bring the row in: carried
// forward, the copy would keep every duplicate the source deleted.
func TestReconstructParquet_anAlterIgnoreUniqueRefuses(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	for name, query := range map[string]string{
		"add unique":           "ALTER IGNORE TABLE `orders` ADD UNIQUE KEY uk_status (status)",
		"add primary key":      "alter online ignore table orders drop primary key, add primary key (id, status)",
		"unique in a column":   "ALTER IGNORE TABLE orders MODIFY status VARCHAR(16) UNIQUE",
		"in a MariaDB comment": "/*M!100100 ALTER IGNORE TABLE orders ADD UNIQUE (status) */",
	} {
		t.Run(name, func(t *testing.T) {
			db, dsn, root, base := lateDDLIndex(t)
			recordAlter(t, db, "orders", 600, base.Add(10*time.Second), query)
			err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(30*time.Second))
			if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
				t.Fatalf("the refresh after %q: %v, want a destructive-DDL refusal", query, err)
			}
			for _, want := range []string{"ALTER TABLE ... IGNORE ...", "on shop.orders", "binlog.000003:600",
				"changed the table's rows without writing row changes"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not say %q: %v", want, err)
				}
			}
			t.Logf("refusal: %v", err)
		})
	}
}

// What deletes no duplicate refuses nothing: an ALTER IGNORE that adds a
// plain index, a UNIQUE without IGNORE (a duplicate would have failed it),
// the words in a comment. The refresh publishes, and the snapshot holds the
// baseline plus the change after them.
func TestReconstructParquet_anAlterIgnoreThatKeepsTheRowsRefusesNothing(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)

	recordAlter(t, db, "orders", 600, base.Add(5*time.Second), "ALTER IGNORE TABLE orders ADD INDEX i_status (status)")
	recordAlter(t, db, "orders", 610, base.Add(6*time.Second), "ALTER TABLE orders ADD UNIQUE KEY uk_status (status)")
	recordAlter(t, db, "orders", 620, base.Add(7*time.Second), "ALTER TABLE orders COMMENT 'alter ignore table orders add unique (status)'")
	recordAlter(t, db, "orders", 630, base.Add(8*time.Second), "ALTER IGNORE TABLE orders DROP PRIMARY KEY")
	rowChange(t, db, "binlog.000003", 700, 800, base.Add(10*time.Second), "1", `{"id":1,"status":"A"}`)
	if err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(30*time.Second)); err != nil {
		t.Fatalf("the refresh refused on a statement that keeps every row: %v", err)
	}
	out, _, _, err := reconstruct.FindBaseline(context.Background(), root, "shop", "orders", base.Add(31*time.Second))
	if err != nil {
		t.Fatalf("FindBaseline: %v", err)
	}
	if got, want := readOrders(t, out), []string{"1=A", "2=paid", "3=shipped"}; !equalStrings(got, want) {
		t.Fatalf("snapshot = %v, want %v", got, want)
	}
}
