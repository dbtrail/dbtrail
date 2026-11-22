package console

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

const (
	newestSnapshotRE3110 = `SELECT COALESCE\(MAX\(snapshot_id\), 0\) FROM schema_snapshots`
	perTableLoadRE3110   = `FROM schema_snapshots s\s+JOIN`
)

// #3110: a MySQL or MariaDB server's bundle comes back from Resolve as it was
// published, and the index is sent nothing: the reload is PostgreSQL's alone.
func TestResolve_MySQLFamilyServerSendsNoQuery_3110(t *testing.T) {
	ctx := context.Background()
	t.Run("boot", func(t *testing.T) {
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		mock.ExpectQuery(newestSnapshotRE3110) // must stay unmet
		cm := newConnManager(nil, false)
		boot := &bundle{db: db, pgSource: false}
		cm.seedBoot(boot, "")
		got, err := cm.Resolve(ctx, bootServerID)
		if err != nil {
			t.Fatal(err)
		}
		if got != boot {
			t.Errorf("Resolve published a new bundle for a MySQL boot server")
		}
		if err := mock.ExpectationsWereMet(); err == nil {
			t.Errorf("Resolve read the newest snapshot of a MySQL index")
		}
	})
	for _, flavor := range []string{"", "mysql", "mariadb"} {
		t.Run("registry flavor="+flavor, func(t *testing.T) {
			db, mock, closeDB := newSQLMock(t)
			defer closeDB()
			mock.ExpectQuery(newestSnapshotRE3110) // must stay unmet
			cm := newConnManager(nil, false)
			e, err := cm.reg.Add(ServerEntry{Name: "m", DSN: "u:p@tcp(127.0.0.1:3306)/idx", Flavor: flavor})
			if err != nil {
				t.Fatal(err)
			}
			b := &bundle{db: db, serverID: e.ID}
			cm.bundles[e.ID] = b
			got, err := cm.Resolve(ctx, e.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got != b {
				t.Errorf("Resolve published a new bundle for a %q server", flavor)
			}
			if err := mock.ExpectationsWereMet(); err == nil {
				t.Errorf("Resolve read the newest snapshot of a %q index", flavor)
			}
		})
	}
}

// #3110: a reload that fails keeps the shapes loaded before, and is not tried
// again for the same newest snapshot within pgShapeRetryAfter: inside the
// window a request costs the MAX read only. Past it, or with a newer shape,
// it is tried again.
func TestFreshPGShapes_failedReloadWaitsOutItsWindow_3110(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	defer forgetPGShapes(db)
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	saved := pgShapeNow
	pgShapeNow = func() time.Time { return now }
	defer func() { pgShapeNow = saved }()

	cm := newConnManager(nil, false)
	boot := &bundle{db: db, pgSource: true}
	cm.seedBoot(boot, "")
	newest := func(id int) {
		mock.ExpectQuery(newestSnapshotRE3110).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(id))
	}
	failLoad := func() {
		mock.ExpectQuery(perTableLoadRE3110).WillReturnError(errors.New("Error 1205: Lock wait timeout exceeded"))
	}

	// First request: shapes unknown, the load fails.
	newest(7)
	failLoad()
	if got := cm.freshPGShapes(context.Background(), boot); got != boot {
		t.Fatal("a failed reload published a bundle")
	}
	st := pgShapeStateFor(db)
	if st.failedID != 7 || !st.failedAt.Equal(now) {
		t.Fatalf("failure not recorded: id %d at %v", st.failedID, st.failedAt)
	}

	// Within the window, same newest snapshot: the MAX read only.
	now = now.Add(10 * time.Second)
	newest(7)
	if got := cm.freshPGShapes(context.Background(), boot); got != boot {
		t.Fatal("a request inside the window published a bundle")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if !st.failedAt.Equal(now.Add(-10 * time.Second)) {
		t.Errorf("the load was tried again inside the window (failure stamped %v)", st.failedAt)
	}

	// A newer shape is tried at once.
	newest(8)
	failLoad()
	cm.freshPGShapes(context.Background(), boot)
	if st.failedID != 8 || !st.failedAt.Equal(now) {
		t.Errorf("a newer shape was not tried: id %d at %v", st.failedID, st.failedAt)
	}

	// Past the window the same one is tried again.
	now = now.Add(pgShapeRetryAfter)
	newest(8)
	failLoad()
	cm.freshPGShapes(context.Background(), boot)
	if !st.failedAt.Equal(now) {
		t.Errorf("not tried again past the window (failure stamped %v)", st.failedAt)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
	if cm.boot != boot {
		t.Error("a failed reload replaced the published bundle")
	}
}
