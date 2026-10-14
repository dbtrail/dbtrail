package query

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// #2385: every branch of the partition-heads statement opens its own TABLE
// instance of binlog_events, which stays in the table cache, so no statement
// may name more than headsPerStatement partitions; and the answers of every
// batch still land on their own partitions.
func TestLoadPartitionHeads_boundedBranchesPerStatement(t *testing.T) {
	const parts = 2*headsPerStatement + 5 // two full batches and a short one
	var heads []string
	db, m, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(_, actual string) error {
		if strings.Contains(actual, "UNION ALL") || strings.Contains(actual, "AS part") {
			heads = append(heads, actual)
		}
		return nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows := sqlmock.NewRows(partCols2152)
	for i := range parts - 1 {
		at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i+1) * time.Hour)
		rows.AddRow(fmt.Sprintf("p_%d", i), strconv.FormatInt(mysqlToSeconds(at), 10))
	}
	rows.AddRow("p_future", "MAXVALUE")
	m.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(rows)
	for lo := 0; lo < parts; lo += headsPerStatement {
		// Each batch answers for its FIRST partition, numbered from the
		// whole list: a batch that numbered from zero again is refused.
		m.ExpectQuery("AS part").WillReturnRows(
			sqlmock.NewRows([]string{"part", "binlog_file", "start_pos"}).AddRow(lo, "binlog.000001", uint64(100+lo)))
	}
	stop := errors.New("stop after the heads")
	m.ExpectQuery("archive_state").WillReturnError(stop)

	_, err = loadPartitionHeadsOnce(context.Background(), db)
	if !errors.Is(err, stop) {
		t.Fatalf("err = %v, want the heads read and the archive read reached", err)
	}
	want := (parts + headsPerStatement - 1) / headsPerStatement
	if len(heads) != want {
		t.Fatalf("head statements = %d, want %d for %d partitions", len(heads), want, parts)
	}
	for i, q := range heads {
		if n := strings.Count(q, "FROM binlog_events"); n > headsPerStatement {
			t.Errorf("statement %d names binlog_events %d times, more than %d", i, n, headsPerStatement)
		}
	}
	if headsPerStatement > 16 {
		t.Errorf("headsPerStatement = %d: each branch is a cached TABLE instance per cache instance (#2385)", headsPerStatement)
	}
}
