package verify

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// A MySQL or MariaDB pair takes each row's latest change in binary log order
// (#2156); a PostgreSQL pair keeps the fetch it always had, field for field.
func TestBaselineFetchMerged_latestInBinlogNotForPostgres(t *testing.T) {
	p := BaselinePair{Schema: "s", Table: "t", PrevSnapshot: time.Unix(100, 0), NewSnapshot: time.Unix(200, 0),
		PrevAnchor: query.BinlogPos{File: "binlog.000001", Pos: 4}, NewAnchor: query.BinlogPos{File: "binlog.000002", Pos: 4}}
	cfg := BaselineConfig{IndexDBName: "idx", NoArchive: true}
	var order query.LatestPerPKOrder

	pg := baselineFetchMerged(cfg, p, true, &order)
	want := query.FetchMergedOptions{Opts: baselineFetchOptions(p, true), DBName: "idx", NoArchive: true, LatestOrder: &order}
	if !reflect.DeepEqual(pg, want) {
		t.Fatalf("PostgreSQL pair: %+v, want %+v", pg, want)
	}
	if mysql := baselineFetchMerged(cfg, p, false, &order); !mysql.LatestInBinlog || mysql.LatestOrder != &order {
		t.Fatalf("MySQL pair: %+v", mysql)
	}
}

// The order note joins a mismatch only.
func TestWithOrderNote(t *testing.T) {
	unproven := query.LatestPerPKOrder{Disagreed: 1, Refused: 1}
	for _, tc := range []struct {
		name   string
		res    TableResult
		order  query.LatestPerPKOrder
		want   string
		noNote bool
	}{
		{"mismatch, unproven", TableResult{Status: StatusMismatch, Detail: "content digest differs."}, unproven, "content digest differs. order of changes unproven: ", false},
		{"mismatch, empty detail", TableResult{Status: StatusMismatch}, unproven, "order of changes unproven: ", false},
		{"mismatch, proven", TableResult{Status: StatusMismatch, Detail: "x"}, query.LatestPerPKOrder{Disagreed: 1, Sorted: 1}, "x", true},
		{"match, unproven", TableResult{Status: StatusMatch, Detail: "y"}, unproven, "y", true},
		{"inconclusive, unproven", TableResult{Status: StatusInconclusive, Detail: "z"}, unproven, "z", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := tc.res
			withOrderNote(&res, tc.order)
			if !strings.HasPrefix(res.Detail, tc.want) || (tc.noNote && res.Detail != tc.want) {
				t.Fatalf("detail %q, want it to start with %q (note: %v)", res.Detail, tc.want, !tc.noNote)
			}
			if res.Status != tc.res.Status {
				t.Fatalf("status changed: %s", res.Status)
			}
		})
	}
}
