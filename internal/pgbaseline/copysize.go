package pgbaseline

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
)

// What a snapshot of a PostgreSQL source takes on disk, as the source
// reports it (#3029): the counterpart of internal/doctor's estimate for a
// MySQL full read, which the console compares with the free space of the
// snapshot folder before it copies anything (consoleapp/backup_disk_pg.go,
// which holds the account of the measurements).
//
// The size is pg_table_size of every table the snapshot copies: the heap and
// its TOAST, without indexes, which a snapshot does not hold. PostgreSQL
// reads it from the files as they are now, so there is no day-old cache to
// turn off, as MySQL has. A partitioned table published through its root has
// no storage of its own, so it is the sum of its leaves.

// CopyEstimateServerLimit is how long the SOURCE may spend on the estimate's
// query before it stops it itself, under CopyEstimateTimeout: what comes back
// is then the server's own error, and no query runs on after nobody listens.
const CopyEstimateServerLimit = 12 * time.Second

// CopyEstimateTimeout bounds the whole read. It runs before every snapshot,
// and a source that does not answer in time must cost a note, not the
// snapshot.
const CopyEstimateTimeout = 15 * time.Second

// copyOtherStorageNamed is how many tables on another storage a note names:
// the largest, which are the ones the sizes are most wrong about.
const copyOtherStorageNamed = 2

// CopyEstimate is what the source reports about the tables a snapshot copies.
type CopyEstimate struct {
	// Bytes is pg_table_size, summed.
	Bytes  int64
	Tables int
	// Unsized counts tables the server gave no size for (dropped while the
	// query ran, or a part of them was). They add nothing to Bytes.
	Unsized int
	// OtherStorage counts tables kept by a table access method other than
	// PostgreSQL's own heap (a columnar or compressed store). What the server
	// reports for those can be a compressed size, which bounds nothing: the
	// copy is written from the rows. OtherStorageTop names the largest.
	OtherStorage    int
	OtherStorageTop []string
}

// copySizeQuery lists the tables of publication $1 with their size, over the
// same view and joins as discoverTables, so the tables sized are the tables
// copied. pg_partition_tree answers with the table itself for one that is not
// partitioned, and with every leaf for a partitioned one.
const copySizeQuery = `
	SELECT pt.schemaname, pt.tablename, sz.bytes, sz.unsized, sz.other
	FROM pg_publication_tables pt
	JOIN pg_namespace n ON n.nspname = pt.schemaname
	JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid
	CROSS JOIN LATERAL (
		SELECT COALESCE(sum(pg_table_size(p.relid)), 0)::bigint AS bytes,
		       (count(*) FILTER (WHERE pg_table_size(p.relid) IS NULL))::int AS unsized,
		       (count(*) FILTER (WHERE am.amname IS NOT NULL AND am.amname <> 'heap'))::int AS other
		FROM pg_partition_tree(c.oid::regclass) p
		LEFT JOIN pg_class lc ON lc.oid = p.relid
		LEFT JOIN pg_am am ON am.oid = lc.relam
		WHERE p.isleaf
	) sz
	WHERE pt.pubname = $1`

// copyTableRow is one row of copySizeQuery.
type copyTableRow struct {
	schema, table  string
	bytes          int64
	unsized, other int
}

// copyTableSum adds rows up as they are read, so a publication of a hundred
// thousand tables costs a few numbers and two names.
type copyTableSum struct {
	est CopyEstimate
	top []copyNamedSize
}

type copyNamedSize struct {
	name string
	size int64
}

// before reports whether a goes ahead of b: larger first, then by name, so
// the names a note prints do not change between two runs over equal sizes.
func (a copyNamedSize) before(b copyNamedSize) bool {
	if a.size != b.size {
		return a.size > b.size
	}
	return a.name < b.name
}

func (s *copyTableSum) add(r copyTableRow) {
	s.est.Tables++
	s.est.Bytes += max(r.bytes, 0)
	if r.unsized > 0 {
		s.est.Unsized++
	}
	if r.other == 0 {
		return
	}
	s.est.OtherStorage++
	n := copyNamedSize{printableName(r.schema) + "." + printableName(r.table), max(r.bytes, 0)}
	at := len(s.top)
	for at > 0 && n.before(s.top[at-1]) {
		at--
	}
	s.top = slices.Insert(s.top, at, n)
	s.top = s.top[:min(len(s.top), copyOtherStorageNamed)]
}

func (s *copyTableSum) estimate() CopyEstimate {
	est := s.est
	est.OtherStorageTop = nil
	for _, n := range s.top {
		est.OtherStorageTop = append(est.OtherStorageTop, n.name)
	}
	return est
}

// printableName keeps a schema or table name on the line it is printed in.
func printableName(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return '?'
		}
		return r
	}, s)
}

// CopyOtherStorageSentence is what a verdict says about tables on another
// storage, "" when there are none. It starts with a space, like the other
// parts of a note's tail.
func CopyOtherStorageSentence(est CopyEstimate) string {
	if est.OtherStorage == 0 {
		return ""
	}
	names := strings.Join(est.OtherStorageTop, ", ")
	if more := est.OtherStorage - len(est.OtherStorageTop); more > 0 {
		names += fmt.Sprintf(" and %d more", more)
	}
	if est.OtherStorage == 1 {
		return fmt.Sprintf(" 1 table is not stored as a PostgreSQL heap (%s): the size the server reports for it can be a compressed one, "+
			"and a snapshot is written from its rows, so it can need more than the sizes say.", names)
	}
	return fmt.Sprintf(" %d tables are not stored as a PostgreSQL heap (%s): the size the server reports for them can be a compressed one, "+
		"and a snapshot is written from their rows, so it can need more than the sizes say.", est.OtherStorage, names)
}

// ReadCopyEstimate sizes the tables a snapshot taken now would copy: the
// publication's, narrowed by filters as Run narrows them. One short-lived
// plain connection; nothing is written. A publication that does not exist, or
// holds no table in scope, answers with Tables 0: the snapshot itself says
// which.
func ReadCopyEstimate(ctx context.Context, queryDSN, publication string, filters event.Filters) (CopyEstimate, error) {
	conn, err := pgx.Connect(ctx, queryDSN)
	if err != nil {
		return CopyEstimate{}, fmt.Errorf("pgbaseline: connect to size the tables to copy: %w", err)
	}
	defer conn.Close(context.Background())
	// A limit the source enforces. One that could not be set is not a reason
	// to skip the check: the wait is still bounded by ctx.
	limit := fmt.Sprintf("SET statement_timeout = %d", CopyEstimateServerLimit.Milliseconds())
	if _, err := conn.Exec(ctx, limit); err != nil {
		slog.Warn("snapshot disk check: the source refused a time limit for the size query", "statement", limit, "error", err)
	}
	rows, err := conn.Query(ctx, copySizeQuery, publication)
	if err != nil {
		return CopyEstimate{}, fmt.Errorf("pgbaseline: sizing the tables of publication %q: %w", publication, err)
	}
	defer rows.Close()
	var sum copyTableSum
	for rows.Next() {
		var r copyTableRow
		if err := rows.Scan(&r.schema, &r.table, &r.bytes, &r.unsized, &r.other); err != nil {
			return CopyEstimate{}, fmt.Errorf("pgbaseline: sizing the tables of publication %q: %w", publication, err)
		}
		if !filters.Matches(r.schema, r.table) {
			continue
		}
		sum.add(r)
	}
	// A limit that fires mid-scan ends the rows early with an error here; a
	// partial sum must never be returned as the whole.
	if err := rows.Err(); err != nil {
		return CopyEstimate{}, fmt.Errorf("pgbaseline: sizing the tables of publication %q: %w", publication, err)
	}
	return sum.estimate(), nil
}
