package query

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
)

// repickBatch bounds the keys one repick read names in its IN list.
const repickBatch = 500

// LatestRepicker is a Fetcher that can also take a set of rows read with
// Options.LimitPerPK = 1 and put in place of each one its key's latest change
// in binary log order (#2156). *Engine and *MergedFetcher are.
type LatestRepicker interface {
	RepickLatestInBinlog(ctx context.Context, opts Options, rows []ResultRow) ([]ResultRow, LatestPerPKOrder, error)
}

// RepickLatestInBinlog takes rows that a FetchMerged with o returned, each
// pk_values' latest change by statement time (o.Opts.LimitPerPK = 1), and
// returns them with each key's row replaced by the key's latest change in
// binary log order, where the index can show that order (#2156), and what was
// decided about it.
//
// It is for a reader whose fetch carries a Limit, which FetchMergedOptions.
// LatestInBinlog does not take: the shim's full-table `_flashback` bounds its
// read by its row cap and recover-cascade bounds each scan for children. The
// first read stays as it was (its overflow check, and which keys it read),
// and a second one asks for the latest change in binary log order:
// FetchMerged with LatestInBinlog, which reads at most two candidates per key
// and decides with query.LatestPerPKInBinlog.
//
//   - When the first read returned fewer rows than its Limit (or had none),
//     it read every key of the window, and so will the second one: it is ONE
//     read of the same window, bounded by the same keys.
//   - Otherwise the Limit cut the keys, and the second read names the keys
//     the first one returned, in batches of repickBatch, with o's other
//     filters.
//
// Each key's row stays in its place. A row with no pk_values is its own key
// and is kept as it is. A key the second read does not find (its changes
// rotated out between the two reads) keeps the first read's row, and the
// count of such keys is logged.
func RepickLatestInBinlog(ctx context.Context, db *sql.DB, engine *Engine, o FetchMergedOptions, rows []ResultRow) ([]ResultRow, LatestPerPKOrder, error) {
	return repickLatestInBinlog(o.Opts, rows, func(opts Options, latest *LatestPerPKOrder) ([]ResultRow, error) {
		h := o
		h.Opts, h.LatestInBinlog, h.LatestOrder = opts, true, latest
		got, _, err := FetchMerged(ctx, db, engine, h)
		return got, err
	})
}

// RepickLatestInBinlog is query.RepickLatestInBinlog over the live index
// only, as Engine.Fetch reads.
func (e *Engine) RepickLatestInBinlog(ctx context.Context, opts Options, rows []ResultRow) ([]ResultRow, LatestPerPKOrder, error) {
	return RepickLatestInBinlog(ctx, e.db, e, FetchMergedOptions{Opts: opts, NoArchive: true, AllowGaps: true}, rows)
}

// RepickLatestInBinlog is query.RepickLatestInBinlog through this fetcher's
// reads: its archives, and an archive it cannot read is an error, as in Fetch.
func (m *MergedFetcher) RepickLatestInBinlog(ctx context.Context, opts Options, rows []ResultRow) ([]ResultRow, LatestPerPKOrder, error) {
	return repickLatestInBinlog(opts, rows, func(o Options, latest *LatestPerPKOrder) ([]ResultRow, error) {
		return m.fetch(ctx, o, latest)
	})
}

// repickLatestInBinlog is RepickLatestInBinlog with the read as fetch: it
// gets o with the batch's keys and fills in the decision.
func repickLatestInBinlog(o Options, rows []ResultRow, fetch func(Options, *LatestPerPKOrder) ([]ResultRow, error)) ([]ResultRow, LatestPerPKOrder, error) {
	var total LatestPerPKOrder
	if o.LimitPerPK != 1 {
		return nil, total, errors.New("RepickLatestInBinlog: the rows must be read with LimitPerPK 1")
	}
	var keys []string
	seen := make(map[string]bool)
	for i := range rows {
		if pk := rows[i].PKValues; pk != "" && !seen[pk] {
			seen[pk] = true
			keys = append(keys, pk)
		}
	}
	if len(keys) == 0 {
		return rows, total, nil
	}
	latest := make(map[string]ResultRow, len(keys))
	read := func(h Options) error {
		h.Limit, h.Order = 0, ""
		var order LatestPerPKOrder
		got, err := fetch(h, &order)
		if err != nil {
			return err
		}
		total.Add(order)
		for _, r := range got {
			if seen[r.PKValues] {
				latest[r.PKValues] = r
			}
		}
		return nil
	}
	if o.Limit == 0 || len(rows) < o.Limit {
		if err := read(o); err != nil {
			return nil, total, err
		}
	} else {
		for len(keys) > 0 {
			batch := keys[:min(repickBatch, len(keys))]
			keys = keys[len(batch):]
			h := o
			h.PKValues, h.PKValuesIn = "", batch
			if err := read(h); err != nil {
				return nil, total, err
			}
		}
	}
	out := make([]ResultRow, len(rows))
	missing := 0
	for i, r := range rows {
		if r.PKValues != "" {
			if l, ok := latest[r.PKValues]; ok {
				r = l
			} else {
				missing++
			}
		}
		out[i] = r
	}
	if missing > 0 {
		slog.Warn("latest change in binary log order: some rows were not found by the second read and keep their latest change by statement time",
			"rows", missing, "schema", o.Schema, "table", o.Table)
	}
	return out, total, nil
}

// Add counts p's keys into o, keeping o's first warning: what a caller that
// decided several reads reports once.
func (o *LatestPerPKOrder) Add(p LatestPerPKOrder) {
	o.Disagreed += p.Disagreed
	o.Sorted += p.Sorted
	o.Refused += p.Refused
	if o.warning == "" {
		o.warning = p.warning
	}
}
