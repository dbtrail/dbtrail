package query

import (
	"context"
	"database/sql"
	"errors"
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
// Limit there fixes WHICH keys are read, and which keys hold a change in the
// window does not depend on the order of their changes; it is only each
// key's latest change that does. So the first read stays as it was (the
// keys, and its overflow check), and this second one asks for the latest
// change in binary log order of those keys alone, with o's other filters, in
// batches of repickBatch keys: FetchMerged with LatestInBinlog, which reads at
// most two candidates per key and decides with query.LatestPerPKInBinlog.
//
// Each key's row stays in its place. A row with no pk_values is its own key
// and is kept as it is. A key the second read does not find (its changes
// rotated out between the two reads) keeps the first read's row.
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
	for len(keys) > 0 {
		batch := keys[:min(repickBatch, len(keys))]
		keys = keys[len(batch):]
		h := o
		h.Limit, h.Order = 0, ""
		h.PKValues, h.PKValuesIn = "", batch
		var order LatestPerPKOrder
		got, err := fetch(h, &order)
		if err != nil {
			return nil, total, err
		}
		total.add(order)
		for _, r := range got {
			latest[r.PKValues] = r
		}
	}
	out := make([]ResultRow, len(rows))
	for i, r := range rows {
		if l, ok := latest[r.PKValues]; ok && r.PKValues != "" {
			r = l
		}
		out[i] = r
	}
	return out, total, nil
}

// add counts p's keys into o, keeping o's first warning.
func (o *LatestPerPKOrder) add(p LatestPerPKOrder) {
	o.Disagreed += p.Disagreed
	o.Sorted += p.Sorted
	o.Refused += p.Refused
	if o.warning == "" {
		o.warning = p.warning
	}
}
