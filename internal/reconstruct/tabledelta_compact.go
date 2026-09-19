package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// MinorCompaction is what CompactTableDeltaMinor produced: the range pair,
// written under OutDir, and what it stands for.
type MinorCompaction struct {
	Lo, Hi          int
	Posdel, Upserts string
	// Merged is how many pairs the range replaces.
	Merged int
}

// CompactTableDeltaMinor merges the pairs lo..hi of a table's chain into ONE
// range pair "<table>.<lo>-<hi>.{posdel,upserts}" written under outDir
// (#1723, the minor compaction). DuckDB only, no Go writer: the result is
// still a delta, so it keeps bintrail_pk / bintrail_op and the writer's
// encodings exactly as the pairs hold them. The posdel is the union of the
// pairs' dead positions; the upserts keep the newest version of every key
// (the same QUALIFY the state SQL applies, so the state read through the
// range is the state read through the pairs). The footer is the LAST
// merged pair's (its binlog anchor, snapshot time, chain start, base
// identity) with the range's two ends: a reader resuming from the range
// resumes where pair hi ended.
//
// The base is never touched, and the chain is not modified here: the caller
// installs the range beside the base (a refresh, by linking it forward in
// place of the pairs) once it is complete. Files are written to a temporary
// name and renamed into place, so a crash leaves no half range under outDir
// that parses as one.
func CompactTableDeltaMinor(ctx context.Context, basePath string, chain *baseline.TableDeltaChain, lo, hi int, outDir string) (*MinorCompaction, error) {
	if chain == nil || chain.Legacy {
		return nil, fmt.Errorf("compact table delta: no numbered chain beside %s", basePath)
	}
	var in []baseline.TableDeltaFile
	for _, f := range chain.Files {
		if f.SeqLo >= lo && f.Seq <= hi {
			in = append(in, f)
		}
	}
	if len(in) < 2 || in[0].SeqLo != lo || in[len(in)-1].Seq != hi {
		return nil, fmt.Errorf("compact table delta: the chain beside %s has no pairs covering exactly %d..%d", basePath, lo, hi)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("compact table delta: %w", err)
	}
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	lit := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	list := func(paths []string) string {
		q := make([]string, len(paths))
		for i, p := range paths {
			q[i] = lit(p)
		}
		return "[" + strings.Join(q, ", ") + "]"
	}
	var posdels, upserts []string
	for _, f := range in {
		posdels = append(posdels, f.Posdel)
		upserts = append(upserts, f.Upserts)
	}
	last := in[len(in)-1]
	// The footer: every key of the last pair's upserts, the two ends
	// replaced. Read as raw key/value so nothing this build does not know
	// (a newer stamp) is dropped on the way.
	// Scanned as BYTES: parquet_kv_metadata types both columns BLOB, and a
	// cast to VARCHAR does not decode the bytes, it ESCAPES them (every
	// newline, quote and non-ASCII byte of a CREATE TABLE would be rewritten
	// into the range's footer, certified by the next manifest).
	rows, err := ddb.QueryContext(ctx, "SELECT key, value FROM parquet_kv_metadata("+lit(last.Upserts)+")")
	if err != nil {
		return nil, fmt.Errorf("read the footer of %s: %w", last.Upserts, err)
	}
	md := map[string]string{}
	for rows.Next() {
		var k, v []byte
		if err := rows.Scan(&k, &v); err != nil {
			rows.Close()
			return nil, err
		}
		md[string(k)] = string(v)
	}
	rows.Close()
	if md[baseline.MetaKeyDeltaChainStart] == "" || md[baseline.MetaKeyDeltaBaseAnchor] == "" {
		return nil, fmt.Errorf("compact table delta: %s carries no chain footer", last.Upserts)
	}
	md[baseline.MetaKeyDeltaSeq] = strconv.Itoa(hi)
	md[baseline.MetaKeyDeltaSeqLo] = strconv.Itoa(lo)
	keys := make([]string, 0, len(md))
	for k := range md {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	kv := make([]string, 0, len(keys))
	for _, k := range keys {
		kv = append(kv, lit(k)+": "+lit(md[k]))
	}
	opts := fmt.Sprintf("(FORMAT PARQUET, COMPRESSION '%s', KV_METADATA {%s})", ParquetWriterCompression, strings.Join(kv, ", "))

	posdelPath, upsertsPath := baseline.TableDeltaRangePaths(filepath.Join(outDir, filepath.Base(basePath)), lo, hi)
	tmpPosdel, tmpUpserts := posdelPath+".tmp", upsertsPath+".tmp"
	fail := func(err error) (*MinorCompaction, error) {
		for _, p := range []string{tmpPosdel, tmpUpserts, posdelPath, upsertsPath} {
			os.Remove(p)
		}
		return nil, err
	}
	// Dead positions: the union, ascending, as WritePosdel writes them.
	q := fmt.Sprintf("COPY (SELECT DISTINCT \"%s\" FROM read_parquet(%s) WHERE \"%s\" IS NOT NULL ORDER BY 1) TO %s %s",
		baseline.TableDeltaPosColumn, list(posdels), baseline.TableDeltaPosColumn, lit(tmpPosdel), opts)
	if _, err := ddb.ExecContext(ctx, q); err != nil {
		return fail(fmt.Errorf("compact dead positions %d..%d of %s: %w", lo, hi, basePath, err))
	}
	// Upserts: the newest version of every key across the merged pairs, in
	// the order the state SQL uses (file name, which is sequence order).
	q = fmt.Sprintf("COPY (SELECT * EXCLUDE (filename) FROM read_parquet(%s, filename=true, union_by_name=true) "+
		"QUALIFY row_number() OVER (PARTITION BY \"%s\" ORDER BY filename DESC) = 1) TO %s %s",
		list(upserts), baseline.TableDeltaPKColumn, lit(tmpUpserts), opts)
	if _, err := ddb.ExecContext(ctx, q); err != nil {
		return fail(fmt.Errorf("compact upserts %d..%d of %s: %w", lo, hi, basePath, err))
	}
	for _, p := range [][2]string{{tmpPosdel, posdelPath}, {tmpUpserts, upsertsPath}} {
		if err := fsyncFile(p[0]); err != nil {
			return fail(err)
		}
		if err := os.Rename(p[0], p[1]); err != nil {
			return fail(err)
		}
	}
	return &MinorCompaction{Lo: lo, Hi: hi, Posdel: posdelPath, Upserts: upsertsPath, Merged: len(in)}, nil
}

func fsyncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// CompactionDir is where a compaction job stages its result for one chain:
// "<compactDir>/<schema>/<table>/<chain start>/". The chain start names the
// chain, so a result for a chain that has since been rewritten is never
// mistaken for the current one.
func CompactionDir(compactDir, schema, table string, chainStart time.Time) string {
	return filepath.Join(compactDir, schema, table, SnapshotDirName(chainStart))
}

// adoptCompaction finds a COMPLETE compaction result (its _SUCCESS written)
// for prev's chain and checks it is exactly a merge of a prefix of that
// chain: one range pair, lo..hi covered by prev's pairs, its footers naming
// prev's chain start and base. A result that does not fit (a chain
// rewritten since, a job of another build) is removed with a warning, so it
// cannot be adopted by mistake later; one without _SUCCESS is a job's
// unfinished work (or a dead job's, which the daemon sweeps at boot) and is
// left alone.
func adoptCompaction(compactDir, schema, table string, prev *tableDelta) (baseline.TableDeltaFile, string, bool) {
	sweepOtherChains(compactDir, schema, table, prev.Meta.DeltaChainStart)
	var none baseline.TableDeltaFile
	dir := CompactionDir(compactDir, schema, table, prev.Meta.DeltaChainStart)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return none, "", false
	}
	if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); err != nil {
		return none, "", false
	}
	discard := func(why string, args ...any) (baseline.TableDeltaFile, string, bool) {
		slog.Warn("table delta compaction not adopted; removing it: "+why,
			append([]any{"schema", schema, "table", table, "dir", dir}, args...)...)
		if err := os.RemoveAll(dir); err != nil {
			slog.Warn("could not remove a compaction result", "dir", dir, "error", err)
		}
		return none, "", false
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && e.Name() != baseline.SuccessMarker {
			names = append(names, e.Name())
		}
	}
	chains, err := baseline.MarkTableDeltaFiles(dir, names)
	if err != nil || len(chains) != 1 {
		return discard("it does not hold exactly one table's range pair", "error", err)
	}
	var r baseline.TableDeltaFile
	for _, c := range chains {
		if c.Legacy || len(c.Files) != 1 {
			return discard("it holds something other than one pair")
		}
		r = c.Files[0]
	}
	if !r.Range() {
		return discard("its pair is not a range", "seq", r.Seq)
	}
	covered := false
	for _, f := range prev.Chain.Files {
		if f.SeqLo == r.SeqLo {
			covered = true
		}
		if f.Seq == r.Seq {
			break
		}
		if f.Seq > r.Seq {
			covered = false
			break
		}
	}
	if !covered || r.Seq > prev.Meta.DeltaSeq {
		return discard("the chain does not cover its range", "lo", r.SeqLo, "hi", r.Seq, "chain_last", prev.Meta.DeltaSeq)
	}
	um, err := baseline.ReadParquetMetadata(r.Upserts)
	if err != nil {
		return discard("its upserts file cannot be read", "error", err)
	}
	pm, err := baseline.ReadParquetMetadata(r.Posdel)
	if err != nil {
		return discard("its dead-positions file cannot be read", "error", err)
	}
	switch {
	case !um.DeltaChainStart.Equal(prev.Meta.DeltaChainStart) || um.DeltaBaseAnchor != prev.Meta.DeltaBaseAnchor || um.DeltaBaseSize != prev.Meta.DeltaBaseSize:
		return discard("it was made for another chain or base")
	case um.DeltaSeqLo != r.SeqLo || um.DeltaSeq != r.Seq || pm.DeltaSeqLo != r.SeqLo || pm.DeltaSeq != r.Seq:
		return discard("its footers do not name its range", "lo", r.SeqLo, "hi", r.Seq)
	case pm.DeltaBaseAnchor != um.DeltaBaseAnchor || !pm.DeltaChainStart.Equal(um.DeltaChainStart) || pm.BinlogFile != um.BinlogFile || pm.BinlogPos != um.BinlogPos:
		return discard("its two files were not written by the same run")
	}
	return r, dir, true
}

// sweepOtherChains removes every staged result for the table that names a
// chain other than the current one (H2 of the review): a chain that ended
// (a rewrite, deltas turned off, the size or age rule) before its result
// was adopted would otherwise leave its merged pairs on disk for good, since
// only the current chain's directory is ever visited.
func sweepOtherChains(compactDir, schema, table string, chainStart time.Time) {
	parent := filepath.Join(compactDir, schema, table)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	keep := SnapshotDirName(chainStart)
	for _, e := range entries {
		if !e.IsDir() || e.Name() == keep {
			continue
		}
		dir := filepath.Join(parent, e.Name())
		if err := os.RemoveAll(dir); err != nil {
			slog.Warn("could not remove a compaction result of an ended chain", "dir", dir, "error", err)
			continue
		}
		slog.Info("removed a compaction result for a chain that has since ended", "schema", schema, "table", table, "dir", dir)
	}
}

// spliceRange returns files with the pairs r covers replaced by r.
func spliceRange(files []baseline.TableDeltaFile, r baseline.TableDeltaFile) []baseline.TableDeltaFile {
	out := make([]baseline.TableDeltaFile, 0, len(files))
	placed := false
	for _, f := range files {
		if f.SeqLo >= r.SeqLo && f.Seq <= r.Seq {
			if !placed {
				out = append(out, r)
				placed = true
			}
			continue
		}
		out = append(out, f)
	}
	return out
}
