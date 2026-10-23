package baseline

import (
	"context"
	"database/sql"
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/keyidx"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// #2496 slice 2: the conversion of a dump writes the key index beside every
// table with an integer key, the manifest leaves it out, and nothing that
// lists a snapshot's files by pattern takes it for a table file or a delta.
func TestRun_writesTheKeyIndexBesideEachTable_2496(t *testing.T) {
	in, out := t.TempDir(), t.TempDir()
	writeDump1938(t, in, false)
	at := time.Date(2026, 10, 10, 13, 51, 42, 0, time.UTC)
	if _, err := Run(context.Background(), Config{InputDir: in, OutputDir: out, Compression: "zstd", Timestamp: at, TableDeltas: true}); err != nil {
		t.Fatal(err)
	}
	snap := filepath.Join(out, snapshotdir.Name(at))
	for _, table := range []string{"a", "b"} {
		base := filepath.Join(snap, "shop", table+".parquet")
		b, err := os.ReadFile(keyidx.PathFor(base))
		if err != nil {
			t.Fatalf("no index beside %s: %v", base, err)
		}
		if string(b[:8]) != keyidx.Magic || binary.BigEndian.Uint64(b[12:20]) != 2 || b[10] != 1 || b[keyidx.HeaderSize(nil)] != 1 {
			t.Fatalf("%s: header %x; want 2 records of one INTEGER key", keyidx.PathFor(base), b[:41])
		}
		if _, err := os.Stat(keyidx.PathFor(base) + ".tmp"); err == nil {
			t.Fatalf("%s: the .tmp is still there", base)
		}
	}
	m, ok, err := baselineintegrity.LoadManifest(snap)
	if err != nil || !ok {
		t.Fatalf("manifest: ok=%v err=%v", ok, err)
	}
	for f := range m.Files {
		if strings.HasSuffix(f, keyidx.Suffix) {
			t.Fatalf("the manifest lists %s", f)
		}
	}
	if len(m.Files) != 6 {
		t.Fatalf("the manifest lists %d files, want two tables with their empty pairs", len(m.Files))
	}
	if _, err := os.Stat(filepath.Join(snap, SuccessMarker)); err != nil {
		t.Fatal("the snapshot was not published")
	}

	// The chain listing sees the pair and not the index; the table is whole.
	base := filepath.Join(snap, "shop", "a.parquet")
	chain, err := ListTableDelta(context.Background(), base)
	if err != nil || chain == nil || len(chain.Files) != 1 {
		t.Fatalf("chain beside the index: %+v, %v", chain, err)
	}
	deltas, err := SnapshotTableDeltas(context.Background(), snap)
	if err != nil || len(deltas) != 2 {
		t.Fatalf("snapshot's deltas: %d, %v", len(deltas), err)
	}

	// The globs, run by DuckDB itself: the follow globs match the base and
	// the pair, the chain globs the pair, neither the index.
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	matches := func(pattern string) []string {
		rows, err := ddb.Query("SELECT file FROM glob('" + strings.ReplaceAll(pattern, "'", "''") + "') ORDER BY 1")
		if err != nil {
			t.Fatalf("%s: %v", pattern, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var f string
			if err := rows.Scan(&f); err != nil {
				t.Fatal(err)
			}
			out = append(out, filepath.Base(f))
		}
		return out
	}
	fp, fu := TableDeltaFollowGlobs(base)
	cp, cu := TableDeltaGlobs(base)
	for _, tc := range []struct {
		pattern string
		want    []string
	}{
		{fp, []string{"a.000000.posdel", "a.parquet"}},
		{fu, []string{"a.000000.upserts", "a.parquet"}},
		{cp, []string{"a.000000.posdel"}},
		{cu, []string{"a.000000.upserts"}},
	} {
		if got := matches(tc.pattern); !slices.Equal(got, tc.want) {
			t.Errorf("%s matches %v, want %v", tc.pattern, got, tc.want)
		}
	}
	// What the S3 listing probes by stem does find it, and the name parser
	// does not read it as a delta file.
	if _, _, _, _, ok := ParseTableDeltaRange("a.keyidx"); ok {
		t.Error("a.keyidx parsed as a delta file")
	}
	if names := matches(deltaProbePattern(base)); !slices.Contains(names, "a.keyidx") {
		t.Errorf("the stem probe %v should see the index; the parser is what ignores it", names)
	}
}

// A dump without a usable PRIMARY KEY line converts and gets no index.
func TestRun_aTableWithoutAKeyGetsNoIndex_2496(t *testing.T) {
	in, out := t.TempDir(), t.TempDir()
	writeDump1938(t, in, false)
	schema := filepath.Join(in, "shop.a-schema.sql")
	b, err := os.ReadFile(schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(schema, []byte(strings.Replace(string(b), ",\n  PRIMARY KEY (`id`)", "", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 10, 13, 51, 42, 0, time.UTC)
	if _, err := Run(context.Background(), Config{InputDir: in, OutputDir: out, Compression: "zstd", Timestamp: at}); err != nil {
		t.Fatal(err)
	}
	snap := filepath.Join(out, snapshotdir.Name(at))
	if _, err := os.Stat(keyidx.PathFor(filepath.Join(snap, "shop", "a.parquet"))); err == nil {
		t.Fatal("a table without a key got an index")
	}
	if _, err := os.Stat(keyidx.PathFor(filepath.Join(snap, "shop", "b.parquet"))); err != nil {
		t.Fatal("the keyed table got no index")
	}
}
