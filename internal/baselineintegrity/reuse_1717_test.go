package baselineintegrity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// #1717: a refresh links most of the previous snapshot's files forward, and
// the manifest used to hash every one of them again. A file that IS the
// previous snapshot's file (same inode, so the same bytes by definition)
// takes that snapshot's recorded digest instead. Anything less certain than
// the same inode — a copy, a rewrite, a prior with no usable manifest — is
// hashed, as before.

func link(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(src, dst); err != nil {
		t.Fatal(err)
	}
}

func manifestOf(t *testing.T, snap string) map[string]string {
	t.Helper()
	m, ok, err := LoadManifest(snap)
	if err != nil || !ok {
		t.Fatalf("LoadManifest(%s): ok=%v err=%v", snap, ok, err)
	}
	return m.Files
}

// setPriorDigest rewrites one recorded digest in a manifest. A reused entry
// then carries the bogus value, which is how a test tells "reused" from
// "hashed again to the same answer".
func setPriorDigest(t *testing.T, snap, rel, digest string) {
	t.Helper()
	m, ok, err := LoadManifest(snap)
	if err != nil || !ok {
		t.Fatal(err)
	}
	m.Files[rel] = digest
	b, _ := json.Marshal(m)
	if err := os.WriteFile(filepath.Join(snap, ManifestName), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestWriteManifestFrom_reusesTheDigestOfALinkedFile(t *testing.T) {
	root := t.TempDir()
	prev, next := filepath.Join(root, "s1"), filepath.Join(root, "s2")
	writeFileT(t, filepath.Join(prev, "db", "big.parquet"), []byte("big bytes"))
	writeFileT(t, filepath.Join(prev, "db", "big.000000.posdel"), []byte("pd"))
	writeFileT(t, filepath.Join(prev, "db", "big.000000.upserts"), []byte("up"))
	writeFileT(t, filepath.Join(prev, "db", "small.parquet"), []byte("small v1"))
	if err := WriteManifest(prev); err != nil {
		t.Fatal(err)
	}
	setPriorDigest(t, prev, "db/big.parquet", "deadbeef")
	setPriorDigest(t, prev, "db/big.000000.upserts", "feedface")

	// The next snapshot: big and its pair linked, small rewritten (new bytes),
	// a new pair for big, and a table that did not exist before.
	link(t, filepath.Join(prev, "db", "big.parquet"), filepath.Join(next, "db", "big.parquet"))
	link(t, filepath.Join(prev, "db", "big.000000.posdel"), filepath.Join(next, "db", "big.000000.posdel"))
	link(t, filepath.Join(prev, "db", "big.000000.upserts"), filepath.Join(next, "db", "big.000000.upserts"))
	writeFileT(t, filepath.Join(next, "db", "big.000001.posdel"), []byte("new pd"))
	writeFileT(t, filepath.Join(next, "db", "big.000001.upserts"), []byte("new up"))
	writeFileT(t, filepath.Join(next, "db", "small.parquet"), []byte("small v2"))
	writeFileT(t, filepath.Join(next, "db", "fresh.parquet"), []byte("fresh"))
	writeFileT(t, filepath.Join(next, "db", "fresh2.parquet"), []byte("fresh too"))
	writeFileT(t, filepath.Join(next, "_SUCCESS"), nil)

	st, err := WriteManifestFrom(next, []string{prev})
	if err != nil {
		t.Fatal(err)
	}
	if st.Reused != 3 || st.Hashed != 5 {
		t.Fatalf("stats = %+v, want 3 reused (big + its pair 0) and 5 hashed", st)
	}
	got := manifestOf(t, next)
	if got["db/big.parquet"] != "deadbeef" || got["db/big.000000.upserts"] != "feedface" {
		t.Fatalf("linked files did not take the prior's digest: %v", got)
	}
	want, _ := CRC32CFile(filepath.Join(next, "db", "small.parquet"))
	if got["db/small.parquet"] != want {
		t.Fatalf("rewritten file was not hashed fresh: %v", got)
	}
	if _, ok := got["db/big.000001.upserts"]; !ok {
		t.Fatalf("new pair missing: %v", got)
	}
	if _, ok := got["_SUCCESS"]; ok || len(got) != 8 {
		t.Fatalf("manifest = %v", got)
	}
}

// TestWriteManifestFrom_hashesWhenTheInodeDiffers: the same relative path
// and even the same bytes are not enough; only the same file is.
func TestWriteManifestFrom_hashesWhenTheInodeDiffers(t *testing.T) {
	root := t.TempDir()
	prev, next := filepath.Join(root, "s1"), filepath.Join(root, "s2")
	writeFileT(t, filepath.Join(prev, "db", "t.parquet"), []byte("same bytes"))
	if err := WriteManifest(prev); err != nil {
		t.Fatal(err)
	}
	setPriorDigest(t, prev, "db/t.parquet", "deadbeef")
	writeFileT(t, filepath.Join(next, "db", "t.parquet"), []byte("same bytes")) // a copy, not a link
	st, err := WriteManifestFrom(next, []string{prev})
	if err != nil {
		t.Fatal(err)
	}
	real, _ := CRC32CFile(filepath.Join(next, "db", "t.parquet"))
	if st.Reused != 0 || st.Hashed != 1 || manifestOf(t, next)["db/t.parquet"] != real {
		t.Fatalf("a copy took the prior's digest: %+v %v", st, manifestOf(t, next))
	}
}

// TestWriteManifestFrom_hashesWhenThePriorCannotVouch: no manifest, an
// unrecognised manifest, an entry missing from it, a prior that is not
// there, and the snapshot itself listed as its own prior.
func TestWriteManifestFrom_hashesWhenThePriorCannotVouch(t *testing.T) {
	newPair := func(t *testing.T) (prev, next string) {
		root := t.TempDir()
		prev, next = filepath.Join(root, "s1"), filepath.Join(root, "s2")
		writeFileT(t, filepath.Join(prev, "db", "t.parquet"), []byte("bytes"))
		link(t, filepath.Join(prev, "db", "t.parquet"), filepath.Join(next, "db", "t.parquet"))
		return prev, next
	}
	real := func(next string) string {
		d, _ := CRC32CFile(filepath.Join(next, "db", "t.parquet"))
		return d
	}
	t.Run("no prior manifest", func(t *testing.T) {
		prev, next := newPair(t)
		st, err := WriteManifestFrom(next, []string{prev})
		if err != nil || st.Reused != 0 || st.Hashed != 1 || manifestOf(t, next)["db/t.parquet"] != real(next) {
			t.Fatalf("a prior with no manifest vouched for a file: %+v %v %v", st, err, manifestOf(t, next))
		}
	})
	t.Run("prior manifest of another version", func(t *testing.T) {
		prev, next := newPair(t)
		if err := os.WriteFile(filepath.Join(prev, ManifestName), []byte(`{"version":2,"algo":"crc32c","files":{"db/t.parquet":"deadbeef"}}`), 0o644); err != nil {
			t.Fatal(err)
		}
		st, _ := WriteManifestFrom(next, []string{prev})
		if st.Reused != 0 || manifestOf(t, next)["db/t.parquet"] != real(next) {
			t.Fatalf("a manifest of another version vouched for a file: %+v %v", st, manifestOf(t, next))
		}
	})
	t.Run("prior manifest of another algo", func(t *testing.T) {
		prev, next := newPair(t)
		if err := os.WriteFile(filepath.Join(prev, ManifestName), []byte(`{"version":1,"algo":"sha256","files":{"db/t.parquet":"deadbeef"}}`), 0o644); err != nil {
			t.Fatal(err)
		}
		st, _ := WriteManifestFrom(next, []string{prev})
		if st.Reused != 0 || manifestOf(t, next)["db/t.parquet"] != real(next) {
			t.Fatalf("a manifest of another algo vouched for a file: %+v %v", st, manifestOf(t, next))
		}
	})
	t.Run("prior file pruned, its manifest still there", func(t *testing.T) {
		// A retention prune racing a refresh: the inode lives on through the
		// new link, the prior's path is gone. Hash, never fail.
		prev, next := newPair(t)
		if err := WriteManifest(prev); err != nil {
			t.Fatal(err)
		}
		setPriorDigest(t, prev, "db/t.parquet", "deadbeef")
		if err := os.Remove(filepath.Join(prev, "db", "t.parquet")); err != nil {
			t.Fatal(err)
		}
		st, err := WriteManifestFrom(next, []string{prev})
		if err != nil || st.Reused != 0 || st.Hashed != 1 || manifestOf(t, next)["db/t.parquet"] != real(next) {
			t.Fatalf("a pruned prior file was not simply hashed: %+v %v %v", st, err, manifestOf(t, next))
		}
	})
	t.Run("the snapshot as its own prior", func(t *testing.T) {
		// A file is always the same file as itself: without the guard a
		// stale manifest left in the snapshot would vouch for it.
		_, next := newPair(t)
		if err := os.WriteFile(filepath.Join(next, ManifestName), []byte(`{"version":1,"algo":"crc32c","files":{"db/t.parquet":"deadbeef"}}`), 0o644); err != nil {
			t.Fatal(err)
		}
		st, err := WriteManifestFrom(next, []string{next})
		if err != nil || st.Reused != 0 || manifestOf(t, next)["db/t.parquet"] != real(next) {
			t.Fatalf("the snapshot vouched for itself: %+v %v %v", st, err, manifestOf(t, next))
		}
	})
	t.Run("prior manifest lacks the entry", func(t *testing.T) {
		prev, next := newPair(t)
		if err := os.WriteFile(filepath.Join(prev, ManifestName), []byte(`{"version":1,"algo":"crc32c","files":{}}`), 0o644); err != nil {
			t.Fatal(err)
		}
		st, _ := WriteManifestFrom(next, []string{prev})
		if st.Reused != 0 || manifestOf(t, next)["db/t.parquet"] != real(next) {
			t.Fatalf("a manifest without the entry vouched for a file: %+v %v", st, manifestOf(t, next))
		}
	})
	t.Run("prior manifest unreadable", func(t *testing.T) {
		prev, next := newPair(t)
		if err := os.WriteFile(filepath.Join(prev, ManifestName), []byte(`{not json`), 0o644); err != nil {
			t.Fatal(err)
		}
		st, err := WriteManifestFrom(next, []string{prev})
		if err != nil || st.Reused != 0 || manifestOf(t, next)["db/t.parquet"] != real(next) {
			t.Fatalf("an unreadable manifest vouched for a file or failed the write: %+v %v %v", st, err, manifestOf(t, next))
		}
	})
	t.Run("absent prior and an empty entry", func(t *testing.T) {
		prev, next := newPair(t)
		st, err := WriteManifestFrom(next, []string{filepath.Join(prev, "gone"), ""})
		if err != nil || st.Reused != 0 || st.Hashed != 1 {
			t.Fatalf("an absent prior failed the write or vouched: %+v %v", st, err)
		}
	})
}

// TestWriteManifestFrom_firstPriorThatVouchesWins: a table carried from an
// older snapshot than the rest (FindBaseline's stale fallback) is found in
// the second prior.
func TestWriteManifestFrom_firstPriorThatVouchesWins(t *testing.T) {
	root := t.TempDir()
	older, newer, next := filepath.Join(root, "s0"), filepath.Join(root, "s1"), filepath.Join(root, "s2")
	writeFileT(t, filepath.Join(older, "db", "cold.parquet"), []byte("cold"))
	if err := WriteManifest(older); err != nil {
		t.Fatal(err)
	}
	setPriorDigest(t, older, "db/cold.parquet", "c01dc01d")
	writeFileT(t, filepath.Join(newer, "db", "hot.parquet"), []byte("hot"))
	if err := WriteManifest(newer); err != nil {
		t.Fatal(err)
	}
	setPriorDigest(t, newer, "db/hot.parquet", "h07h07h0")
	link(t, filepath.Join(older, "db", "cold.parquet"), filepath.Join(next, "db", "cold.parquet"))
	link(t, filepath.Join(newer, "db", "hot.parquet"), filepath.Join(next, "db", "hot.parquet"))
	st, err := WriteManifestFrom(next, []string{newer, older})
	if err != nil || st.Reused != 2 {
		t.Fatalf("%+v %v", st, err)
	}
	got := manifestOf(t, next)
	if got["db/cold.parquet"] != "c01dc01d" || got["db/hot.parquet"] != "h07h07h0" {
		t.Fatalf("%v", got)
	}
}

// TestWriteManifest_isWriteManifestFromWithNoPriors pins the old entry point.
func TestWriteManifest_isWriteManifestFromWithNoPriors(t *testing.T) {
	snap := t.TempDir()
	writeFileT(t, filepath.Join(snap, "db", "t.parquet"), []byte("x"))
	if err := WriteManifest(snap); err != nil {
		t.Fatal(err)
	}
	real, _ := CRC32CFile(filepath.Join(snap, "db", "t.parquet"))
	if manifestOf(t, snap)["db/t.parquet"] != real {
		t.Fatal("WriteManifest changed")
	}
}
