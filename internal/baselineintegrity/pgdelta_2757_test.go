package baselineintegrity

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// #2757: the two files of a PostgreSQL table delta are manifested files, and
// what a write of one can leave behind is not. The names are pgdelta's
// (DeadSuffix, NewSuffix, and its temporary names); this package cannot
// import it, so the test spells them.
func TestManifest_coversAPostgresTableDelta_2757(t *testing.T) {
	for name, want := range map[string]bool{
		"orders.000000.pgdead":          true,
		"orders.000000.pgnew":           true,
		"orders.000000.pgnew.tmp-4821":  false,
		"orders.000000.pgdead.tmp-4821": false,
		"orders.000000.PGNEW":           false,
		"orders.pgnewer":                false,
		// What it covered before, as before.
		"orders.parquet":        true,
		"orders.000001.posdel":  true,
		"orders.000001.upserts": true,
		"orders.keyidx":         false,
		"_SUCCESS":              false,
	} {
		if got := manifested(name); got != want {
			t.Errorf("manifested(%q) = %v, want %v", name, got, want)
		}
	}
}

// A snapshot's manifest lists the delta's files with the table's, a damaged
// one is refused, and a delta linked forward from a prior snapshot takes the
// digest that snapshot recorded.
func TestManifest_postgresTableDeltaIsCheckedAndReused_2757(t *testing.T) {
	ForgetVerified()
	t.Cleanup(ForgetVerified)
	root := t.TempDir()
	prior := filepath.Join(root, "2026-05-01T12-00-00Z")
	files := map[string]string{
		"public/orders.parquet":              "the table file",
		"public/orders.000000.pgdead":        "rows that are gone",
		"public/orders.000000.pgnew":         "rows as they are now",
		"public/orders.000000.pgnew.tmp-771": "a write that was cut short",
	}
	for rel, body := range files {
		p := filepath.Join(prior, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteManifest(prior); err != nil {
		t.Fatal(err)
	}
	m, ok, err := LoadManifest(prior)
	if err != nil || !ok {
		t.Fatalf("LoadManifest: %v, %v", ok, err)
	}
	if len(m.Files) != 3 {
		t.Fatalf("the manifest lists %v; want the table file and the delta's two", m.Files)
	}
	for _, rel := range []string{"public/orders.parquet", "public/orders.000000.pgdead", "public/orders.000000.pgnew"} {
		if m.Files[rel] == "" {
			t.Fatalf("%s is not in the manifest: %v", rel, m.Files)
		}
	}

	// Linked into the next snapshot: the digests are the prior's, not read
	// again.
	next := filepath.Join(root, "2026-05-01T13-00-00Z")
	if err := os.MkdirAll(filepath.Join(next, "public"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"orders.parquet", "orders.000000.pgdead", "orders.000000.pgnew"} {
		if err := os.Link(filepath.Join(prior, "public", name), filepath.Join(next, "public", name)); err != nil {
			t.Skipf("no hard links here: %v", err)
		}
	}
	st, err := WriteManifestFrom(next, []string{prior})
	if err != nil {
		t.Fatal(err)
	}
	if st.Reused != 3 || st.Hashed != 0 {
		t.Fatalf("reused %d, hashed %d; want the three digests taken from the prior", st.Reused, st.Hashed)
	}

	// Each of the delta's files, damaged, is refused; whole, it passes. A
	// new file of the same length under the same name, as a bad copy is.
	for _, name := range []string{"orders.000000.pgdead", "orders.000000.pgnew"} {
		p := filepath.Join(next, "public", name)
		if err := ValidateLocalFile(p); err != nil {
			t.Fatalf("%s, whole: %v", name, err)
		}
		whole, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		bad := append([]byte(nil), whole...)
		bad[0] ^= 0xff
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, bad, 0o644); err != nil {
			t.Fatal(err)
		}
		ForgetVerified()
		if err := ValidateLocalFile(p); !errors.Is(err, ErrIntegrity) {
			t.Fatalf("%s, damaged: %v; want ErrIntegrity", name, err)
		}
		if err := ValidateLocalFileOnce(p); !errors.Is(err, ErrIntegrity) {
			t.Fatalf("%s, damaged, through the memo: %v; want ErrIntegrity", name, err)
		}
		// The prior's own file is the other inode, untouched.
		if err := ValidateLocalFile(filepath.Join(prior, "public", name)); err != nil {
			t.Fatalf("the prior's %s: %v", name, err)
		}
	}
}
