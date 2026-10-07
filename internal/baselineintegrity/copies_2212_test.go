package baselineintegrity

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// #2212: a table copied inside S3 is not on disk, so the new snapshot's
// manifest cannot hash it. It carries the digest the SOURCE snapshot's
// manifest recorded instead, which is right for the same reason a hard link
// reuses one (#1717): the bytes are the same. Edge cases: a digest found; no
// manifest at the source (a snapshot from before #636); a manifest without
// the file; a manifest of another version; a manifest that cannot be read
// (an error, never "no digest"); and that reading the digest never GETs the
// table's own bytes.
func TestS3ManifestDigest_2212(t *testing.T) {
	s := installStub(t)
	data := []byte("table bytes")
	s.objects["bkt-dig/srv/2026-10-07T11-00-00Z/_MANIFEST"] = manifestJSON(t, map[string]string{"shop/orders.parquet": crcHex(data)})
	s.objects["bkt-dig/srv/2026-10-07T11-00-00Z/shop/orders.parquet"] = data
	old, _ := json.Marshal(Manifest{Version: 99, Algo: "crc32c", Files: map[string]string{"shop/orders.parquet": "x"}})
	s.objects["bkt-dig-v99/srv/2026-10-07T11-00-00Z/_MANIFEST"] = old
	s.errs["bkt-dig-err/srv/2026-10-07T11-00-00Z/_MANIFEST"] = errors.New("SlowDown")

	cases := []struct {
		path    string
		want    string
		ok, err bool
	}{
		{"s3://bkt-dig/srv/2026-10-07T11-00-00Z/shop/orders.parquet", crcHex(data), true, false},
		{"s3://bkt-dig/srv/2026-10-07T11-00-00Z/shop/missing.parquet", "", false, false},
		{"s3://bkt-dig-none/srv/2026-10-07T11-00-00Z/shop/orders.parquet", "", false, false},
		{"s3://bkt-dig-v99/srv/2026-10-07T11-00-00Z/shop/orders.parquet", "", false, false},
		{"s3://bkt-dig-err/srv/2026-10-07T11-00-00Z/shop/orders.parquet", "", false, true},
		{"s3://bkt-dig/orders.parquet", "", false, false},
	}
	for _, tc := range cases {
		got, ok, err := S3ManifestDigest(context.Background(), tc.path)
		if got != tc.want || ok != tc.ok || (err != nil) != tc.err {
			t.Errorf("%s: (%q, %v, %v), want (%q, %v, err=%v)", tc.path, got, ok, err, tc.want, tc.ok, tc.err)
		}
	}
	if n := s.gets["bkt-dig/srv/2026-10-07T11-00-00Z/shop/orders.parquet"]; n != 0 {
		t.Fatalf("reading the digest fetched the table's bytes %d times", n)
	}
}

// The new manifest lists the copied files with the carried digests, beside
// the files hashed on disk; a carried entry never overrides a file that is
// on disk (that one is hashed, it is what will be uploaded).
func TestWriteManifestWith_listsCopiedFiles_2212(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "shop", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("local"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := WriteManifestWith(dir, nil, map[string]string{
		"crm/leads.parquet":   "0000abcd",
		"shop/orders.parquet": "deadbeef",
	})
	if err != nil {
		t.Fatal(err)
	}
	m, ok, err := LoadManifest(dir)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if m.Files["crm/leads.parquet"] != "0000abcd" {
		t.Errorf("copied file: %q", m.Files["crm/leads.parquet"])
	}
	if m.Files["shop/orders.parquet"] != crcHex([]byte("local")) {
		t.Errorf("a carried digest overrode the file on disk: %q", m.Files["shop/orders.parquet"])
	}
	if st.Carried != 1 || st.Hashed != 1 {
		t.Errorf("stats = %+v, want 1 carried and 1 hashed", st)
	}
}
