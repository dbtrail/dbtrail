package console

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// Per-server S3 store (#1575): the registry validates and normalizes the
// three values, refuses two servers routing one bucket differently, and
// publishes the bucket→store table every S3 client and DuckDB session reads.

func clearStores(t *testing.T) {
	t.Helper()
	storage.SetBucketStores(nil)
	t.Cleanup(func() { storage.SetBucketStores(nil) })
}

func TestRegistryS3Store_validationAndNormalization(t *testing.T) {
	clearStores(t)
	r, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []ServerEntry{
		{Name: "path-in-url", DSN: "u:p@tcp(h:3306)/db", S3Endpoint: "http://minio:9000/bucket"},
		{Name: "creds-in-url", DSN: "u:p@tcp(h:3306)/db", S3Endpoint: "http://AKIA:hunter2@minio:9000"},
		{Name: "style-alone", DSN: "u:p@tcp(h:3306)/db", S3PathStyle: "vhost"},
		{Name: "bad-style", DSN: "u:p@tcp(h:3306)/db", S3Endpoint: "http://minio:9000", S3PathStyle: "virtual"},
		{Name: "bad-region", DSN: "u:p@tcp(h:3306)/db", S3Region: "us east 1"},
	} {
		_, err := r.Add(bad)
		if !errors.Is(err, storage.ErrBucketStoreConfig) {
			t.Errorf("%s: err = %v, want ErrBucketStoreConfig", bad.Name, err)
		}
		if err != nil && strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%s: the error echoes the credential: %v", bad.Name, err)
		}
	}
	if r.Len() != 0 {
		t.Fatalf("a refused entry was stored: %d entries", r.Len())
	}
	added, err := r.Add(ServerEntry{Name: "ok", DSN: "u:p@tcp(h:3306)/db", ArchiveS3: "s3://arch/p/",
		S3Endpoint: " http://minio:9000/ ", S3PathStyle: " VHOST ", S3Region: " us-east-1 "})
	if err != nil {
		t.Fatal(err)
	}
	if added.S3Endpoint != "http://minio:9000" || added.S3PathStyle != "vhost" || added.S3Region != "us-east-1" {
		t.Errorf("stored values not normalized: %+v", added)
	}
	st, ok := storage.BucketStoreFor("arch")
	if !ok || st.Endpoint.URL != "http://minio:9000" || st.Endpoint.PathStyle || st.Region != "us-east-1" {
		t.Errorf("table after Add: %+v, %v", st, ok)
	}
}

func TestRegistryS3Store_conflictRefused(t *testing.T) {
	clearStores(t)
	r, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	a, err := r.Add(ServerEntry{Name: "A", DSN: "u:p@tcp(h:3306)/a", ArchiveS3: "s3://shared/a/", S3Endpoint: "http://minio:9000"})
	if err != nil {
		t.Fatal(err)
	}
	// Same bucket, different store: refused, naming the bucket and the
	// other server so the operator knows which two settings must agree.
	_, err = r.Add(ServerEntry{Name: "B", DSN: "u:p@tcp(h:3306)/b", BaselineS3: "s3://shared/b/", S3Endpoint: "https://s3.wasabisys.com"})
	if !errors.Is(err, ErrS3StoreConflict) {
		t.Fatalf("different store on a shared bucket: err = %v, want ErrS3StoreConflict", err)
	}
	for _, want := range []string{`"shared"`, `"A"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("conflict error does not name %s: %v", want, err)
		}
	}
	if r.Len() != 1 {
		t.Fatal("the refused entry was stored")
	}
	// Same bucket, same store (typed "path" vs defaulted): fine.
	b, err := r.Add(ServerEntry{Name: "B", DSN: "u:p@tcp(h:3306)/b", BaselineS3: "s3://shared/b/", S3Endpoint: "http://minio:9000/", S3PathStyle: "path"})
	if err != nil {
		t.Fatalf("same store on a shared bucket must be accepted: %v", err)
	}
	// Same bucket, NO store on the newcomer: accepted; it gets the bucket's
	// routing, which is the documented per-bucket rule.
	// A server with no store reads the bucket from the ambient endpoint, so
	// it cannot join a bucket that has a store.
	_, err = r.Add(ServerEntry{Name: "C", DSN: "u:p@tcp(h:3306)/c", ArchiveS3: "s3://shared/c/"})
	if !errors.Is(err, ErrS3StoreConflict) || !strings.Contains(err.Error(), `"A"`) {
		t.Fatalf("a store-less server on a routed bucket: err = %v, want ErrS3StoreConflict naming A", err)
	}
	t.Log(err)

	// The other order: a store on a bucket a store-less server already uses
	// would take that server over, and deleting the store would hand it back.
	if _, err := r.Add(ServerEntry{Name: "E", DSN: "u:p@tcp(h:3306)/e", ArchiveS3: "s3://plain/e/"}); err != nil {
		t.Fatal(err)
	}
	_, err = r.Add(ServerEntry{Name: "F", DSN: "u:p@tcp(h:3306)/f", BaselineS3: "s3://plain/f/", S3Endpoint: "http://minio:9000"})
	if !errors.Is(err, ErrS3StoreConflict) || !strings.Contains(err.Error(), `"E"`) {
		t.Fatalf("a store on a bucket a store-less server uses: err = %v, want ErrS3StoreConflict naming E", err)
	}
	t.Log(err)
	if _, ok := storage.BucketStoreFor("plain"); ok {
		t.Error("the refused store was published")
	}
	// A different bucket is nobody's business.
	if _, err := r.Add(ServerEntry{Name: "D", DSN: "u:p@tcp(h:3306)/d", ArchiveS3: "s3://other/d/", S3Endpoint: "https://s3.wasabisys.com", S3Region: "eu-central-1"}); err != nil {
		t.Fatalf("a different bucket must be accepted: %v", err)
	}
	// Update A to disagree with B: refused, and A is unchanged.
	a.S3PathStyle = "vhost"
	if err := r.Update(a); !errors.Is(err, ErrS3StoreConflict) {
		t.Fatalf("update into a conflict: err = %v, want ErrS3StoreConflict", err)
	}
	if got, _ := r.Get(a.ID); got.S3PathStyle != "" {
		t.Error("a refused update changed the stored entry")
	}
	// Once B is gone, A may change.
	if err := r.Delete(b.ID); err != nil {
		t.Fatal(err)
	}
	if err := r.Update(a); err != nil {
		t.Fatalf("update after the conflicting server is gone: %v", err)
	}
	if st, ok := storage.BucketStoreFor("shared"); !ok || st.Endpoint.PathStyle {
		t.Errorf("table after update: %+v, %v (want vhost)", st, ok)
	}
}

func TestRegistryS3Store_syncsTheTable(t *testing.T) {
	clearStores(t)
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	a, err := r.Add(ServerEntry{Name: "A", DSN: "u:p@tcp(h:3306)/a", ArchiveS3: "s3://arch/a/", BaselineS3: "s3://base/a/", S3Endpoint: "http://minio:9000"})
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []string{"arch", "base"} {
		if _, ok := storage.BucketStoreFor(b); !ok {
			t.Errorf("bucket %s not routed after Add", b)
		}
	}
	a.S3Region = "us-west-1"
	if err := r.Update(a); err != nil {
		t.Fatal(err)
	}
	if st, _ := storage.BucketStoreFor("arch"); st.Region != "us-west-1" {
		t.Errorf("table not refreshed after Update: %+v", st)
	}
	// A fresh load of the same file fills the table from disk.
	storage.SetBucketStores(nil)
	if _, err := LoadRegistry(path); err != nil {
		t.Fatal(err)
	}
	if st, ok := storage.BucketStoreFor("base"); !ok || st.Region != "us-west-1" || st.Endpoint.URL != "http://minio:9000" {
		t.Errorf("table after LoadRegistry: %+v, %v", st, ok)
	}
	if err := r.Delete(a.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := storage.BucketStoreFor("arch"); ok {
		t.Error("bucket still routed after the only server naming it was deleted")
	}

	// A hand-edited file with two servers routing one bucket differently:
	// both entries load (the file is not refused), the bucket is left out of
	// the table, and the other bucket is routed as usual.
	conflict := "version: 1\nservers:\n" +
		"  - id: aaaaaaaaaaaaaaaa\n    name: A\n    index_dsn: u:p@tcp(h:3306)/a\n    archive_s3: s3://shared/a/\n    s3_endpoint: http://minio:9000\n" +
		"  - id: bbbbbbbbbbbbbbbb\n    name: B\n    index_dsn: u:p@tcp(h:3306)/b\n    archive_s3: s3://shared/b/\n    s3_endpoint: https://s3.wasabisys.com\n" +
		"  - id: cccccccccccccccc\n    name: C\n    index_dsn: u:p@tcp(h:3306)/c\n    archive_s3: s3://own/c/\n    s3_endpoint: https://s3.wasabisys.com\n    s3_region: eu-central-1\n"
	cpath := filepath.Join(t.TempDir(), "conflict.yaml")
	if err := os.WriteFile(cpath, []byte(conflict), 0o600); err != nil {
		t.Fatal(err)
	}
	cr, err := LoadRegistry(cpath)
	if err != nil {
		t.Fatal(err)
	}
	if cr.Len() != 3 {
		t.Fatalf("loaded %d entries, want 3", cr.Len())
	}
	if _, ok := storage.BucketStoreFor("shared"); ok {
		t.Error("a conflicted bucket must not be routed to either store")
	}
	if st, ok := storage.BucketStoreFor("own"); !ok || st.Region != "eu-central-1" {
		t.Errorf("the unconflicted bucket lost its store: %+v, %v", st, ok)
	}

	mixed := "version: 1\nservers:\n" +
		"  - id: bbbbbbbbbbbbbbbb\n    name: B\n    index_dsn: u:p@tcp(h:3306)/b\n    archive_s3: s3://mixed/b/\n" +
		"  - id: aaaaaaaaaaaaaaaa\n    name: A\n    index_dsn: u:p@tcp(h:3306)/a\n    archive_s3: s3://mixed/a/\n    s3_endpoint: http://minio:9000\n"
	mpath := filepath.Join(t.TempDir(), "mixed.yaml")
	if err := os.WriteFile(mpath, []byte(mixed), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRegistry(mpath); err != nil {
		t.Fatal(err)
	}
	if _, ok := storage.BucketStoreFor("mixed"); ok {
		t.Error("a bucket a store-less server also names must not be routed to the other server's store")
	}
	// The other order: the store first, the store-less server second.
	reversed := "version: 1\nservers:\n" +
		"  - id: aaaaaaaaaaaaaaaa\n    name: A\n    index_dsn: u:p@tcp(h:3306)/a\n    archive_s3: s3://mixed/a/\n    s3_endpoint: http://minio:9000\n" +
		"  - id: bbbbbbbbbbbbbbbb\n    name: B\n    index_dsn: u:p@tcp(h:3306)/b\n    archive_s3: s3://mixed/b/\n"
	rpath := filepath.Join(t.TempDir(), "reversed.yaml")
	if err := os.WriteFile(rpath, []byte(reversed), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRegistry(rpath); err != nil {
		t.Fatal(err)
	}
	if _, ok := storage.BucketStoreFor("mixed"); ok {
		t.Error("store first, store-less second: the bucket must not be routed either")
	}
}

func TestRegistryS3Store_storeNeedsItsOwnLocation(t *testing.T) {
	clearStores(t)
	r, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Add(ServerEntry{Name: "nowhere", DSN: "u:p@tcp(h:3306)/n", S3Endpoint: "http://minio:9000"})
	if !errors.Is(err, storage.ErrBucketStoreConfig) || !strings.Contains(err.Error(), "Archive to S3") {
		t.Fatalf("a store with no location of its own: err = %v, want ErrBucketStoreConfig naming the fields to set", err)
	}
	t.Log(err)
	_, err = r.Add(ServerEntry{Name: "malformed", DSN: "u:p@tcp(h:3306)/m", ArchiveS3: "s3://ok/a/", BaselineS3: "bucket/b/", S3Region: "eu-west-1"})
	if !errors.Is(err, storage.ErrBucketStoreConfig) || !strings.Contains(err.Error(), `"bucket/b/"`) {
		t.Fatalf("a store beside a malformed location: err = %v, want ErrBucketStoreConfig naming it", err)
	}
	t.Log(err)
	if r.Len() != 0 {
		t.Fatal("a refused entry was stored")
	}
	if _, err := r.Add(ServerEntry{Name: "plain", DSN: "u:p@tcp(h:3306)/p"}); err != nil {
		t.Fatalf("a server with no store needs no location: %v", err)
	}
	a, err := r.Add(ServerEntry{Name: "a", DSN: "u:p@tcp(h:3306)/a", ArchiveS3: "s3://arch/a/", S3Endpoint: "http://minio:9000"})
	if err != nil {
		t.Fatal(err)
	}
	a.ArchiveS3 = ""
	if err := r.Update(a); !errors.Is(err, storage.ErrBucketStoreConfig) {
		t.Fatalf("clearing the only location under a store: err = %v, want ErrBucketStoreConfig", err)
	}
	if _, ok := storage.BucketStoreFor("arch"); !ok {
		t.Error("the refused update unrouted the bucket")
	}
}

// A hand-edited file with an invalid store: the entry loads, the store is
// ignored (warned at load), and an edit that does NOT touch the store or
// its buckets still saves, so "stop monitoring" and the backup schedule are
// not held hostage by a typo in the YAML. An edit that touches it is refused.
func TestRegistryS3Store_invalidHandEditDoesNotBlockUnrelatedSaves(t *testing.T) {
	clearStores(t)
	file := "version: 1\nservers:\n  - id: aaaaaaaaaaaaaaaa\n    name: A\n    index_dsn: u:p@tcp(h:3306)/a\n    archive_s3: s3://arch/a/\n    s3_endpoint: minio:9000\n" +
		"  - id: bbbbbbbbbbbbbbbb\n    name: B\n    index_dsn: u:p@tcp(h:3306)/b\n    archive_s3: s3://arch/b/\n    s3_endpoint: http://minio:9000\n"
	path := filepath.Join(t.TempDir(), "servers.yaml")
	if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	a, ok := r.Get("aaaaaaaaaaaaaaaa")
	if !ok {
		t.Fatal("entry did not load")
	}
	if _, routed := storage.BucketStoreFor("arch"); routed {
		t.Error("an invalid store counts as no store, so the bucket it shares with B's store must not be routed")
	}
	a.MonitorDesired = true
	if err := r.Update(a); err != nil {
		t.Fatalf("an unrelated edit was refused over the hand-edited store: %v", err)
	}
	a.S3Region = "us-east-1" // touches the store: now the invalid endpoint is checked
	if err := r.Update(a); !errors.Is(err, storage.ErrBucketStoreConfig) {
		t.Fatalf("an edit touching the store must re-validate it: err = %v", err)
	}
	a.S3Region = ""
	a.ArchiveS3 = "s3://other/a/" // a new bucket for the same (invalid) store: checked too
	if err := r.Update(a); !errors.Is(err, storage.ErrBucketStoreConfig) {
		t.Fatalf("an edit changing the buckets must re-validate the store: err = %v", err)
	}
	_, err = r.Add(ServerEntry{Name: "C", DSN: "u:p@tcp(h:3306)/c", ArchiveS3: "s3://arch/c/", S3Endpoint: "http://minio:9000"})
	if !errors.Is(err, ErrS3StoreConflict) || !strings.Contains(err.Error(), `"A"`) {
		t.Fatalf("a store on a bucket a server with an invalid store names: err = %v, want ErrS3StoreConflict naming A", err)
	}
}

// The HTTP surface: the three values round-trip through the masked DTO,
// a bad value is a 400 that names the field, a conflict is a 422, and a PUT
// that leaves them blank clears them (the form always sends them).
func TestServersAPI_s3Store(t *testing.T) {
	clearStores(t)
	srv := newRegistryServer(t)
	rec, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"a","host":"h","port":"3306","user":"u","password":"p","dbname":"a","archive_s3":"s3://shared/a/",`+
			`"s3_endpoint":"http://minio:9000/","s3_path_style":"","s3_region":"us-east-1"}`)
	if rec.Code != 201 {
		t.Fatalf("create: %d %s", rec.Code, body)
	}
	var a serverDTO
	if err := json.Unmarshal(body, &a); err != nil {
		t.Fatal(err)
	}
	if a.S3Endpoint != "http://minio:9000" || a.S3PathStyle != "" || a.S3Region != "us-east-1" {
		t.Errorf("DTO after create: endpoint=%q style=%q region=%q", a.S3Endpoint, a.S3PathStyle, a.S3Region)
	}
	rec, body = doServersReq(t, srv, "GET", "/api/servers/"+a.ID, "")
	if rec.Code != 200 || !strings.Contains(string(body), `"s3_endpoint":"http://minio:9000"`) {
		t.Errorf("GET does not carry the store: %d %s", rec.Code, body)
	}
	if _, ok := storage.BucketStoreFor("shared"); !ok {
		t.Error("the create did not publish the store")
	}
	rec, body = doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"t","host":"h","port":"3306","user":"u","password":"p","dbname":"t","baseline_s3":" s3://trimmed/t/ ","s3_region":"eu-west-1"}`)
	if rec.Code != 201 || !strings.Contains(string(body), `"baseline_s3":"s3://trimmed/t/"`) {
		t.Errorf("a padded Backups S3 location: %d %s, want 201 with it trimmed", rec.Code, body)
	}
	if _, ok := storage.BucketStoreFor("trimmed"); !ok {
		t.Error("a padded Backups S3 location left its bucket unrouted")
	}

	rec, body = doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"bad","host":"h","port":"3306","user":"u","dbname":"b","s3_endpoint":"minio:9000"}`)
	if rec.Code != 400 || !strings.Contains(string(body), "endpoint") {
		t.Errorf("bad endpoint: %d %s, want 400 naming the endpoint", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"b","host":"h","port":"3306","user":"u","dbname":"b","baseline_s3":"s3://shared/b/","s3_endpoint":"https://s3.wasabisys.com"}`)
	// The quotes travel JSON-escaped in the error body.
	if rec.Code != 422 || !strings.Contains(string(body), `server \"a\"`) {
		t.Errorf("conflict: %d %s, want 422 naming server a", rec.Code, body)
	}

	// A PUT with the fields blank clears the store, and the table follows.
	rec, body = doServersReq(t, srv, "PUT", "/api/servers/"+a.ID,
		`{"name":"a","host":"h","port":"3306","user":"u","dbname":"a","archive_s3":"s3://shared/a/","s3_endpoint":"","s3_path_style":"","s3_region":""}`)
	if rec.Code != 200 {
		t.Fatalf("clearing PUT: %d %s", rec.Code, body)
	}
	if strings.Contains(string(body), "s3_endpoint") {
		t.Errorf("cleared store still in the DTO: %s", body)
	}
	if _, ok := storage.BucketStoreFor("shared"); ok {
		t.Error("the cleared store is still published")
	}
	// The DTO tags are the names the form reads and sends.
	dto, _ := json.Marshal(serverDTO{S3Endpoint: "e", S3PathStyle: "path", S3Region: "r"})
	for _, key := range []string{`"s3_endpoint":"e"`, `"s3_path_style":"path"`, `"s3_region":"r"`} {
		if !strings.Contains(string(dto), key) {
			t.Errorf("DTO lacks %s: %s", key, dto)
		}
	}
}

// The form: three visible fields under Archive to S3, prefilled on edit and
// always sent, so a PUT (which REPLACES the entry) neither wipes nor
// invents them.
func TestServerFormCarriesTheS3StoreFields(t *testing.T) {
	js := readAsset(t, "app.js")
	form := jsFunctionBody(t, js, "buildServerForm")
	for _, field := range []string{`srvField("S3 endpoint", "s3_endpoint"`, `name: "s3_path_style"`, `srvField("S3 region", "s3_region"`, `opt("vhost"`, `opt("path"`} {
		if !strings.Contains(form, field) {
			t.Errorf("buildServerForm lacks %s", field)
		}
	}
	show := jsFunctionBody(t, js, "showServerForm")
	if !strings.Contains(show, `"s3_endpoint", "s3_path_style", "s3_region"`) {
		t.Error("showServerForm's prefill list does not cover the S3 store fields; an edit would submit them blank and CLEAR the store")
	}
	body := jsFunctionBody(t, js, "serverFormBody")
	for _, send := range []string{"s3_endpoint: f.s3_endpoint.value", "s3_path_style: f.s3_path_style.value", "s3_region: f.s3_region.value"} {
		if !strings.Contains(body, send) {
			t.Errorf("serverFormBody does not send %s; the PUT would clear it", send)
		}
	}
}
