package console

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// Per-server S3 keys and the S3 half of Test connection (#1575, second slice).

const (
	keyID     = "AKIASTOREKEY"
	keySecret = "Store/Secret+Value42"
)

func keyedEntry(name, bucketLoc, endpoint, id, secret string) ServerEntry {
	return ServerEntry{Name: name, DSN: "u:p@tcp(h:3306)/" + name, ArchiveS3: bucketLoc, S3Endpoint: endpoint,
		S3AccessKeyID: id, S3SecretAccessKey: secret}
}

func TestRegistryS3Keys_validationAndPersistence(t *testing.T) {
	clearStores(t)
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []ServerEntry{
		keyedEntry("id-alone", "s3://k/a/", "http://minio:9000", keyID, ""),
		keyedEntry("secret-alone", "s3://k/a/", "http://minio:9000", "", keySecret),
		keyedEntry("line-break", "s3://k/a/", "http://minio:9000", keyID, "Store/Secret\n+Value42"),
	} {
		_, err := r.Add(bad)
		if !errors.Is(err, storage.ErrBucketStoreConfig) {
			t.Errorf("%s: err = %v, want ErrBucketStoreConfig", bad.Name, err)
		}
		if err != nil && (strings.Contains(err.Error(), "Secret") || strings.Contains(err.Error(), keyID)) {
			t.Errorf("%s: the error echoes a key: %v", bad.Name, err)
		}
	}
	// Keys alone, with no location of their own, route nothing: refused like
	// any other store.
	if _, err := r.Add(keyedEntry("no-location", "", "", keyID, keySecret)); !errors.Is(err, storage.ErrBucketStoreConfig) {
		t.Errorf("keys with no S3 location: err = %v, want ErrBucketStoreConfig", err)
	}
	if r.Len() != 0 {
		t.Fatalf("a refused entry was stored: %d", r.Len())
	}
	// Keys alone on an AWS bucket (another account) need the bucket's region.
	if _, err := r.Add(keyedEntry("aws-no-region", "s3://k/a/", "", keyID, keySecret)); !errors.Is(err, storage.ErrBucketStoreConfig) {
		t.Errorf("keys with no endpoint and no region: err = %v, want ErrBucketStoreConfig", err)
	}
	withRegion := keyedEntry("aws-other-account", "s3://k/a/", "", " "+keyID+" ", " "+keySecret+" ")
	withRegion.S3Region = "eu-west-1"
	added, err := r.Add(withRegion)
	if err != nil {
		t.Fatal(err)
	}
	if added.S3AccessKeyID != keyID || added.S3SecretAccessKey != keySecret {
		t.Errorf("keys not trimmed: id=%q", added.S3AccessKeyID)
	}
	if st, ok := storage.BucketStoreFor("k"); !ok || st.AccessKeyID != keyID || st.SecretKey != keySecret {
		t.Errorf("a keys-only store was not published: ok=%v id=%q", ok, st.AccessKeyID)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "s3_access_key_id: "+keyID) || !strings.Contains(string(raw), "s3_secret_access_key: "+keySecret) {
		t.Errorf("registry file lacks the key fields:\n%s", raw)
	}
	storage.SetBucketStores(nil)
	reloaded, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := reloaded.Get(added.ID); got.S3SecretAccessKey != keySecret {
		t.Error("the secret did not survive a reload")
	}
	if st, ok := storage.BucketStoreFor("k"); !ok || st.SecretKey != keySecret {
		t.Error("a reload did not publish the keyed store")
	}
}

// A hand-edited file with half a pair loads, warns, and routes the bucket as
// store-less, never with a key and no secret.
func TestRegistryS3Keys_halfPairInFileIsIgnored(t *testing.T) {
	clearStores(t)
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	body := "version: 1\nservers:\n  - id: aaaaaaaaaaaaaaaa\n    name: A\n    dsn: u:p@tcp(h:3306)/a\n    archive_s3: s3://half/a/\n" +
		"    s3_endpoint: http://minio:9000\n    s3_access_key_id: " + keyID + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(prev)
	if _, err := LoadRegistry(path); err != nil {
		t.Fatal(err)
	}
	if _, ok := storage.BucketStoreFor("half"); ok {
		t.Error("half a key pair was published as a store")
	}
	if !strings.Contains(logged.String(), "S3 store settings are invalid") {
		t.Errorf("no warning for half a key pair: %s", logged.String())
	}
}

// A bucket has one pair of keys: a second server on it with other keys is
// refused, naming the difference and never a value. Rotating the keys of a
// shared bucket is still possible without deleting anything.
func TestRegistryS3Keys_conflictAndRotation(t *testing.T) {
	clearStores(t)
	r, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	a, err := r.Add(keyedEntry("A", "s3://shared/a/", "http://minio:9000", keyID, keySecret))
	if err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string]ServerEntry{
		"no keys":      keyedEntry("B", "s3://shared/b/", "http://minio:9000", "", ""),
		"other secret": keyedEntry("B", "s3://shared/b/", "http://minio:9000", keyID, "OtherSecretValue"),
	} {
		_, err := r.Add(b)
		if !errors.Is(err, ErrS3StoreConflict) {
			t.Fatalf("%s: err = %v, want ErrS3StoreConflict", name, err)
		}
		if !strings.Contains(err.Error(), "different S3 keys") || !strings.Contains(err.Error(), `"A"`) {
			t.Errorf("%s: the refusal does not say the keys differ on server A: %v", name, err)
		}
		for _, leak := range []string{keySecret, "OtherSecretValue"} {
			if strings.Contains(err.Error(), leak) {
				t.Errorf("%s: the refusal carries a secret: %v", name, err)
			}
		}
	}
	b, err := r.Add(keyedEntry("B", "s3://shared/b/", "http://minio:9000", keyID, keySecret))
	if err != nil {
		t.Fatalf("same keys on a shared bucket: %v", err)
	}
	// An edit that changes only the access key is a store change too.
	otherID := a
	otherID.S3AccessKeyID = "AKIAOTHERKEY"
	if err := r.Update(otherID); !errors.Is(err, ErrS3StoreConflict) {
		t.Fatalf("changing only the access key on a shared bucket: err = %v, want ErrS3StoreConflict", err)
	}

	// An edit that changes only the secret is a store change: re-checked,
	// refused while B disagrees, and A is left as it was.
	rotated := a
	rotated.S3SecretAccessKey = "RotatedSecretValue"
	if err := r.Update(rotated); !errors.Is(err, ErrS3StoreConflict) {
		t.Fatalf("rotating one side of a shared bucket: err = %v, want ErrS3StoreConflict", err)
	}
	if got, _ := r.Get(a.ID); got.S3SecretAccessKey != keySecret {
		t.Error("a refused rotation changed the stored secret")
	}

	// The documented rotation: take the location (and store) off A, rotate
	// B, give A the location back with the new keys.
	off := a
	off.ArchiveS3, off.S3Endpoint, off.S3AccessKeyID, off.S3SecretAccessKey = "", "", "", ""
	if err := r.Update(off); err != nil {
		t.Fatalf("step 1, A leaves the bucket: %v", err)
	}
	b.S3SecretAccessKey = "RotatedSecretValue"
	if err := r.Update(b); err != nil {
		t.Fatalf("step 2, B rotates: %v", err)
	}
	if err := r.Update(rotated); err != nil {
		t.Fatalf("step 3, A comes back with the new keys: %v", err)
	}
	if st, ok := storage.BucketStoreFor("shared"); !ok || st.SecretKey != "RotatedSecretValue" {
		t.Errorf("table after rotation: ok=%v", ok)
	}
}

func decodeDTO(t *testing.T, body []byte) serverDTO {
	t.Helper()
	var d serverDTO
	if err := json.Unmarshal(body, &d); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	return d
}

// assertNoSecret: the secret value and a field named for it never appear in a
// response body. has_s3_secret_access_key contains the field's name, so the
// check is on the quoted key.
func assertNoSecret(t *testing.T, what string, body []byte, secrets ...string) {
	t.Helper()
	if strings.Contains(string(body), `"s3_secret_access_key"`) {
		t.Errorf("%s: the response has a secret field: %s", what, body)
	}
	for _, s := range append(secrets, keySecret) {
		if strings.Contains(string(body), s) {
			t.Errorf("%s: the response carries a secret: %s", what, body)
		}
	}
}

func TestServersAPI_s3Keys(t *testing.T) {
	srv := newRegistryServer(t)
	const loc = `"host":"h","port":"3306","user":"u","dbname":"a","archive_s3":"s3://kb/a/","s3_endpoint":"http://minio:9000","s3_path_style":"","s3_region":""`
	put := func(id, extra string) (*httptest.ResponseRecorder, []byte) {
		return doServersReq(t, srv, "PUT", "/api/servers/"+id, `{"name":"a",`+loc+extra+`}`)
	}

	rec, body := doServersReq(t, srv, "POST", "/api/servers", `{"name":"a",`+loc+`,"s3_access_key_id":"`+keyID+`"}`)
	if rec.Code != 400 || !strings.Contains(string(body), "secret key") {
		t.Errorf("create with an access key and no secret: %d %s, want 400 asking for the secret key", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "POST", "/api/servers", `{"name":"a",`+loc+`,"s3_access_key_id":"`+keyID+`","s3_secret_access_key":"`+keySecret+`"}`)
	if rec.Code != 201 {
		t.Fatalf("create with keys: %d %s", rec.Code, body)
	}
	assertNoSecret(t, "POST", body)
	a := decodeDTO(t, body)
	if a.S3AccessKeyID != keyID || !a.HasS3SecretAccessKey {
		t.Errorf("DTO after create: id=%q has_secret=%v", a.S3AccessKeyID, a.HasS3SecretAccessKey)
	}
	for _, path := range []string{"/api/servers/" + a.ID, "/api/servers"} {
		rec, body := doServersReq(t, srv, "GET", path, "")
		if rec.Code != 200 || !strings.Contains(string(body), `"has_s3_secret_access_key":true`) {
			t.Errorf("GET %s: %d %s", path, rec.Code, body)
		}
		assertNoSecret(t, "GET "+path, body)
	}
	secretOf := func() string { e, _ := srv.cm.reg.Get(a.ID); return e.S3SecretAccessKey }

	// Same access key, secret omitted: kept.
	if rec, body := put(a.ID, `,"s3_access_key_id":"`+keyID+`"`); rec.Code != 200 {
		t.Fatalf("edit keeping the secret: %d %s", rec.Code, body)
	} else {
		assertNoSecret(t, "PUT keep", body)
	}
	if secretOf() != keySecret {
		t.Error("an edit that omitted the secret lost it")
	}
	// A new access key with the old secret would save keys that never sign.
	if rec, body := put(a.ID, `,"s3_access_key_id":"AKIANEWKEY"`); rec.Code != 400 || !strings.Contains(string(body), "secret key") {
		t.Errorf("new access key, secret omitted: %d %s, want 400", rec.Code, body)
	}
	if e, _ := srv.cm.reg.Get(a.ID); e.S3AccessKeyID != keyID {
		t.Error("a refused edit changed the access key")
	}
	// A secret with no access key: refused, never silently dropped.
	if rec, body := put(a.ID, `,"s3_access_key_id":"","s3_secret_access_key":"LoneSecretValue"`); rec.Code != 400 || !strings.Contains(string(body), "needs its access key") {
		t.Errorf("secret without an access key: %d %s, want 400", rec.Code, body)
	} else {
		assertNoSecret(t, "PUT lone secret", body, "LoneSecretValue")
	}
	// Typed: replaced.
	if rec, body := put(a.ID, `,"s3_access_key_id":"AKIANEWKEY","s3_secret_access_key":" NewSecretValue "`); rec.Code != 200 {
		t.Fatalf("edit replacing both keys: %d %s", rec.Code, body)
	} else {
		assertNoSecret(t, "PUT replace", body, "NewSecretValue")
	}
	if secretOf() != "NewSecretValue" {
		t.Errorf("secret after replace = %q", secretOf())
	}
	// Access key cleared, secret omitted: both go.
	rec, body = put(a.ID, `,"s3_access_key_id":""`)
	if rec.Code != 200 {
		t.Fatalf("clearing the keys: %d %s", rec.Code, body)
	}
	if strings.Contains(string(body), "s3_access_key_id") || strings.Contains(string(body), "has_s3_secret_access_key") {
		t.Errorf("cleared keys still in the DTO: %s", body)
	}
	if e, _ := srv.cm.reg.Get(a.ID); e.S3AccessKeyID != "" || e.S3SecretAccessKey != "" {
		t.Error("clearing the access key left a key behind")
	}
}

// The other endpoints that save a server start from the stored entry; a
// backup-settings save must not wipe the keys.
func TestBackupSettings_keepsS3Keys(t *testing.T) {
	clearStores(t)
	srv := newBackupSettingsServer(t, BackupSettingsDefaults{}, "", "")
	a, err := srv.cm.reg.Add(keyedEntry("A", "s3://bk/a/", "http://minio:9000", keyID, keySecret))
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/api/backup-settings/servers/"+a.ID, strings.NewReader(`{"baseline_s3":"s3://bk/backups/"}`))
	req.SetPathValue("id", a.ID)
	srv.handleBackupSettingsServerUpdate(rec, req)
	if rec.Code != 200 {
		t.Fatalf("backup settings save: %d %s", rec.Code, rec.Body.String())
	}
	if e, _ := srv.cm.reg.Get(a.ID); e.S3AccessKeyID != keyID || e.S3SecretAccessKey != keySecret {
		t.Error("a backup-settings save wiped the S3 keys")
	}
	assertNoSecret(t, "backup settings", rec.Body.Bytes())
}

// probeFake is an S3-compatible store that records each request's
// Authorization header and answers with status.
type probeFake struct {
	mu     sync.Mutex
	auths  []string
	status int
	srv    *httptest.Server
}

func newProbeFake(t *testing.T, status int) *probeFake {
	t.Helper()
	f := &probeFake{status: status}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.auths = append(f.auths, r.Method+" "+r.URL.Path+" "+r.Header.Get("Authorization"))
		f.mu.Unlock()
		w.WriteHeader(f.status)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *probeFake) seen() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.auths, "\n")
}

func (f *probeFake) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.auths)
}

// isolateProbeEnv gives the probe an ambient chain with a recognizable key
// and no way to reach AWS or instance metadata.
func isolateProbeEnv(t *testing.T) {
	t.Helper()
	for k, v := range map[string]string{
		"AWS_ACCESS_KEY_ID": "AKIAAMBIENT", "AWS_SECRET_ACCESS_KEY": "ambientsecret", "AWS_SESSION_TOKEN": "",
		"AWS_REGION": "", "AWS_DEFAULT_REGION": "", "AWS_PROFILE": "", "AWS_EC2_METADATA_DISABLED": "true",
		"AWS_CONFIG_FILE": "/nonexistent/aws-config", "AWS_SHARED_CREDENTIALS_FILE": "/nonexistent/aws-credentials",
		"AWS_ENDPOINT_URL": "", "AWS_ENDPOINT_URL_S3": "", storage.EnvS3Endpoint: "", storage.EnvS3PathStyle: "",
	} {
		t.Setenv(k, v)
	}
}

type probeBody struct {
	OK bool            `json:"ok"`
	S3 []s3ProbeResult `json:"s3"`
}

// The probe signs with credentials the operator did not type (the daemon's
// own chain, a saved secret) only toward what the saved server already sends
// them to. Both test routes are servers:read.
func TestServersTest_probeNeverSignsForANewDestination(t *testing.T) {
	isolateProbeEnv(t)
	srv := newRegistryServer(t)
	savedHost, newHost, keyedHost := newProbeFake(t, 200), newProbeFake(t, 200), newProbeFake(t, 200)
	plain, err := srv.cm.reg.Add(ServerEntry{Name: "plain", DSN: "u:p@tcp(h:3306)/p", ArchiveS3: "s3://probe-p/p/", S3Endpoint: savedHost.srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	keyed, err := srv.cm.reg.Add(keyedEntry("keyed", "s3://probe-k/k/", keyedHost.srv.URL, "AKIASAVED", "SavedSecretValue"))
	if err != nil {
		t.Fatal(err)
	}
	keyless := func(endpoint, bucket string) string {
		return `{` + deadDSN + `,"archive_s3":"s3://` + bucket + `/x/","s3_endpoint":"` + endpoint + `","s3_path_style":"","s3_region":"","s3_access_key_id":""}`
	}
	held := func(name string, pb probeBody, raw string) {
		t.Helper()
		if len(pb.S3) != 1 || !pb.S3[0].NeedsKeys || pb.S3[0].OK {
			t.Errorf("%s: %s, want needs_keys and nothing probed", name, raw)
		}
	}

	// A new endpoint, no keys, from the create form: nothing is signed.
	pb, raw := doProbe(t, srv, "/api/servers/test", keyless(newHost.srv.URL, "probe-n"))
	held("create form, new endpoint", pb, raw)
	// The saved server's own endpoint, no keys: the daemon's credentials
	// already go there.
	pb, raw = doProbe(t, srv, "/api/servers/"+plain.ID+"/test", keyless(savedHost.srv.URL, "probe-p"))
	if len(pb.S3) != 1 || !pb.S3[0].OK || !strings.Contains(savedHost.seen(), "Credential=AKIAAMBIENT/") {
		t.Errorf("saved keyless endpoint: %s\n%s", raw, savedHost.seen())
	}
	// Another endpoint typed over that saved server: held.
	pb, raw = doProbe(t, srv, "/api/servers/"+plain.ID+"/test", keyless(newHost.srv.URL, "probe-p"))
	held("edit form, new endpoint", pb, raw)
	// Keys cleared on the form over a keyed saved store: the daemon's
	// credentials never went to that host.
	pb, raw = doProbe(t, srv, "/api/servers/"+keyed.ID+"/test", keyless(keyedHost.srv.URL, "probe-k"))
	held("edit form, keys cleared", pb, raw)
	if newHost.count() != 0 || keyedHost.count() != 0 {
		t.Errorf("a held probe reached a store: new=%d keyed=%d", newHost.count(), keyedHost.count())
	}

	// The saved keyless server's endpoint, but a bucket it does not name: the
	// daemon's role would answer for any bucket name. Held.
	before := savedHost.count()
	pb, raw = doProbe(t, srv, "/api/servers/"+plain.ID+"/test", keyless(savedHost.srv.URL, "probe-elsewhere"))
	held("edit form, another bucket", pb, raw)
	if savedHost.count() != before {
		t.Error("the daemon's credentials signed for a bucket the saved server does not name")
	}
	// An empty or blank secret is not typed keys.
	for _, blank := range []string{`""`, `"   "`} {
		body := strings.TrimSuffix(keyless(newHost.srv.URL, "probe-n"), "}") + `,"s3_secret_access_key":` + blank + `}`
		pb, raw = doProbe(t, srv, "/api/servers/test", body)
		held("secret "+blank, pb, raw)
	}
	if newHost.count() != 0 || keyedHost.count() != 0 {
		t.Errorf("a held probe reached a store: new=%d keyed=%d", newHost.count(), keyedHost.count())
	}

	// No endpoint (a region pin): the ambient destination. From the create
	// form it is held like any other; from the saved server, for its bucket,
	// the daemon's credentials go there anyway.
	ambient := newProbeFake(t, 200)
	t.Setenv(storage.EnvS3Endpoint, ambient.srv.URL)
	regionOnly := `{` + deadDSN + `,"archive_s3":"s3://probe-r/x/","s3_endpoint":"","s3_region":"eu-west-1","s3_access_key_id":""}`
	pb, raw = doProbe(t, srv, "/api/servers/test", regionOnly)
	held("create form, region only", pb, raw)
	pinned, err := srv.cm.reg.Add(ServerEntry{Name: "pinned", DSN: "u:p@tcp(h:3306)/r", ArchiveS3: "s3://probe-r/x/", S3Region: "eu-west-1"})
	if err != nil {
		t.Fatal(err)
	}
	pb, raw = doProbe(t, srv, "/api/servers/"+pinned.ID+"/test", regionOnly)
	if len(pb.S3) != 1 || !pb.S3[0].OK || !strings.Contains(ambient.seen(), "Credential=AKIAAMBIENT/") {
		t.Errorf("saved region-only store: %s\n%s", raw, ambient.seen())
	}
}

// The saved secret signs only for the buckets the saved server names: an
// unchanged store with another bucket typed would otherwise check any bucket
// name with keys the reader never saw.
func TestServersTest_savedSecretOnlyForSavedBuckets(t *testing.T) {
	isolateProbeEnv(t)
	srv := newRegistryServer(t)
	host := newProbeFake(t, 200)
	e, err := srv.cm.reg.Add(keyedEntry("s", "s3://probe-own/s/", host.srv.URL, "AKIASAVED", "SavedSecretValue"))
	if err != nil {
		t.Fatal(err)
	}
	pb, raw := doProbe(t, srv, "/api/servers/"+e.ID+"/test",
		`{"archive_s3":"s3://probe-own/s/","baseline_s3":"s3://probe-other/b/","s3_endpoint":"`+host.srv.URL+`","s3_path_style":"","s3_region":"","s3_access_key_id":"AKIASAVED"}`)
	if len(pb.S3) != 2 {
		t.Fatalf("results: %s", raw)
	}
	if pb.S3[0].Bucket != "probe-own" || !pb.S3[0].OK {
		t.Errorf("the saved bucket: %+v", pb.S3[0])
	}
	if pb.S3[1].Bucket != "probe-other" || !pb.S3[1].NeedsSecret || pb.S3[1].OK {
		t.Errorf("a bucket the saved server does not name: %+v, want needs_secret", pb.S3[1])
	}
	if strings.Contains(host.seen(), "probe-other") || host.count() != 1 {
		t.Errorf("the saved secret signed for another bucket:\n%s", host.seen())
	}
}

// The row's Test checks the SAVED store; when the daemon is not applying it
// (a hand-edited conflict, a bucket reserved for --baseline-s3), a green
// check would say the opposite of what uploads and reads do.
func TestServersTest_rowTestFlagsAStoreNotInUse(t *testing.T) {
	isolateProbeEnv(t)
	srv := newRegistryServer(t)
	host := newProbeFake(t, 200)
	e, err := srv.cm.reg.Add(keyedEntry("s", "s3://probe-u/s/", host.srv.URL, "AKIASAVED", "SavedSecretValue"))
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/servers/" + e.ID + "/test"
	pb, raw := doProbe(t, srv, path, `{}`)
	if len(pb.S3) != 1 || !pb.S3[0].OK || pb.S3[0].NotApplied {
		t.Errorf("an applied store: %s", raw)
	}
	storage.SetBucketStores(nil)
	pb, raw = doProbe(t, srv, path, `{}`)
	if len(pb.S3) != 1 || !pb.S3[0].NotApplied {
		t.Errorf("a saved store the daemon does not use: %s, want not_applied", raw)
	}
	// The table holds a store for the bucket, but not this one (other keys).
	other, err := storage.NewBucketStore(host.srv.URL, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if other, err = other.WithKeys("AKIAOTHER", "OtherSecretValue"); err != nil {
		t.Fatal(err)
	}
	storage.SetBucketStores(map[string]storage.BucketStore{"probe-u": other})
	pb, raw = doProbe(t, srv, path, `{}`)
	if len(pb.S3) != 1 || !pb.S3[0].NotApplied {
		t.Errorf("the daemon applies a different store to the bucket: %s, want not_applied", raw)
	}
	// The edit form testing the saved store unchanged (secret blank, or typed
	// again) is the same question: flagged too.
	for _, secret := range []string{``, `,"s3_secret_access_key":"SavedSecretValue"`} {
		pb, raw = doProbe(t, srv, path, `{"archive_s3":"s3://probe-u/s/","s3_endpoint":"`+host.srv.URL+`","s3_access_key_id":"AKIASAVED"`+secret+`}`)
		if len(pb.S3) != 1 || !pb.S3[0].NotApplied {
			t.Errorf("the form testing the saved store (%q): %s, want not_applied", secret, raw)
		}
	}
	// A form test of different values is of unsaved settings; the table
	// cannot be expected to hold them.
	pb, raw = doProbe(t, srv, path, `{"archive_s3":"s3://probe-u/s/","s3_endpoint":"`+host.srv.URL+`","s3_access_key_id":"AKIATYPED","s3_secret_access_key":"TypedSecretValue"}`)
	if len(pb.S3) != 1 || pb.S3[0].NotApplied {
		t.Errorf("a form test of other keys flagged not_applied: %s", raw)
	}
}

func doProbe(t *testing.T, srv *Server, path, body string) (probeBody, string) {
	t.Helper()
	rec, raw := doServersReq(t, srv, "POST", path, body)
	if rec.Code != 200 {
		t.Fatalf("POST %s: %d %s", path, rec.Code, raw)
	}
	var pb probeBody
	if err := json.Unmarshal(raw, &pb); err != nil {
		t.Fatal(err)
	}
	return pb, string(raw)
}

// deadDSN fails fast: the S3 half must run whatever the index connection does.
const deadDSN = `"dsn":"u:p@tcp(127.0.0.1:1)/idx"`

func TestServersTest_s3ProbeUsesTheCandidateStore(t *testing.T) {
	isolateProbeEnv(t)
	srv := newRegistryServer(t)
	candidate, saved := newProbeFake(t, 200), newProbeFake(t, 200)
	// A saved server routes the same bucket to another store with other keys:
	// the probe must not read the table.
	if _, err := srv.cm.reg.Add(keyedEntry("saved", "s3://probe-b/s/", saved.srv.URL, "AKIASAVED", "SavedSecretValue")); err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(prev)

	pb, raw := doProbe(t, srv, "/api/servers/test", `{`+deadDSN+`,"archive_s3":"s3://probe-b/c/","baseline_s3":"s3://probe-c/c/",`+
		`"s3_endpoint":"`+candidate.srv.URL+`","s3_access_key_id":"AKIACANDIDATE","s3_secret_access_key":"CandidateSecretValue"}`)
	if pb.OK {
		t.Fatal("the dead DSN probed ok; the fixture does not isolate the S3 half")
	}
	if len(pb.S3) != 2 || pb.S3[0].Bucket != "probe-b" || !pb.S3[0].OK || pb.S3[1].Bucket != "probe-c" || !pb.S3[1].OK {
		t.Fatalf("s3 results: %s", raw)
	}
	if n := strings.Count(candidate.seen(), "Credential=AKIACANDIDATE/"); n != 2 {
		t.Errorf("candidate requests signed with the candidate key = %d, want 2:\n%s", n, candidate.seen())
	}
	if saved.seen() != "" {
		t.Errorf("the probe reached the saved store: %s", saved.seen())
	}
	for _, leak := range []string{"CandidateSecretValue", "SavedSecretValue"} {
		if strings.Contains(raw, leak) || strings.Contains(logged.String(), leak) {
			t.Errorf("the probe response or log carries a secret")
		}
	}
	if !strings.Contains(logged.String(), "probe-b") || !strings.Contains(logged.String(), candidate.srv.URL) {
		t.Errorf("the probe does not log the bucket and endpoint it contacted: %s", logged.String())
	}

	// No store at all: no S3 results, and nothing contacted.
	_, raw = doProbe(t, srv, "/api/servers/test", `{`+deadDSN+`,"archive_s3":"s3://probe-b/c/"}`)
	if strings.Contains(raw, `"s3"`) {
		t.Errorf("a server with no store reported S3 results: %s", raw)
	}
}

// A failing store is one request, not the SDK's default retries, and the
// failure is the bucket's result.
func TestServersTest_s3ProbeFailsFastAndReports(t *testing.T) {
	isolateProbeEnv(t)
	srv := newRegistryServer(t)
	failing := newProbeFake(t, 500)
	pb, raw := doProbe(t, srv, "/api/servers/test", `{`+deadDSN+`,"archive_s3":"s3://probe-f/c/","s3_endpoint":"`+failing.srv.URL+`","s3_access_key_id":"AKIATYPED","s3_secret_access_key":"TypedSecretValue"}`)
	if len(pb.S3) != 1 || pb.S3[0].OK || pb.S3[0].Error == "" || pb.S3[0].NeedsSecret {
		t.Fatalf("failing store: %s", raw)
	}
	if n := failing.count(); n != 1 {
		t.Errorf("a failing store got %d requests, want 1 (no retries):\n%s", n, failing.seen())
	}
	// A store typed with no location of its own has nothing to test.
	pb, raw = doProbe(t, srv, "/api/servers/test", `{`+deadDSN+`,"s3_endpoint":"`+failing.srv.URL+`"}`)
	if len(pb.S3) != 1 || pb.S3[0].OK || !strings.Contains(pb.S3[0].Error, "Archive to S3") {
		t.Errorf("a store with no location: %s", raw)
	}
	// An endpoint that does not validate is the result, not a 400.
	pb, raw = doProbe(t, srv, "/api/servers/test", `{`+deadDSN+`,"archive_s3":"s3://probe-f/c/","s3_endpoint":"minio:9000"}`)
	if len(pb.S3) != 1 || pb.S3[0].OK || !strings.Contains(pb.S3[0].Error, "endpoint") {
		t.Errorf("an invalid endpoint: %s", raw)
	}
}

// The stored secret is reused only for the SAME store: same endpoint, same
// addressing, same access key. The test routes are servers:read, so anything
// else would let a reader send the saved secret's signatures to a host of
// their choosing.
func TestServersTest_storedSecretOnlyForTheSameStore(t *testing.T) {
	isolateProbeEnv(t)
	srv := newRegistryServer(t)
	stored, other := newProbeFake(t, 200), newProbeFake(t, 200)
	e, err := srv.cm.reg.Add(keyedEntry("s", "s3://probe-s/s/", stored.srv.URL, "AKIASAVED", "SavedSecretValue"))
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/servers/" + e.ID + "/test"
	form := func(endpoint, style, id string) string {
		return `{"archive_s3":"s3://probe-s/s/","baseline_s3":"","s3_endpoint":"` + endpoint + `","s3_path_style":"` + style + `","s3_region":"","s3_access_key_id":"` + id + `"}`
	}

	// The row's Test button sends {}: the saved server, keys and all.
	pb, raw := doProbe(t, srv, path, `{}`)
	if len(pb.S3) != 1 || !pb.S3[0].OK || !strings.Contains(stored.seen(), "Credential=AKIASAVED/") {
		t.Fatalf("row test of a saved keyed store: %s\n%s", raw, stored.seen())
	}
	// The form, unchanged, secret left blank: the saved secret is used, also
	// when the endpoint is spelled differently but normalizes the same.
	for _, spelling := range []string{stored.srv.URL, stored.srv.URL + "/", " " + strings.ToUpper(stored.srv.URL[:4]) + stored.srv.URL[4:] + " "} {
		before := stored.count()
		pb, raw = doProbe(t, srv, path, form(spelling, "", "AKIASAVED"))
		if len(pb.S3) != 1 || !pb.S3[0].OK || pb.S3[0].NeedsSecret || stored.count() != before+1 {
			t.Errorf("unchanged form (%q), blank secret: %s", spelling, raw)
		}
	}

	for name, body := range map[string]string{
		"other endpoint":   form(other.srv.URL, "", "AKIASAVED"),
		"other addressing": form(stored.srv.URL, "vhost", "AKIASAVED"),
		"other access key": form(stored.srv.URL, "", "AKIAOTHER"),
	} {
		before := stored.count()
		pb, raw := doProbe(t, srv, path, body)
		if len(pb.S3) != 1 || !pb.S3[0].NeedsSecret || pb.S3[0].OK {
			t.Errorf("%s, blank secret: %s, want needs_secret", name, raw)
		}
		if other.count() != 0 || stored.count() != before {
			t.Errorf("%s, blank secret: a store was contacted without the operator typing the secret", name)
		}
	}
	// The same store tested from the create form (no id) never gets a saved
	// secret.
	pb, raw = doProbe(t, srv, "/api/servers/test", `{`+deadDSN+`,`+strings.TrimPrefix(form(stored.srv.URL, "", "AKIASAVED"), "{"))
	if len(pb.S3) != 1 || !pb.S3[0].NeedsSecret {
		t.Errorf("create-form test with a blank secret: %s, want needs_secret", raw)
	}
	// Typed: used, whatever the endpoint.
	pb, raw = doProbe(t, srv, path, strings.TrimSuffix(form(other.srv.URL, "", "AKIATYPED"), "}")+`,"s3_secret_access_key":"TypedSecretValue"}`)
	if len(pb.S3) != 1 || !pb.S3[0].OK || !strings.Contains(other.seen(), "Credential=AKIATYPED/") {
		t.Errorf("typed secret: %s\n%s", raw, other.seen())
	}
	if strings.Contains(raw, "SavedSecretValue") || strings.Contains(raw, "TypedSecretValue") {
		t.Error("a probe response carries a secret")
	}
}

// A saved keys-only store (no endpoint) signs at the ambient endpoint with its
// own keys, from the row and from the unchanged form; another endpoint typed
// over it holds for the secret.
func TestServersTest_keysOnlyStore(t *testing.T) {
	isolateProbeEnv(t)
	ambient, other := newProbeFake(t, 200), newProbeFake(t, 200)
	t.Setenv(storage.EnvS3Endpoint, ambient.srv.URL)
	srv := newRegistryServer(t)
	e := keyedEntry("acct", "s3://probe-acct/a/", "", "AKIASAVED", "SavedSecretValue")
	e.S3Region = "eu-west-1"
	saved, err := srv.cm.reg.Add(e)
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/servers/" + saved.ID + "/test"
	if pb, raw := doProbe(t, srv, path, `{}`); len(pb.S3) != 1 || !pb.S3[0].OK || pb.S3[0].NotApplied {
		t.Errorf("row test: %s", raw)
	}
	form := func(endpoint string) string {
		return `{"archive_s3":"s3://probe-acct/a/","s3_endpoint":"` + endpoint + `","s3_region":"eu-west-1","s3_access_key_id":"AKIASAVED"}`
	}
	if pb, raw := doProbe(t, srv, path, form("")); len(pb.S3) != 1 || !pb.S3[0].OK {
		t.Errorf("unchanged form: %s", raw)
	}
	if n := strings.Count(ambient.seen(), "Credential=AKIASAVED/"); n != 2 || strings.Contains(ambient.seen(), "AKIAAMBIENT") {
		t.Errorf("keys-only store requests signed with its keys = %d, want 2:\n%s", n, ambient.seen())
	}
	if pb, raw := doProbe(t, srv, path, form(other.srv.URL)); len(pb.S3) != 1 || !pb.S3[0].NeedsSecret || other.count() != 0 {
		t.Errorf("another endpoint over a keys-only store, blank secret: %s", raw)
	}
}

// A store that accepts the connection and never answers ends the probe at
// the timeout, as that bucket's failure.
func TestServersTest_s3ProbeTimesOut(t *testing.T) {
	isolateProbeEnv(t)
	prev := s3ProbeTimeout
	s3ProbeTimeout = 100 * time.Millisecond
	t.Cleanup(func() { s3ProbeTimeout = prev })
	release := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(hang.Close)
	t.Cleanup(func() { close(release) })
	srv := newRegistryServer(t)
	// The request runs aside with a deadline of its own: without the probe's
	// timeout it would never return, and the test must fail, not hang.
	done := make(chan []byte, 1)
	go func() {
		_, raw := doServersReq(t, srv, "POST", "/api/servers/test", `{`+deadDSN+`,"archive_s3":"s3://probe-hang/x/","s3_endpoint":"`+hang.URL+`","s3_access_key_id":"AKIATYPED","s3_secret_access_key":"TypedSecretValue"}`)
		done <- raw
	}()
	var raw []byte
	select {
	case raw = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the probe did not return against a store that never answers")
	}
	var pb probeBody
	if err := json.Unmarshal(raw, &pb); err != nil {
		t.Fatal(err)
	}
	if len(pb.S3) != 1 || pb.S3[0].OK || pb.S3[0].Error == "" {
		t.Errorf("a hanging store: %s", raw)
	}
}

func TestS3ProbeErrorScrubsTheSecret(t *testing.T) {
	err := errors.New("signature mismatch for Store/Secret+Value42 at https://minio")
	got := s3ProbeError(err, keySecret)
	if strings.Contains(got, keySecret) || !strings.Contains(got, "signature mismatch") {
		t.Errorf("s3ProbeError = %q", got)
	}
	if got := s3ProbeError(err, ""); got != err.Error() {
		t.Errorf("no secret: %q", got)
	}
}
