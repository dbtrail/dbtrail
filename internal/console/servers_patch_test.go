package console

import (
	"strings"
	"testing"
)

// PATCH /api/servers/{id} changes what the body names and keeps the rest.
// Each case names one field; everything else of the stored entry must come
// out byte for byte what it was.
func TestServersPatchChangesOnlyWhatIsSent(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		check func(before, after *ServerEntry)
	}{
		{"the schema filter", `{"schemas":"shop"}`, func(b, a *ServerEntry) {
			if a.Schemas != "shop" {
				t.Errorf("schemas = %q", a.Schemas)
			}
			b.Schemas = a.Schemas
		}},
		{"the schema filter, cleared on purpose", `{"schemas":""}`, func(b, a *ServerEntry) {
			if a.Schemas != "" {
				t.Errorf("schemas = %q", a.Schemas)
			}
			b.Schemas = ""
		}},
		{"the name", `{"name":"store"}`, func(b, a *ServerEntry) {
			if a.Name != "store" {
				t.Errorf("name = %q", a.Name)
			}
			b.Name = a.Name
		}},
		{"the bucket rotated hours go to", `{"archive_s3":"s3://bucket/archive/"}`, func(b, a *ServerEntry) {
			if a.ArchiveS3 != "s3://bucket/archive/" {
				t.Errorf("archive_s3 = %q", a.ArchiveS3)
			}
			b.ArchiveS3 = a.ArchiveS3
		}},
		{"the S3 store, with the bucket it is for", `{"archive_s3":"s3://bucket/archive/","s3_endpoint":"https://minio.example.com","s3_region":"us-east-1","s3_access_key_id":"AKIA1","s3_secret_access_key":"sec"}`, func(b, a *ServerEntry) {
			b.ArchiveS3 = a.ArchiveS3
			if a.S3Endpoint != "https://minio.example.com" || a.S3Region != "us-east-1" || a.S3AccessKeyID != "AKIA1" || a.S3SecretAccessKey != "sec" {
				t.Errorf("store = %q %q %q", a.S3Endpoint, a.S3Region, a.S3AccessKeyID)
			}
			b.S3Endpoint, b.S3Region, b.S3PathStyle, b.S3AccessKeyID, b.S3SecretAccessKey = a.S3Endpoint, a.S3Region, a.S3PathStyle, a.S3AccessKeyID, a.S3SecretAccessKey
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv, _ := newSupervisorServer(t)
			id := addedWithSchemas(t, srv)
			// A forwarding account too: it has a keep rule of its own.
			if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/route", `{"user":"shop_fwd","password":"Fw9-abc"}`); rec.Code != 200 {
				t.Fatalf("route: %d %s", rec.Code, body)
			}
			before := entryOf(t, srv, id)
			rec, body := doServersReq(t, srv, "PATCH", "/api/servers/"+id, c.body)
			if rec.Code != 200 {
				t.Fatalf("code=%d body=%s", rec.Code, body)
			}
			after := entryOf(t, srv, id)
			want := before
			c.check(&want, &after)
			if a, b := entryJSON(t, after), entryJSON(t, want); a != b {
				t.Errorf("something else changed:\n got %s\nwant %s", a, b)
			}
		})
	}
}

// The same body on PUT clears what it does not name: the reason PATCH
// exists. If PUT ever keeps them, the comment on handleServersPatch is what
// is out of date.
func TestServersPutClearsWhatPatchKeeps(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	id := addedWithSchemas(t, srv)
	if rec, body := doServersReq(t, srv, "PATCH", "/api/servers/"+id, `{"archive_s3":"s3://bucket/archive/"}`); rec.Code != 200 {
		t.Fatalf("patch: %d %s", rec.Code, body)
	}
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id, `{"name":"shop"}`); rec.Code != 200 {
		t.Fatalf("put: %d %s", rec.Code, body)
	}
	e := entryOf(t, srv, id)
	if e.Schemas != "" || e.ArchiveS3 != "" {
		t.Errorf("PUT kept schemas=%q archive_s3=%q", e.Schemas, e.ArchiveS3)
	}
}

func TestServersPatchSourceLogin(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	id := addedWithSchemas(t, srv)
	// While its changes are being recorded the source is not re-pointed
	// under the stream: the edit says to stop first, and changes nothing.
	before := entryOf(t, srv, id)
	rec, body := doServersReq(t, srv, "PATCH", "/api/servers/"+id, `{"source_password":"New-pw9"}`)
	if rec.Code != 409 || !strings.Contains(string(body), "stop monitoring") {
		t.Fatalf("while recording: code=%d body=%s", rec.Code, body)
	}
	if got := entryOf(t, srv, id); entryJSON(t, got) != entryJSON(t, before) {
		t.Fatal("a refused edit changed the server")
	}
	if rec, body := doServersReq(t, srv, "POST", "/api/servers/"+id+"/monitor/stop", ""); rec.Code != 200 {
		t.Fatalf("stop: %d %s", rec.Code, body)
	}
	_ = ctrl
	stopped := entryOf(t, srv, id)
	// The password alone: host, port and user stay.
	rec, body = doServersReq(t, srv, "PATCH", "/api/servers/"+id, `{"source_password":"New-pw9"}`)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	after := entryOf(t, srv, id)
	if !strings.Contains(after.SourceDSN, "dbtrail:New-pw9@") || !strings.Contains(after.SourceDSN, "db.example.com") {
		t.Errorf("source = %q", after.SourceDSN)
	}
	if strings.Contains(string(body), "New-pw9") {
		t.Errorf("the answer carries the password: %s", body)
	}
	want := stopped
	want.SourceDSN = after.SourceDSN
	// The forwarding account lives on the source's address and follows it;
	// none is set here, so nothing else may move.
	want.RouteDSN = after.RouteDSN
	if a, b := entryJSON(t, after), entryJSON(t, want); a != b {
		t.Errorf("something else changed:\n got %s\nwant %s", a, b)
	}
	if after.Schemas != "shop,billing" {
		t.Errorf("schemas = %q", after.Schemas)
	}
	// The host alone keeps the user and the new password.
	rec, body = doServersReq(t, srv, "PATCH", "/api/servers/"+id, `{"source_host":"db2.example.com"}`)
	if rec.Code != 200 {
		t.Fatalf("host: code=%d body=%s", rec.Code, body)
	}
	if got := entryOf(t, srv, id).SourceDSN; !strings.Contains(got, "dbtrail:New-pw9@") || !strings.Contains(got, "db2.example.com") {
		t.Errorf("after the host change: %q", got)
	}
}

func TestServersPatchRefusals(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	id := addedWithSchemas(t, srv)
	before := entryJSON(t, entryOf(t, srv, id))
	for name, c := range map[string]struct {
		path, body string
		want       int
	}{
		"nothing named":                      {"/api/servers/" + id, `{}`, 400},
		"not JSON":                           {"/api/servers/" + id, `schemas=x`, 400},
		"a new S3 access key with no secret": {"/api/servers/" + id, `{"archive_s3":"s3://b/a/","s3_access_key_id":"AKIA2"}`, 400},
		"an S3 store with no bucket":         {"/api/servers/" + id, `{"s3_region":"us-east-1"}`, 400},
		"another flavor":                     {"/api/servers/" + id, `{"flavor":"postgres"}`, 400},
		"unknown server":                     {"/api/servers/nope", `{"schemas":"x"}`, 404},
		"the command-line server":            {"/api/servers/" + bootServerID, `{"schemas":"x"}`, 409},
	} {
		rec, body := doServersReq(t, srv, "PATCH", c.path, c.body)
		if rec.Code != c.want {
			t.Errorf("%s: code=%d, want %d: %s", name, rec.Code, c.want, body)
		}
	}
	if after := entryJSON(t, entryOf(t, srv, id)); after != before {
		t.Errorf("a refused edit changed the server:\n got %s\nwant %s", after, before)
	}
}
