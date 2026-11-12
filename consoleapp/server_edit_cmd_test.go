package consoleapp

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func namedFlags(names ...string) func(string) bool {
	set := map[string]bool{}
	for _, n := range names {
		set[n] = true
	}
	return func(n string) bool { return set[n] }
}

func TestEditBody(t *testing.T) {
	cases := []struct {
		name    string
		f       editFlags
		named   []string
		tty     bool
		secret  string
		stdin   string
		want    map[string]any
		source  bool
		wantErr string
	}{
		{name: "rename", f: editFlags{Name: " store "}, named: []string{"name"}, want: map[string]any{"name": "store"}},
		{name: "schemas", f: editFlags{Schemas: "a,b"}, named: []string{"schemas"}, want: map[string]any{"schemas": "a,b"}},
		// Named and empty is a value: every schema. Not named is not sent.
		{name: "schemas cleared", f: editFlags{}, named: []string{"schemas"}, want: map[string]any{"schemas": ""}},
		{name: "host and port", f: editFlags{Host: "[2001:db8::1]", Port: "3307"}, named: []string{"host", "port"}, want: map[string]any{"source_host": "2001:db8::1", "source_port": "3307"}, source: true},
		{name: "password asked", f: editFlags{AskPassword: true}, tty: true, secret: "n3w #pw", want: map[string]any{"source_password": "n3w #pw"}, source: true},
		{name: "password from stdin", f: editFlags{PasswordStdin: true}, stdin: " pw \r\nmore\n", want: map[string]any{"source_password": " pw "}, source: true},
		{name: "archive bucket", f: editFlags{ArchiveS3: "s3://b/a/"}, named: []string{"archive-s3"}, want: map[string]any{"archive_s3": "s3://b/a/"}},
		{name: "archive bucket removed", f: editFlags{}, named: []string{"archive-s3"}, want: map[string]any{"archive_s3": ""}},
		{name: "S3 keys", f: editFlags{S3AccessKey: "AKIA1", S3SecretStdin: true}, named: []string{"s3-access-key"}, stdin: "sec\n", want: map[string]any{"s3_access_key_id": "AKIA1", "s3_secret_access_key": "sec"}},
		{name: "back to the machine's credentials", f: editFlags{}, named: []string{"s3-access-key"}, want: map[string]any{"s3_access_key_id": ""}},
		{name: "several at once", f: editFlags{Name: "store", Schemas: "a"}, named: []string{"name", "schemas"}, want: map[string]any{"name": "store", "schemas": "a"}},

		{name: "nothing named", f: editFlags{Name: "ignored"}, wantErr: "nothing to change"},
		{name: "empty name", f: editFlags{}, named: []string{"name"}, wantErr: "--name is empty"},
		{name: "empty host", f: editFlags{}, named: []string{"host"}, wantErr: "--host is empty"},
		{name: "bad port", f: editFlags{Port: "33o6"}, named: []string{"port"}, wantErr: "1 to 65535"},
		{name: "host with a port in it", f: editFlags{Host: "db1:3307"}, named: []string{"host"}, wantErr: "-P"},
		{name: "-p and --password-stdin", f: editFlags{AskPassword: true, PasswordStdin: true}, wantErr: "use one"},
		{name: "-p with no terminal", f: editFlags{AskPassword: true}, wantErr: "--password-stdin"},
		{name: "empty password typed", f: editFlags{AskPassword: true}, tty: true, wantErr: "empty"},
		{name: "empty stdin", f: editFlags{PasswordStdin: true}, wantErr: "stdin"},
		{name: "stdin is only a newline", f: editFlags{PasswordStdin: true}, stdin: "\n", wantErr: "empty"},
		{name: "two secrets on one stdin", f: editFlags{PasswordStdin: true, S3SecretStdin: true, S3AccessKey: "A"}, named: []string{"s3-access-key"}, stdin: "x\n", wantErr: "both read stdin"},
		{name: "a secret with no access key", f: editFlags{S3SecretStdin: true}, stdin: "x\n", wantErr: "--s3-access-key"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ask := &fakeAsker{t: t, tty: c.tty, secret: c.secret}
			body, source, err := editBody(c.f, namedFlags(c.named...), ask, strings.NewReader(c.stdin), "shop")
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("got %v, %v; want an error with %q", body, err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(body) != len(c.want) {
				t.Fatalf("body = %v, want %v", body, c.want)
			}
			for k, v := range c.want {
				if body[k] != v {
					t.Errorf("%s = %q, want %q (body %v)", k, body[k], v, body)
				}
			}
			if source != c.source {
				t.Errorf("source = %v, want %v", source, c.source)
			}
		})
	}
}

func editServers(d *fakeDaemon, state string) {
	d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{
		{"id": "id1", "name": "shop", "kind": "registry", "has_source": true, "source_host": "db1", "source_port": "3306", "source_user": "dbtrail", "monitor_state": state},
	}})
}

func callsFrom(d *fakeDaemon, first string) []string {
	for i, c := range d.calls {
		if c == first {
			return d.calls[i:]
		}
	}
	return nil
}

func TestServerEditThatDoesNotTouchTheLogin(t *testing.T) {
	d, c := newFakeDaemon(t)
	editServers(d, "running")
	d.on("PATCH /api/servers/id1", 200, map[string]any{"id": "id1", "name": "store", "has_source": true, "source_host": "db1", "source_port": "3306"})
	var out bytes.Buffer
	err := runServerEdit(context.Background(), c, "shop", editFlags{Name: "store", Schemas: "a,b"}, namedFlags("name", "schemas"), &fakeAsker{t: t}, strings.NewReader(""), &out)
	if err != nil {
		t.Fatal(err)
	}
	// One request, with the two fields and nothing else; recording is not
	// touched.
	body := d.bodies["PATCH /api/servers/id1"]
	if len(body) != 2 || body["name"] != "store" || body["schemas"] != "a,b" {
		t.Errorf("body = %v", body)
	}
	for _, call := range d.calls {
		if strings.Contains(call, "/monitor/") || strings.HasPrefix(call, "PUT ") {
			t.Errorf("made %s", call)
		}
	}
	if !strings.Contains(out.String(), `Changed "shop": renamed to "store", schemas a,b.`) {
		t.Errorf("output:\n%s", out.String())
	}
}

func TestServerEditOfTheLoginStopsSavesAndStarts(t *testing.T) {
	d, c := newFakeDaemon(t)
	editServers(d, "running")
	d.on("POST /api/servers/id1/monitor/stop", 200, map[string]any{})
	d.on("PATCH /api/servers/id1", 200, map[string]any{"id": "id1", "name": "shop", "has_source": true, "source_host": "db1", "source_port": "3306"})
	d.on("POST /api/servers/id1/monitor/start", 200, map[string]any{"started": true})
	var out bytes.Buffer
	err := runServerEdit(context.Background(), c, "shop", editFlags{PasswordStdin: true}, namedFlags(), &fakeAsker{t: t}, strings.NewReader("n3w pw\n"), &out)
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	got := strings.Join(callsFrom(d, "POST /api/servers/id1/monitor/stop"), " | ")
	if got != "POST /api/servers/id1/monitor/stop | PATCH /api/servers/id1 | POST /api/servers/id1/monitor/start" {
		t.Errorf("calls = %s", got)
	}
	if body := d.bodies["PATCH /api/servers/id1"]; len(body) != 1 || body["source_password"] != "n3w pw" {
		t.Errorf("body = %v", body)
	}
	if strings.Contains(out.String(), "n3w") || !strings.Contains(out.String(), "a new password") || !strings.Contains(out.String(), `Recording "shop" again`) {
		t.Errorf("output:\n%s", out.String())
	}
}

func TestServerEditOfTheLoginFailures(t *testing.T) {
	flags, in := editFlags{PasswordStdin: true}, "pw\n"
	t.Run("the stop fails: nothing is changed", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		editServers(d, "running")
		d.on("POST /api/servers/id1/monitor/stop", 500, map[string]any{"error": "busy"})
		err := runServerEdit(context.Background(), c, "shop", flags, namedFlags(), &fakeAsker{t: t}, strings.NewReader(in), io.Discard)
		if err == nil || !strings.Contains(err.Error(), "nothing was changed") {
			t.Fatalf("err = %v", err)
		}
		for _, call := range d.calls {
			if strings.HasPrefix(call, "PATCH") {
				t.Error("saved after a stop that failed")
			}
		}
	})
	t.Run("the save is refused: recording is put back", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		editServers(d, "running")
		d.on("POST /api/servers/id1/monitor/stop", 200, map[string]any{})
		d.on("PATCH /api/servers/id1", 400, map[string]any{"error": "host is required"})
		d.on("POST /api/servers/id1/monitor/start", 200, map[string]any{"started": true})
		err := runServerEdit(context.Background(), c, "shop", flags, namedFlags(), &fakeAsker{t: t}, strings.NewReader(in), io.Discard)
		if err == nil || !strings.Contains(err.Error(), "being recorded as before") || !strings.Contains(err.Error(), "host is required") {
			t.Fatalf("err = %v", err)
		}
		if d.calls[len(d.calls)-1] != "POST /api/servers/id1/monitor/start" {
			t.Errorf("recording was not put back: %v", d.calls)
		}
	})
	t.Run("the save is refused and recording does not come back: both are said", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		editServers(d, "running")
		d.on("POST /api/servers/id1/monitor/stop", 200, map[string]any{})
		d.on("PATCH /api/servers/id1", 400, map[string]any{"error": "host is required"})
		d.on("POST /api/servers/id1/monitor/start", 200, map[string]any{"started": false})
		err := runServerEdit(context.Background(), c, "shop", flags, namedFlags(), &fakeAsker{t: t}, strings.NewReader(in), io.Discard)
		if err == nil || !strings.Contains(err.Error(), "did not start again") || !strings.Contains(err.Error(), "server start shop") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("saved, and the checks fail against the new login", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		editServers(d, "running")
		d.on("POST /api/servers/id1/monitor/stop", 200, map[string]any{})
		d.on("PATCH /api/servers/id1", 200, map[string]any{"id": "id1", "name": "shop"})
		d.on("POST /api/servers/id1/monitor/start", 200, map[string]any{"started": false, "doctor": map[string]any{
			"failed": 1, "checks": []map[string]any{{"name": "Source MySQL connection", "status": "fail", "detail": "Access denied"}}}})
		var out bytes.Buffer
		err := runServerEdit(context.Background(), c, "shop", flags, namedFlags(), &fakeAsker{t: t}, strings.NewReader(in), &out)
		if err == nil || !strings.Contains(err.Error(), "the change is saved") || !strings.Contains(err.Error(), "server start shop") {
			t.Fatalf("err = %v", err)
		}
		if !strings.Contains(out.String(), "Access denied") {
			t.Errorf("the failed check is not printed:\n%s", out.String())
		}
	})
	t.Run("an answer that does not say whether it started is not a start", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		editServers(d, "running")
		d.on("POST /api/servers/id1/monitor/stop", 200, map[string]any{})
		d.on("PATCH /api/servers/id1", 200, map[string]any{"id": "id1", "name": "shop"})
		d.on("POST /api/servers/id1/monitor/start", 200, map[string]any{})
		var out bytes.Buffer
		err := runServerEdit(context.Background(), c, "shop", flags, namedFlags(), &fakeAsker{t: t}, strings.NewReader(in), &out)
		if err == nil || strings.Contains(out.String(), "Recording") {
			t.Fatalf("err = %v, out = %s", err, out.String())
		}
	})
}

// On a server that is stopped, the login is changed with no stop and no
// start: starting it is not what was asked.
func TestServerEditOfTheLoginOnAStoppedServer(t *testing.T) {
	d, c := newFakeDaemon(t)
	editServers(d, "stopped")
	d.on("PATCH /api/servers/id1", 200, map[string]any{"id": "id1", "name": "shop"})
	var out bytes.Buffer
	if err := runServerEdit(context.Background(), c, "shop", editFlags{PasswordStdin: true}, namedFlags(), &fakeAsker{t: t}, strings.NewReader("pw\n"), &out); err != nil {
		t.Fatal(err)
	}
	for _, call := range d.calls {
		if strings.Contains(call, "/monitor/") {
			t.Errorf("made %s", call)
		}
	}
	if !strings.Contains(out.String(), "server start shop") {
		t.Errorf("output:\n%s", out.String())
	}
}

// What an edit says it changed names each field, tells a cleared setting
// from a set one, and never carries a secret.
func TestEditedText(t *testing.T) {
	before := ctlServer{Name: "shop"}
	saved := ctlServer{Name: "store", SourceHost: "db2", SourcePort: "3307", SourceUser: "dbt"}
	got := editedText(before, saved, map[string]any{
		"name": "store", "source_host": "db2", "source_user": "dbt", "source_password": "s3cret",
		"schemas": "", "archive_s3": "", "s3_region": "", "s3_endpoint": "https://minio.example.com",
		"s3_access_key_id": "", "s3_secret_access_key": "sec2",
	})
	for _, want := range []string{`Changed "shop"`, `renamed to "store"`, "address db2:3307", "user dbt", "a new password",
		"every schema", "no S3 archive", "S3 region cleared", "S3 endpoint https://minio.example.com", "the machine's own S3 credentials"} {
		if !strings.Contains(got, want) {
			t.Errorf("lacks %q: %s", want, got)
		}
	}
	if strings.Contains(got, "s3cret") || strings.Contains(got, "sec2") {
		t.Errorf("carries a secret: %s", got)
	}
}

func TestServerStartStop(t *testing.T) {
	d, c := newFakeDaemon(t)
	editServers(d, "stopped")
	d.on("POST /api/servers/id1/monitor/start", 200, map[string]any{"started": true})
	d.on("POST /api/servers/id1/monitor/stop", 200, map[string]any{})
	var out bytes.Buffer
	if err := runServerStartStop(context.Background(), c, "shop", true, &out); err != nil || !strings.Contains(out.String(), `Recording "shop".`) {
		t.Fatalf("start: %v\n%s", err, out.String())
	}
	out.Reset()
	if err := runServerStartStop(context.Background(), c, "shop", false, &out); err != nil || !strings.Contains(out.String(), "Stopped recording") {
		t.Fatalf("stop: %v\n%s", err, out.String())
	}
	d.on("POST /api/servers/id1/monitor/start", 200, map[string]any{"started": false, "doctor": map[string]any{
		"failed": 1, "checks": []map[string]any{{"name": "binlog_format=ROW", "status": "fail", "detail": "is MIXED"}}}})
	out.Reset()
	err := runServerStartStop(context.Background(), c, "shop", true, &out)
	if err == nil || !strings.Contains(out.String(), "is MIXED") || strings.Contains(out.String(), "Recording") {
		t.Fatalf("a failed check: %v\n%s", err, out.String())
	}
}

func TestSnapshotLocation(t *testing.T) {
	t.Run("show", func(t *testing.T) {
		for _, c := range []struct {
			row  map[string]any
			want string
		}{
			{map[string]any{"id": "id1", "resolved_dir": "/var/lib/bintrail/snapshots/id1", "resolved_s3": "s3://b/s/"}, "kept in /var/lib/bintrail/snapshots/id1 and sent to s3://b/s/"},
			{map[string]any{"id": "id1", "resolved_dir": "/data/s"}, "They are not sent to S3"},
			{map[string]any{"id": "id1", "resolved_s3": "s3://b/s/"}, "kept in s3://b/s/ only"},
			{map[string]any{"id": "id1"}, "no place is set"},
		} {
			d, cl := newFakeDaemon(t)
			editServers(d, "running")
			d.on("GET /api/backup-settings", 200, map[string]any{"servers": []map[string]any{{"id": "other", "resolved_dir": "/x"}, c.row}})
			var out bytes.Buffer
			if err := runSnapshotLocation(context.Background(), cl, "shop", locFlags{}, namedFlags(), &out); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), c.want) {
				t.Errorf("output lacks %q:\n%s", c.want, out.String())
			}
		}
	})
	t.Run("set S3 and its retention: only those fields travel", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		editServers(d, "running")
		d.on("PUT /api/backup-settings/servers/id1", 200, map[string]any{"id": "id1", "resolved_dir": "/data/s", "resolved_s3": "s3://b/s/"})
		var out bytes.Buffer
		err := runSnapshotLocation(context.Background(), c, "shop", locFlags{S3: " s3://b/s/ ", S3DeleteAfter: "30d"}, namedFlags("s3", "s3-delete-after"), &out)
		if err != nil {
			t.Fatal(err)
		}
		body := d.bodies["PUT /api/backup-settings/servers/id1"]
		if len(body) != 2 || body["baseline_s3"] != "s3://b/s/" || body["baseline_s3_retain"] != "30d" {
			t.Errorf("body = %v", body)
		}
		if !strings.Contains(out.String(), "sent to s3://b/s/") || !strings.Contains(out.String(), "deleted once it is 30d old") {
			t.Errorf("output:\n%s", out.String())
		}
	})
	t.Run("a first S3 destination: the schedule's new S3 interval is said", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		editServers(d, "running")
		d.on("PUT /api/backup-settings/servers/id1", 200, map[string]any{"id": "id1", "resolved_dir": "/data/s", "resolved_s3": "s3://b/s/", "schedule_upload_set": "1d"})
		var out bytes.Buffer
		if err := runSnapshotLocation(context.Background(), c, "shop", locFlags{S3: "s3://b/s/"}, namedFlags("s3"), &out); err != nil {
			t.Fatal(err)
		}
		t.Logf("output:\n%s", out.String())
		if !strings.Contains(out.String(), "Its schedule now sends a snapshot to S3 every 1d, not at each update. Change it on the Snapshots page.\n") {
			t.Errorf("output does not say the interval the daemon started:\n%s", out.String())
		}
	})
	t.Run("stop sending to S3, keep-newest 0", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		editServers(d, "running")
		d.on("PUT /api/backup-settings/servers/id1", 200, map[string]any{"id": "id1", "resolved_dir": "/data/s"})
		if err := runSnapshotLocation(context.Background(), c, "shop", locFlags{}, namedFlags("s3", "keep-newest"), io.Discard); err != nil {
			t.Fatal(err)
		}
		body := d.bodies["PUT /api/backup-settings/servers/id1"]
		if len(body) != 2 || body["baseline_s3"] != "" || body["keep_newest"] != float64(0) {
			t.Errorf("body = %v", body)
		}
	})
	t.Run("refusals", func(t *testing.T) {
		for name, c := range map[string]struct {
			f     locFlags
			named []string
			want  string
		}{
			"not an s3 address":    {locFlags{S3: "bucket/prefix"}, []string{"s3"}, "s3://bucket/prefix/"},
			"empty folder":         {locFlags{}, []string{"dir"}, "every server keeps its snapshots in a folder"},
			"negative keep-newest": {locFlags{KeepNewest: -1}, []string{"keep-newest"}, "0 (keep every snapshot)"},
		} {
			d, cl := newFakeDaemon(t)
			editServers(d, "running")
			err := runSnapshotLocation(context.Background(), cl, "shop", c.f, namedFlags(c.named...), io.Discard)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("%s: err = %v", name, err)
			}
			if len(d.calls) != 1 {
				t.Errorf("%s: calls = %v", name, d.calls)
			}
		}
	})
	t.Run("the daemon refuses in its own words", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		editServers(d, "running")
		d.on("PUT /api/backup-settings/servers/id1", 400, map[string]any{"error": "another server already writes to that location"})
		err := runSnapshotLocation(context.Background(), c, "shop", locFlags{S3: "s3://b/s/"}, namedFlags("s3"), io.Discard)
		if err == nil || !strings.Contains(err.Error(), "already writes") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("an answer that is not the settings", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		editServers(d, "running")
		d.on("PUT /api/backup-settings/servers/id1", 200, map[string]any{})
		if err := runSnapshotLocation(context.Background(), c, "shop", locFlags{S3: "s3://b/s/"}, namedFlags("s3"), io.Discard); err == nil {
			t.Fatal("no error")
		}
	})
}
