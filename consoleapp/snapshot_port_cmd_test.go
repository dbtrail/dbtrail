package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func fastPoll(t *testing.T) {
	t.Helper()
	old := snapshotPoll
	snapshotPoll = time.Millisecond
	t.Cleanup(func() { snapshotPoll = old })
}

// seq answers a path with each of its answers in turn, the last one forever.
func (d *fakeDaemon) seq(key string, answers ...map[string]any) {
	i := 0
	d.handlers[key] = func(map[string]any) (int, any) {
		a := answers[min(i, len(answers)-1)]
		i++
		return 200, a
	}
}

func baselineAnswer(fields map[string]any) map[string]any {
	return map[string]any{"baseline": fields}
}

func TestSnapshotRunWaitsForTheCopy(t *testing.T) {
	fastPoll(t)
	d, c := newFakeDaemon(t)
	twoServers(d)
	d.on("POST /api/servers/id1/baseline", 202, baselineAnswer(map[string]any{"state": "running"}))
	d.seq("GET /api/servers/id1/baseline",
		baselineAnswer(map[string]any{"state": "running"}),
		baselineAnswer(map[string]any{"state": "running"}),
		baselineAnswer(map[string]any{"state": "succeeded", "tables": 12}))
	var out bytes.Buffer
	if err := runSnapshotRun(context.Background(), c, "shop", true, &out); err != nil {
		t.Fatalf("err = %v\n%s", err, out.String())
	}
	for _, s := range []string{`The copy of "shop" is ready: 12 tables.`, "snapshot schedule shop --every 1h", "port on"} {
		if !strings.Contains(out.String(), s) {
			t.Errorf("output lacks %q:\n%s", s, out.String())
		}
	}
	polls := 0
	for _, call := range d.calls {
		if call == "GET /api/servers/id1/baseline" {
			polls++
		}
	}
	if polls != 3 {
		t.Errorf("asked %d times, want 3: it must go on until the read ends and not after", polls)
	}
}

func TestSnapshotRunFailure(t *testing.T) {
	fastPoll(t)
	d, c := newFakeDaemon(t)
	twoServers(d)
	d.on("POST /api/servers/id1/baseline", 202, baselineAnswer(map[string]any{"state": "running"}))
	d.seq("GET /api/servers/id1/baseline", baselineAnswer(map[string]any{"state": "failed", "last_error": "mydumper: access denied; you need RELOAD"}))
	var out bytes.Buffer
	err := runSnapshotRun(context.Background(), c, "shop", true, &out)
	if !errors.Is(err, errSnapshotFailed) || !strings.Contains(err.Error(), "you need RELOAD") {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(out.String(), "is ready") {
		t.Errorf("a failed read says ready:\n%s", out.String())
	}
}

// A read whose only failure is the upload left a complete copy on this
// machine. Calling that "no copy" would send someone to read it all again.
func TestSnapshotRunFailedUploadIsNotNoCopy(t *testing.T) {
	fastPoll(t)
	d, c := newFakeDaemon(t)
	twoServers(d)
	d.on("POST /api/servers/id1/baseline", 202, baselineAnswer(map[string]any{"state": "running"}))
	d.seq("GET /api/servers/id1/baseline", baselineAnswer(map[string]any{"state": "failed", "published": true, "last_error": "s3: AccessDenied"}))
	err := runSnapshotRun(context.Background(), c, "shop", true, io.Discard)
	if err == nil || errors.Is(err, errSnapshotFailed) || !strings.Contains(err.Error(), "ready on this machine") || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("err = %v", err)
	}
}

func TestSnapshotRunEdges(t *testing.T) {
	fastPoll(t)
	t.Run("ended at once, in the answer to the request itself", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id1/baseline", 202, baselineAnswer(map[string]any{"state": "succeeded", "tables": 1}))
		var out bytes.Buffer
		if err := runSnapshotRun(context.Background(), c, "shop", true, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "ready: 1 table.") {
			t.Errorf("output:\n%s", out.String())
		}
	})
	t.Run("an answer with no state is not success", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id1/baseline", 202, map[string]any{})
		var out bytes.Buffer
		if err := runSnapshotRun(context.Background(), c, "shop", true, &out); err == nil || strings.Contains(out.String(), "is ready") {
			t.Fatalf("err = %v, out = %s", err, out.String())
		}
	})
	t.Run("idle after running is not success either", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id1/baseline", 202, baselineAnswer(map[string]any{"state": "running"}))
		d.seq("GET /api/servers/id1/baseline", baselineAnswer(map[string]any{"state": "idle"}))
		if err := runSnapshotRun(context.Background(), c, "shop", true, io.Discard); err == nil {
			t.Fatal("a read that went back to idle (DBTrail restarted) passed as done")
		}
	})
	t.Run("the daemon refuses to start it", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id1/baseline", 409, map[string]any{"error": "a read of this server is already running"})
		err := runSnapshotRun(context.Background(), c, "shop", true, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "already running") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("--no-wait starts and returns", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id1/baseline", 202, baselineAnswer(map[string]any{"state": "running"}))
		var out bytes.Buffer
		if err := runSnapshotRun(context.Background(), c, "shop", false, &out); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), "is ready") || !strings.Contains(out.String(), "snapshot status shop") {
			t.Errorf("output:\n%s", out.String())
		}
	})
	t.Run("Ctrl-C stops the waiting and says the read goes on", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id1/baseline", 202, baselineAnswer(map[string]any{"state": "running"}))
		d.seq("GET /api/servers/id1/baseline", baselineAnswer(map[string]any{"state": "running"}))
		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(20 * time.Millisecond); cancel() }()
		err := runSnapshotRun(ctx, c, "shop", true, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "The read goes on") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("asking how it goes fails: the read was still started", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id1/baseline", 202, baselineAnswer(map[string]any{"state": "running"}))
		d.on("GET /api/servers/id1/baseline", 500, map[string]any{"error": "boom"})
		err := runSnapshotRun(context.Background(), c, "shop", true, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "was started") || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unknown server asks nothing of it", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		if err := runSnapshotRun(context.Background(), c, "nope", true, io.Discard); err == nil {
			t.Fatal("no error")
		}
		if len(d.calls) != 1 {
			t.Errorf("calls = %v", d.calls)
		}
	})
	t.Run("tables left out are said", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id1/baseline", 202, baselineAnswer(map[string]any{"state": "succeeded", "tables": 5,
			"left_out_tables": []map[string]any{{"table": "a.b"}}, "left_out_tables_omitted": 2}))
		var out bytes.Buffer
		if err := runSnapshotRun(context.Background(), c, "shop", true, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "3 tables left out") {
			t.Errorf("output:\n%s", out.String())
		}
	})
}

func TestSnapshotStatus(t *testing.T) {
	for _, c := range []struct {
		fields map[string]any
		want   string
	}{
		{map[string]any{"state": "running", "since": "2026-11-11T10:00:00Z"}, "a read is running since 2026-11-11T10:00:00Z"},
		{map[string]any{"state": "succeeded", "tables": 3, "finished_at": "2026-11-11T10:05:00Z"}, "ended well at 2026-11-11T10:05:00Z: 3 tables"},
		{map[string]any{"state": "failed", "last_error": "disk full"}, "the copy was not taken: disk full"},
		{map[string]any{"state": "failed", "published": true, "last_error": "s3 down"}, "ready on this machine"},
		{map[string]any{"state": "idle"}, "snapshot run shop"},
		{map[string]any{}, "snapshot run shop"},
	} {
		d, cl := newFakeDaemon(t)
		twoServers(d)
		d.on("GET /api/servers/id1/baseline", 200, baselineAnswer(c.fields))
		var out bytes.Buffer
		if err := runSnapshotStatus(context.Background(), cl, "shop", &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), c.want) {
			t.Errorf("%v: output lacks %q:\n%s", c.fields, c.want, out.String())
		}
	}
}

func TestSnapshotSchedule(t *testing.T) {
	d, c := newFakeDaemon(t)
	twoServers(d)
	d.on("PUT /api/servers/id1/backup-schedule", 200, map[string]any{})
	d.on("DELETE /api/servers/id1/backup-schedule", 200, map[string]any{})
	var out bytes.Buffer
	if err := runSnapshotSchedule(context.Background(), c, "shop", " 1h ", &out); err != nil {
		t.Fatal(err)
	}
	if got := d.bodies["PUT /api/servers/id1/backup-schedule"]["every"]; got != "1h" {
		t.Errorf("every = %v", got)
	}
	if !strings.Contains(out.String(), "updated every 1h") {
		t.Errorf("output: %s", out.String())
	}
	out.Reset()
	if err := runSnapshotSchedule(context.Background(), c, "shop", "", &out); err != nil {
		t.Fatal(err)
	}
	if d.calls[len(d.calls)-1] != "DELETE /api/servers/id1/backup-schedule" || !strings.Contains(out.String(), "no longer updated") {
		t.Errorf("calls = %v, out = %s", d.calls, out.String())
	}

	d.on("PUT /api/servers/id1/backup-schedule", 400, map[string]any{"error": `"every" must be a duration such as 30m or 1h`})
	if err := runSnapshotSchedule(context.Background(), c, "shop", "often", io.Discard); err == nil || !strings.Contains(err.Error(), "duration") {
		t.Fatalf("err = %v", err)
	}
}

func TestPortOn(t *testing.T) {
	t.Run("first time: the password is printed", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/servers", 200, map[string]any{"servers": []any{}})
		d.on("PUT /api/flashback", 200, map[string]any{"enabled": true, "host": "127.0.0.1", "port": "3309", "has_password": true, "password": "pw-shown-once"})
		var out bytes.Buffer
		if err := runPortSet(context.Background(), c, true, &out); err != nil {
			t.Fatal(err)
		}
		if got := d.bodies["PUT /api/flashback"]; got["enabled"] != true || len(got) != 1 {
			t.Errorf("body = %v", got)
		}
		for _, s := range []string{"mysql -h 127.0.0.1 -P 3309 -u <server name> -p", "pw-shown-once", "Engine Routing is off"} {
			if !strings.Contains(out.String(), s) {
				t.Errorf("output lacks %q:\n%s", s, out.String())
			}
		}
	})
	t.Run("already had a password: says which, and how to get another", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/servers", 200, map[string]any{"servers": []any{}})
		d.on("PUT /api/flashback", 200, map[string]any{"enabled": true, "host": "127.0.0.1", "port": "3309", "has_password": true, "password_created_at": "2026-11-01T00:00:00Z"})
		var out bytes.Buffer
		if err := runPortSet(context.Background(), c, true, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "2026-11-01") || !strings.Contains(out.String(), "port new-password") {
			t.Errorf("output:\n%s", out.String())
		}
	})
	t.Run("on with no password at all is an error", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/servers", 200, map[string]any{"servers": []any{}})
		d.on("PUT /api/flashback", 200, map[string]any{"enabled": true})
		if err := runPortSet(context.Background(), c, true, io.Discard); err == nil || !strings.Contains(err.Error(), "new-password") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("saved as on and not answering is an error that says why", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/servers", 200, map[string]any{"servers": []any{}})
		d.on("PUT /api/flashback", 200, map[string]any{"enabled": true, "has_password": true, "password": "pw", "error": "the router is not running"})
		var out bytes.Buffer
		err := runPortSet(context.Background(), c, true, &out)
		if err == nil || !strings.Contains(err.Error(), "router is not running") {
			t.Fatalf("err = %v", err)
		}
		// The password was made and is shown once: it must not be lost
		// because the port did not come up.
		if !strings.Contains(out.String(), "pw") {
			t.Errorf("the password was dropped:\n%s", out.String())
		}
	})
	t.Run("the daemon took it and the port is still off", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/servers", 200, map[string]any{"servers": []any{}})
		d.on("PUT /api/flashback", 200, map[string]any{"enabled": false})
		if err := runPortSet(context.Background(), c, true, io.Discard); err == nil || !strings.Contains(err.Error(), "still off") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("an answer that is not about the port", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/servers", 200, map[string]any{"servers": []any{}})
		d.on("PUT /api/flashback", 200, map[string]any{})
		if err := runPortSet(context.Background(), c, true, io.Discard); err == nil {
			t.Fatal("no error")
		}
	})
	t.Run("a port this DBTrail does not manage is refused in its own words", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/servers", 200, map[string]any{"servers": []any{}})
		d.on("PUT /api/flashback", 409, map[string]any{"error": "the MySQL port is set on the command line"})
		if err := runPortSet(context.Background(), c, true, io.Discard); err == nil || !strings.Contains(err.Error(), "command line") {
			t.Fatalf("err = %v", err)
		}
	})
}

// With one server to connect as, the mysql line carries its name; with
// several, none, or a list that does not load, a placeholder.
func TestPortOnNamesTheOnlyServer(t *testing.T) {
	on := map[string]any{"enabled": true, "host": "127.0.0.1", "port": "3309", "has_password": true, "password": "pw"}
	for _, c := range []struct {
		name    string
		servers any
		status  int
		want    string
	}{
		{"one", []map[string]any{{"id": "a", "name": "shop", "has_source": true}, {"id": "b", "name": "old-index"}}, 200, "-u shop -p"},
		{"two", []map[string]any{{"id": "a", "name": "shop", "has_source": true}, {"id": "b", "name": "billing", "has_source": true}}, 200, "-u <server name> -p"},
		{"none", []map[string]any{}, 200, "-u <server name> -p"},
		{"a name the shell would split", []map[string]any{{"id": "a", "name": "my shop", "has_source": true}}, 200, "-u <server name> -p"},
		{"the list does not load", nil, 500, "-u <server name> -p"},
	} {
		d, cl := newFakeDaemon(t)
		d.on("PUT /api/flashback", 200, on)
		d.on("GET /api/servers", c.status, map[string]any{"servers": c.servers, "error": "boom"})
		var out bytes.Buffer
		if err := runPortSet(context.Background(), cl, true, &out); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !strings.Contains(out.String(), c.want) || !strings.Contains(out.String(), "pw") {
			t.Errorf("%s: output lacks %q:\n%s", c.name, c.want, out.String())
		}
	}
}

func TestPortOffStatusPasswordRouting(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("GET /api/servers", 200, map[string]any{"servers": []any{}})
	d.on("PUT /api/flashback", 200, map[string]any{"enabled": false})
	var out bytes.Buffer
	if err := runPortSet(context.Background(), c, false, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "is off") || strings.Contains(out.String(), "Password") {
		t.Errorf("off:\n%s", out.String())
	}

	d.on("GET /api/flashback", 200, map[string]any{"enabled": true, "host": "0.0.0.0", "port": "3310", "routing_on": true, "notice": "A proxy in front needs a monitor login."})
	out.Reset()
	if err := runPortStatus(context.Background(), c, &out); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"-h 0.0.0.0 -P 3310", "Engine Routing is on", "monitor login"} {
		if !strings.Contains(out.String(), s) {
			t.Errorf("status lacks %q:\n%s", s, out.String())
		}
	}

	d.on("POST /api/flashback/password", 200, map[string]any{"enabled": true, "password": "new-pw"})
	out.Reset()
	if err := runPortNewPassword(context.Background(), c, &out); err != nil || !strings.Contains(out.String(), "new-pw") {
		t.Fatalf("new-password: %v\n%s", err, out.String())
	}
	d.on("POST /api/flashback/password", 200, map[string]any{"enabled": true})
	if err := runPortNewPassword(context.Background(), c, io.Discard); err == nil {
		t.Fatal("a new-password answer with no password passed as done")
	}

	d.on("PUT /api/flashback", 200, map[string]any{"enabled": false, "routing_on": true})
	out.Reset()
	if err := runPortRouting(context.Background(), c, true, &out); err != nil {
		t.Fatal(err)
	}
	if got := d.bodies["PUT /api/flashback"]; got["routing"] != true || len(got) != 1 {
		t.Errorf("routing body = %v: it must not touch whether the port is on", got)
	}
	if !strings.Contains(out.String(), "The MySQL port is off") {
		t.Errorf("routing on with the port off does not say so:\n%s", out.String())
	}
	if err := runPortRouting(context.Background(), c, false, io.Discard); err == nil || !strings.Contains(err.Error(), "still on") {
		t.Fatalf("routing off while the daemon says on: err = %v", err)
	}
}

// After `server add`, the next step is a command, not the web interface.
func TestServerAddPointsAtTheNextCommand(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/identify", 200, map[string]any{"flavor": "mysql"})
	d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "shop", "doctor": passReport()})
	var out bytes.Buffer
	if err := runServerAdd(context.Background(), c, addInput{Host: "db1", Port: "3306", User: "u", Password: "p"}, false, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "bintrail-console snapshot run shop") || strings.Contains(out.String(), "web interface") {
		t.Errorf("output:\n%s", out.String())
	}
}
