package consoleapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/console"
)

// fakeAsker answers for the person at the terminal. A question with no
// prepared answer fails the test: a command must not ask what it was told.
type fakeAsker struct {
	t       *testing.T
	tty     bool
	lines   map[string]string // question -> answer ("" = take the default)
	secret  string
	asked   []string
	secrets int
}

func (a *fakeAsker) Interactive() bool { return a.tty }

func (a *fakeAsker) Line(q, def string) (string, error) {
	a.asked = append(a.asked, q)
	ans, ok := a.lines[q]
	if !ok {
		a.t.Fatalf("asked %q, which the test did not expect", q)
	}
	if ans == "" {
		return def, nil
	}
	return ans, nil
}

func (a *fakeAsker) Secret(q string) (string, error) {
	a.secrets++
	return a.secret, nil
}

func writeCnf(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "my.cnf")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestResolveAddInput(t *testing.T) {
	full := "[client]\nhost=cnfhost\nport=3310\nuser=cnfuser\npassword=cnfpass\n"
	cases := []struct {
		name    string
		flags   addFlags
		cnf     string
		ask     fakeAsker
		stdin   string
		want    addInput
		wantErr string
		secrets int
	}{
		{
			name:  "everything on the command line, password asked",
			flags: addFlags{Host: " db1 ", Port: "3307", User: "u", AskPassword: true},
			ask:   fakeAsker{tty: true, secret: "pw"},
			want:  addInput{Host: "db1", Port: "3307", User: "u", Password: "pw"}, secrets: 1,
		},
		{
			name:  "password is asked without -p when nothing else gives it",
			flags: addFlags{Host: "db1", User: "u"},
			ask:   fakeAsker{tty: true, secret: "pw"},
			want:  addInput{Host: "db1", Port: "3306", User: "u", Password: "pw"}, secrets: 1,
		},
		{
			name:  "the option file alone",
			flags: addFlags{},
			cnf:   full,
			want:  addInput{Host: "cnfhost", Port: "3310", User: "cnfuser", Password: "cnfpass"},
		},
		{
			name:  "the command line wins over the option file",
			flags: addFlags{Host: "flaghost", User: "flaguser"},
			cnf:   full,
			want:  addInput{Host: "flaghost", Port: "3310", User: "flaguser", Password: "cnfpass"},
		},
		{
			name:  "-p means not the file's password",
			flags: addFlags{AskPassword: true},
			cnf:   full,
			ask:   fakeAsker{tty: true, secret: "typed"},
			want:  addInput{Host: "cnfhost", Port: "3310", User: "cnfuser", Password: "typed"}, secrets: 1,
		},
		{
			name:  "[dbtrail] wins over [client]",
			flags: addFlags{},
			cnf:   "[client]\nhost=h\nuser=root\npassword=rootpw\n[dbtrail]\nuser=dbtrail\npassword=dpw\n",
			want:  addInput{Host: "h", Port: "3306", User: "dbtrail", Password: "dpw"},
		},
		{
			name:  "password from stdin, one line, its newline dropped, spaces kept",
			flags: addFlags{Host: "db1", User: "u", PasswordStdin: true},
			stdin: " p w \r\nsecond line\n",
			want:  addInput{Host: "db1", Port: "3306", User: "u", Password: " p w "},
		},
		{
			name:  "password from stdin with no newline at the end",
			flags: addFlags{Host: "db1", User: "u", PasswordStdin: true},
			stdin: "pw",
			want:  addInput{Host: "db1", Port: "3306", User: "u", Password: "pw"},
		},
		{
			name:  "on a terminal, what is missing is asked, with defaults",
			flags: addFlags{},
			ask:   fakeAsker{tty: true, lines: map[string]string{"Host": "db9", "Port": "", "User": ""}, secret: "pw"},
			want:  addInput{Host: "db9", Port: "3306", User: "dbtrail", Password: "pw"}, secrets: 1,
		},
		{
			name:  "a host given means the port is not asked",
			flags: addFlags{Host: "db1"},
			ask:   fakeAsker{tty: true, lines: map[string]string{"User": "u"}, secret: "pw"},
			want:  addInput{Host: "db1", Port: "3306", User: "u", Password: "pw"}, secrets: 1,
		},
		{
			name:  "name, schemas and flavor pass through, trimmed and lowered",
			flags: addFlags{Host: "db1", User: "u", Name: " shop ", Schemas: " a,b ", Flavor: " MariaDB ", PasswordStdin: true},
			stdin: "pw\n",
			want:  addInput{Host: "db1", Port: "3306", User: "u", Password: "pw", Name: "shop", Schemas: "a,b", Flavor: "mariadb"},
		},

		{
			name:  "an IPv6 address in brackets loses them",
			flags: addFlags{Host: "[2001:db8::1]", User: "u", PasswordStdin: true}, stdin: "pw\n",
			want: addInput{Host: "2001:db8::1", Port: "3306", User: "u", Password: "pw"},
		},
		{
			name:  "the option file's include lines are counted",
			flags: addFlags{}, cnf: "!includedir /etc/mysql/conf.d/\n" + full,
			want: addInput{Host: "cnfhost", Port: "3310", User: "cnfuser", Password: "cnfpass", IncludesSkipped: 1},
		},
		{name: "host and port in one word", flags: addFlags{Host: "db1:3307", User: "u", PasswordStdin: true}, stdin: "pw\n", wantErr: "-P"},
		{name: "a URL as the host", flags: addFlags{Host: "mysql://db1", User: "u", PasswordStdin: true}, stdin: "pw\n", wantErr: "not a host"},
		{name: "user@host as the host", flags: addFlags{Host: "root@db1", User: "u", PasswordStdin: true}, stdin: "pw\n", wantErr: "not a host"},
		{
			name:  "the option file on stdin",
			flags: addFlags{DefaultsFile: "-"}, stdin: full,
			want: addInput{Host: "cnfhost", Port: "3310", User: "cnfuser", Password: "cnfpass"},
		},
		{
			name:  "the option file on stdin, the command line still wins",
			flags: addFlags{DefaultsFile: "-", Host: "flaghost"}, stdin: full,
			want: addInput{Host: "flaghost", Port: "3310", User: "cnfuser", Password: "cnfpass"},
		},
		// Stdin is spent on the file: nothing may be asked for, even on a terminal.
		{name: "option file on stdin with no password never asks", flags: addFlags{DefaultsFile: "-"}, ask: fakeAsker{tty: true}, stdin: "[client]\nhost=h\nuser=u\n", wantErr: "add a password line"},
		{name: "option file on stdin with no host never asks", flags: addFlags{DefaultsFile: "-"}, ask: fakeAsker{tty: true}, stdin: "[client]\nuser=u\npassword=p\n", wantErr: "-h"},
		{name: "option file on stdin and -p", flags: addFlags{DefaultsFile: "-", AskPassword: true}, ask: fakeAsker{tty: true}, stdin: full, wantErr: "add a password line"},
		{name: "option file on stdin and --password-stdin", flags: addFlags{DefaultsFile: "-", PasswordStdin: true}, stdin: full, wantErr: "both read stdin"},
		{name: "empty stdin as the option file", flags: addFlags{DefaultsFile: "-"}, stdin: "", wantErr: "-h"},
		{name: "no host, not a terminal", flags: addFlags{User: "u"}, wantErr: "-h"},
		{name: "no user, not a terminal", flags: addFlags{Host: "db1"}, wantErr: "-u"},
		{name: "no password, not a terminal", flags: addFlags{Host: "db1", User: "u"}, wantErr: "--password-stdin"},
		{name: "-p, not a terminal", flags: addFlags{Host: "db1", User: "u", AskPassword: true}, wantErr: "not a terminal"},
		{name: "empty password typed", flags: addFlags{Host: "db1", User: "u"}, ask: fakeAsker{tty: true}, wantErr: "empty", secrets: 1},
		{name: "empty stdin", flags: addFlags{Host: "db1", User: "u", PasswordStdin: true}, wantErr: "stdin"},
		{name: "stdin is only a newline", flags: addFlags{Host: "db1", User: "u", PasswordStdin: true}, stdin: "\n", wantErr: "empty"},
		{name: "stdin password never asks for the rest", flags: addFlags{PasswordStdin: true}, ask: fakeAsker{tty: true}, stdin: "pw\n", wantErr: "-h"},
		{name: "-p and --password-stdin", flags: addFlags{Host: "db1", User: "u", AskPassword: true, PasswordStdin: true}, wantErr: "use one"},
		{name: "port is not a number", flags: addFlags{Host: "db1", Port: "33o6", User: "u"}, wantErr: "1 to 65535"},
		{name: "port is zero", flags: addFlags{Host: "db1", Port: "0", User: "u"}, wantErr: "1 to 65535"},
		{name: "port is too big", flags: addFlags{Host: "db1", Port: "70000", User: "u"}, wantErr: "1 to 65535"},
		{name: "bad port in the option file", flags: addFlags{}, cnf: "[client]\nhost=h\nport=abc\nuser=u\npassword=p\n", wantErr: "1 to 65535"},
		{name: "postgres", flags: addFlags{Host: "db1", User: "u", Flavor: "postgres"}, wantErr: "mysql or mariadb"},
		{name: "option file with a socket and no host", flags: addFlags{}, cnf: "[client]\nsocket=/tmp/mysql.sock\nuser=u\npassword=p\n", wantErr: "socket"},
		{name: "host typed as nothing", flags: addFlags{}, ask: fakeAsker{tty: true, lines: map[string]string{"Host": ""}}, wantErr: "no host"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := c.flags
			if c.cnf != "" && f.DefaultsFile == "" {
				f.DefaultsFile = writeCnf(t, c.cnf)
			}
			ask := c.ask
			ask.t = t
			got, err := resolveAddInput(f, &ask, strings.NewReader(c.stdin))
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("got %+v, %v; want an error with %q", got, err, c.wantErr)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				// The forwarding account has its own test
				// (TestResolveForward); this one is about the capture login.
				got.FwdAsk, got.ask = false, nil
				if got != c.want {
					t.Fatalf("got %+v\nwant %+v", got, c.want)
				}
			}
			if ask.secrets != c.secrets {
				t.Errorf("asked for the password %d times, want %d", ask.secrets, c.secrets)
			}
		})
	}
}

func TestResolveAddInputMissingOptionFile(t *testing.T) {
	_, err := resolveAddInput(addFlags{DefaultsFile: "/no/such/my.cnf"}, &fakeAsker{t: t}, strings.NewReader(""))
	if err == nil || !strings.Contains(err.Error(), "/no/such/my.cnf") {
		t.Fatalf("got %v, want an error naming the file", err)
	}
}

// fakeDaemon stands in for the running DBTrail: it checks the token and
// records what it was asked.
type fakeDaemon struct {
	t        *testing.T
	token    string
	handlers map[string]func(body map[string]any) (int, any)
	calls    []string
	bodies   map[string]map[string]any
}

func newFakeDaemon(t *testing.T) (*fakeDaemon, *ctlClient) {
	d := &fakeDaemon{t: t, token: "tok", handlers: map[string]func(map[string]any) (int, any){}, bodies: map[string]map[string]any{}}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+d.token {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized: missing or invalid token"})
			return
		}
		key := r.Method + " " + r.URL.Path
		d.calls = append(d.calls, key)
		var body map[string]any
		if raw, _ := io.ReadAll(r.Body); len(raw) > 0 {
			if r.Header.Get("Content-Type") != "application/json" {
				t.Errorf("%s: a body with Content-Type %q", key, r.Header.Get("Content-Type"))
			}
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Errorf("%s: body is not JSON: %v", key, err)
			}
		}
		d.bodies[key] = body
		h, ok := d.handlers[key]
		if !ok {
			t.Errorf("unexpected call %s", key)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		status, out := h(body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if out != nil {
			_ = json.NewEncoder(w).Encode(out)
		}
	}))
	t.Cleanup(ts.Close)
	return d, &ctlClient{base: ts.URL, token: d.token, hc: ts.Client()}
}

func (d *fakeDaemon) on(key string, status int, out any) {
	d.handlers[key] = func(map[string]any) (int, any) { return status, out }
}

func passReport() map[string]any {
	return map[string]any{"passed": 2, "failed": 0, "checks": []map[string]any{
		{"name": "Connects to the database", "status": "pass"},
		{"name": "binlog_format is ROW", "status": "pass"},
	}}
}

func TestServerAddSuccess(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/identify", 200, map[string]any{"addr": "db1:3306", "flavor": "mariadb", "version": "11.4.2"})
	d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "shop", "started": true, "doctor": passReport()})
	var out bytes.Buffer
	in := addInput{Host: "db1", Port: "3306", User: "dbtrail", Password: "s3cret#1", Name: "shop", Schemas: "a,b"}
	if err := runServerAdd(context.Background(), c, in, false, &out); err != nil {
		t.Fatalf("err = %v\n%s", err, out.String())
	}
	got := d.bodies["POST /api/servers/check"]
	want := map[string]any{
		"flavor": "mariadb", "name": "shop", "source_host": "db1", "source_port": "3306",
		"source_user": "dbtrail", "source_password": "s3cret#1", "schemas": "a,b",
	}
	if got["no_draft"] != true {
		t.Errorf("check body lacks no_draft: the Connect screen's saved form would be replaced: %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("check body %s = %v, want %v", k, got[k], v)
		}
	}
	for _, s := range []string{"Checking db1:3306 as dbtrail", "ok    Connects to the database", `Added "shop"`} {
		if !strings.Contains(out.String(), s) {
			t.Errorf("output lacks %q:\n%s", s, out.String())
		}
	}
	if strings.Contains(out.String(), "s3cret") {
		t.Errorf("the password was printed:\n%s", out.String())
	}
}

// --flavor overrides what the server is taken for, not the look at the
// address: the loopback-in-a-container fix must still happen.
func TestServerAddFlavorGivenWinsOverIdentify(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/identify", 200, map[string]any{"kind": "loopback_in_container", "suggest": "host.docker.internal:3306", "flavor": "mysql"})
	d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "db1", "doctor": passReport()})
	in := addInput{Host: "127.0.0.1", Port: "3306", User: "u", Password: "p", Flavor: "mariadb"}
	if err := runServerAdd(context.Background(), c, in, false, io.Discard); err != nil {
		t.Fatal(err)
	}
	body := d.bodies["POST /api/servers/check"]
	if body["flavor"] != "mariadb" || body["source_host"] != "host.docker.internal" {
		t.Errorf("check body = %v", body)
	}
}

// Anything but "that is not an address" from the first call ends the command:
// going on would bury the real reason under the checks.
func TestServerAddIdentifyFailureStops(t *testing.T) {
	for _, status := range []int{403, 500} {
		d, c := newFakeDaemon(t)
		d.on("POST /api/servers/identify", status, map[string]any{"error": "this console is read-only"})
		err := runServerAdd(context.Background(), c, addInput{Host: "db1", Port: "3306", User: "u", Password: "p"}, false, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Errorf("status %d: err = %v", status, err)
		}
		if len(d.calls) != 1 {
			t.Errorf("status %d: went on to %v", status, d.calls)
		}
	}
}

func TestServerAddUnknownFlavorIsMySQL(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/identify", 200, map[string]any{"addr": "db1:3306", "kind": "unreachable"})
	d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "db1", "doctor": passReport()})
	if err := runServerAdd(context.Background(), c, addInput{Host: "db1", Port: "3306", User: "u", Password: "p"}, false, io.Discard); err != nil {
		t.Fatal(err)
	}
	if f := d.bodies["POST /api/servers/check"]["flavor"]; f != "mysql" {
		t.Errorf("flavor = %v, want mysql", f)
	}
}

// A 127.0.0.1 typed for a DBTrail in a container: the daemon says where a
// database does answer, and the command uses it and says so.
func TestServerAddLoopbackInContainerUsesTheSuggestion(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/identify", 200, map[string]any{"addr": "127.0.0.1:3306", "kind": "loopback_in_container", "suggest": "host.docker.internal:3306", "flavor": "mysql"})
	d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "x", "doctor": passReport()})
	var out bytes.Buffer
	if err := runServerAdd(context.Background(), c, addInput{Host: "127.0.0.1", Port: "3306", User: "u", Password: "p"}, false, &out); err != nil {
		t.Fatal(err)
	}
	if h := d.bodies["POST /api/servers/check"]["source_host"]; h != "host.docker.internal" {
		t.Errorf("source_host = %v, want host.docker.internal", h)
	}
	if !strings.Contains(out.String(), "host.docker.internal:3306") {
		t.Errorf("the change of address was not said:\n%s", out.String())
	}
}

// An identify that the daemon refuses for a reason of its own (a bad
// address) must not stop the checks, which explain it better.
func TestServerAddIdentifyRefusalGoesOnToTheChecks(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/identify", 400, map[string]any{"error": "bad address"})
	d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "x", "doctor": passReport()})
	if err := runServerAdd(context.Background(), c, addInput{Host: "db1", Port: "3306", User: "u", Password: "p"}, false, io.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestServerAddFailedChecks(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/identify", 200, map[string]any{"flavor": "mysql"})
	d.on("POST /api/servers/check", 200, map[string]any{"ok": false, "name": "db1", "doctor": map[string]any{
		"passed": 1, "failed": 2, "checks": []map[string]any{
			{"name": "Connects to the database", "status": "pass"},
			{"name": "binlog_format", "status": "fail", "detail": "binlog_format is MIXED", "remediation": "Run:\n  SET PERSIST binlog_format = ROW;"},
			{"name": "Every table has a key", "status": "fail", "detail": "2 tables have none", "statements": []string{"ALTER TABLE a.t ADD PRIMARY KEY (id);", "ALTER TABLE a.u ADD PRIMARY KEY (id);"}},
			{"name": "GTID", "status": "warn", "detail": "gtid_mode is OFF"},
		}}})
	var out bytes.Buffer
	err := runServerAdd(context.Background(), c, addInput{Host: "db1", Port: "3306", User: "u", Password: "p"}, false, &out)
	if !errors.Is(err, errNotAdded) {
		t.Fatalf("err = %v, want errNotAdded", err)
	}
	for _, s := range []string{
		"FAIL  binlog_format", "binlog_format is MIXED", "SET PERSIST binlog_format = ROW;",
		"ALTER TABLE a.u ADD PRIMARY KEY (id);", "warn  GTID", "gtid_mode is OFF",
	} {
		if !strings.Contains(out.String(), s) {
			t.Errorf("output lacks %q:\n%s", s, out.String())
		}
	}
	if strings.Contains(out.String(), "Added") {
		t.Errorf("a failed add says Added:\n%s", out.String())
	}
	if want := "the database was not added: 2 checks failed. Fix them and run the command again"; err.Error() != want {
		t.Errorf("err = %q\nwant  %q", err, want)
	}
}

// A warning about something optional keeps to one line, as the web interface
// folds it; -v prints the rest. A warning that is not optional always prints.
func TestServerAddFoldsOptionalWarnings(t *testing.T) {
	report := map[string]any{"passed": 1, "failed": 0, "checks": []map[string]any{
		{"name": "Connects", "status": "pass"},
		{"name": "Statement capture", "status": "warn", "optional": true, "detail": "binlog_rows_query_log_events=OFF", "remediation": "SET PERSIST binlog_rows_query_log_events = ON;"},
		{"name": "Full read lock permissions", "status": "warn", "detail": "missing: PROCESS", "remediation": "GRANT PROCESS ON *.* TO `dbtrail`@`%`;"},
	}}
	for _, verbose := range []bool{false, true} {
		d, c := newFakeDaemon(t)
		d.on("POST /api/servers/identify", 200, map[string]any{"flavor": "mysql"})
		d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "x", "doctor": report})
		var out bytes.Buffer
		if err := runServerAdd(context.Background(), c, addInput{Host: "db1", Port: "3306", User: "u", Password: "p", Flavor: "mysql"}, verbose, &out); err != nil {
			t.Fatal(err)
		}
		o := out.String()
		if !strings.Contains(o, "tip   Statement capture") || !strings.Contains(o, "warn  Full read lock permissions") || !strings.Contains(o, "GRANT PROCESS") {
			t.Errorf("verbose=%v:\n%s", verbose, o)
		}
		if got := strings.Contains(o, "SET PERSIST binlog_rows_query_log_events"); got != verbose {
			t.Errorf("verbose=%v: optional fix printed = %v:\n%s", verbose, got, o)
		}
		if got := strings.Contains(o, "-v prints"); got == verbose {
			t.Errorf("verbose=%v: the pointer to -v printed = %v:\n%s", verbose, got, o)
		}
	}
}

func TestServerAddOneFailedCheckIsSingular(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/identify", 200, map[string]any{"flavor": "mysql"})
	d.on("POST /api/servers/check", 200, map[string]any{"ok": false, "doctor": map[string]any{
		"failed": 1, "checks": []map[string]any{{"name": "Login", "status": "fail", "detail": "access denied"}}}})
	err := runServerAdd(context.Background(), c, addInput{Host: "db1", Port: "3306", User: "u", Password: "p"}, false, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "1 check failed. Fix it and") {
		t.Errorf("err = %v", err)
	}
}

func TestServerAddChecksPassedAndCaptureDidNotStart(t *testing.T) {
	for _, kept := range []bool{false, true} {
		d, c := newFakeDaemon(t)
		d.on("POST /api/servers/identify", 200, map[string]any{"flavor": "mysql"})
		d.on("POST /api/servers/check", 200, map[string]any{"ok": false, "name": "db1", "doctor": passReport(), "error": "index database could not be created", "kept": kept})
		err := runServerAdd(context.Background(), c, addInput{Host: "db1", Port: "3306", User: "u", Password: "p"}, false, io.Discard)
		// Whether anything was left behind is the whole point of the two
		// sentinels: a script must be able to tell them apart.
		want, other := errNotAdded, errAddedStopped
		if kept {
			want, other = errAddedStopped, errNotAdded
		}
		if !errors.Is(err, want) || errors.Is(err, other) {
			t.Fatalf("kept=%v: err = %v", kept, err)
		}
		if !strings.Contains(err.Error(), "index database could not be created") {
			t.Errorf("kept=%v: the reason is missing: %v", kept, err)
		}
	}
}

func TestServerAddAnswerThatSaysNothingIsAnError(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/identify", 200, map[string]any{"flavor": "mysql"})
	d.on("POST /api/servers/check", 200, map[string]any{})
	if err := runServerAdd(context.Background(), c, addInput{Host: "db1", Port: "3306", User: "u", Password: "p"}, false, io.Discard); err == nil {
		t.Fatal("an answer with neither ok nor a reason passed as success")
	}
}

func TestServerAddDaemonRefusal(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/identify", 200, map[string]any{"flavor": "mysql"})
	d.on("POST /api/servers/check", 409, map[string]any{"error": `a server named "shop" already exists`})
	err := runServerAdd(context.Background(), c, addInput{Host: "db1", Port: "3306", User: "u", Password: "p", Name: "shop"}, false, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("err = %v, want the daemon's own sentence", err)
	}
}

func TestStaleTokenIsExplained(t *testing.T) {
	_, c := newFakeDaemon(t)
	c.token = "from-a-previous-run"
	c.accessFile = "/var/lib/bintrail/console-cli.json"
	err := runServerList(context.Background(), c, "text", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "/var/lib/bintrail/console-cli.json") || !strings.Contains(err.Error(), "each time it starts") {
		t.Fatalf("err = %v", err)
	}
	// Also when the very first call of `add` is the one refused.
	err = runServerAdd(context.Background(), c, addInput{Host: "db1", Port: "3306", User: "u", Password: "p"}, false, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "did not accept the token") {
		t.Fatalf("add: err = %v", err)
	}
}

func TestNothingListening(t *testing.T) {
	c := &ctlClient{base: "http://127.0.0.1:1", token: "x", hc: &http.Client{}}
	err := runServerList(context.Background(), c, "text", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "Is DBTrail running?") {
		t.Fatalf("err = %v", err)
	}
}

func TestNotDBTrailAnswering(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "<html>nginx</html>")
	}))
	defer ts.Close()
	c := &ctlClient{base: ts.URL, token: "x", hc: ts.Client()}
	err := runServerList(context.Background(), c, "text", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "502") || strings.Contains(err.Error(), "nginx") {
		t.Fatalf("err = %v", err)
	}
}

func twoServers(d *fakeDaemon) {
	d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{
		{"id": "boot", "name": "boot", "kind": "ephemeral"},
		{"id": "id1", "name": "shop", "kind": "registry", "has_source": true, "source_host": "db1", "source_port": "3306", "source_user": "dbtrail", "monitor_state": "running", "has_route": true, "route_user": "shop_fwd"},
		{"id": "id2", "name": "Billing", "kind": "registry", "has_source": true, "source_host": "2001:db8::1", "source_port": "3307", "source_user": "dbtrail"},
		{"id": "id3", "name": "old-index", "kind": "registry", "has_source": false},
	}})
}

func TestServerList(t *testing.T) {
	d, c := newFakeDaemon(t)
	twoServers(d)
	var out bytes.Buffer
	if err := runServerList(context.Background(), c, "text", &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	want := []string{
		"NAME       ADDRESS             USER     RECORDING            FORWARDS AS  MODE",
		"boot       -                   -        set at startup       -            fast",
		"shop       db1:3306            dbtrail  running              shop_fwd     fast (default)",
		"Billing    [2001:db8::1]:3307  dbtrail  not by this DBTrail  -            fast (default)",
		"old-index  -                   -        -                    -            -",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Errorf("got:\n%s\nwant:\n%s", out.String(), strings.Join(want, "\n"))
	}
}

func TestServerListEmptyAndJSON(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("GET /api/servers", 200, map[string]any{"servers": []any{}})
	var out bytes.Buffer
	if err := runServerList(context.Background(), c, "text", &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "No servers yet") || !strings.Contains(out.String(), "server add") {
		t.Errorf("empty list:\n%s", out.String())
	}
	out.Reset()
	if err := runServerList(context.Background(), c, "json", &out); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out.String()) != "[]" {
		t.Errorf("empty json = %q, want []", out.String())
	}

	twoServers(d)
	out.Reset()
	if err := runServerList(context.Background(), c, "json", &out); err != nil {
		t.Fatal(err)
	}
	var got []ctlServer
	if err := json.Unmarshal(out.Bytes(), &got); err != nil || len(got) != 4 || got[1].MonitorState != "running" {
		t.Errorf("json = %s (%v)", out.String(), err)
	}
}

// A 200 that carries no list is not "no servers": it is not DBTrail.
func TestServerListAnswerWithoutAList(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("GET /api/servers", 200, map[string]any{})
	var out bytes.Buffer
	err := runServerList(context.Background(), c, "text", &out)
	if err == nil || strings.Contains(out.String(), "No servers yet") {
		t.Fatalf("err = %v, out = %q", err, out.String())
	}
}

// A redirect is not followed: the POST would become a GET of somewhere else.
func TestRedirectIsNotFollowed(t *testing.T) {
	hits := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	defer ts.Close()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("BINTRAIL_CONSOLE_AUTH", filepath.Join(t.TempDir(), "console-auth.yaml"))
	t.Setenv("BINTRAIL_CONSOLE_TOKEN", "tok")
	if _, err := os.Stat(packagedStateDir + "/" + console.LocalAccessFileName); err == nil {
		t.Skip("this machine runs a packaged DBTrail")
	}
	c, err := resolveCtl(ts.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := runServerList(context.Background(), c, "text", io.Discard); err == nil || !strings.Contains(err.Error(), "302") {
		t.Fatalf("err = %v", err)
	}
	if hits != 1 {
		t.Errorf("made %d requests, want 1", hits)
	}
}

func TestPasswordOnCommandLine(t *testing.T) {
	for _, c := range []struct {
		args string
		want bool
	}{
		{"server add -h db1 -u dbtrail -p", false},
		{"server add -h db1 -u dbtrail --password", false},
		{"server add -vp -h db1", false},
		{"server add -hpepe -upaul -P3306", false}, // a p inside another option's value
		{"server add --defaults-file /p/my.cnf", false},
		{"server add -h db1 -- -pX", false},
		{"server add -h db1 -u dbtrail -phunter2", true}, // would parse as -p -h unter2
		{"server add -pPass", true},                      // would parse as -p -P ass
		{"server add -puser1", true},                     // would parse as -p -u ser1
		{"server add -pSECRET", true},
		{"server add -pv", true},
		{"server add -vpX", true},
		{"server add --password=SECRET", true},
		{"server add --password=", true},
		{"server add --password-stdin=SECRET", true},
		{"server add -h db1 --password-stdin", false},
	} {
		if got := passwordOnCommandLine(strings.Fields(c.args)); got != c.want {
			t.Errorf("%q: got %v, want %v", c.args, got, c.want)
		}
	}
	if strings.Contains(noPasswordOnCommandLine, "%") {
		t.Error("the refusal must be a fixed sentence: it may never repeat what was typed")
	}
}

// What the parser would have done with `-phunter2` had it not been caught:
// this is the reason passwordOnCommandLine runs before the values are used.
func TestAttachedPasswordWouldBecomeAHost(t *testing.T) {
	fs := serverAddCmd.Flags()
	t.Cleanup(func() { srvAdd = addFlags{} })
	if err := fs.Parse([]string{"-h", "db1", "-phunter2"}); err != nil {
		t.Skipf("the parser now refuses it by itself: %v", err)
	}
	if srvAdd.Host != "unter2" {
		t.Skipf("the parser reads it differently now (host %q)", srvAdd.Host)
	}
	if !passwordOnCommandLine([]string{"-h", "db1", "-phunter2"}) {
		t.Fatal("a piece of a password would be used as the host")
	}
}

func TestFindServer(t *testing.T) {
	servers := []ctlServer{{ID: "id1", Name: "shop"}, {ID: "id2", Name: "Shop"}, {ID: "id3", Name: "Billing"}}
	for _, c := range []struct{ name, wantID, wantErr string }{
		{"shop", "id1", ""},
		{"Shop", "id2", ""},
		{"id3", "id3", ""},
		{"billing", "id3", ""},
		{"SHOP", "", "capital letters"},
		{"nope", "", "shop, Shop, Billing"},
		{"", "", "there is no server"},
	} {
		got, err := findServer(servers, c.name)
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("%q: got %+v, %v; want error with %q", c.name, got, err, c.wantErr)
			}
			continue
		}
		if err != nil || got.ID != c.wantID {
			t.Errorf("%q: got %+v, %v; want %s", c.name, got, err, c.wantID)
		}
	}
	if _, err := findServer(nil, "x"); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("empty list: %v", err)
	}
}

func TestServerRemove(t *testing.T) {
	t.Run("yes stops then deletes", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id1/monitor/stop", 200, map[string]any{})
		d.on("DELETE /api/servers/id1", 204, nil)
		var out bytes.Buffer
		if err := runServerRemove(context.Background(), c, "shop", true, &fakeAsker{t: t}, &out); err != nil {
			t.Fatal(err)
		}
		n := len(d.calls)
		if n < 2 || d.calls[n-2] != "POST /api/servers/id1/monitor/stop" || d.calls[n-1] != "DELETE /api/servers/id1" {
			t.Errorf("calls = %v", d.calls)
		}
		if !strings.Contains(out.String(), `Removed "shop"`) {
			t.Errorf("output: %s", out.String())
		}
	})
	t.Run("a server with no source is deleted without a stop", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("DELETE /api/servers/id3", 204, nil)
		if err := runServerRemove(context.Background(), c, "old-index", true, &fakeAsker{t: t}, io.Discard); err != nil {
			t.Fatal(err)
		}
	})
	for _, answer := range []string{"", "n", "nope", "maybe"} {
		t.Run("answer "+answer+" changes nothing", func(t *testing.T) {
			d, c := newFakeDaemon(t)
			twoServers(d)
			ask := &fakeAsker{t: t, tty: true, lines: map[string]string{}}
			ask.lines[`Stop recording "shop" (db1:3306) and take it off the list? What was already recorded is kept. [y/N]`] = answer
			var out bytes.Buffer
			if err := runServerRemove(context.Background(), c, "shop", false, ask, &out); err != nil {
				t.Fatal(err)
			}
			for _, call := range d.calls {
				if strings.HasPrefix(call, "DELETE") || strings.HasSuffix(call, "/stop") {
					t.Errorf("made %s after the answer %q", call, answer)
				}
			}
			if !strings.Contains(out.String(), "Nothing was changed") {
				t.Errorf("output: %s", out.String())
			}
		})
	}
	t.Run("answer YES removes", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id1/monitor/stop", 200, map[string]any{})
		d.on("DELETE /api/servers/id1", 204, nil)
		ask := &fakeAsker{t: t, tty: true, lines: map[string]string{
			`Stop recording "shop" (db1:3306) and take it off the list? What was already recorded is kept. [y/N]`: "YES"}}
		if err := runServerRemove(context.Background(), c, "shop", false, ask, io.Discard); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("no terminal and no --yes is refused before anything", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		err := runServerRemove(context.Background(), c, "shop", false, &fakeAsker{t: t}, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "--yes") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a stop that fails removes nothing", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id1/monitor/stop", 500, map[string]any{"error": "stop monitoring: busy"})
		err := runServerRemove(context.Background(), c, "shop", true, &fakeAsker{t: t}, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "busy") || !strings.Contains(err.Error(), "was not removed") || !strings.Contains(err.Error(), "may now be marked as stopped") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a DBTrail that records nothing has nothing to stop: the delete goes on", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id2/monitor/stop", 403, map[string]any{"error": "this console is read-only"})
		d.on("DELETE /api/servers/id2", 204, nil)
		var out bytes.Buffer
		if err := runServerRemove(context.Background(), c, "Billing", true, &fakeAsker{t: t}, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), `Removed "Billing"`) {
			t.Errorf("output: %s", out.String())
		}
	})
	t.Run("the server DBTrail was started with is refused in its own words", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("DELETE /api/servers/boot", 409, map[string]any{"error": "the command-line server cannot be deleted; stop the console instead"})
		err := runServerRemove(context.Background(), c, "boot", true, &fakeAsker{t: t}, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "cannot be deleted") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a delete that fails after the stop says both", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("POST /api/servers/id1/monitor/stop", 200, map[string]any{})
		d.on("DELETE /api/servers/id1", 409, map[string]any{"error": "registry is locked"})
		err := runServerRemove(context.Background(), c, "shop", true, &fakeAsker{t: t}, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "was stopped") || !strings.Contains(err.Error(), "registry is locked") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unknown name", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		err := runServerRemove(context.Background(), c, "nope", true, &fakeAsker{t: t}, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "boot, shop, Billing, old-index") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestResolveCtl(t *testing.T) {
	write := func(t *testing.T, dir string, a console.LocalAccess) string {
		t.Helper()
		p := filepath.Join(dir, console.LocalAccessFileName)
		data, _ := json.Marshal(a)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	// Keep the machine's own files out of every case.
	isolate := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		t.Setenv("HOME", dir)
		t.Setenv("BINTRAIL_CONSOLE_AUTH", filepath.Join(dir, "state", "console-auth.yaml"))
		t.Setenv("BINTRAIL_CONSOLE_TOKEN", "")
		t.Setenv("BINTRAIL_CONSOLE_LISTEN", "")
		if err := os.MkdirAll(filepath.Join(dir, "state"), 0o700); err != nil {
			t.Fatal(err)
		}
		return filepath.Join(dir, "state")
	}
	if _, err := os.Stat(packagedStateDir + "/" + console.LocalAccessFileName); err == nil {
		t.Skip("this machine runs a packaged DBTrail; its file would answer for every case")
	}

	t.Run("the daemon's file, found beside the login file", func(t *testing.T) {
		state := isolate(t)
		p := write(t, state, console.LocalAccess{URL: "http://127.0.0.1:8090", Token: "filetok"})
		c, err := resolveCtl("", "")
		if err != nil {
			t.Fatal(err)
		}
		if c.base != "http://127.0.0.1:8090" || c.token != "filetok" || c.accessFile != p {
			t.Errorf("got %+v", c)
		}
	})
	t.Run("the file wins over the environment's token", func(t *testing.T) {
		state := isolate(t)
		write(t, state, console.LocalAccess{URL: "http://127.0.0.1:8090", Token: "filetok"})
		t.Setenv("BINTRAIL_CONSOLE_TOKEN", "envtok")
		c, err := resolveCtl("", "")
		if err != nil || c.token != "filetok" {
			t.Fatalf("got %+v, %v", c, err)
		}
		// --url naming that same DBTrail changes nothing.
		c, err = resolveCtl("http://127.0.0.1:8090/", "")
		if err != nil || c.token != "filetok" || c.base != "http://127.0.0.1:8090" {
			t.Fatalf("same address by --url: got %+v, %v", c, err)
		}
	})
	// The file's token opens the DBTrail of this machine, all of it. Sent to
	// the address of --url it would hand that to whoever answers there.
	t.Run("--url to another DBTrail never carries the file's token", func(t *testing.T) {
		state := isolate(t)
		write(t, state, console.LocalAccess{URL: "http://127.0.0.1:8090", Token: "filetok"})
		if c, err := resolveCtl("https://dbtrail.example.com/", ""); err == nil || !strings.Contains(err.Error(), "BINTRAIL_CONSOLE_TOKEN") {
			t.Fatalf("no token of its own: got %+v, %v", c, err)
		}
		t.Setenv("BINTRAIL_CONSOLE_TOKEN", "envtok")
		c, err := resolveCtl("https://dbtrail.example.com/", "")
		if err != nil || c.base != "https://dbtrail.example.com" || c.token != "envtok" || c.accessFile != "" {
			t.Fatalf("got %+v, %v", c, err)
		}
	})
	// The attack: a folder another user prepared, with a .bintrail.env that
	// points at an access file of theirs, or at a token and a port of
	// theirs. Every other command loads that file; these must not.
	t.Run("a .bintrail.env in the working directory decides nothing", func(t *testing.T) {
		state := isolate(t)
		evil := t.TempDir()
		write(t, evil, console.LocalAccess{URL: "http://127.0.0.1:9999", Token: "theirs"})
		env := "BINTRAIL_CONSOLE_AUTH=" + filepath.Join(evil, "console-auth.yaml") + "\n" +
			"BINTRAIL_CONSOLE_TOKEN=theirs\nBINTRAIL_CONSOLE_LISTEN=127.0.0.1:9999\n"
		// The victim's shell sets none of the three, which is what lets the
		// folder's file fill them in. (isolate set them; t.Setenv puts them
		// back after the test.)
		for _, k := range []string{"BINTRAIL_CONSOLE_AUTH", "BINTRAIL_CONSOLE_TOKEN", "BINTRAIL_CONSOLE_LISTEN"} {
			os.Unsetenv(k)
		}
		// Applied as the program applies .bintrail.env when it starts.
		t.Cleanup(cli.ApplyEnvFileForTest(env))
		if os.Getenv("BINTRAIL_CONSOLE_TOKEN") != "theirs" {
			t.Fatal("the folder's file was not applied, so nothing below is checked")
		}
		// With no DBTrail of the victim's own: nothing to reach, not theirs.
		if c, err := resolveCtl("", ""); err == nil {
			t.Fatalf("followed the folder's file to %+v", c)
		}
		// With one, where a DBTrail run by this account keeps it: the
		// victim's own, untouched.
		mine := filepath.Join(filepath.Dir(state), ".config", "bintrail")
		if err := os.MkdirAll(mine, 0o700); err != nil {
			t.Fatal(err)
		}
		write(t, mine, console.LocalAccess{URL: "http://127.0.0.1:8090", Token: "mine"})
		c, err := resolveCtl("", "")
		if err != nil || c.token != "mine" || c.base != "http://127.0.0.1:8090" {
			t.Fatalf("got %+v, %v", c, err)
		}
	})
	t.Run("no file: the environment's token and listen address", func(t *testing.T) {
		isolate(t)
		t.Setenv("BINTRAIL_CONSOLE_TOKEN", "envtok")
		t.Setenv("BINTRAIL_CONSOLE_LISTEN", "0.0.0.0:9000")
		c, err := resolveCtl("", "")
		if err != nil {
			t.Fatal(err)
		}
		if c.base != "http://127.0.0.1:9000" || c.token != "envtok" || c.accessFile != "" {
			t.Errorf("got %+v", c)
		}
	})
	t.Run("no file, no token: says where it looked and what to do", func(t *testing.T) {
		state := isolate(t)
		_, err := resolveCtl("", "")
		if err == nil || !strings.Contains(err.Error(), filepath.Join(state, console.LocalAccessFileName)) ||
			!strings.Contains(err.Error(), "./dbtrail ") {
			t.Fatalf("err = %v", err)
		}
	})
	// A folder someone else prepared can feed the environment through
	// .bintrail.env. Neither a file nor the environment may send a command,
	// with its token and then a database password, to another machine.
	t.Run("a file that names another machine is refused", func(t *testing.T) {
		state := isolate(t)
		for _, u := range []string{"http://203.0.113.9:8090", "https://dbtrail.example.com", "ftp://127.0.0.1", "127.0.0.1:8090"} {
			write(t, state, console.LocalAccess{URL: u, Token: "filetok"})
			if c, err := resolveCtl("", ""); err == nil || !strings.Contains(err.Error(), "--url") {
				t.Errorf("%s: got %+v, %v", u, c, err)
			}
		}
	})
	t.Run("a listen address in the environment that names another machine is refused", func(t *testing.T) {
		isolate(t)
		t.Setenv("BINTRAIL_CONSOLE_TOKEN", "envtok")
		t.Setenv("BINTRAIL_CONSOLE_LISTEN", "203.0.113.9:8090")
		if c, err := resolveCtl("", ""); err == nil || !strings.Contains(err.Error(), "not this machine") {
			t.Fatalf("got %+v, %v", c, err)
		}
	})
	t.Run("--url may name another machine: it was typed", func(t *testing.T) {
		state := isolate(t)
		write(t, state, console.LocalAccess{URL: "http://203.0.113.9:8090", Token: "filetok"})
		t.Setenv("BINTRAIL_CONSOLE_TOKEN", "envtok")
		c, err := resolveCtl("https://dbtrail.example.com", "")
		if err != nil || c.base != "https://dbtrail.example.com" || c.token != "envtok" {
			t.Fatalf("got %+v, %v", c, err)
		}
	})
	// With no home folder the default path is anchored in the working
	// directory, and an env file can also name a home: both would make the
	// folder the command is run in the place its access file is looked for.
	t.Run("a folder cannot stand in for the home folder", func(t *testing.T) {
		isolate(t)
		evil := t.TempDir()
		dir := filepath.Join(evil, ".config", "bintrail")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		write(t, dir, console.LocalAccess{URL: "http://127.0.0.1:9999", Token: "theirs"})
		t.Chdir(evil)
		os.Unsetenv("BINTRAIL_CONSOLE_AUTH")

		os.Unsetenv("HOME") // t.Setenv in isolate puts it back
		if c, err := resolveCtl("", ""); err == nil {
			t.Fatalf("no home folder: took the working directory's file: %+v", c)
		}
		t.Cleanup(cli.ApplyEnvFileForTest("HOME=" + evil + "\n"))
		if os.Getenv("HOME") != evil {
			t.Fatal("the env file's HOME was not applied, so nothing below is checked")
		}
		if c, err := resolveCtl("", ""); err == nil {
			t.Fatalf("a home folder named by an env file: took its file: %+v", c)
		}
	})
	t.Run("--access-file is the only place looked", func(t *testing.T) {
		state := isolate(t)
		write(t, state, console.LocalAccess{URL: "http://127.0.0.1:8090", Token: "default"})
		other := t.TempDir()
		p := write(t, other, console.LocalAccess{URL: "http://127.0.0.1:7000", Token: "other"})
		c, err := resolveCtl("", p)
		if err != nil || c.token != "other" || c.base != "http://127.0.0.1:7000" {
			t.Fatalf("got %+v, %v", c, err)
		}
		if _, err := resolveCtl("", filepath.Join(other, "missing.json")); err == nil {
			t.Fatal("a missing --access-file fell back to somewhere else")
		}
	})
	t.Run("a broken file is an error, not a fall through to the token", func(t *testing.T) {
		state := isolate(t)
		if err := os.WriteFile(filepath.Join(state, console.LocalAccessFileName), []byte("{"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("BINTRAIL_CONSOLE_TOKEN", "envtok")
		if c, err := resolveCtl("", ""); err == nil {
			t.Fatalf("got %+v, want an error", c)
		}
	})
	t.Run("a file this account may not read says so", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root reads everything")
		}
		state := isolate(t)
		p := write(t, state, console.LocalAccess{URL: "http://127.0.0.1:8090", Token: "filetok"})
		if err := os.Chmod(p, 0); err != nil {
			t.Fatal(err)
		}
		_, err := resolveCtl("", "")
		if err == nil || !strings.Contains(err.Error(), "sudo") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestOnThisMachine(t *testing.T) {
	for in, want := range map[string]bool{
		"http://127.0.0.1:8090":      true,
		"https://127.0.0.1:8443":     true,
		"http://[::1]:8090":          true,
		"http://localhost:8090":      true,
		"http://LOCALHOST:8090":      true,
		"http://203.0.113.9:8090":    false,
		"http://dbtrail.example.com": false,
		"http://127.0.0.1.evil.test": false,
		"http://localhost.evil.test": false,
		"http://evil.test/127.0.0.1": false,
		"http://127.0.0.1@evil.test": false,
		"":                           false,
		"127.0.0.1:8090":             false,
		"file:///etc/passwd":         false,
	} {
		if got := onThisMachine(in); got != want {
			t.Errorf("onThisMachine(%q) = %v, want %v", in, got, want)
		}
	}
	// An address this machine holds on one of its interfaces counts.
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok && !n.IP.IsLoopback() && n.IP.To4() != nil {
			if u := "http://" + net.JoinHostPort(n.IP.String(), "8090"); !onThisMachine(u) {
				t.Errorf("%s is this machine's own address and was refused", u)
			}
			break
		}
	}
}

func TestListenToLocal(t *testing.T) {
	for in, want := range map[string]string{
		"":               "127.0.0.1:8090",
		"0.0.0.0:8090":   "127.0.0.1:8090",
		":8090":          "127.0.0.1:8090",
		"[::]:8090":      "[::1]:8090",
		"10.0.0.5:9000":  "10.0.0.5:9000",
		"localhost:8090": "localhost:8090",
	} {
		if got := listenToLocal(in); got != want {
			t.Errorf("listenToLocal(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestServerForwardUser(t *testing.T) {
	saved := map[string]any{"id": "id1", "name": "shop", "has_source": true, "has_route": true, "route_user": "shop_fwd"}
	t.Run("set, password asked", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("PUT /api/servers/id1/route", 200, saved)
		var out bytes.Buffer
		ask := &fakeAsker{t: t, tty: true, secret: "Fw9 #abc"}
		if err := runServerForwardUser(context.Background(), c, "shop", fwdFlags{User: " shop_fwd "}, ask, strings.NewReader(""), &out); err != nil {
			t.Fatal(err)
		}
		// Only the two fields of this one setting travel: nothing here can
		// clear another field of the server.
		body := d.bodies["PUT /api/servers/id1/route"]
		if len(body) != 2 || body["user"] != "shop_fwd" || body["password"] != "Fw9 #abc" {
			t.Errorf("body = %v", body)
		}
		if !strings.Contains(out.String(), `"shop" forwards as shop_fwd`) || strings.Contains(out.String(), "Fw9") {
			t.Errorf("output:\n%s", out.String())
		}
		for _, call := range d.calls {
			if call == "PUT /api/servers/id1" {
				t.Error("used the edit route, which replaces the whole server")
			}
		}
	})
	t.Run("password from stdin", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("PUT /api/servers/id1/route", 200, saved)
		if err := runServerForwardUser(context.Background(), c, "shop", fwdFlags{User: "shop_fwd", PasswordStdin: true}, &fakeAsker{t: t}, strings.NewReader("pw\r\nmore\n"), io.Discard); err != nil {
			t.Fatal(err)
		}
		if got := d.bodies["PUT /api/servers/id1/route"]["password"]; got != "pw" {
			t.Errorf("password = %q", got)
		}
	})
	t.Run("remove", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("DELETE /api/servers/id1/route", 200, map[string]any{"id": "id1", "name": "shop"})
		var out bytes.Buffer
		if err := runServerForwardUser(context.Background(), c, "shop", fwdFlags{Remove: true}, &fakeAsker{t: t}, strings.NewReader(""), &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "has no forwarding user now") {
			t.Errorf("output:\n%s", out.String())
		}
	})
	t.Run("the user DBTrail captures with is refused before anything is saved", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("PUT /api/servers/id1/route", 200, map[string]any{"id": "id1", "name": "shop", "has_route": true, "route_user": "dbtrail", "route_is_capture": true})
		err := runServerForwardUser(context.Background(), c, "shop", fwdFlags{User: "dbtrail", PasswordStdin: true}, &fakeAsker{t: t}, strings.NewReader("pw\n"), io.Discard)
		if err == nil || !strings.Contains(err.Error(), "never forwards as that one") {
			t.Fatalf("err = %v", err)
		}
		if slices.Contains(d.calls, "PUT /api/servers/id1/route") {
			t.Fatal("the capture user was sent to be saved")
		}
	})
	t.Run("the daemon says it is the capture user under another spelling", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("PUT /api/servers/id1/route", 200, map[string]any{"id": "id1", "name": "shop", "has_route": true, "route_user": "DBTrail", "route_is_capture": true})
		err := runServerForwardUser(context.Background(), c, "shop", fwdFlags{User: "DBTrail", PasswordStdin: true}, &fakeAsker{t: t}, strings.NewReader("pw\n"), io.Discard)
		if err == nil || !strings.Contains(err.Error(), "never forwards as that one") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("the daemon took it and the user is not the one asked for", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("PUT /api/servers/id1/route", 200, map[string]any{"id": "id1", "name": "shop"})
		err := runServerForwardUser(context.Background(), c, "shop", fwdFlags{User: "shop_fwd", PasswordStdin: true}, &fakeAsker{t: t}, strings.NewReader("pw\n"), io.Discard)
		if err == nil || !strings.Contains(err.Error(), "does not forward as shop_fwd") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("the daemon refuses in its own words", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		twoServers(d)
		d.on("PUT /api/servers/id1/route", 400, map[string]any{"error": "a forwarding account applies to MySQL and MariaDB sources only"})
		err := runServerForwardUser(context.Background(), c, "shop", fwdFlags{User: "u", PasswordStdin: true}, &fakeAsker{t: t}, strings.NewReader("pw\n"), io.Discard)
		if err == nil || !strings.Contains(err.Error(), "MySQL and MariaDB sources only") {
			t.Fatalf("err = %v", err)
		}
	})
	for name, c := range map[string]struct {
		f     fwdFlags
		tty   bool
		stdin string
		want  string
	}{
		"no user":                    {fwdFlags{}, true, "", "-u"},
		"remove with a user":         {fwdFlags{Remove: true, User: "u"}, true, "", "--remove takes nothing else"},
		"remove with -p":             {fwdFlags{Remove: true, AskPassword: true}, true, "", "--remove takes nothing else"},
		"-p and --password-stdin":    {fwdFlags{User: "u", AskPassword: true, PasswordStdin: true}, true, "", "use one"},
		"no terminal, no stdin flag": {fwdFlags{User: "u"}, false, "", "--password-stdin"},
		"empty password typed":       {fwdFlags{User: "u"}, true, "", "empty"},
		"empty stdin":                {fwdFlags{User: "u", PasswordStdin: true}, false, "", "stdin"},
		"stdin is only a newline":    {fwdFlags{User: "u", PasswordStdin: true}, false, "\n", "empty"},
	} {
		t.Run(name, func(t *testing.T) {
			d, cl := newFakeDaemon(t)
			twoServers(d)
			err := runServerForwardUser(context.Background(), cl, "shop", c.f, &fakeAsker{t: t, tty: c.tty}, strings.NewReader(c.stdin), io.Discard)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want one with %q", err, c.want)
			}
			for _, call := range d.calls {
				if strings.HasSuffix(call, "/route") {
					t.Errorf("made %s", call)
				}
			}
		})
	}
}

// Engine Routing on is not enough: a server with no user to forward as, or
// with the capture user saved as one, refuses those statements. Said, with
// the command that fixes it; one that is set is not mentioned.
func TestPortRoutingOnNamesServersThatCannotForward(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("PUT /api/flashback", 200, map[string]any{"enabled": true, "routing_on": true})
	d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{
		{"id": "a", "name": "shop", "has_source": true, "has_route": true, "route_user": "shop_fwd"},
		{"id": "b", "name": "billing", "has_source": true},
		{"id": "c", "name": "crm", "has_source": true, "has_route": true, "route_user": "dbtrail", "route_is_capture": true},
		{"id": "d", "name": "old-index"},
	}})
	var out bytes.Buffer
	if err := runPortRouting(context.Background(), c, true, &out); err != nil {
		t.Fatal(err)
	}
	o := out.String()
	for _, want := range []string{"server forward-user billing -u <user> -p", "server forward-user crm -u <user> -p"} {
		if !strings.Contains(o, want) {
			t.Errorf("output lacks %q:\n%s", want, o)
		}
	}
	if strings.Contains(o, "forward-user shop") || strings.Contains(o, "old-index") {
		t.Errorf("names a server that needs nothing:\n%s", o)
	}
}

// #2686: what `server add` says about the MySQL port, from the answer of the
// DBTrail that added the server.
func TestServerAddSaysWhereThePortStands(t *testing.T) {
	on, off := true, false
	port := func(m map[string]any) map[string]any {
		return map[string]any{"ok": true, "name": "shop", "started": true, "doctor": passReport(), "port": m}
	}
	for _, tc := range []struct {
		name    string
		answer  map[string]any
		want    []string
		wantNot []string
	}{
		{"turned on by this add",
			port(map[string]any{"enabled": on, "turned_on": true, "routing_on": true, "host": "", "port": "3309", "password": "bfp_abc123"}),
			[]string{"The MySQL port is on now, with Engine Routing on.", "mysql -h 127.0.0.1 -P 3309 -u shop -p",
				"Password (shown only now, keep it): bfp_abc123", `Statements for "shop" are answered once its first copy is ready.`,
				"server forward-user shop -u <user> -p", "snapshot run shop"},
			[]string{"port new-password", "was not turned on", "is off"}},
		{"turned on and no password in the answer",
			port(map[string]any{"enabled": on, "turned_on": true, "routing_on": true, "port": "3309"}),
			[]string{"The MySQL port is on now", "No password came with the answer. Make one:", "port new-password"},
			[]string{"shown only now"}},
		{"already on, routing on",
			port(map[string]any{"enabled": on, "turned_on": false, "routing_on": true, "host": "10.0.0.5", "port": "3310", "has_password": true}),
			[]string{"The MySQL port is on, with Engine Routing on, and takes the password it already has:", "mysql -h 10.0.0.5 -P 3310 -u shop -p"},
			[]string{"shown only now", "on now", "new-password"}},
		{"already on, routing off",
			port(map[string]any{"enabled": on, "routing_on": false, "port": "3309", "has_password": true}),
			[]string{"with Engine Routing off, and takes the password it already has"},
			[]string{"shown only now"}},
		{"on and not answering",
			port(map[string]any{"enabled": on, "turned_on": true, "routing_on": true, "port": "3309", "password": "bfp_x", "error": "address already in use"}),
			[]string{"Password (shown only now, keep it): bfp_x", "It is not answering: address already in use"}, nil},
		{"on and nothing answers it (an older compose file)",
			port(map[string]any{"enabled": on, "turned_on": true, "routing_on": true, "port": "3309", "password": "bfp_x", "notice": "NOTHING ANSWERS ON IT: this docker-compose.yml has no router service."}),
			[]string{"Password (shown only now, keep it): bfp_x", "NOTHING ANSWERS ON IT: this docker-compose.yml has no router service."}, nil},
		{"could not be turned on",
			port(map[string]any{"enabled": off, "not_turned_on": "its setting could not be saved: read-only file system."}),
			[]string{"The MySQL port was not turned on: its setting could not be saved: read-only file system.\n"},
			[]string{"shown only now", "mysql -h", "system..", "Turn it on"}},
		{"saved as off",
			port(map[string]any{"enabled": off}),
			[]string{"The MySQL port is off, as it was saved. Turn it on:", "port on"},
			[]string{"shown only now", "mysql -h", "was not turned on"}},
		{"a DBTrail that says nothing about the port",
			map[string]any{"ok": true, "name": "shop", "started": true, "doctor": passReport()},
			[]string{`Added "shop"`, "snapshot run shop"},
			[]string{"MySQL port", "mysql -h", "Password"}},
		{"an answer about the port that does not say whether it is on",
			port(map[string]any{"turned_on": true, "password": "bfp_x"}),
			[]string{`Added "shop"`},
			[]string{"MySQL port", "bfp_x"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, c := newFakeDaemon(t)
			d.on("POST /api/servers/identify", 200, map[string]any{"addr": "db1:3306", "flavor": "mysql", "version": "8.4.3"})
			d.on("POST /api/servers/check", 200, tc.answer)
			var out bytes.Buffer
			in := addInput{Host: "db1", Port: "3306", User: "dbtrail", Password: "s3cret#1", Name: "shop"}
			if err := runServerAdd(context.Background(), c, in, false, &out); err != nil {
				t.Fatalf("err = %v\n%s", err, out.String())
			}
			for _, w := range tc.want {
				if !strings.Contains(out.String(), w) {
					t.Errorf("output lacks %q:\n%s", w, out.String())
				}
			}
			for _, w := range tc.wantNot {
				if strings.Contains(out.String(), w) {
					t.Errorf("output says %q:\n%s", w, out.String())
				}
			}
			if strings.Contains(out.String(), "s3cret") {
				t.Errorf("the source's password was printed:\n%s", out.String())
			}
		})
	}
}

// A server name the shell would split is not put in the mysql line as it is.
func TestServerAddPortLineLeavesAnUnsafeNameToThePerson(t *testing.T) {
	on := true
	var out bytes.Buffer
	printNewServerPort(&out, &ctlNewServerPort{ctlPort: ctlPort{Enabled: &on, RoutingOn: true, Password: "bfp_x"}, TurnedOn: true}, "my shop; rm", true)
	if !strings.Contains(out.String(), "-u <server name> -p") || strings.Contains(out.String(), "-u my shop") {
		t.Fatalf("the mysql line:\n%s", out.String())
	}
}

// Only a name the shell reads as one plain word goes into the mysql line.
func TestMysqlLineUser(t *testing.T) {
	for name, want := range map[string]string{
		"shop": "shop", "db1.example.com:3306": "db1.example.com:3306", "a_b-c@d": "a_b-c@d", "Shop2": "Shop2",
		"": "<server name>", "my shop": "<server name>", "a\tb": "<server name>", "a\nb": "<server name>",
		"a;b": "<server name>", "$(id)": "<server name>", "a`b`": "<server name>", "a'b": "<server name>",
		`a"b`: "<server name>", "a{b,c}": "<server name>", "a[bc]": "<server name>", "a#b": "<server name>",
		"~root": "<server name>", "a%b": "<server name>", "a*": "<server name>", "a\\b": "<server name>",
		"tienda-ñ": "<server name>", "a\u00a0b": "<server name>", "a=b": "<server name>", "a/b": "<server name>",
	} {
		if got := mysqlLineUser(name); got != want {
			t.Errorf("mysqlLineUser(%q) = %q, want %q", name, got, want)
		}
	}
}
