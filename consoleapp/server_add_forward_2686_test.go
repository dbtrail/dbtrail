package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// #2686, slice 2: the forwarding account in `server add`.

func TestResolveForward(t *testing.T) {
	const cnfFwd = "[client]\nhost=db1\nuser=dbtrail\npassword=capture-pw\n[dbtrail-forward]\nuser=shop_fwd\npassword=fwd-pw\n"
	for _, tc := range []struct {
		name     string
		flags    addFlags
		cnf      string // "" = no option file
		tty      bool
		secret   string
		wantUser string
		wantPW   string
		wantAsk  bool
		wantErr  string
		secrets  int
	}{
		{name: "nothing named, a terminal: asked once the server is added", flags: addFlags{Host: "db1", User: "dbtrail", AskPassword: true},
			tty: true, secret: "capture-pw", wantAsk: true, secrets: 1},
		{name: "nothing named, no terminal: not asked", cnf: "[client]\nhost=db1\nuser=dbtrail\npassword=capture-pw\n"},
		{name: "--no-forward on a terminal: not asked", flags: addFlags{Host: "db1", User: "dbtrail", AskPassword: true, NoForward: true},
			tty: true, secret: "capture-pw", secrets: 1},
		{name: "the option file's group", cnf: cnfFwd, wantUser: "shop_fwd", wantPW: "fwd-pw"},
		{name: "the group's password with --forward-user naming the same user", cnf: cnfFwd, flags: addFlags{ForwardUser: "shop_fwd"},
			wantUser: "shop_fwd", wantPW: "fwd-pw"},
		{name: "--forward-user names another user: the group's password is not used, and there is no terminal", cnf: cnfFwd,
			flags: addFlags{ForwardUser: "report_fwd"}, wantErr: "no password for the forwarding user report_fwd"},
		{name: "--forward-user on a terminal: its password is asked now", flags: addFlags{Host: "db1", User: "dbtrail", AskPassword: true, ForwardUser: "shop_fwd"},
			tty: true, secret: "same-pw", wantUser: "shop_fwd", wantPW: "same-pw", secrets: 2},
		{name: "a group with a user and no password, no terminal",
			cnf: "[client]\nhost=db1\nuser=dbtrail\npassword=capture-pw\n[dbtrail-forward]\nuser=shop_fwd\n", wantErr: "no password for the forwarding user shop_fwd"},
		{name: "a group with a password and no user",
			cnf: "[client]\nhost=db1\nuser=dbtrail\npassword=capture-pw\n[dbtrail-forward]\npassword=fwd-pw\n", wantErr: "has a password and no user"},
		{name: "the group names the capture user",
			cnf: "[client]\nhost=db1\nuser=dbtrail\npassword=capture-pw\n[dbtrail-forward]\nuser=dbtrail\npassword=x\n", wantErr: "never forwards as that one"},
		{name: "--forward-user names the capture user", cnf: "[client]\nhost=db1\nuser=dbtrail\npassword=capture-pw\n",
			flags: addFlags{ForwardUser: "dbtrail"}, wantErr: "never forwards as that one"},
		{name: "--no-forward and --forward-user", flags: addFlags{Host: "db1", User: "dbtrail", AskPassword: true, NoForward: true, ForwardUser: "shop_fwd"},
			tty: true, secret: "capture-pw", wantErr: "say opposite things", secrets: 1},
		{name: "--no-forward and the group", cnf: cnfFwd, flags: addFlags{NoForward: true}, wantErr: "[dbtrail-forward] in the option file say opposite things"},
		// [client] alone is never read as the forwarding account: its user is
		// the capture user.
		{name: "[client] is not the forwarding account", cnf: "[client]\nhost=db1\nuser=dbtrail\npassword=capture-pw\n[dbtrail]\nuser=dbtrail\n"},
		{name: "[dbtrail] is not the forwarding account either", cnf: "[client]\nhost=db1\npassword=capture-pw\n[dbtrail]\nuser=dbtrail\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.flags
			if tc.cnf != "" {
				f.DefaultsFile = writeCnf(t, tc.cnf)
			}
			ask := &fakeAsker{t: t, tty: tc.tty, secret: tc.secret}
			in, err := resolveAddInput(f, ask, strings.NewReader(""))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if in.FwdUser != tc.wantUser || in.FwdPassword != tc.wantPW || in.FwdAsk != tc.wantAsk {
				t.Fatalf("forward user %q password %q ask %v, want %q %q %v", in.FwdUser, in.FwdPassword, in.FwdAsk, tc.wantUser, tc.wantPW, tc.wantAsk)
			}
			if in.FwdAsk && in.ask == nil {
				t.Fatal("to be asked, with nobody to ask")
			}
			if ask.secrets != tc.secrets {
				t.Fatalf("asked %d secrets, want %d", ask.secrets, tc.secrets)
			}
		})
	}
}

// The option file on stdin (the Docker command sends it so) is read once,
// and both groups come out of it.
func TestResolveForwardFromAnOptionFileOnStdin(t *testing.T) {
	cnf := "[client]\nhost=db1\nuser=dbtrail\npassword=capture-pw\n[dbtrail-forward]\nuser=shop_fwd\npassword=fwd-pw\n"
	in, err := resolveAddInput(addFlags{DefaultsFile: "-"}, &fakeAsker{t: t, tty: true}, strings.NewReader(cnf))
	if err != nil {
		t.Fatal(err)
	}
	if in.Password != "capture-pw" || in.FwdUser != "shop_fwd" || in.FwdPassword != "fwd-pw" || in.FwdAsk {
		t.Fatalf("got %+v", in)
	}
	// Stdin was the file: nothing is left to ask with.
	in, err = resolveAddInput(addFlags{DefaultsFile: "-"}, &fakeAsker{t: t, tty: true}, strings.NewReader("[client]\nhost=db1\nuser=dbtrail\npassword=capture-pw\n"))
	if err != nil || in.FwdAsk {
		t.Fatalf("file on stdin, no group: ask %v err %v", in.FwdAsk, err)
	}
}

func addedAnswer() map[string]any {
	return map[string]any{"ok": true, "name": "shop", "started": true, "doctor": passReport(),
		"server": map[string]any{"id": "id9", "name": "shop", "has_source": true, "source_user": "dbtrail"},
		"port":   map[string]any{"enabled": true, "turned_on": true, "routing_on": true, "port": "3309", "password": "bfp_portpw"}}
}

func addDaemon(t *testing.T) (*fakeDaemon, *ctlClient) {
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/identify", 200, map[string]any{"addr": "db1:3306", "flavor": "mysql", "version": "8.4.3"})
	d.on("POST /api/servers/check", 200, addedAnswer())
	return d, c
}

func TestServerAddSetsTheForwardingUserItWasGiven(t *testing.T) {
	d, c := addDaemon(t)
	d.on("PUT /api/servers/id9/route", 200, map[string]any{"id": "id9", "name": "shop", "has_route": true, "route_user": "shop_fwd"})
	var out bytes.Buffer
	in := addInput{Host: "db1", Port: "3306", User: "dbtrail", Password: "capture-pw", Name: "shop", FwdUser: "shop_fwd", FwdPassword: "fwd-pw"}
	if err := runServerAdd(context.Background(), c, in, false, &out); err != nil {
		t.Fatalf("err = %v\n%s", err, out.String())
	}
	body := d.bodies["PUT /api/servers/id9/route"]
	if len(body) != 2 || body["user"] != "shop_fwd" || body["password"] != "fwd-pw" {
		t.Fatalf("route body = %v", body)
	}
	o := out.String()
	pw, fw := strings.Index(o, "bfp_portpw"), strings.Index(o, `"shop" forwards as shop_fwd.`)
	if pw < 0 || fw < 0 || pw > fw {
		t.Fatalf("the port's password must come before the forwarding user is set:\n%s", o)
	}
	if strings.Contains(o, "Until it has a forwarding user") || strings.Contains(o, "fwd-pw") || strings.Contains(o, "capture-pw") {
		t.Fatalf("output:\n%s", o)
	}
}

func TestServerAddWhoseForwardingUserIsRefusedStaysAdded(t *testing.T) {
	d, c := addDaemon(t)
	d.on("PUT /api/servers/id9/route", 400, map[string]any{"error": "shop_fwd cannot log in to db1:3306: Access denied"})
	var out bytes.Buffer
	in := addInput{Host: "db1", Port: "3306", User: "dbtrail", Password: "capture-pw", Name: "shop", FwdUser: "shop_fwd", FwdPassword: "bad"}
	err := runServerAdd(context.Background(), c, in, false, &out)
	if !errors.Is(err, errAddedNoForward) {
		t.Fatalf("err = %v, want errAddedNoForward", err)
	}
	for _, w := range []string{"Access denied", "server forward-user shop -u shop_fwd -p"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("err lacks %q: %v", w, err)
		}
	}
	o := out.String()
	for _, w := range []string{`Added "shop"`, "bfp_portpw", "snapshot run shop"} {
		if !strings.Contains(o, w) {
			t.Errorf("output lacks %q:\n%s", w, o)
		}
	}
}

func TestServerAddAsksForTheForwardingUser(t *testing.T) {
	t.Run("Enter skips it", func(t *testing.T) {
		d, c := addDaemon(t)
		ask := &fakeAsker{t: t, tty: true, lines: map[string]string{"Forwarding user (Enter to skip)": ""}}
		var out bytes.Buffer
		in := addInput{Host: "db1", Port: "3306", User: "dbtrail", Password: "capture-pw", Name: "shop", FwdAsk: true, ask: ask}
		if err := runServerAdd(context.Background(), c, in, false, &out); err != nil {
			t.Fatalf("err = %v\n%s", err, out.String())
		}
		if slices.Contains(d.calls, "PUT /api/servers/id9/route") || ask.secrets != 0 {
			t.Fatalf("calls %v, secrets %d", d.calls, ask.secrets)
		}
		o := out.String()
		if !strings.Contains(o, "Skipped. Set it later:  bintrail-console server forward-user shop -u <user> -p") {
			t.Fatalf("output:\n%s", o)
		}
		if strings.Index(o, "bfp_portpw") > strings.Index(o, "The forwarding user:") {
			t.Fatalf("the port's password must come before the question:\n%s", o)
		}
	})
	t.Run("a user and its password", func(t *testing.T) {
		d, c := addDaemon(t)
		d.on("PUT /api/servers/id9/route", 200, map[string]any{"id": "id9", "name": "shop", "has_route": true, "route_user": "shop_fwd"})
		ask := &fakeAsker{t: t, tty: true, secret: "fwd-pw", lines: map[string]string{"Forwarding user (Enter to skip)": " shop_fwd "}}
		var out bytes.Buffer
		in := addInput{Host: "db1", Port: "3306", User: "dbtrail", Password: "capture-pw", Name: "shop", FwdAsk: true, ask: ask}
		if err := runServerAdd(context.Background(), c, in, false, &out); err != nil {
			t.Fatalf("err = %v\n%s", err, out.String())
		}
		if b := d.bodies["PUT /api/servers/id9/route"]; b["user"] != "shop_fwd" || b["password"] != "fwd-pw" {
			t.Fatalf("route body = %v", b)
		}
	})
	t.Run("the capture user typed at the question", func(t *testing.T) {
		d, c := addDaemon(t)
		ask := &fakeAsker{t: t, tty: true, lines: map[string]string{"Forwarding user (Enter to skip)": "dbtrail"}}
		var out bytes.Buffer
		in := addInput{Host: "db1", Port: "3306", User: "dbtrail", Password: "capture-pw", Name: "shop", FwdAsk: true, ask: ask}
		err := runServerAdd(context.Background(), c, in, false, &out)
		if !errors.Is(err, errAddedNoForward) || !strings.Contains(err.Error(), "never forwards as that one") {
			t.Fatalf("err = %v", err)
		}
		if slices.Contains(d.calls, "PUT /api/servers/id9/route") || ask.secrets != 0 {
			t.Fatalf("calls %v, secrets %d", d.calls, ask.secrets)
		}
	})
	t.Run("not asked when the checks failed", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("POST /api/servers/identify", 200, map[string]any{"addr": "db1:3306", "flavor": "mysql", "version": "8.4.3"})
		d.on("POST /api/servers/check", 200, map[string]any{"ok": false, "name": "shop", "doctor": map[string]any{"failed": 1, "checks": []map[string]any{{"name": "binlog_format is ROW", "status": "fail"}}}})
		ask := &fakeAsker{t: t, tty: true}
		in := addInput{Host: "db1", Port: "3306", User: "dbtrail", Password: "capture-pw", Name: "shop", FwdAsk: true, ask: ask}
		if err := runServerAdd(context.Background(), c, in, false, &bytes.Buffer{}); !errors.Is(err, errNotAdded) {
			t.Fatalf("err = %v", err)
		}
		if len(ask.asked) != 0 {
			t.Fatalf("asked %v after failed checks", ask.asked)
		}
	})
	t.Run("an answer that does not say which server was saved", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("POST /api/servers/identify", 200, map[string]any{"addr": "db1:3306", "flavor": "mysql", "version": "8.4.3"})
		d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "shop", "started": true, "doctor": passReport()})
		in := addInput{Host: "db1", Port: "3306", User: "dbtrail", Password: "capture-pw", Name: "shop", FwdUser: "shop_fwd", FwdPassword: "x"}
		err := runServerAdd(context.Background(), c, in, false, &bytes.Buffer{})
		if !errors.Is(err, errAddedNoForward) || !strings.Contains(err.Error(), "did not say which server") {
			t.Fatalf("err = %v", err)
		}
	})
}
