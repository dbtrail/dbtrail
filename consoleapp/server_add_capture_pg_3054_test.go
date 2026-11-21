package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

// #3054, PR (b): at `server add` a PostgreSQL is asked once whether the port
// may forward with the user DBTrail connects with. Whole outputs, against
// DBTrail's real web handlers.

const pgCaptureQuestion = "\nThe copy answers this database's statements on the PostgreSQL port. With your yes, the port may also send to your PostgreSQL, read-only, a SELECT whose every table is a table of the copy, as the user DBTrail already connects with (dbtrail), when the copy does not answer it; anything else is refused and not sent. To let the port read more, set a forwarding user of its own instead:  bintrail-console server forward-user <server> -u <user> -p\n"

const pgCapturePrompt = "Let the port forward as dbtrail? (yes/no)"

const pgCaptureYes = `"shop" forwards with the user DBTrail captures with (dbtrail): the PostgreSQL port sends to your PostgreSQL, read-only, a SELECT the copy does not answer, and only when every table it reads is a table of the copy. Anything else is refused and not sent. A server that hides tables or columns sends nothing.` + "\n\n"

const pgCaptureNoAtQuestion = "No: the copy answers alone, and a statement it cannot answer gets an error. Turn it on later:  bintrail-console server forward-user shop --capture-account\n\n"

const pgCaptureNoFlag = `The port does not forward for "shop" (--no-capture-account): the copy answers alone, and a statement it cannot answer gets an error. Turn it on later:  bintrail-console server forward-user shop --capture-account` + "\n\n"

const pgCaptureNotAsked = `Not asked whether the port may forward with dbtrail: nobody is at a terminal to answer, and nothing said yes, so the port does not forward for "shop" and the copy answers alone. Turn it on:  bintrail-console server forward-user shop --capture-account
(or, when adding, --capture-account, or capture-account=yes under [dbtrail-forward] in the option file)` + "\n\n"

func TestServerAddPostgresCaptureAnswerOutputInEveryCase(t *testing.T) {
	type answer struct {
		capture captureAnswer
		typed   []string // at the question, in turn; "" is Enter
		fwdUser string
	}
	heads := map[string]string{"port turned on": pgAddedHead, "port already on": pgAlreadyOnHead}
	for _, port := range []string{"port turned on", "port already on"} {
		head := heads[port]
		for _, tc := range []struct {
			name      string
			a         answer
			want      string
			wantSaved bool
		}{
			{"asked, Enter is yes", answer{capture: captureAsk, typed: []string{""}}, head + "\n" + pgCaptureQuestion + pgCaptureYes + pgNextLine, true},
			{"asked, yes typed", answer{capture: captureAsk, typed: []string{" YES "}}, head + "\n" + pgCaptureQuestion + pgCaptureYes + pgNextLine, true},
			{"asked, no", answer{capture: captureAsk, typed: []string{"n"}}, head + "\n" + pgCaptureQuestion + pgCaptureNoAtQuestion + pgNextLine, false},
			{"asked, something else, then no", answer{capture: captureAsk, typed: []string{"maybe", "no"}},
				head + "\n" + pgCaptureQuestion + "Answer yes or no (Enter is yes).\n" + pgCaptureNoAtQuestion + pgNextLine, false},
			{"--capture-account", answer{capture: captureYes}, head + "\n" + pgCaptureYes + pgNextLine, true},
			{"--no-capture-account", answer{capture: captureNo}, head + "\n" + pgCaptureNoFlag + pgNextLine, false},
			{"no terminal and nothing said", answer{capture: captureNotAsked}, head + "\n" + pgCaptureNotAsked + pgNextLine, false},
			{"--forward-user: set as before, nothing asked", answer{fwdUser: "app_ro"}, head + "\n" + pgForwardsWant("app_ro") + pgNextLine, false},
			{"--no-forward: nothing forwarded", answer{}, head + map[bool]string{true: "", false: "\n"}[port == "port turned on"] + pgNoForwardYet + "\n" + pgNextLine, false},
		} {
			t.Run(port+", "+tc.name, func(t *testing.T) {
				d, c := newPGAddDaemon(t, pgAddSourceURL)
				if port == "port already on" {
					d.port = pgPortAlreadyOn()
				}
				in := pgAddInput()
				in.Capture = tc.a.capture
				var ask *seqAsker
				if tc.a.capture == captureAsk {
					ask = &seqAsker{t: t, tty: true}
					in.ask = &turnAsker{seqAsker: ask, answers: tc.a.typed}
				}
				if tc.a.fwdUser != "" {
					in.FwdUser, in.FwdPassword = tc.a.fwdUser, "fwd-pw"
				}
				var out bytes.Buffer
				if err := runServerAdd(context.Background(), c, in, false, &out); err != nil {
					t.Fatalf("err = %v\n%s", err, out.String())
				}
				got := afterAdded(t, out.String())
				if got != tc.want {
					t.Fatalf("output:\n%s\nwant:\n%s", got, tc.want)
				}
				e, _ := d.reg.Get(d.id)
				if e.ForwardWithCaptureAccount != tc.wantSaved {
					t.Fatalf("saved answer %v, want %v", e.ForwardWithCaptureAccount, tc.wantSaved)
				}
				if strings.Contains(got, "capture-pw") || strings.Contains(got, "fwd-pw") {
					t.Fatalf("a password in the output:\n%s", got)
				}
				if port == "port turned on" {
					t.Logf("%s:\n%s", tc.name, got)
				}
			})
		}
	}
}

// turnAsker answers Line with each answer in turn, and records the question.
type turnAsker struct {
	*seqAsker
	answers []string
}

func (a *turnAsker) Line(q, def string) (string, error) {
	a.asked = append(a.asked, q)
	if q != pgCapturePrompt {
		a.t.Fatalf("asked %q, want %q", q, pgCapturePrompt)
	}
	if def != "yes" {
		a.t.Fatalf("the default is %q, want yes", def)
	}
	if len(a.answers) == 0 {
		a.t.Fatalf("asked %q once more than the test answers", q)
	}
	ans := a.answers[0]
	a.answers = a.answers[1:]
	if strings.TrimSpace(ans) == "" {
		return def, nil
	}
	return ans, nil
}

func TestServerAddPostgresCaptureAnswerRefusals(t *testing.T) {
	t.Run("three answers that are neither yes nor no", func(t *testing.T) {
		d, c := newPGAddDaemon(t, pgAddSourceURL)
		in := pgAddInput()
		in.Capture, in.ask = captureAsk, &turnAsker{seqAsker: &seqAsker{t: t, tty: true}, answers: []string{"maybe", "?", "oui"}}
		var out bytes.Buffer
		err := runServerAdd(context.Background(), c, in, false, &out)
		const want = "the database was added and is recording, and the port does not forward with the user DBTrail connects with: the answer was neither yes nor no. Turn it on later:  bintrail-console server forward-user shop --capture-account"
		if !errors.Is(err, errAddedNoCapture) || err.Error() != want {
			t.Fatalf("err = %v\nwant  %s", err, want)
		}
		if e, _ := d.reg.Get(d.id); e.ForwardWithCaptureAccount {
			t.Fatal("saved")
		}
		if !strings.HasSuffix(out.String(), pgNextLine) {
			t.Fatalf("the next step is not said:\n%s", out.String())
		}
	})
	t.Run("an answer that does not say which server was saved", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "shop", "started": true, "doctor": passReport()})
		in := pgAddInput()
		in.Capture = captureYes
		err := runServerAdd(context.Background(), c, in, false, &bytes.Buffer{})
		const want = "the database was added and is recording, and the port does not forward with the user DBTrail connects with: DBTrail did not say which server it saved. Turn it on:  bintrail-console server forward-user shop --capture-account"
		if !errors.Is(err, errAddedNoCapture) || err.Error() != want {
			t.Fatalf("err = %v\nwant  %s", err, want)
		}
	})
	t.Run("DBTrail took it and did not save it", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "shop", "started": true, "doctor": passReport(),
			"server": map[string]any{"id": "id9", "name": "shop", "has_source": true, "source_user": "dbtrail"}})
		d.on("PUT /api/servers/id9/route", 200, map[string]any{"id": "id9", "name": "shop"})
		in := pgAddInput()
		in.Capture = captureYes
		err := runServerAdd(context.Background(), c, in, false, &bytes.Buffer{})
		const want = "the database was added and is recording, and the port does not forward with the user DBTrail connects with: DBTrail took the request and did not save it. Turn it on:  bintrail-console server forward-user shop --capture-account"
		if !errors.Is(err, errAddedNoCapture) || err.Error() != want {
			t.Fatalf("err = %v\nwant  %s", err, want)
		}
		if b := d.bodies["PUT /api/servers/id9/route"]; len(b) != 1 || b["capture_account"] != true {
			t.Fatalf("route body = %v", b)
		}
	})
}

// What resolveAddInput settles, from flags, the option file (on disk or on
// stdin, as the Docker dbtrail command sends it) and the terminal.
func TestResolveAddInputPostgresCapture(t *testing.T) {
	const plain = "[client]\nhost=pg1\nuser=dbtrail\npassword=capture-pw\n"
	pgFlags := func(f addFlags) addFlags {
		f.Flavor, f.Database, f.Publication, f.Slot = "postgres", "shop", "bintrail_pub", "bintrail_shop"
		return f
	}
	term := addFlags{Host: "pg1", User: "dbtrail", AskPassword: true}
	for _, tc := range []struct {
		name    string
		flags   addFlags
		cnf     string
		stdin   bool // the option file on stdin (--defaults-file -)
		tty     bool
		want    captureAnswer
		wantErr string // the whole message
	}{
		{name: "a terminal, nothing said: asked", flags: term, tty: true, want: captureAsk},
		{name: "no terminal, nothing said: no, and said so", cnf: plain, want: captureNotAsked},
		{name: "the option file on stdin, on a terminal: nobody can be asked", cnf: plain, stdin: true, tty: true, want: captureNotAsked},
		{name: "--capture-account, no terminal", cnf: plain, flags: addFlags{CaptureAccount: true}, want: captureYes},
		{name: "--capture-account on a terminal: not asked", flags: addFlags{Host: "pg1", User: "dbtrail", AskPassword: true, CaptureAccount: true}, tty: true, want: captureYes},
		{name: "--no-capture-account", cnf: plain, flags: addFlags{NoCaptureAccount: true}, want: captureNo},
		{name: "--no-capture-account on a terminal: not asked", flags: addFlags{Host: "pg1", User: "dbtrail", AskPassword: true, NoCaptureAccount: true}, tty: true, want: captureNo},
		{name: "capture-account=yes on stdin", cnf: plain + "[dbtrail-forward]\ncapture-account=yes\n", stdin: true, want: captureYes},
		{name: "capture_account = YES, with an underscore", cnf: plain + "[dbtrail-forward]\ncapture_account = YES\n", want: captureYes},
		{name: "capture-account alone on its line", cnf: plain + "[dbtrail-forward]\ncapture-account\n", want: captureYes},
		{name: "capture-account=no", cnf: plain + "[dbtrail-forward]\ncapture-account=no\n", want: captureNo},
		{name: "capture-account=0 on a terminal: not asked", flags: term, tty: true, cnf: "[dbtrail-forward]\ncapture-account=0\n", want: captureNo},
		{name: "capture-account in another group is not read", cnf: plain + "[dbtrail]\ncapture-account=yes\n", want: captureNotAsked},
		{name: "the command line wins over the file", cnf: plain + "[dbtrail-forward]\ncapture-account=yes\n", flags: addFlags{NoCaptureAccount: true}, want: captureNo},
		{name: "a forwarding user in the file: set as before, not asked", cnf: plain + "[dbtrail-forward]\nuser=app_ro\npassword=fwd-pw\n", want: captureNone},
		{name: "--forward-user with --no-capture-account", cnf: plain + "[dbtrail-forward]\nuser=app_ro\npassword=fwd-pw\n", flags: addFlags{NoCaptureAccount: true}, want: captureNone},
		{name: "--no-forward: nothing", cnf: plain, flags: addFlags{NoForward: true}, want: captureNone},
		{name: "--no-forward with --no-capture-account", cnf: plain, flags: addFlags{NoForward: true, NoCaptureAccount: true}, want: captureNone},

		{name: "both flags", cnf: plain, flags: addFlags{CaptureAccount: true, NoCaptureAccount: true},
			wantErr: "--capture-account and --no-capture-account say opposite things: use one"},
		{name: "a value that is neither yes nor no", cnf: plain + "[dbtrail-forward]\ncapture-account=maybe\n",
			wantErr: "capture-account under [dbtrail-forward] in the option file is yes or no"},
		{name: "--no-forward with --capture-account", cnf: plain, flags: addFlags{NoForward: true, CaptureAccount: true},
			wantErr: "--no-forward and --capture-account say opposite things: use one"},
		{name: "--no-forward with the file's yes", cnf: plain + "[dbtrail-forward]\ncapture-account=yes\n", flags: addFlags{NoForward: true},
			wantErr: "--no-forward and capture-account=yes under [dbtrail-forward] in the option file say opposite things: use one"},
		{name: "--capture-account with a forwarding user", cnf: plain + "[dbtrail-forward]\nuser=app_ro\npassword=fwd-pw\n", flags: addFlags{CaptureAccount: true},
			wantErr: "--capture-account and the forwarding user app_ro both say who the port forwards as: use one"},
		{name: "the file says both", cnf: plain + "[dbtrail-forward]\nuser=app_ro\npassword=fwd-pw\ncapture-account=yes\n",
			wantErr: "capture-account=yes under [dbtrail-forward] in the option file and the forwarding user app_ro both say who the port forwards as: use one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := pgFlags(tc.flags)
			stdin := strings.NewReader("")
			if tc.cnf != "" {
				if tc.stdin {
					f.DefaultsFile, stdin = "-", strings.NewReader(tc.cnf)
				} else {
					f.DefaultsFile = writeCnf(t, tc.cnf)
				}
			}
			ask := &seqAsker{t: t, tty: tc.tty, secrets: []string{"capture-pw"}}
			in, err := resolveAddInput(f, ask, stdin)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("err = %v\nwant  %s", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if in.Capture != tc.want || in.FwdAsk {
				t.Fatalf("capture %v ask-for-a-user %v, want %v false", in.Capture, in.FwdAsk, tc.want)
			}
			if len(ask.asked) != 0 {
				t.Fatalf("asked before the add: %q", ask.asked)
			}
		})
	}
}

// A MySQL or MariaDB add is as it was: the new flags and the file's key are
// refused, a file without the key reads as before, and nothing about the
// capture account is settled, asked or printed.
func TestServerAddMySQLUnchangedBy3054(t *testing.T) {
	const plain = "[client]\nhost=db1\nuser=dbtrail\npassword=capture-pw\n"
	const refused = "--capture-account, --no-capture-account and capture-account in the option file are a PostgreSQL's: the MySQL port forwards only as a forwarding user of its own"
	for _, f := range []addFlags{{CaptureAccount: true}, {NoCaptureAccount: true}, {Flavor: "mariadb", CaptureAccount: true}} {
		f.DefaultsFile = writeCnf(t, plain)
		if _, err := resolveAddInput(f, &seqAsker{t: t}, strings.NewReader("")); err == nil || err.Error() != refused {
			t.Errorf("%+v: err = %v", f, err)
		}
	}
	f := addFlags{DefaultsFile: writeCnf(t, plain+"[dbtrail-forward]\ncapture-account=yes\n")}
	if _, err := resolveAddInput(f, &seqAsker{t: t}, strings.NewReader("")); err == nil || err.Error() != refused {
		t.Errorf("the file's key: err = %v", err)
	}
	// On a terminal, nothing named: the forwarding user is asked for, as
	// before, and the capture account is not.
	in, err := resolveAddInput(addFlags{Host: "db1", User: "dbtrail", AskPassword: true}, &seqAsker{t: t, tty: true, secrets: []string{"capture-pw"}}, strings.NewReader(""))
	if err != nil || !in.FwdAsk || in.Capture != captureNone {
		t.Fatalf("on a terminal: %+v, %v", in, err)
	}
	// The whole output of an add that asks, Enter skipping: no line about
	// the capture account.
	d, c := addDaemon(t)
	ask := &fakeAsker{t: t, tty: true, lines: map[string]string{"Forwarding user (Enter to skip)": ""}}
	var out bytes.Buffer
	in = addInput{Host: "db1", Port: "3306", User: "dbtrail", Password: "capture-pw", Name: "shop", FwdAsk: true, ask: ask}
	if err := runServerAdd(context.Background(), c, in, false, &out); err != nil {
		t.Fatalf("err = %v\n%s", err, out.String())
	}
	if o := strings.ToLower(out.String()); strings.Contains(o, "capture") || strings.Contains(o, "connects with") {
		t.Fatalf("a MySQL add says something about the capture account:\n%s", out.String())
	}
	if len(ask.asked) != 1 || ask.asked[0] != "Forwarding user (Enter to skip)" {
		t.Fatalf("asked %q", ask.asked)
	}
	for _, call := range d.calls {
		if strings.HasSuffix(call, "/route") {
			t.Fatalf("calls %v", d.calls)
		}
	}
}
