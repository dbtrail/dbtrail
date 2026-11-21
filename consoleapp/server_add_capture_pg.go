package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/dbtrail/dbtrail/internal/mycnf"
)

// The capture account at `server add` for a PostgreSQL (#3054). The port may
// forward with the user DBTrail connects with, once somebody says yes
// (PUT /api/servers/{id}/route {"capture_account": true}). `server add` asks
// it once the server is added, on a terminal, Enter meaning yes; without a
// terminal only --capture-account or capture-account=yes under
// [dbtrail-forward] says yes, and nothing said is no. It replaces the
// question for a forwarding user of its own (#3020), which stays reachable
// with --forward-user, [dbtrail-forward] user= and `server forward-user`.
// A MySQL or MariaDB add never reaches any of this.

// captureAnswer is what `server add` does about the capture account.
type captureAnswer int

const (
	// captureNone: nothing to do (a MySQL, a forwarding user named, or
	// --no-forward).
	captureNone captureAnswer = iota
	// captureAsk: asked once the server is added.
	captureAsk
	// captureYes: --capture-account or capture-account=yes.
	captureYes
	// captureNo: --no-capture-account or capture-account=no.
	captureNo
	// captureNotAsked: nobody to ask and nothing said: no, and said so.
	captureNotAsked
)

// parseCaptureValue reads a capture-account line's value.
func parseCaptureValue(v string) (on bool, ok bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "y", "yes", "true", "on":
		return true, true
	case "0", "n", "no", "false", "off":
		return false, true
	}
	return false, false
}

// resolveCapture settles the answer before anything is saved, for a
// PostgreSQL whose forwarding user resolveForward already settled. Every
// refusal here happens with nothing saved.
func resolveCapture(in *addInput, f addFlags, file mycnf.Client, ask asker, canAsk bool) error {
	if f.CaptureAccount && f.NoCaptureAccount {
		return errors.New("--capture-account and --no-capture-account say opposite things: use one")
	}
	said, saidYes, from := false, false, ""
	switch {
	case f.CaptureAccount:
		said, saidYes, from = true, true, "--capture-account"
	case f.NoCaptureAccount:
		said, saidYes, from = true, false, "--no-capture-account"
	case file.CaptureAccount != "":
		on, ok := parseCaptureValue(file.CaptureAccount)
		if !ok {
			return errors.New("capture-account under [dbtrail-forward] in the option file is yes or no")
		}
		said, saidYes, from = true, on, "capture-account=yes under [dbtrail-forward] in the option file"
	}
	switch {
	case f.NoForward:
		if said && saidYes {
			return fmt.Errorf("--no-forward and %s say opposite things: use one", from)
		}
		return nil
	case in.FwdUser != "":
		// A forwarding user of its own is set as before, and nothing is
		// asked about the capture account.
		if said && saidYes {
			return fmt.Errorf("%s and the forwarding user %s both say who the port forwards as: use one", from, in.FwdUser)
		}
		return nil
	case said && saidYes:
		in.Capture = captureYes
	case said:
		in.Capture = captureNo
	case canAsk:
		in.Capture, in.ask = captureAsk, ask
	default:
		in.Capture = captureNotAsked
	}
	return nil
}

// captureQuestion is what `server add` says before it asks.
func captureQuestion(user string) string {
	return fmt.Sprintf("\nThe copy answers this database's statements on the PostgreSQL port. With your yes, the port may also send to your PostgreSQL, read-only, a SELECT whose every table is a table of the copy, as the user DBTrail already connects with (%s), when the copy does not answer it; anything else is refused and not sent. To let the port read more, set a forwarding user of its own instead:  bintrail-console server forward-user <server> -u <user> -p\n", user)
}

// errAddedNoCapture: the database was added and is recording, and the yes
// was not saved.
var errAddedNoCapture = errors.New("the database was added and is recording, and the port does not forward with the user DBTrail connects with")

// addCaptureAnswerPG does what in.Capture says for a PostgreSQL `server add`
// just added. A failure leaves the server added and recording, and says so.
func addCaptureAnswerPG(ctx context.Context, c *ctlClient, in addInput, res ctlCheckResponse, out io.Writer) error {
	later := mysqlLineUser(res.Name)
	turnOn := fmt.Sprintf("bintrail-console server forward-user %s --capture-account", later)
	on := false
	switch in.Capture {
	case captureNone:
		return nil
	case captureNo:
		fmt.Fprintf(out, "The port does not forward for %q (--no-capture-account): the copy answers alone, and a statement it cannot answer gets an error. Turn it on later:  %s\n\n", res.Name, turnOn)
		return nil
	case captureNotAsked:
		fmt.Fprintf(out, "Not asked whether the port may forward with %s: nobody is at a terminal to answer, and nothing said yes, so the port does not forward for %q and the copy answers alone. Turn it on:  %s\n(or, when adding, --capture-account, or capture-account=yes under [dbtrail-forward] in the option file)\n\n", in.User, res.Name, turnOn)
		return nil
	case captureYes:
		on = true
	case captureAsk:
		fmt.Fprint(out, captureQuestion(in.User))
		for tries := 0; ; tries++ {
			answer, err := in.ask.Line(fmt.Sprintf("Let the port forward as %s? (yes/no)", in.User), "yes")
			if err != nil {
				return fmt.Errorf("%w: %v. Turn it on later:  %s", errAddedNoCapture, err, turnOn)
			}
			var ok bool
			if on, ok = parseCaptureValue(answer); ok {
				break
			}
			if tries == 2 {
				return fmt.Errorf("%w: the answer was neither yes nor no. Turn it on later:  %s", errAddedNoCapture, turnOn)
			}
			fmt.Fprintln(out, "Answer yes or no (Enter is yes).")
		}
		if !on {
			fmt.Fprintf(out, "No: the copy answers alone, and a statement it cannot answer gets an error. Turn it on later:  %s\n\n", turnOn)
			return nil
		}
	}
	if res.Server == nil || res.Server.ID == "" {
		return fmt.Errorf("%w: DBTrail did not say which server it saved. Turn it on:  %s", errAddedNoCapture, turnOn)
	}
	var saved ctlServer
	if err := c.do(ctx, http.MethodPut, "/api/servers/"+res.Server.ID+"/route", map[string]bool{"capture_account": true}, &saved); err != nil {
		return fmt.Errorf("%w: %v\nTurn it on once that is fixed:  %s", errAddedNoCapture, err, turnOn)
	}
	if !saved.ForwardWithCaptureAccount {
		return fmt.Errorf("%w: DBTrail took the request and did not save it. Turn it on:  %s", errAddedNoCapture, turnOn)
	}
	if saved.Name == "" {
		saved.Name = res.Name
	}
	fmt.Fprintf(out, "%s\n\n", captureAnswerLine(saved))
	return nil
}
