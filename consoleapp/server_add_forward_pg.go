package consoleapp

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The forwarding user of a PostgreSQL in `server add` (#3020). What is given
// is settled by resolveForward, as for a MySQL: --forward-user or
// [dbtrail-forward] in the option file (the question once the server is added
// is about the capture account since #3054, server_add_capture_pg.go). It is saved by setForwardUser, the one `server forward-user` saves
// with, so a user that command refuses is refused here the same way. Only the
// sentences are a PostgreSQL's: its port, and what it sends to the source.

// pgForwardsAsLine says what a PostgreSQL server's forwarding user is for,
// once it is saved, and that saving it tried no login: DBTrail's web server
// links no PostgreSQL client, so a wrong password is the port's to find. The
// same lines for `server forward-user` and `server add`.
func pgForwardsAsLine(name, user string) string {
	return fmt.Sprintf("%q forwards as %s: the PostgreSQL port sends to your PostgreSQL, as that user and read-only, every statement the copy is not known to answer as PostgreSQL does, and a client's transaction blocks.\nThe login was not tried: a wrong password, or a role that cannot log in, shows with the first statement the port sends there. To check it, run BEGIN; SELECT 1; COMMIT; through the PostgreSQL port.", name, user)
}

// pgNoForwardYetLine says where a PostgreSQL server stands when it forwards
// with nothing, and the two commands that change that.
func pgNoForwardYetLine(name string) string {
	return fmt.Sprintf("Until %q forwards, the PostgreSQL port does not serve it: a client that connects gets an error that says why and how to serve it. Let the port forward with the user DBTrail connects with:\n  bintrail-console server forward-user %s --capture-account\nor set a forwarding user of its own:\n  bintrail-console server forward-user %s -u <user> -p", name, mysqlLineUser(name), mysqlLineUser(name))
}

// addForwardUserPG is addForwardUser for a PostgreSQL: it sets the
// forwarding account of a server `server add` just added, the one
// resolveForward settled from --forward-user or [dbtrail-forward]. It asks
// for none: since #3054 the question at add is about the capture account. A
// failure leaves the server added and recording, and says so
// (errAddedNoForward).
func addForwardUserPG(ctx context.Context, c *ctlClient, in addInput, res ctlCheckResponse, out io.Writer) error {
	later := mysqlLineUser(res.Name)
	user, password := in.FwdUser, in.FwdPassword
	if user == "" {
		// Not asked for (#3054): the question at add is whether the port may
		// forward with the capture account (addCaptureAnswerPG).
		return nil
	}
	if res.Server == nil || res.Server.ID == "" {
		return fmt.Errorf("%w: DBTrail did not say which server it saved. Set it:  bintrail-console server forward-user %s -u %s -p", errAddedNoForward, later, pgShellUser(user))
	}
	s := *res.Server
	if s.Name == "" {
		s.Name = res.Name
	}
	if s.SourceUser == "" {
		s.SourceUser = in.User
	}
	// The refusals name the source by its family.
	s.Flavor = console.FlavorPostgres
	if err := setForwardUser(ctx, c, s, user, password); err != nil {
		// Refused for being the capture user: the command to run names
		// another user, not the one that was just refused.
		again := pgShellUser(user)
		if user == s.SourceUser || strings.Contains(err.Error(), "never forwards") {
			again = "<user>"
		}
		return fmt.Errorf("%w: %v\nSet it once that is fixed:  bintrail-console server forward-user %s -u %s -p", errAddedNoForward, err, later, again)
	}
	fmt.Fprintf(out, "%s\n\n", pgForwardsAsLine(res.Name, user))
	return nil
}

// pgShellUser is a role's name as it goes after -u in a command printed for
// pasting. A PostgreSQL role may hold spaces, quotes and capitals (CREATE
// ROLE "App RO"); such a name is not pasted bare, where the shell would split
// it or read its quotes: the person types it.
func pgShellUser(user string) string {
	if user == "" {
		return "<user>"
	}
	for _, c := range user {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '@', c == '-':
		default:
			return "<user>"
		}
	}
	return user
}
