package consoleapp

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The forwarding user of a PostgreSQL in `server add` (#3020). What is given
// is settled by resolveForward, as for a MySQL: --forward-user,
// [dbtrail-forward] in the option file, or a question once the server is
// added. It is saved by setForwardUser, the one `server forward-user` saves
// with, so a user that command refuses is refused here the same way. Only the
// sentences are a PostgreSQL's: its port, and what it sends to the source.

// pgForwardsAsLine says what a PostgreSQL server's forwarding user is for,
// once it is set. One sentence for `server forward-user` and `server add`.
func pgForwardsAsLine(name, user string) string {
	return fmt.Sprintf("%q forwards as %s: the PostgreSQL port sends to your PostgreSQL, as that user and read-only, every statement the copy is not known to answer as PostgreSQL does, and a client's transaction blocks.", name, user)
}

// pgNoForwardYetLine says where a PostgreSQL server stands without a
// forwarding user, and the command that sets one.
func pgNoForwardYetLine(name string) string {
	return fmt.Sprintf("Until it has a forwarding user, the copy answers them alone, and a statement it cannot answer gets an error:\n  bintrail-console server forward-user %s -u <user> -p", mysqlLineUser(name))
}

// addForwardUserPG is addForwardUser for a PostgreSQL: it sets the
// forwarding account of a server `server add` just added, the one
// resolveForward settled or one asked for now. Enter at the question skips
// it. A failure leaves the server added and recording, and says so
// (errAddedNoForward).
func addForwardUserPG(ctx context.Context, c *ctlClient, in addInput, res ctlCheckResponse, out io.Writer) error {
	later := mysqlLineUser(res.Name)
	user, password := in.FwdUser, in.FwdPassword
	if user == "" {
		if !in.FwdAsk || in.ask == nil {
			return nil
		}
		fmt.Fprintf(out, "\nThe forwarding user: what DBTrail's PostgreSQL port runs statements as on your PostgreSQL, read-only, when the copy is not known to answer them as PostgreSQL does, and a client's transaction blocks. A role of its own that can log in, with SELECT on what the port should read and no more.\n")
		var err error
		if user, err = in.ask.Line("Forwarding user (Enter to skip)", ""); err != nil {
			return fmt.Errorf("%w: %v", errAddedNoForward, err)
		}
		if user = strings.TrimSpace(user); user == "" {
			fmt.Fprintf(out, "Skipped: the copy answers alone. Set it later:  bintrail-console server forward-user %s -u <user> -p\n\n", later)
			return nil
		}
		if user == in.User {
			return fmt.Errorf("%w. %s is the user DBTrail reads %q with, and the port never forwards as that one. Set another one:  bintrail-console server forward-user %s -u <user> -p",
				errAddedNoForward, user, res.Name, later)
		}
		if password, err = in.ask.Secret(fmt.Sprintf("Password of %s on your PostgreSQL", user)); err != nil {
			return fmt.Errorf("%w: %v", errAddedNoForward, err)
		}
		if password == "" {
			return fmt.Errorf("%w: the password is empty. Set it:  bintrail-console server forward-user %s -u %s -p", errAddedNoForward, later, pgShellUser(user))
		}
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
		return fmt.Errorf("%w: %v\nSet it once that is fixed:  bintrail-console server forward-user %s -u %s -p", errAddedNoForward, err, later, pgShellUser(user))
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
