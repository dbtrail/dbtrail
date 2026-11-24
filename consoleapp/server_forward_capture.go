package consoleapp

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/dbtrail/dbtrail/internal/console"
)

// runForwardCaptureAnswer is `server forward-user <server> --capture-account`
// and `--no-capture-account` (#3054): it saves the answer about forwarding
// with the user DBTrail captures with, and says what the server's port does
// with it now.
func runForwardCaptureAnswer(ctx context.Context, c *ctlClient, name string, on bool, out io.Writer) error {
	s, err := serverByName(ctx, c, name)
	if err != nil {
		return err
	}
	var saved ctlServer
	if err := c.do(ctx, http.MethodPut, "/api/servers/"+s.ID+"/route", map[string]bool{"capture_account": on}, &saved); err != nil {
		return err
	}
	if saved.ForwardWithCaptureAccount != on {
		return fmt.Errorf("DBTrail took the request and did not save the answer for %q. See `bintrail-console server list`", s.Name)
	}
	fmt.Fprintln(out, captureAnswerLine(saved))
	if saved.CaptureAccountWarning != "" {
		fmt.Fprintf(out, "Warning: %s\n", saved.CaptureAccountWarning)
	}
	return nil
}

// captureAnswerLine says, for a server whose answer was just saved, which
// account its port forwards with now.
func captureAnswerLine(s ctlServer) string {
	pg := s.Flavor == console.FlavorPostgres
	switch {
	case !s.ForwardWithCaptureAccount && s.HasRoute:
		return fmt.Sprintf("%q does not forward with the user DBTrail captures with. It forwards as its forwarding user, %s, as before.", s.Name, s.RouteUser)
	case !s.ForwardWithCaptureAccount && pg:
		return fmt.Sprintf("%q does not forward with the user DBTrail captures with. It has no forwarding user of its own, so the PostgreSQL port does not serve it: a client that connects gets an error that says why and how to serve it.", s.Name)
	case !s.ForwardWithCaptureAccount:
		return fmt.Sprintf("%q does not forward with the user DBTrail captures with. It has no forwarding user, so a statement the copy cannot answer is refused.", s.Name)
	case !pg && s.HasRoute:
		return fmt.Sprintf("Saved, and not used: the MySQL port does not forward with the user DBTrail captures with. %q keeps forwarding as its forwarding user, %s.", s.Name, s.RouteUser)
	case !pg:
		return fmt.Sprintf("Saved, and not used yet: the MySQL port does not forward with the user DBTrail captures with. Until %q has a forwarding user, a statement the copy cannot answer is refused:\n  bintrail-console server forward-user %s -u <user> -p", s.Name, mysqlLineUser(s.Name))
	case s.HasRoute:
		return fmt.Sprintf("Saved. %q keeps forwarding as its forwarding user, %s, while that one is set. Removed (--remove), the user DBTrail captures with takes its place.", s.Name, s.RouteUser)
	case s.RouteUnreadable:
		return fmt.Sprintf("Saved, and not used yet: %q has a saved forwarding user that cannot be read, and while it is saved nothing is forwarded. Remove it:  bintrail-console server forward-user %s --remove", s.Name, mysqlLineUser(s.Name))
	case s.CaptureAccountRefused != "":
		return fmt.Sprintf("Saved, and not used: %s.", s.CaptureAccountRefused)
	}
	return fmt.Sprintf("%q forwards with the user DBTrail captures with (%s): the PostgreSQL port sends to your PostgreSQL, read-only, a SELECT the copy does not answer, and only when every table it reads is a table of the copy. Anything else is refused and not sent. A server that hides tables or columns sends nothing.", s.Name, s.SourceUser)
}
