package consoleapp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/console"
)

// The commands in this file's family (`server add`, `server list`, ...) do
// not do the work themselves: they ask the daemon that is already running,
// over the same API the web interface uses. So a server added from a
// terminal goes through the same checks, gets the same name and is saved in
// the same place as one added from the browser.
//
// How a command finds the daemon and is let in, in order:
//
//  1. The file the daemon writes when it starts (console.LocalAccess): its
//     address and a token for that run. Nothing to configure.
//  2. BINTRAIL_CONSOLE_TOKEN, the operator's own token, with the address
//     from --url, BINTRAIL_CONSOLE_LISTEN or the default. For a daemon too
//     old to write the file, or one on another machine.

// packagedStateDir is where the Linux packages keep the daemon's files. A
// shell there has none of the service's environment, so the path the
// service was told is looked for by name.
const packagedStateDir = "/var/lib/bintrail"

// ctlTimeout bounds one request. The checks of `server add` reach a database
// that may be far away, one round trip each, so it is generous.
const ctlTimeout = 10 * time.Minute

type ctlClient struct {
	base  string
	token string
	// accessFile is the LocalAccess file the token came from, "" when it
	// came from the environment. For the message of a refused token.
	accessFile string
	hc         *http.Client
}

// ctlAPIError is an answer the daemon gave that was not a success.
type ctlAPIError struct {
	Status int
	Msg    string
}

func (e *ctlAPIError) Error() string { return e.Msg }

// addCtlFlags gives a command group the two ways to point at a daemon.
func addCtlFlags(cmd *cobra.Command, url, accessFile *string) {
	cmd.PersistentFlags().StringVar(url, "url", "", "Address of the running DBTrail, e.g. http://127.0.0.1:8090 (default: the one it wrote down when it started)")
	cmd.PersistentFlags().StringVar(accessFile, "access-file", "", "The file a running DBTrail writes for commands on its machine (default: "+console.LocalAccessFileName+" beside its login file)")
}

// localAccessCandidates lists where the daemon's LocalAccess file may be.
func localAccessCandidates(explicit string) []string {
	if explicit != "" {
		return []string{explicit}
	}
	out := []string{console.LocalAccessPath(os.Getenv("BINTRAIL_CONSOLE_AUTH"))}
	if p := packagedStateDir + "/" + console.LocalAccessFileName; p != out[0] {
		out = append(out, p)
	}
	return out
}

// resolveCtl works out which daemon to talk to and with what.
func resolveCtl(url, accessFile string) (*ctlClient, error) {
	cli.LoadEnvFile()
	paths := localAccessCandidates(accessFile)
	var acc console.LocalAccess
	var accPath string
	var denied []string
	for _, p := range paths {
		a, err := console.ReadLocalAccess(p)
		if err == nil {
			acc, accPath = a, p
			break
		}
		switch {
		case errors.Is(err, fs.ErrPermission):
			denied = append(denied, p)
		case errors.Is(err, fs.ErrNotExist):
		default:
			return nil, err
		}
	}
	c := &ctlClient{hc: &http.Client{
		Timeout: ctlTimeout,
		// DBTrail never redirects its API. Followed, a redirect would turn a
		// POST into a GET of somewhere else and report whatever that said.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
	switch {
	case accPath != "":
		c.token, c.accessFile, c.base = acc.Token, accPath, acc.URL
	case os.Getenv("BINTRAIL_CONSOLE_TOKEN") != "":
		c.token = os.Getenv("BINTRAIL_CONSOLE_TOKEN")
		c.base = "http://" + listenToLocal(os.Getenv("BINTRAIL_CONSOLE_LISTEN"))
	case len(denied) > 0:
		return nil, fmt.Errorf("this account may not read %s, the file DBTrail wrote for commands on its machine.\nRun the command with sudo, or as the account the service runs as", strings.Join(denied, " or "))
	default:
		return nil, fmt.Errorf(`this command asks the running DBTrail, and found no way to reach it.
It looked for %s, which DBTrail writes when it starts.

  With Docker, run the command inside the container:
      docker compose exec bintrail bintrail-console %s
  With the Linux packages, check the service is running:
      systemctl status bintrail-console
  Anywhere else, set BINTRAIL_CONSOLE_TOKEN to the token DBTrail was started with`,
			strings.Join(paths, " or "), strings.Join(os.Args[1:], " "))
	}
	if url != "" {
		c.base = url
	}
	c.base = strings.TrimRight(c.base, "/")
	return c, nil
}

// listenToLocal turns a bind address into one to dial from this machine.
func listenToLocal(listen string) string {
	if listen == "" {
		return "127.0.0.1:8090"
	}
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	return net.JoinHostPort(host, port)
}

// do sends one request. body and out may be nil. A status outside 2xx is a
// *ctlAPIError carrying the daemon's own sentence.
func (c *ctlClient) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return fmt.Errorf("%s is not an address DBTrail can be reached at: %w", c.base, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		var certErr *tls.CertificateVerificationError
		var uerr *url.Error
		switch {
		case errors.As(err, &certErr):
			return fmt.Errorf("%s answered with a certificate this machine does not accept for that address: %w\nGive the name the certificate was issued for: --url https://<name>", c.base, err)
		case errors.As(err, &uerr) && uerr.Timeout():
			return fmt.Errorf("%s did not answer within %s. What was asked may still have been done: see `bintrail-console server list`", c.base, ctlTimeout)
		}
		return fmt.Errorf(`nothing answered at %s: %w
Is DBTrail running?  Docker: docker compose ps   Linux packages: systemctl status bintrail-console`, c.base, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return fmt.Errorf("the answer from %s was cut short: %w", c.base, err)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		from := "BINTRAIL_CONSOLE_TOKEN"
		if c.accessFile != "" {
			from = c.accessFile
		}
		return &ctlAPIError{Status: resp.StatusCode, Msg: fmt.Sprintf(
			"DBTrail at %s did not accept the token from %s.\nIt makes a new one each time it starts; if another DBTrail runs on this machine, point at the right one with --url and --access-file",
			c.base, from)}
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(data, &e) != nil || e.Error == "" {
			e.Error = fmt.Sprintf("%s answered %s, which is not something DBTrail says. Is that address DBTrail's, and is it http or https there?", c.base, resp.Status)
		}
		return &ctlAPIError{Status: resp.StatusCode, Msg: e.Error}
	}
	if out == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%s answered something that is not DBTrail's: %w", c.base, err)
	}
	return nil
}

// asker asks the person at the terminal. Commands take it as a value so a
// test can answer for them.
type asker interface {
	// Interactive reports whether there is anyone to ask.
	Interactive() bool
	// Line asks a question whose answer may be shown; def is the answer of
	// an empty line.
	Line(question, def string) (string, error)
	// Secret asks a question whose answer must not be echoed.
	Secret(question string) (string, error)
}

// terminalAsker asks on stderr and reads stdin, so stdout stays clean for
// what the command prints.
type terminalAsker struct {
	in       *bufio.Reader
	isTerm   func() bool
	readPass func() ([]byte, error)
}

func (a *terminalAsker) Interactive() bool { return a.isTerm() }

func (a *terminalAsker) Line(question, def string) (string, error) {
	if def != "" {
		fmt.Fprintf(os.Stderr, "%s [%s]: ", question, def)
	} else {
		fmt.Fprintf(os.Stderr, "%s: ", question)
	}
	line, err := a.in.ReadString('\n')
	if err != nil && line == "" {
		return "", fmt.Errorf("no answer to %q: %w", question, err)
	}
	if line = strings.TrimSpace(line); line == "" {
		return def, nil
	}
	return line, nil
}

func (a *terminalAsker) Secret(question string) (string, error) {
	fmt.Fprintf(os.Stderr, "%s: ", question)
	p, err := a.readPass()
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", fmt.Errorf("read password: %w", err)
	}
	return string(p), nil
}
