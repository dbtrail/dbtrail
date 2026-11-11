package consoleapp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/console"
)

// `server edit`, `server start|stop` and `snapshot location`: changing a
// server that is already added, from a terminal.
//
// An edit sends only what it was asked to change, to the route that keeps
// the rest (PATCH /api/servers/{id}); the snapshot location has had such a
// route since it moved to its own page (PUT /api/backup-settings/servers/{id}).

// editFlags is what `server edit` was given. Each value counts only when
// its flag was named: an empty --schemas is "every schema", not "unchanged".
type editFlags struct {
	Name, Host, Port, User        string
	AskPassword, PasswordStdin    bool
	Schemas                       string
	ArchiveS3                     string
	S3Endpoint, S3Region, S3Style string
	S3AccessKey                   string
	S3SecretStdin                 bool
}

var srvEdit editFlags

var serverEditCmd = &cobra.Command{
	Use:   "edit <server>",
	Short: "Change a server that is already added: its name, address, login, schemas or S3",
	Long: `Changes what you name and leaves the rest of the server as it is.

  bintrail-console server edit shop -p                      a new password for DBTrail's user
  bintrail-console server edit shop -h db2.example.com      the database moved
  bintrail-console server edit shop --name store            rename it
  bintrail-console server edit shop --schemas shop,billing  record these schemas only
  bintrail-console server edit shop --schemas ""            record every schema again
  bintrail-console server edit shop --archive-s3 s3://bucket/archive/

Changing the address, user or password stops recording, saves, runs the
checks against the new login and starts again. If the checks fail the change
stays saved and recording stays stopped, with the fix printed:
"bintrail-console server start <server>" starts it once fixed.

S3. --archive-s3 is where the hours that leave the index are kept. The
snapshots' own S3 location is "bintrail-console snapshot location". DBTrail
uses the machine's own AWS credentials unless you give it others, for this
server's buckets:

  echo "$SECRET" | bintrail-console server edit shop --s3-access-key AKIA... --s3-secret-stdin
  bintrail-console server edit shop --s3-endpoint https://minio.example.com --s3-region us-east-1

The name is also the user name on the MySQL port: renaming changes what
clients log in as.`,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("name one server: server edit <server> [what to change]\n" + noPasswordOnCommandLine)
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		if passwordOnCommandLine(os.Args[1:]) {
			return errors.New(noPasswordOnCommandLine)
		}
		c, err := resolveCtl(srvURL, srvAccessFile)
		if err != nil {
			return err
		}
		return runServerEdit(cmd.Context(), c, args[0], srvEdit, cmd.Flags().Changed, newTerminalAsker(), os.Stdin, cmdOut())
	},
}

func init() {
	f := serverEditCmd.Flags()
	f.Bool("help", false, "Help for edit")
	f.StringVar(&srvEdit.Name, "name", "", "A new name. It is the user name on the MySQL port")
	f.StringVarP(&srvEdit.Host, "host", "h", "", "The database's new host")
	f.StringVarP(&srvEdit.Port, "port", "P", "", "The database's new port")
	f.StringVarP(&srvEdit.User, "user", "u", "", "The user DBTrail logs in as")
	f.BoolVarP(&srvEdit.AskPassword, "password", "p", false, "Ask for that user's new password (it is never taken on the command line)")
	f.BoolVar(&srvEdit.PasswordStdin, "password-stdin", false, "Read that user's new password from the first line of stdin")
	f.StringVar(&srvEdit.Schemas, "schemas", "", `Comma-separated schemas to record; "" records every user schema`)
	f.StringVar(&srvEdit.ArchiveS3, "archive-s3", "", `Where the hours that leave the index are kept, as s3://bucket/prefix/; "" stops keeping them`)
	f.StringVar(&srvEdit.S3Endpoint, "s3-endpoint", "", `Endpoint of an S3-compatible store for this server's buckets; "" is AWS`)
	f.StringVar(&srvEdit.S3Region, "s3-region", "", "Region of this server's buckets")
	f.StringVar(&srvEdit.S3Style, "s3-path-style", "", `How the store is addressed: path or virtual; "" lets DBTrail choose`)
	f.StringVar(&srvEdit.S3AccessKey, "s3-access-key", "", `Access key for this server's buckets; "" goes back to the machine's own credentials`)
	f.BoolVar(&srvEdit.S3SecretStdin, "s3-secret-stdin", false, "Read the secret key of --s3-access-key from the first line of stdin")
	serverEditCmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		if passwordOnCommandLine(os.Args[1:]) {
			return errors.New(noPasswordOnCommandLine)
		}
		return err
	})

	verb := func(use, short string, start bool) *cobra.Command {
		return &cobra.Command{
			Use: use + " <server>", Short: short, Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				c, err := resolveCtl(srvURL, srvAccessFile)
				if err != nil {
					return err
				}
				return runServerStartStop(cmd.Context(), c, args[0], start, cmdOut())
			},
		}
	}
	serverCmd.AddCommand(serverEditCmd,
		verb("start", "Start recording a server's changes, after its checks pass", true),
		verb("stop", "Stop recording a server's changes. It stays on the list", false))

	loc := &cobra.Command{
		Use:   "location <server>",
		Short: "Say where a server's snapshots are kept: a folder, S3, or both",
		Long: `With nothing else, prints where the server's snapshots are kept. With an
option, changes that one setting and leaves the others.

  bintrail-console snapshot location shop
  bintrail-console snapshot location shop --s3 s3://bucket/snapshots/
  bintrail-console snapshot location shop --s3 s3://bucket/snapshots/ --s3-delete-after 30d
  bintrail-console snapshot location shop --dir /data/snapshots --keep-newest 3
  bintrail-console snapshot location shop --s3 ""        stop sending them to S3
  bintrail-console snapshot location shop --s3-only      keep them in S3 only

The credentials for that bucket are the machine's own, or the ones set with
"bintrail-console server edit <server> --s3-access-key".`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveCtl(srvURL, srvAccessFile)
			if err != nil {
				return err
			}
			return runSnapshotLocation(cmd.Context(), c, args[0], snapLoc, cmd.Flags().Changed, cmdOut())
		},
	}
	lf := loc.Flags()
	lf.StringVar(&snapLoc.Dir, "dir", "", "Folder on DBTrail's machine the snapshots are kept in")
	lf.StringVar(&snapLoc.S3, "s3", "", `Where the snapshots are also sent, as s3://bucket/prefix/; "" stops sending them`)
	lf.StringVar(&snapLoc.S3DeleteAfter, "s3-delete-after", "", `Delete a snapshot from S3 once it is this old: 30d, 12h; "" never deletes`)
	lf.IntVar(&snapLoc.KeepNewest, "keep-newest", 0, "How many snapshots to keep in the folder; 0 keeps every one")
	lf.BoolVar(&snapLoc.S3Only, "s3-only", false, "Keep the snapshots in S3 only, with no folder on DBTrail's machine")
	snapshotCmd.AddCommand(loc)
}

func firstLine(r io.Reader, what string) (string, error) {
	line, err := bufio.NewReader(r).ReadString('\n')
	if err != nil && line == "" {
		return "", fmt.Errorf("%s: nothing to read on stdin (%v)", what, err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return "", fmt.Errorf("%s: the first line of stdin is empty", what)
	}
	return line, nil
}

// editBody turns the flags that were named into the fields to change.
// source says the change is to where or how DBTrail logs in.
func editBody(f editFlags, named func(string) bool, ask asker, stdin io.Reader, serverName string) (body map[string]any, source bool, err error) {
	body = map[string]any{}
	if f.AskPassword && f.PasswordStdin {
		return nil, false, errors.New("-p and --password-stdin both say where the password comes from: use one")
	}
	if f.PasswordStdin && f.S3SecretStdin {
		return nil, false, errors.New("--password-stdin and --s3-secret-stdin both read stdin: change one, then the other")
	}
	if f.S3SecretStdin && !named("s3-access-key") {
		return nil, false, errors.New("--s3-secret-stdin is the secret of an access key: name it with --s3-access-key")
	}
	set := func(flag, field, value string, allowEmpty bool) error {
		if !named(flag) {
			return nil
		}
		value = strings.TrimSpace(value)
		if value == "" && !allowEmpty {
			return fmt.Errorf("--%s is empty", flag)
		}
		body[field] = value
		return nil
	}
	for _, s := range []struct {
		flag, field, value string
		allowEmpty         bool
	}{
		{"name", "name", f.Name, false},
		{"host", "source_host", f.Host, false},
		{"port", "source_port", f.Port, false},
		{"user", "source_user", f.User, false},
		{"schemas", "schemas", f.Schemas, true},
		{"archive-s3", "archive_s3", f.ArchiveS3, true},
		{"s3-endpoint", "s3_endpoint", f.S3Endpoint, true},
		{"s3-region", "s3_region", f.S3Region, true},
		{"s3-path-style", "s3_path_style", f.S3Style, true},
		{"s3-access-key", "s3_access_key_id", f.S3AccessKey, true},
	} {
		if err := set(s.flag, s.field, s.value, s.allowEmpty); err != nil {
			return nil, false, err
		}
	}
	if p, ok := body["source_port"].(string); ok {
		if n, perr := strconv.Atoi(p); perr != nil || n < 1 || n > 65535 {
			return nil, false, fmt.Errorf("the port is a number from 1 to 65535, not %q", p)
		}
	}
	if h, ok := body["source_host"].(string); ok {
		if len(h) > 2 && h[0] == '[' && h[len(h)-1] == ']' {
			h = h[1 : len(h)-1]
			body["source_host"] = h
		}
		if strings.ContainsAny(h, " \t/@[]") || strings.Count(h, ":") == 1 {
			return nil, false, fmt.Errorf("%q is not a host name or address. The port goes in -P", h)
		}
	}
	switch {
	case f.PasswordStdin:
		pw, perr := firstLine(stdin, "--password-stdin")
		if perr != nil {
			return nil, false, perr
		}
		body["source_password"] = pw
	case f.AskPassword:
		if !ask.Interactive() {
			return nil, false, errors.New("-p: this is not a terminal, so the password cannot be asked for. Use --password-stdin")
		}
		pw, perr := ask.Secret(fmt.Sprintf("New password of DBTrail's user for %s", serverName))
		if perr != nil {
			return nil, false, perr
		}
		if pw == "" {
			return nil, false, errors.New("the password is empty")
		}
		body["source_password"] = pw
	}
	if f.S3SecretStdin {
		secret, serr := firstLine(stdin, "--s3-secret-stdin")
		if serr != nil {
			return nil, false, serr
		}
		body["s3_secret_access_key"] = secret
	}
	if len(body) == 0 {
		return nil, false, errors.New("nothing to change: name what to change, for example --name, -p or --schemas")
	}
	for _, k := range []string{"source_host", "source_port", "source_user", "source_password"} {
		if _, ok := body[k]; ok {
			source = true
		}
	}
	return body, source, nil
}

func runServerEdit(ctx context.Context, c *ctlClient, name string, f editFlags, named func(string) bool, ask asker, stdin io.Reader, out io.Writer) error {
	s, err := serverByName(ctx, c, name)
	if err != nil {
		return err
	}
	body, source, err := editBody(f, named, ask, stdin, s.Name)
	if err != nil {
		return err
	}
	path := "/api/servers/" + s.ID
	recording := s.MonitorState == "running" || s.MonitorState == "pending" || s.MonitorState == "stalled" || s.MonitorState == "failed" || s.MonitorState == "lost_position"
	if !source || !recording {
		var saved ctlServer
		if err := c.do(ctx, http.MethodPatch, path, body, &saved); err != nil {
			return err
		}
		fmt.Fprintln(out, editedText(s, saved, body))
		if source && s.HasSource {
			fmt.Fprintf(out, "It is not being recorded. Start it:  bintrail-console server start %s\n", savedName(s, saved))
		}
		return nil
	}

	// A change of address or login, on a server being recorded: the stream
	// is stopped first (the daemon refuses to re-point one under it), and
	// started again with the checks run against what was just saved.
	fmt.Fprintf(out, "Stopping the recording of %q to change how DBTrail reaches it ...\n", s.Name)
	if err := c.do(ctx, http.MethodPost, path+"/monitor/stop", nil, nil); err != nil {
		return fmt.Errorf("nothing was changed: the recording of %q could not be stopped: %w", s.Name, err)
	}
	var saved ctlServer
	if err := c.do(ctx, http.MethodPatch, path, body, &saved); err != nil {
		// Not saved: put the recording back the way it was.
		if started, serr := startRecording(ctx, c, s.ID, io.Discard); serr != nil || !started {
			return fmt.Errorf("the change was refused: %w\nRecording of %q was stopped for it and did not start again: bintrail-console server start %s", err, s.Name, s.Name)
		}
		return fmt.Errorf("the change was refused, and %q is being recorded as before: %w", s.Name, err)
	}
	fmt.Fprintln(out, editedText(s, saved, body))
	started, err := startRecording(ctx, c, s.ID, out)
	now := savedName(s, saved)
	switch {
	case err != nil:
		return fmt.Errorf("the change is saved, and recording did not start again: %w\nbintrail-console server start %s", err, now)
	case !started:
		return fmt.Errorf("the change is saved, and recording did not start again: a check failed against the new address or login. Fix it, then: bintrail-console server start %s", now)
	}
	fmt.Fprintf(out, "Recording %q again.\n", now)
	return nil
}

func savedName(before, saved ctlServer) string {
	if saved.Name != "" {
		return saved.Name
	}
	return before.Name
}

// editedText says what an edit changed, by field, never a secret's value.
func editedText(before, saved ctlServer, body map[string]any) string {
	var parts []string
	say := func(field, text string) {
		if _, ok := body[field]; ok {
			parts = append(parts, text)
		}
	}
	say("name", fmt.Sprintf("renamed to %q", saved.Name))
	if _, h := body["source_host"]; h || body["source_port"] != nil {
		parts = append(parts, "address "+saved.address())
	}
	say("source_user", "user "+saved.SourceUser)
	say("source_password", "a new password")
	if v, ok := body["schemas"].(string); ok {
		if v == "" {
			parts = append(parts, "every schema")
		} else {
			parts = append(parts, "schemas "+v)
		}
	}
	if v, ok := body["archive_s3"].(string); ok {
		if v == "" {
			parts = append(parts, "no S3 archive")
		} else {
			parts = append(parts, "archive "+v)
		}
	}
	say("s3_endpoint", "S3 endpoint")
	say("s3_region", "S3 region")
	say("s3_path_style", "S3 addressing")
	if v, ok := body["s3_access_key_id"].(string); ok {
		if v == "" {
			parts = append(parts, "the machine's own S3 credentials")
		} else {
			parts = append(parts, "S3 access key "+v)
		}
	}
	return fmt.Sprintf("Changed %q: %s.", before.Name, strings.Join(parts, ", "))
}

// startRecording asks the daemon to start a server's stream. It prints the
// checks when they are what stopped it, and reports whether it started.
func startRecording(ctx context.Context, c *ctlClient, id string, out io.Writer) (bool, error) {
	var res struct {
		Doctor  *console.DoctorReport `json:"doctor"`
		Started *bool                 `json:"started"`
	}
	if err := c.do(ctx, http.MethodPost, "/api/servers/"+id+"/monitor/start", nil, &res); err != nil {
		return false, err
	}
	if res.Started == nil {
		return false, fmt.Errorf("%s answered, and not whether recording started. Is that address DBTrail's?", c.base)
	}
	if !*res.Started {
		printChecks(out, res.Doctor, false)
	}
	return *res.Started, nil
}

func runServerStartStop(ctx context.Context, c *ctlClient, name string, start bool, out io.Writer) error {
	s, err := serverByName(ctx, c, name)
	if err != nil {
		return err
	}
	if !start {
		if err := c.do(ctx, http.MethodPost, "/api/servers/"+s.ID+"/monitor/stop", nil, nil); err != nil {
			return err
		}
		fmt.Fprintf(out, "Stopped recording %q. Changes made on it from now are not recorded until:  bintrail-console server start %s\n", s.Name, s.Name)
		return nil
	}
	started, err := startRecording(ctx, c, s.ID, out)
	if err != nil {
		return err
	}
	if !started {
		return fmt.Errorf("%q is not being recorded: a check failed. Fix it and run the command again", s.Name)
	}
	fmt.Fprintf(out, "Recording %q.\n", s.Name)
	return nil
}

// The daemon's route and fields for where snapshots are kept keep the names
// they had before the product said "snapshot" (#1573). Spelled once each.
const (
	snapshotSettingsRoute = "/api/backup-settings"
	locKeyDir             = "baseline_dir"
	locKeyS3              = "baseline_s3"
	locKeyS3Retain        = "baseline_s3_retain"
)

// locFlags is what `snapshot location` was given.
type locFlags struct {
	Dir, S3, S3DeleteAfter string
	KeepNewest             int
	S3Only                 bool
}

var snapLoc locFlags

// ctlLocation is one server's snapshot settings, as the daemon answers them.
type ctlLocation struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	BaselineDir string `json:"baseline_dir"`
	BaselineS3  string `json:"baseline_s3"`
	ResolvedDir string `json:"resolved_dir"`
	ResolvedS3  string `json:"resolved_s3"`
}

func (l ctlLocation) text(name string) string {
	dir, s3 := l.ResolvedDir, l.ResolvedS3
	if dir == "" {
		dir = l.BaselineDir
	}
	if s3 == "" {
		s3 = l.BaselineS3
	}
	switch {
	case dir != "" && s3 != "":
		return fmt.Sprintf("%s: snapshots are kept in %s and sent to %s.", name, dir, s3)
	case dir != "":
		return fmt.Sprintf("%s: snapshots are kept in %s. They are not sent to S3.", name, dir)
	case s3 != "":
		return fmt.Sprintf("%s: snapshots are kept in %s only.", name, s3)
	}
	return fmt.Sprintf("%s: no place is set for its snapshots.", name)
}

func runSnapshotLocation(ctx context.Context, c *ctlClient, name string, f locFlags, named func(string) bool, out io.Writer) error {
	s, err := serverByName(ctx, c, name)
	if err != nil {
		return err
	}
	body := map[string]any{}
	if named("dir") {
		if strings.TrimSpace(f.Dir) == "" {
			return errors.New("--dir is empty. To keep snapshots in S3 only, use --s3-only")
		}
		body[locKeyDir] = strings.TrimSpace(f.Dir)
	}
	if named("s3-only") {
		if named("dir") && f.S3Only {
			return errors.New("--dir names a folder and --s3-only says there is none: use one")
		}
		// false puts the folder back, the default one unless --dir names it.
		body["local_copy"] = !f.S3Only
	}
	if named("s3") {
		v := strings.TrimSpace(f.S3)
		if v != "" && !strings.HasPrefix(v, "s3://") {
			return fmt.Errorf("--s3 is s3://bucket/prefix/, not %q", v)
		}
		body[locKeyS3] = v
	}
	if named("s3-delete-after") {
		body[locKeyS3Retain] = strings.TrimSpace(f.S3DeleteAfter)
	}
	if named("keep-newest") {
		if f.KeepNewest < 0 {
			return errors.New("--keep-newest is 0 (keep every snapshot) or more")
		}
		body["keep_newest"] = f.KeepNewest
	}
	if len(body) == 0 {
		var all struct {
			Servers *[]ctlLocation `json:"servers"`
		}
		if err := c.do(ctx, http.MethodGet, snapshotSettingsRoute, nil, &all); err != nil {
			return err
		}
		if all.Servers == nil {
			return fmt.Errorf("%s answered, and not with the snapshot settings. Is that address DBTrail's?", c.base)
		}
		for _, l := range *all.Servers {
			if l.ID == s.ID {
				fmt.Fprintln(out, l.text(s.Name))
				return nil
			}
		}
		return fmt.Errorf("DBTrail lists no snapshot settings for %q", s.Name)
	}
	var saved ctlLocation
	if err := c.do(ctx, http.MethodPut, snapshotSettingsRoute+"/servers/"+s.ID, body, &saved); err != nil {
		return err
	}
	if saved.ID == "" {
		return fmt.Errorf("%s answered, and not with %q's snapshot settings. See `bintrail-console snapshot location %s`", c.base, s.Name, s.Name)
	}
	fmt.Fprintln(out, saved.text(s.Name))
	if v, ok := body[locKeyS3Retain].(string); ok {
		if v == "" {
			fmt.Fprintln(out, "Snapshots in S3 are never deleted by DBTrail.")
		} else {
			fmt.Fprintf(out, "A snapshot in S3 is deleted once it is %s old.\n", v)
		}
	}
	if v, ok := body["keep_newest"].(int); ok {
		if v == 0 {
			fmt.Fprintln(out, "Every snapshot in the folder is kept.")
		} else {
			fmt.Fprintf(out, "The folder keeps the newest %s.\n", plural(v, "snapshot"))
		}
	}
	return nil
}
