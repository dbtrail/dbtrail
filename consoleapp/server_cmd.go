package consoleapp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/mycnf"
)

// `server add|list|remove`: the web interface's "+ Add server", from a
// terminal. The options of `add` are the mysql client's (-h -P -u -p,
// --defaults-file), because whoever runs this has typed those a thousand
// times.

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Add, list and remove the databases DBTrail reads, from the terminal",
	Long: `Add, list and remove the databases DBTrail reads.

These commands ask the DBTrail that is already running, so they do what the
web interface's "+ Add server" does: the same checks, the same result.

  bintrail-console server add -h db1.example.com -u dbtrail -p
  bintrail-console server add --defaults-file ~/.my.cnf
  bintrail-console server list
  bintrail-console server remove shop

With Docker Compose, the installer leaves a dbtrail command beside
docker-compose.yml that runs these inside the container:

  ./dbtrail server add -h host.docker.internal -u dbtrail -p`,
}

var (
	srvURL        string
	srvAccessFile string

	srvAdd     addFlags
	srvFormat  string
	srvYes     bool
	srvVerbose bool
)

// addFlags is what `server add` was given on its command line.
type addFlags struct {
	Host          string
	Port          string
	User          string
	AskPassword   bool
	PasswordStdin bool
	DefaultsFile  string
	Name          string
	Schemas       string
	Flavor        string
	// ForwardUser is the forwarding account (#2686): the user the MySQL port
	// runs statements as on this database when the copy cannot answer.
	// NoForward: do not ask for one.
	ForwardUser string
	NoForward   bool
}

var serverAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Check a database and start recording its changes",
	Long: `Checks a MySQL or MariaDB and, when every check passes, saves it and starts
recording its changes. A check that fails is printed with its fix, and
nothing is saved.

The options are the mysql client's. On a terminal, what you leave out is
asked for.

  bintrail-console server add -h db1.example.com -P 3306 -u dbtrail -p
  bintrail-console server add --defaults-file ~/.my.cnf --name shop
  echo "$PASSWORD" | bintrail-console server add -h db1 -u dbtrail --password-stdin

--defaults-file reads host, port, user and password from [client], and from
[dbtrail] when the file has one, which wins. An option on the command line
wins over the file.

The password is never taken on the command line, where the shell history
and ps would keep it: -p asks for it, --password-stdin reads it from a
pipe, --defaults-file reads it from a file.

The forwarding user is asked for once the server is added, on a terminal:
the user DBTrail's MySQL port runs statements as on this database when the
copy cannot answer them. Give it your application's grants and no more.
Enter skips it. Without a terminal, put it in the option file:

  [dbtrail-forward]
  user=shop_fwd
  password=<its password>

--forward-user names it on the command line (its password is then asked
for, or read from that group); --no-forward does not ask.

DBTrail's user on your database, if it does not exist yet:

  CREATE USER 'dbtrail'@'%' IDENTIFIED BY <a password>;
  GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'dbtrail'@'%';
  GRANT RELOAD, BACKUP_ADMIN, PROCESS, SHOW VIEW ON *.* TO 'dbtrail'@'%';

MariaDB has no BACKUP_ADMIN: leave that word out. On Amazon RDS and Aurora
the last line is these two instead:

  GRANT LOCK TABLES, SHOW VIEW ON *.* TO 'dbtrail'@'%';
  GRANT SELECT ON mysql.rds_configuration TO 'dbtrail'@'%';`,
	// Not cobra.NoArgs: its error repeats the argument, and the argument a
	// mysql user leaves here is a password (`-p SECRET`).
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) > 0 {
			return errors.New("server add takes options only, and found a word that belongs to none.\n" + noPasswordOnCommandLine)
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		if passwordOnCommandLine(os.Args[1:]) {
			return errors.New(noPasswordOnCommandLine)
		}
		// Before any question: a person should not type a password to be
		// told DBTrail is not running.
		c, err := resolveCtl(srvURL, srvAccessFile)
		if err != nil {
			return err
		}
		in, err := resolveAddInput(srvAdd, newTerminalAsker(), os.Stdin)
		if err != nil {
			return err
		}
		if in.IncludesSkipped > 0 {
			file := srvAdd.DefaultsFile
			if file == "-" {
				file = "the option file"
			}
			fmt.Fprintf(os.Stderr, "note: %s has %s, which DBTrail does not follow. Only that file was read.\n",
				file, plural(in.IncludesSkipped, "!include line"))
		}
		return runServerAdd(cmd.Context(), c, in, srvVerbose, cmdOut())
	},
}

var serverListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List the databases DBTrail reads, and whether each is being recorded",
	Args:    cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if srvFormat != "text" && srvFormat != "json" {
			return fmt.Errorf("--format is text or json, not %q", srvFormat)
		}
		c, err := resolveCtl(srvURL, srvAccessFile)
		if err != nil {
			return err
		}
		return runServerList(cmd.Context(), c, srvFormat, cmdOut())
	},
}

var serverRemoveCmd = &cobra.Command{
	Use:     "remove <name>",
	Aliases: []string{"rm"},
	Short:   "Stop recording a database and take it off the list",
	Long: `Stops recording a database's changes and takes it off the list. What DBTrail
already recorded and copied is not deleted.

Asks before doing it; --yes does not ask.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := resolveCtl(srvURL, srvAccessFile)
		if err != nil {
			return err
		}
		return runServerRemove(cmd.Context(), c, args[0], srvYes, newTerminalAsker(), cmdOut())
	},
}

func init() {
	addCtlFlags(serverCmd, &srvURL, &srvAccessFile)

	f := serverAddCmd.Flags()
	// -h is the host here, as in mysql. Declaring --help with no letter is
	// what frees it: cobra only adds its own "-h, --help" when none exists.
	f.Bool("help", false, "Help for add")
	f.StringVarP(&srvAdd.Host, "host", "h", "", "Host of the database")
	f.StringVarP(&srvAdd.Port, "port", "P", "", "Port of the database (default 3306)")
	f.StringVarP(&srvAdd.User, "user", "u", "", "User DBTrail logs in as")
	f.BoolVarP(&srvAdd.AskPassword, "password", "p", false, "Ask for the password (it is never taken on the command line)")
	f.BoolVar(&srvAdd.PasswordStdin, "password-stdin", false, "Read the password from the first line of stdin (for scripts)")
	f.StringVar(&srvAdd.DefaultsFile, "defaults-file", "", "Read host, port, user and password from this MySQL option file: [client], then [dbtrail]. - reads it from stdin")
	f.StringVar(&srvAdd.Name, "name", "", "What to call the server (default: made from its address). It is the user name on DBTrail's MySQL port")
	f.StringVar(&srvAdd.Schemas, "schemas", "", "Comma-separated schemas to record (default: every user schema)")
	f.StringVar(&srvAdd.Flavor, "flavor", "", "mysql or mariadb (default: asked of the server)")
	f.StringVar(&srvAdd.ForwardUser, "forward-user", "", "The user the MySQL port runs statements as on this database when the copy cannot answer (default: asked, or [dbtrail-forward] in --defaults-file)")
	f.BoolVar(&srvAdd.NoForward, "no-forward", false, "Do not ask for a forwarding user")
	f.BoolVarP(&srvVerbose, "verbose", "v", false, "Also print how to turn on each optional improvement")
	serverAddCmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		// `-pSECRET` and `--password=SECRET` are what a mysql user types, and
		// the parser's own error repeats what was typed. Say it without.
		if passwordOnCommandLine(os.Args[1:]) {
			return errors.New(noPasswordOnCommandLine)
		}
		return err
	})

	serverListCmd.Flags().StringVar(&srvFormat, "format", "text", "Output format: text or json")
	serverRemoveCmd.Flags().BoolVar(&srvYes, "yes", false, "Do not ask for confirmation")

	fu := serverForwardUserCmd.Flags()
	fu.StringVarP(&srvFwd.User, "user", "u", "", "The user on your MySQL that DBTrail forwards statements as")
	fu.BoolVarP(&srvFwd.AskPassword, "password", "p", false, "Ask for the password (it is never taken on the command line)")
	fu.BoolVar(&srvFwd.PasswordStdin, "password-stdin", false, "Read the password from the first line of stdin (for scripts)")
	fu.BoolVar(&srvFwd.Remove, "remove", false, "Remove the forwarding user")
	serverForwardUserCmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		if passwordOnCommandLine(os.Args[1:]) {
			return errors.New(noPasswordOnCommandLine)
		}
		return err
	})

	serverCmd.AddCommand(serverAddCmd, serverListCmd, serverRemoveCmd, serverForwardUserCmd)
	rootCmd.AddCommand(serverCmd)
}

const noPasswordOnCommandLine = "The password is never taken on the command line, where the shell history and ps would keep it.\nUse -p alone to be asked for it, --password-stdin, or --defaults-file"

// passwordOnCommandLine reports whether the arguments carry a password the
// way the mysql client takes one: `-pSECRET` or `--password=SECRET`. Here -p
// is a switch, so the flag parser would read `-phunter2` as -p followed by
// -h unter2: no error, and a piece of the password sent off as a host name.
// It must be caught before the parsed values are used for anything.
func passwordOnCommandLine(args []string) bool {
	for _, a := range args {
		if a == "--" {
			return false
		}
		// --password-stdin=X too: the parser's error for a switch given a
		// value repeats the value.
		if strings.HasPrefix(a, "--password=") || strings.HasPrefix(a, "--password-stdin=") {
			return true
		}
		if !strings.HasPrefix(a, "-") || strings.HasPrefix(a, "--") {
			continue
		}
		// A group of one-letter options: -vp is fine, -pv is not (it is what
		// a password starting with v looks like), and after h, P or u the
		// rest of the word is that option's value.
		for i := 1; i < len(a); i++ {
			if a[i] == 'h' || a[i] == 'P' || a[i] == 'u' {
				break
			}
			if a[i] == 'p' && i+1 < len(a) {
				return true
			}
		}
	}
	return false
}

// fwdFlags is what `server forward-user` was given on its command line.
type fwdFlags struct {
	User          string
	AskPassword   bool
	PasswordStdin bool
	Remove        bool
}

var srvFwd fwdFlags

var serverForwardUserCmd = &cobra.Command{
	Use:   "forward-user <server>",
	Short: "Name the user DBTrail runs statements as on your MySQL when the copy cannot answer",
	Long: `With Engine Routing on (bintrail-console port routing on), a statement the
copy cannot answer runs on your MySQL. It runs as this user, one per server.
Give it the grants of the application that connects to the port, and no more:
whoever has the port's password can run what this user can.

  bintrail-console server forward-user shop -u shop_fwd -p
  echo "$PASSWORD" | bintrail-console server forward-user shop -u shop_fwd --password-stdin
  bintrail-console server forward-user shop --remove

On your MySQL, for an application that reads the shop schema:

  CREATE USER 'shop_fwd'@'%' IDENTIFIED BY <a password>;
  GRANT SELECT ON shop.* TO 'shop_fwd'@'%';

It changes this one setting and leaves the rest of the server as it is.`,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("name one server: server forward-user <server> -u <user> -p\n" + noPasswordOnCommandLine)
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
		return runServerForwardUser(cmd.Context(), c, args[0], srvFwd, newTerminalAsker(), os.Stdin, cmdOut())
	},
}

func runServerForwardUser(ctx context.Context, c *ctlClient, name string, f fwdFlags, ask asker, stdin io.Reader, out io.Writer) error {
	user := strings.TrimSpace(f.User)
	switch {
	case f.Remove && (user != "" || f.AskPassword || f.PasswordStdin):
		return errors.New("--remove takes nothing else: it removes the forwarding user")
	case !f.Remove && user == "":
		return errors.New("which user? Give it with -u, or --remove to remove the one set")
	case f.AskPassword && f.PasswordStdin:
		return errors.New("-p and --password-stdin both say where the password comes from: use one")
	}
	s, err := serverByName(ctx, c, name)
	if err != nil {
		return err
	}
	path := "/api/servers/" + s.ID + "/route"
	if f.Remove {
		if err := c.do(ctx, http.MethodDelete, path, nil, nil); err != nil {
			return err
		}
		fmt.Fprintf(out, "%q has no forwarding user now: with Engine Routing on, a statement the copy cannot answer is refused.\n", s.Name)
		return nil
	}
	var password string
	switch {
	case f.PasswordStdin:
		line, rerr := bufio.NewReader(stdin).ReadString('\n')
		if rerr != nil && line == "" {
			return fmt.Errorf("--password-stdin: nothing to read on stdin (%v)", rerr)
		}
		password = strings.TrimRight(line, "\r\n")
	case !ask.Interactive():
		return errors.New("no password: this is not a terminal, so it cannot be asked for. Use --password-stdin")
	default:
		if password, err = ask.Secret(fmt.Sprintf("Password of %s on your MySQL", user)); err != nil {
			return err
		}
	}
	if password == "" {
		return errors.New("the password is empty")
	}
	if err := setForwardUser(ctx, c, s, user, password); err != nil {
		return err
	}
	fmt.Fprintf(out, "%q forwards as %s. Engine Routing uses it once it is on:  bintrail-console port routing on\n", s.Name, user)
	return nil
}

// setForwardUser saves user as server s's forwarding account and checks
// that DBTrail says so. The capture user is refused before anything is sent:
// the port never forwards as it, and saving it would leave an account that
// does nothing. Shared by `server forward-user` and `server add`.
func setForwardUser(ctx context.Context, c *ctlClient, s ctlServer, user, password string) error {
	if s.SourceUser != "" && user == s.SourceUser {
		return fmt.Errorf("%s is the user DBTrail reads %q with, and the port never forwards as that one: it can read far more than an application should. Create another user on your MySQL, with the application's grants, and set that one", user, s.Name)
	}
	var saved ctlServer
	if err := c.do(ctx, http.MethodPut, "/api/servers/"+s.ID+"/route", map[string]string{"user": user, "password": password}, &saved); err != nil {
		return err
	}
	if !saved.HasRoute || saved.RouteUser != user {
		return fmt.Errorf("DBTrail took the request and %q does not forward as %s. See `bintrail-console server list`", s.Name, user)
	}
	// Saved, and of no use: the daemon knows the capture user better than
	// this command does (a name the list did not carry).
	if saved.RouteIsCapture {
		return fmt.Errorf("%s is the user DBTrail reads %q with, and the port never forwards as that one: it can read far more than an application should. Create another user on your MySQL, with the application's grants, and set that one", user, s.Name)
	}
	return nil
}

// addForwardUser sets the forwarding account of a server `server add` just
// added (#2686): the one resolveForward settled, or one asked for now.
// Enter at the question skips it. A failure leaves the server added and
// recording, and says so (errAddedNoForward).
func addForwardUser(ctx context.Context, c *ctlClient, in addInput, res ctlCheckResponse, out io.Writer) error {
	user, password := in.FwdUser, in.FwdPassword
	if user == "" {
		if !in.FwdAsk || in.ask == nil {
			return nil
		}
		fmt.Fprintf(out, "\nThe forwarding user: what DBTrail's MySQL port runs statements as on this database when the copy cannot answer. Give it your application's grants and no more.\n")
		var err error
		if user, err = in.ask.Line("Forwarding user (Enter to skip)", ""); err != nil {
			return fmt.Errorf("%w: %v", errAddedNoForward, err)
		}
		if user = strings.TrimSpace(user); user == "" {
			fmt.Fprintf(out, "Skipped. Set it later:  bintrail-console server forward-user %s -u <user> -p\n\n", mysqlLineUser(res.Name))
			return nil
		}
		if user == in.User {
			return fmt.Errorf("%w. %s is the user DBTrail reads %q with, and the port never forwards as that one. Set another one:  bintrail-console server forward-user %s -u <user> -p",
				errAddedNoForward, user, res.Name, mysqlLineUser(res.Name))
		}
		if password, err = in.ask.Secret(fmt.Sprintf("Password of %s", user)); err != nil {
			return fmt.Errorf("%w: %v", errAddedNoForward, err)
		}
		if password == "" {
			return fmt.Errorf("%w: the password is empty. Set it:  bintrail-console server forward-user %s -u %s -p", errAddedNoForward, mysqlLineUser(res.Name), user)
		}
	}
	if res.Server == nil || res.Server.ID == "" {
		return fmt.Errorf("%w: DBTrail did not say which server it saved. Set it:  bintrail-console server forward-user %s -u %s -p", errAddedNoForward, mysqlLineUser(res.Name), user)
	}
	s := *res.Server
	if s.Name == "" {
		s.Name = res.Name
	}
	if s.SourceUser == "" {
		s.SourceUser = in.User
	}
	if err := setForwardUser(ctx, c, s, user, password); err != nil {
		return fmt.Errorf("%w: %v\nSet it once that is fixed:  bintrail-console server forward-user %s -u %s -p", errAddedNoForward, err, mysqlLineUser(res.Name), user)
	}
	fmt.Fprintf(out, "%q forwards as %s.\n\n", res.Name, user)
	return nil
}

func newTerminalAsker() asker {
	return &terminalAsker{
		in:       bufio.NewReader(os.Stdin),
		isTerm:   func() bool { return term.IsTerminal(int(os.Stdin.Fd())) },
		readPass: func() ([]byte, error) { return term.ReadPassword(int(os.Stdin.Fd())) },
	}
}

// addInput is a database to add, with nothing left to ask.
type addInput struct {
	Host, Port, User, Password string
	Name, Schemas, Flavor      string
	// FwdUser and FwdPassword are the forwarding account, when it is known
	// before the server is added (a flag, the option file, or both).
	// FwdAsk: none is known, and there is someone to ask once the server is
	// added. ask is who is asked; nil asks nobody.
	FwdUser, FwdPassword string
	FwdAsk               bool
	ask                  asker
	// IncludesSkipped counts the option file's !include lines, which were
	// not followed: worth saying when what they hold is what is missing.
	IncludesSkipped int
}

// resolveAddInput settles what to add from, in order of who wins: the
// command line, the option file, the person at the terminal.
func resolveAddInput(f addFlags, ask asker, stdin io.Reader) (addInput, error) {
	in := addInput{
		Host: strings.TrimSpace(f.Host), Port: strings.TrimSpace(f.Port), User: strings.TrimSpace(f.User),
		Name: strings.TrimSpace(f.Name), Schemas: strings.TrimSpace(f.Schemas),
		Flavor: strings.ToLower(strings.TrimSpace(f.Flavor)),
	}
	switch in.Flavor {
	case "", doctor.FlavorMySQL, doctor.FlavorMariaDB:
	default:
		return addInput{}, fmt.Errorf("--flavor is mysql or mariadb, not %q (a PostgreSQL is added in the web interface)", f.Flavor)
	}
	if f.AskPassword && f.PasswordStdin {
		return addInput{}, errors.New("-p and --password-stdin both say where the password comes from: use one")
	}

	// "-" is the option file on stdin, for a caller whose file is not on the
	// disk this program sees (the dbtrail command of a Docker install runs it
	// in a container). Stdin is then spent: nothing can be asked for.
	fileOnStdin := f.DefaultsFile == "-"
	if fileOnStdin && f.PasswordStdin {
		return addInput{}, errors.New("--defaults-file - and --password-stdin both read stdin: put the password in the option file")
	}
	var fwdFile mycnf.Client
	if f.DefaultsFile != "" {
		var file mycnf.File
		var err error
		if fileOnStdin {
			file, err = mycnf.ReadFile(stdin, "on stdin")
		} else {
			file, err = mycnf.Open(f.DefaultsFile)
		}
		if err != nil {
			return addInput{}, err
		}
		cnf, err := file.Client("client", "dbtrail")
		if err != nil {
			return addInput{}, err
		}
		// The forwarding account is its own group and nothing else: read
		// with [client], it would take the capture user, which the port
		// never forwards as.
		if fwdFile, err = file.Client("dbtrail-forward"); err != nil {
			return addInput{}, err
		}
		if in.Host == "" {
			in.Host = cnf.Host
		}
		if in.Port == "" {
			in.Port = cnf.Port
		}
		if in.User == "" {
			in.User = cnf.User
		}
		// -p and --password-stdin say "not the file's".
		if !f.AskPassword && !f.PasswordStdin {
			in.Password = cnf.Password
		}
		in.IncludesSkipped = len(cnf.Includes)
		if in.Host == "" && cnf.Socket != "" {
			return addInput{}, fmt.Errorf("%s connects through a socket (%s) and names no host. DBTrail connects over the network: add -h, or a host line under [dbtrail]", f.DefaultsFile, cnf.Socket)
		}
	}

	interactive := ask.Interactive() && !f.PasswordStdin && !fileOnStdin
	var err error
	if in.Host == "" {
		if !interactive {
			return addInput{}, errors.New("which database? Give its host with -h (or in --defaults-file)")
		}
		if in.Host, err = ask.Line("Host", ""); err != nil {
			return addInput{}, err
		}
		if in.Host == "" {
			return addInput{}, errors.New("no host given")
		}
		if in.Port == "" {
			if in.Port, err = ask.Line("Port", "3306"); err != nil {
				return addInput{}, err
			}
		}
	}
	// An IPv6 address in brackets is the same address; a host:port in one
	// word is not a host.
	if h := in.Host; len(h) > 2 && h[0] == '[' && h[len(h)-1] == ']' {
		in.Host = h[1 : len(h)-1]
	}
	if strings.ContainsAny(in.Host, " \t/@[]") || (strings.Count(in.Host, ":") == 1) {
		return addInput{}, fmt.Errorf("%q is not a host name or address. The port goes in -P", in.Host)
	}
	if in.Port == "" {
		in.Port = "3306"
	}
	if n, perr := strconv.Atoi(in.Port); perr != nil || n < 1 || n > 65535 {
		return addInput{}, fmt.Errorf("the port is a number from 1 to 65535, not %q", in.Port)
	}
	if in.User == "" {
		if !interactive {
			return addInput{}, errors.New("which user? Give it with -u (or in --defaults-file)")
		}
		if in.User, err = ask.Line("User", "dbtrail"); err != nil {
			return addInput{}, err
		}
	}

	switch {
	case f.PasswordStdin:
		line, rerr := bufio.NewReader(stdin).ReadString('\n')
		if rerr != nil && line == "" {
			return addInput{}, fmt.Errorf("--password-stdin: nothing to read on stdin (%v)", rerr)
		}
		in.Password = strings.TrimRight(line, "\r\n")
	case in.Password == "":
		if fileOnStdin {
			return addInput{}, errors.New("the option file has no password. It was read from stdin, so the password cannot be asked for: add a password line to it")
		}
		if !ask.Interactive() {
			return addInput{}, errors.New("no password: this is not a terminal, so it cannot be asked for. Use --password-stdin or --defaults-file")
		}
		if in.Password, err = ask.Secret(fmt.Sprintf("Password of %s on %s", in.User, in.Host)); err != nil {
			return addInput{}, err
		}
	}
	if in.Password == "" {
		return addInput{}, errors.New("the password is empty. DBTrail needs a user with a password")
	}
	if err := resolveForward(&in, f, fwdFile, ask, ask.Interactive() && !f.PasswordStdin && !fileOnStdin); err != nil {
		return addInput{}, err
	}
	return in, nil
}

// resolveForward settles the forwarding account before anything is checked
// or saved (#2686): from --forward-user, from the option file's
// [dbtrail-forward], or, when neither names one, to be asked for once the
// server is added. Every refusal here happens with nothing saved.
func resolveForward(in *addInput, f addFlags, file mycnf.Client, ask asker, canAsk bool) error {
	flagUser := strings.TrimSpace(f.ForwardUser)
	if f.NoForward && (flagUser != "" || file.User != "") {
		named := "--forward-user"
		if flagUser == "" {
			named = "[dbtrail-forward] in the option file"
		}
		return fmt.Errorf("--no-forward and %s say opposite things: use one", named)
	}
	if f.NoForward {
		return nil
	}
	in.FwdUser = flagUser
	if in.FwdUser == "" {
		in.FwdUser = file.User
	}
	if in.FwdUser == "" {
		if file.Password != "" {
			return errors.New("[dbtrail-forward] in the option file has a password and no user: add a user line")
		}
		if canAsk {
			in.FwdAsk, in.ask = true, ask
		}
		return nil
	}
	if in.FwdUser == in.User {
		return fmt.Errorf("%s is the user DBTrail reads this database with, and the port never forwards as that one: it can read far more than an application should. Name another user, with your application's grants", in.FwdUser)
	}
	// The file's password goes with the file's user, not with a different
	// user named on the command line.
	if file.User == "" || file.User == in.FwdUser {
		in.FwdPassword = file.Password
	}
	if in.FwdPassword != "" {
		return nil
	}
	if !canAsk {
		return fmt.Errorf("no password for the forwarding user %s: this is not a terminal, so it cannot be asked for. Put it under [dbtrail-forward] in the option file, or use --no-forward", in.FwdUser)
	}
	pw, err := ask.Secret(fmt.Sprintf("Password of the forwarding user %s", in.FwdUser))
	if err != nil {
		return err
	}
	if pw == "" {
		return errors.New("the forwarding user's password is empty")
	}
	in.FwdPassword = pw
	return nil
}

// What the daemon answers, as far as these commands read it.
type ctlCheckResponse struct {
	OK     bool                  `json:"ok"`
	Name   string                `json:"name"`
	Doctor *console.DoctorReport `json:"doctor"`
	Error  string                `json:"error"`
	Kept   bool                  `json:"kept"`
	// Port is the MySQL port after the server was added (#2686). Absent
	// from a DBTrail older than that, which says nothing about the port.
	Port *ctlNewServerPort `json:"port"`
	// Server is the server as it was saved; its id is where the forwarding
	// account goes.
	Server *ctlServer `json:"server"`
}

// ctlNewServerPort is the port as the answer to an added server gives it:
// its state and, when this add turned it on, the password made for it.
type ctlNewServerPort struct {
	ctlPort
	TurnedOn    bool   `json:"turned_on"`
	NotTurnedOn string `json:"not_turned_on"`
}

type ctlServer struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	HasSource  bool   `json:"has_source"`
	SourceHost string `json:"source_host"`
	SourcePort string `json:"source_port"`
	SourceUser string `json:"source_user"`
	Schemas    string `json:"schemas,omitempty"`
	// HasRoute and RouteUser are the forwarding account, when one is set.
	HasRoute  bool   `json:"has_route,omitempty"`
	RouteUser string `json:"route_user,omitempty"`
	// RouteIsCapture: the saved forwarding account is the one DBTrail
	// captures with, which the port refuses to forward as.
	RouteIsCapture bool `json:"route_is_capture,omitempty"`
	// SQLFresh is the mode this server's entry asks for on the MySQL port
	// (fresh, or fast); SQLFreshSaved says it was chosen, as opposed to the
	// default. What the router applies is `port mode`'s to say.
	SQLFresh      bool `json:"sql_fresh"`
	SQLFreshSaved bool `json:"sql_fresh_saved"`
	// MonitorState is whether its changes are being recorded: running,
	// pending, stopped, failed... Empty for a server with no source, and on
	// a DBTrail that records nothing itself (`serve`).
	MonitorState string `json:"monitor_state,omitempty"`
}

// mode is how `server list` shows the mode saved for the server's statements
// on the MySQL port: fresh or fast, "(default)" when nothing was chosen. The
// server DBTrail was started with has no choice of its own and is fast; one
// with no source is not answered on the port.
func (s ctlServer) mode() string {
	switch {
	case s.Kind == "ephemeral":
		return "fast"
	case !s.HasSource:
		return "-"
	}
	m := "fast"
	if s.SQLFresh {
		m = "fresh"
	}
	if !s.SQLFreshSaved {
		m += " (default)"
	}
	return m
}

// recording is the word `server list` shows for the server's state.
func (s ctlServer) recording() string {
	switch {
	case s.MonitorState != "":
		return s.MonitorState
	case s.Kind == "ephemeral":
		// The one DBTrail was started with: it has no entry to stop or remove.
		return "set at startup"
	case s.HasSource:
		return "not by this DBTrail"
	}
	return "-"
}

func (s ctlServer) address() string {
	if s.SourceHost == "" {
		return "-"
	}
	return net.JoinHostPort(s.SourceHost, s.SourcePort)
}

// errNotAdded is every way `server add` ends with nothing saved after the
// daemon was asked; errAddedStopped is the one way it ends with the server
// saved and not recording.
var (
	errNotAdded     = errors.New("the database was not added")
	errAddedStopped = errors.New("the database is on the list, stopped")
	// errAddedNoForward: the database was added and is recording, and its
	// forwarding user was not set.
	errAddedNoForward = errors.New("the database was added and is recording, without a forwarding user")
)

func runServerAdd(ctx context.Context, c *ctlClient, in addInput, verbose bool, out io.Writer) error {
	// What answers there, before any login: its flavor, and the one mistake
	// worth fixing without asking, a 127.0.0.1 typed for a DBTrail that runs
	// in a container, where that address is the container itself. --flavor
	// overrides the flavor it finds, not the question.
	{
		var id doctor.Identification
		err := c.do(ctx, http.MethodPost, "/api/servers/identify",
			map[string]string{"source_host": in.Host, "source_port": in.Port}, &id)
		var apiErr *ctlAPIError
		switch {
		case err == nil:
			if id.Kind == doctor.KindLoopbackInContainer && id.Suggest != "" {
				if h, p, serr := net.SplitHostPort(id.Suggest); serr == nil {
					fmt.Fprintf(out, "DBTrail runs in a container, where %s is the container itself. A database answers at %s: using that.\n",
						in.Host, id.Suggest)
					in.Host, in.Port = h, p
				}
			}
			if in.Flavor == "" {
				in.Flavor = id.Flavor
			}
		case errors.As(err, &apiErr) && apiErr.Status == http.StatusBadRequest:
			// An address it would not even try. The checks below say why.
		default:
			return err
		}
	}
	if in.Flavor == "" {
		in.Flavor = doctor.FlavorMySQL
	}

	fmt.Fprintf(out, "Checking %s as %s ...\n", net.JoinHostPort(in.Host, in.Port), in.User)
	var res ctlCheckResponse
	err := c.do(ctx, http.MethodPost, "/api/servers/check", map[string]any{
		"flavor": in.Flavor, "name": in.Name,
		"source_host": in.Host, "source_port": in.Port,
		"source_user": in.User, "source_password": in.Password,
		"schemas": in.Schemas,
		// Not the Connect screen: leave the form someone may be filling in
		// a browser as it is.
		"no_draft": true,
	}, &res)
	if err != nil {
		return err
	}
	failed := printChecks(out, res.Doctor, verbose)
	switch {
	case res.OK:
		fmt.Fprintf(out, "\nAdded %q. DBTrail is recording its changes.\n", res.Name)
		// The port's password exists in this answer only: printed before the
		// forwarding user is asked for or set, so neither a Ctrl-C at the
		// question nor a refused account loses it.
		printNewServerPort(out, res.Port, res.Name, in.FwdUser == "")
		if err := addForwardUser(ctx, c, in, res, out); err != nil {
			fmt.Fprintf(out, "Next, the first copy of its tables:  bintrail-console snapshot run %s\n", res.Name)
			return err
		}
		fmt.Fprintf(out, "Next, the first copy of its tables:  bintrail-console snapshot run %s\n", res.Name)
		return nil
	case failed > 0:
		it := "them"
		if failed == 1 {
			it = "it"
		}
		return fmt.Errorf("%w: %s failed. Fix %s and run the command again", errNotAdded, plural(failed, "check"), it)
	case res.Error != "" && res.Kept:
		return fmt.Errorf("%w: the checks passed, and recording did not start: %s\nSee `bintrail-console server list`", errAddedStopped, res.Error)
	case res.Error != "":
		return fmt.Errorf("%w: the checks passed, and recording did not start: %s", errNotAdded, res.Error)
	default:
		return errors.New("DBTrail did not say whether the database was added. See `bintrail-console server list`")
	}
}

// printNewServerPort says where the MySQL port stands after a server was
// added (#2686). Adding the first server turns the port on, with Engine
// Routing on, and this is the one place its password is shown. Nothing is
// printed for a DBTrail that says nothing about the port (an older one): the
// lines after it are then the ones it always printed.
func printNewServerPort(out io.Writer, p *ctlNewServerPort, name string, noForwardYet bool) {
	if p == nil || p.Enabled == nil {
		return
	}
	user := mysqlLineUser(name)
	switch {
	case p.TurnedOn:
		fmt.Fprintf(out, "\nThe MySQL port is on now, with Engine Routing on. The user is the server's name:\n\n  mysql -h %s -u %s -p\n\n", p.where(), user)
		if p.Password != "" {
			fmt.Fprintf(out, "Password (shown only now, keep it): %s\n", p.Password)
		} else {
			// Turned on by this request and no password in the answer: the
			// person must not be left thinking one was shown.
			fmt.Fprintln(out, "No password came with the answer. Make one:  bintrail-console port new-password")
		}
		fmt.Fprintf(out, "Statements for %q are answered once its first copy is ready.\n", name)
		if noForwardYet {
			fmt.Fprintf(out, "Until it has a forwarding user, the copy answers them alone:\n  bintrail-console server forward-user %s -u <user> -p\n", user)
		}
	case *p.Enabled:
		routing := "off"
		if p.RoutingOn {
			routing = "on"
		}
		fmt.Fprintf(out, "\nThe MySQL port is on, with Engine Routing %s, and takes the password it already has:\n\n  mysql -h %s -u %s -p\n", routing, p.where(), user)
	case p.NotTurnedOn != "":
		fmt.Fprintf(out, "\nThe MySQL port was not turned on: %s.\n", strings.TrimRight(p.NotTurnedOn, "."))
	default:
		fmt.Fprintln(out, "\nThe MySQL port is off, as it was saved. Turn it on:  bintrail-console port on")
	}
	if p.Error != "" && *p.Enabled {
		fmt.Fprintf(out, "It is not answering: %s\n", p.Error)
	}
	// What the daemon knows stops the port from answering (an older compose
	// file with no router): said, never left out after "it is on now".
	if p.Notice != "" && *p.Enabled {
		fmt.Fprintf(out, "%s\n", p.Notice)
	}
	fmt.Fprintln(out)
}

// printChecks prints the report: a line per check and, under one that failed
// or warned, why and what fixes it. A warning about something DBTrail works
// fine without is kept to its one line unless verbose, as the web interface
// folds it under "Optional improvements". It returns how many failed.
func printChecks(out io.Writer, r *console.DoctorReport, verbose bool) int {
	if r == nil {
		return 0
	}
	label := map[string]string{"pass": "ok  ", "fail": "FAIL", "warn": "warn", "skip": "skip"}
	folded := 0
	for _, ck := range r.Checks {
		l, ok := label[ck.Status]
		if !ok {
			l = ck.Status
		}
		if ck.Status == "warn" && ck.Optional {
			l = "tip "
		}
		fmt.Fprintf(out, "  %s  %s\n", l, ck.Name)
		if ck.Status == "pass" || ck.Status == "skip" {
			continue
		}
		if ck.Optional && ck.Status == "warn" && !verbose {
			folded++
			continue
		}
		for _, text := range []string{ck.Detail, ck.Remediation} {
			for line := range strings.SplitSeq(strings.TrimRight(text, " \t\r\n"), "\n") {
				if strings.TrimSpace(line) != "" {
					fmt.Fprintf(out, "        %s\n", strings.TrimRight(line, " \t\r"))
				}
			}
		}
		for _, st := range ck.Statements {
			fmt.Fprintf(out, "        %s\n", st)
		}
	}
	if folded > 0 {
		fmt.Fprintf(out, "  (tip = optional, DBTrail works without it. -v prints how to turn each on.)\n")
	}
	return r.Failed
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return strconv.Itoa(n) + " " + word + "s"
}

// listServers reads the list. An answer with no list in it is an error: a
// 200 from something that is not DBTrail must not read as "no servers".
func listServers(ctx context.Context, c *ctlClient) ([]ctlServer, error) {
	var res struct {
		Servers *[]ctlServer `json:"servers"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/servers", nil, &res); err != nil {
		return nil, err
	}
	if res.Servers == nil {
		return nil, fmt.Errorf("%s answered, and not with a list of servers. Is that address DBTrail's?", c.base)
	}
	return *res.Servers, nil
}

func runServerList(ctx context.Context, c *ctlClient, format string, out io.Writer) error {
	servers, err := listServers(ctx, c)
	if err != nil {
		return err
	}
	if format == "json" {
		if servers == nil {
			servers = []ctlServer{}
		}
		return printJSONTo(out, servers)
	}
	if len(servers) == 0 {
		fmt.Fprintln(out, "No servers yet. Add one:  bintrail-console server add -h <host> -u dbtrail -p")
		return nil
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tADDRESS\tUSER\tRECORDING\tFORWARDS AS\tMODE")
	for _, s := range servers {
		user := s.SourceUser
		if user == "" {
			user = "-"
		}
		fwd := s.RouteUser
		switch {
		case !s.HasRoute || fwd == "":
			fwd = "-"
		case s.RouteIsCapture:
			fwd += " (refused: it is the user above)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", s.Name, s.address(), user, s.recording(), fwd, s.mode())
	}
	return tw.Flush()
}

// findServer picks the server a person named: by its exact name or id, or,
// when exactly one differs only in letter case, that one.
func findServer(servers []ctlServer, name string) (ctlServer, error) {
	var loose []ctlServer
	for _, s := range servers {
		if s.Name == name || s.ID == name {
			return s, nil
		}
		if strings.EqualFold(s.Name, name) {
			loose = append(loose, s)
		}
	}
	switch len(loose) {
	case 1:
		return loose[0], nil
	case 0:
		names := make([]string, 0, len(servers))
		for _, s := range servers {
			names = append(names, s.Name)
		}
		if len(names) == 0 {
			return ctlServer{}, fmt.Errorf("there is no server %q: the list is empty", name)
		}
		return ctlServer{}, fmt.Errorf("there is no server %q. The list has: %s", name, strings.Join(names, ", "))
	default:
		return ctlServer{}, fmt.Errorf("%q matches %d servers that differ only in capital letters: type the name exactly", name, len(loose))
	}
}

func runServerRemove(ctx context.Context, c *ctlClient, name string, yes bool, ask asker, out io.Writer) error {
	servers, err := listServers(ctx, c)
	if err != nil {
		return err
	}
	s, err := findServer(servers, name)
	if err != nil {
		return err
	}
	if !yes {
		if !ask.Interactive() {
			return errors.New("this is not a terminal, so it cannot ask for confirmation: add --yes")
		}
		answer, err := ask.Line(fmt.Sprintf("Stop recording %q (%s) and take it off the list? What was already recorded is kept. [y/N]", s.Name, s.address()), "")
		if err != nil {
			return err
		}
		if a := strings.ToLower(answer); a != "y" && a != "yes" {
			fmt.Fprintln(out, "Nothing was changed.")
			return nil
		}
	}
	stopped := false
	if s.HasSource {
		err := c.do(ctx, http.MethodPost, "/api/servers/"+s.ID+"/monitor/stop", nil, nil)
		var apiErr *ctlAPIError
		switch {
		case err == nil:
			stopped = true
		case errors.As(err, &apiErr) && apiErr.Status == http.StatusForbidden:
			// A DBTrail that records nothing (`serve`) has nothing to stop.
		default:
			// The stop saves "do not record this" before it stops the
			// stream, so a failure here may already have changed that.
			return fmt.Errorf("%q was not removed: its recording could not be stopped: %w\nIt may now be marked as stopped: see `bintrail-console server list`", s.Name, err)
		}
	}
	if err := c.do(ctx, http.MethodDelete, "/api/servers/"+s.ID, nil, nil); err != nil {
		if stopped {
			return fmt.Errorf("recording of %q was stopped, and the server is still on the list: %w", s.Name, err)
		}
		return err
	}
	fmt.Fprintf(out, "Removed %q.\n", s.Name)
	return nil
}
