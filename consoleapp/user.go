package consoleapp

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/term"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/console"
)

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// The `user` subcommands manage the console's single login credential — a
// username + bcrypt hash in a 0600 YAML file (default
// ~/.config/bintrail/console-auth.yaml). A RUNNING server picks up
// set-password on the next login attempt (the file is re-read per login); it
// does NOT revoke live sessions — rotate via the console's change-password
// dialog for immediate revocation, or restart the server.
//
// There is deliberately no way to pass the password as a flag or env var:
// flags land in shell history and `ps`, env vars in `docker inspect`. The
// choices are an interactive prompt or --password-stdin.

var userCmd = &cobra.Command{
	Use:   "user",
	Short: "Manage the web interface's password login (single-user)",
}

var (
	usrAuthFile        string
	usrUsername        string
	usrPasswordStdin   bool
	usrSkipIfUnchanged bool
	usrYes             bool
	usrFormat          string
)

var userSetPasswordCmd = &cobra.Command{
	Use:   "set-password",
	Short: "Set (or rotate) the web interface login password",
	Long: `Sets the web interface's username+password credential, enabling password login.

Prompts twice on a terminal; use --password-stdin to read one line from stdin
in scripts. A running server honors the new password on the next login
attempt without a restart (live sessions survive a CLI rotation; rotate from
the web interface to also revoke them).`,
	RunE: runUserSetPassword,
}

var userRemoveCmd = &cobra.Command{
	Use:   "remove",
	Short: "Remove the password credential (the web interface reverts to token-only auth)",
	RunE:  runUserRemove,
}

var userStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show the configured web interface user (never prints secrets)",
	RunE:  runUserStatus,
}

func init() {
	for _, c := range []*cobra.Command{userSetPasswordCmd, userRemoveCmd, userStatusCmd} {
		c.Flags().StringVar(&usrAuthFile, "auth-file", "", "Path to the web interface auth file (default ~/.config/bintrail/console-auth.yaml)")
	}
	userSetPasswordCmd.Flags().StringVar(&usrUsername, "username", "", `Login username (default "admin", or the currently configured one)`)
	userSetPasswordCmd.Flags().BoolVar(&usrPasswordStdin, "password-stdin", false, "Read the password from the first line of stdin (for scripts)")
	userSetPasswordCmd.Flags().BoolVar(&usrSkipIfUnchanged, "skip-if-unchanged", false, "Exit 0 without writing when the stored credential already matches (idempotent runs)")
	userRemoveCmd.Flags().BoolVar(&usrYes, "yes", false, "Do not ask for confirmation")
	userStatusCmd.Flags().StringVar(&usrFormat, "format", "text", "Output format: text or json")
	userCmd.AddCommand(userSetPasswordCmd, userRemoveCmd, userStatusCmd)
	rootCmd.AddCommand(userCmd)
}

// resolveAuthPath applies flag > env > default for the auth-file location.
// Every error path downstream prints the RESOLVED path: with no home
// directory the default anchors beside the working directory instead of a
// config directory, and the path is the only way to see that.
func resolveAuthPath(cmd *cobra.Command) string {
	cli.LoadEnvFile()
	if usrAuthFile != "" {
		return usrAuthFile
	}
	if !cmd.Flags().Changed("auth-file") {
		if v := os.Getenv("BINTRAIL_CONSOLE_AUTH"); v != "" {
			return v
		}
	}
	return console.DefaultAuthPath()
}

func runUserSetPassword(cmd *cobra.Command, args []string) error {
	path := resolveAuthPath(cmd)

	var password string
	switch {
	case usrPasswordStdin:
		sc := bufio.NewScanner(os.Stdin)
		if !sc.Scan() {
			return fmt.Errorf("--password-stdin: no input on stdin (%v)", sc.Err())
		}
		password = strings.TrimRight(sc.Text(), "\r\n")
	case term.IsTerminal(int(os.Stdin.Fd())):
		p, err := promptPasswordTwice()
		if err != nil {
			return err
		}
		password = p
	default:
		return errors.New("stdin is not a terminal: pass --password-stdin to read the password from stdin")
	}

	if err := console.ValidateNewPassword(password); err != nil {
		return err
	}

	existing, err := console.LoadAuthFile(path)
	if err != nil {
		return err
	}
	if usrSkipIfUnchanged && existing != nil {
		user := usrUsername
		if user == "" {
			user = existing.Username
		}
		if existing.VerifyPassword(user, password) {
			fmt.Fprintf(os.Stderr, "Credential unchanged for user %q (%s); nothing written.\n", user, path)
			return nil
		}
	}

	if err := console.SetAuthPassword(path, usrUsername, password); err != nil {
		if errors.Is(err, bcrypt.ErrPasswordTooLong) {
			return errors.New("password exceeds bcrypt's 72-byte limit; choose a shorter one")
		}
		return err
	}
	a, err := console.LoadAuthFile(path)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Web interface password set for user %q (%s).\nA running server accepts it on the next login; no restart needed.\n", a.Username, path)
	return nil
}

func promptPasswordTwice() (string, error) {
	fmt.Fprint(os.Stderr, "New password: ")
	p1, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", fmt.Errorf("read password: %w", err)
	}
	fmt.Fprint(os.Stderr, "Retype to confirm: ")
	p2, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", fmt.Errorf("read password: %w", err)
	}
	if string(p1) != string(p2) {
		return "", errors.New("passwords do not match")
	}
	return string(p1), nil
}

func runUserRemove(cmd *cobra.Command, args []string) error {
	path := resolveAuthPath(cmd)
	a, err := console.LoadAuthFile(path)
	if err != nil {
		return err
	}
	if a == nil {
		fmt.Fprintf(os.Stderr, "No password is configured (%s); nothing to remove.\n", path)
		return nil
	}
	if !usrYes {
		fmt.Fprintf(os.Stderr, "Remove the password credential for user %q (%s)? The web interface reverts to token-only auth. [y/N] ", a.Username, path)
		sc := bufio.NewScanner(os.Stdin)
		if !sc.Scan() || !strings.EqualFold(strings.TrimSpace(sc.Text()), "y") {
			return errors.New("aborted")
		}
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove the web interface auth file %s: %w", path, err)
	}
	fmt.Fprintf(os.Stderr, "Password credential removed (%s). The web interface now requires its access token.\n", path)
	fmt.Fprintln(os.Stderr, "NOTE: a running server stops accepting NEW password logins immediately, but")
	fmt.Fprintln(os.Stderr, "live sessions ride out their TTL; restart it to revoke them. A server bound")
	fmt.Fprintln(os.Stderr, "to a non-loopback address with no --token will refuse its next restart.")
	return nil
}

func runUserStatus(cmd *cobra.Command, args []string) error {
	path := resolveAuthPath(cmd)
	if usrFormat != "text" && usrFormat != "json" {
		return fmt.Errorf("invalid format %q: must be text or json", usrFormat)
	}
	a, err := console.LoadAuthFile(path)
	if err != nil {
		return err
	}
	if usrFormat == "json" {
		out := map[string]any{"configured": a != nil, "path": path}
		if a != nil {
			cost, _ := bcrypt.Cost([]byte(a.PasswordBcrypt))
			out["username"] = a.Username
			out["hash"] = fmt.Sprintf("bcrypt(cost=%d)", cost)
			out["updated_at"] = a.UpdatedAt
			out["read_only"] = a.ReadOnly()
		}
		return printJSON(out)
	}
	if a == nil {
		fmt.Printf("Password login: not configured (%s)\n", path)
		return nil
	}
	cost, _ := bcrypt.Cost([]byte(a.PasswordBcrypt))
	fmt.Printf("Password login: configured\n  username:   %s\n  hash:       bcrypt(cost=%d)\n  updated_at: %s\n  file:       %s\n", a.Username, cost, a.UpdatedAt, path)
	if a.ReadOnly() {
		fmt.Println("  note:       written by a newer bintrail; logins work, changes refused")
	}
	return nil
}
