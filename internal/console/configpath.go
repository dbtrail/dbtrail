package console

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// configPathWarnOnce keeps the homeless warning to one line per process. Every
// Default*Path() runs inside a run function, several of them per boot and some
// per cycle, so warning on each call would bury the log it belongs in.
var configPathWarnOnce sync.Once

// configPath returns ~/.config/bintrail/<name> — the console's on-disk state
// directory, shared by the server registry, the credential file, the managed
// MCP token, and (as siblings derived from the registry path) the verify and
// baseline run histories.
//
// With no home directory — a daemon started by a process manager that passes
// no HOME, systemd DynamicUser, a scrubbed container — the fallback anchors in
// the working directory EXPLICITLY. The obvious spelling,
// filepath.Join(".", ".config", ...), does NOT do that: Join Cleans the
// leading "." away and yields a RELATIVE path. That designates the SAME
// directory this does, since resolution happens against the working directory
// either way and nothing outside tests calls os.Chdir — so this moved no data
// and there is nothing to migrate. What it buys is a path that can be READ: a
// failure now names a directory the operator can go and look at, and the
// anchor is stated rather than left to whenever the syscall runs (#1487: every
// baseline refresh logged `open .config/bintrail/.baseline-history-…: no such
// file or directory`, which names nothing findable).
func configPath(name string) string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		wd, wdErr := os.Getwd()
		if wdErr != nil {
			// Neither a home nor a working directory: there is nothing left to
			// anchor to. Return the bare relative path rather than an empty
			// one so the caller's error at least names the file.
			return filepath.Join(".config", "bintrail", name)
		}
		warnHomelessConfigDir(filepath.Join(wd, ".config", "bintrail"))
		home = wd
	}
	return filepath.Join(home, ".config", "bintrail", name)
}

// warnHomelessConfigDir reports, once, that the saved state is anchored beside
// the working directory instead of a config directory.
//
// This is not decoration. Every reader on this path is quiet by design: a
// missing registry loads as an EMPTY registry with no error, a missing auth
// file lands in the first-run password setup, a missing managed MCP token just
// 401s its clients. So a homeless daemon comes up with no servers and an offer
// to create a password, which looks like a console nobody configured yet
// rather than one detached from its state. Under systemd's default
// WorkingDirectory=/ (and in a container, whose default cwd is also /) that
// state sits in /.config/bintrail: it persists under systemd, but in a
// container it lives in the writable layer and dies with the container.
//
// Warning, never fatal. The console is a recovery path; it does not refuse to
// boot over where its own state file landed.
func warnHomelessConfigDir(dir string) {
	configPathWarnOnce.Do(func() {
		slog.Warn("no home directory could be resolved, so the saved state is anchored beside the working directory instead of a config directory. An existing registry elsewhere will not be found and the console will come up with no servers, as if it had never been configured",
			"dir", dir,
			"fix", "set HOME for the service, or pass explicit paths (CLI: --servers-file on serve, --console-servers-file on watch, or BINTRAIL_CONSOLE_SERVERS; --auth-file or BINTRAIL_CONSOLE_AUTH; --mcp-token-file, --console-mcp-token-file on watch, or BINTRAIL_CONSOLE_MCP_TOKEN_FILE)")
	})
}
