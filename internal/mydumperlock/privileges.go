// Package mydumperlock checks whether a source grants the privileges
// mydumper's point-consistent sync mode needs, BEFORE mydumper is launched.
//
// It exists because the failure is not clean: granting BACKUP_ADMIN without
// RELOAD/FLUSH_TABLES makes the pinned mydumper build SEGFAULT rather than
// report a privilege error (#800, reproduced on amd64 and arm64). It lives in
// its own package because BOTH baseline surfaces need it — `bintrail dump` and
// the console's in-process pipeline — and since #1377 made the point-consistent
// mode the DEFAULT, a surface without this check walks into that crash by
// doing nothing unusual.
package mydumperlock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
)

// requiresBackupAdmin reports whether the connected server needs the
// BACKUP_ADMIN privilege for FTWRL's `LOCK INSTANCE FOR BACKUP` step.
// BACKUP_ADMIN is a MySQL 8.0+ dynamic privilege (also present on Percona
// Server 8.0+, a MySQL 8.0 fork) — it does not exist on MariaDB (any version)
// or MySQL 5.7, and neither issues `LOCK INSTANCE FOR BACKUP`, so FTWRL there
// needs only RELOAD/FLUSH_TABLES (#800 review). Requiring BACKUP_ADMIN
// unconditionally would make point-consistent mode permanently unusable on
// those sources: `GRANT BACKUP_ADMIN` itself errors on MariaDB, since the
// privilege doesn't exist there at all — there would be no way forward.
//
// Detection is a single SELECT VERSION(), mirroring the same MariaDB
// substring check metadata.DetectFlavor uses (self-contained here rather than
// imported, since this also needs the major version number DetectFlavor
// doesn't expose). An unparseable version string defaults to false (does NOT
// require BACKUP_ADMIN) — the safe direction: if the server actually needs it
// and this under-requires, mydumper's own LOCK INSTANCE FOR BACKUP check
// still fails cleanly on its own (missing BACKUP_ADMIN alone is always a
// clean failure, verified — never the segfault); the dangerous direction is
// over-requiring a privilege that doesn't exist on the source's actual
// flavor, which is exactly the bug this function exists to avoid.
func requiresBackupAdmin(ctx context.Context, db *sql.DB) (bool, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return false, fmt.Errorf("point-consistent baseline: cannot read source VERSION() to determine privilege requirements: %w", err)
	}
	if strings.Contains(strings.ToLower(version), "mariadb") {
		return false, nil
	}
	major, ok := serverMajorVersion(version)
	if !ok {
		return false, nil
	}
	return major >= 8, nil
}

// serverMajorVersion extracts the leading major version number from a
// SELECT VERSION() string (e.g. "8.0.46" → 8, "5.7.44" → 5, "10.11.6-MariaDB"
// → 10). Returns ok=false for anything it cannot parse.
func serverMajorVersion(version string) (major int, ok bool) {
	dot := strings.IndexByte(version, '.')
	if dot <= 0 {
		return 0, false
	}
	n, err := strconv.Atoi(version[:dot])
	if err != nil {
		return 0, false
	}
	return n, true
}

// Remedy identifies the CALLING surface, so a refusal can name that surface's
// own way of selecting a different lock mode. It is a parameter because the
// refusal text is the one actionable sentence an operator gets, and naming the
// other surface's knob is worse than naming none: `bintrail dump` does not read
// the console's environment variable, and the console has no flags. Both
// callers reach this on their DEFAULT path since #1377, so this is the first
// thing an upgrading least-privilege deployment sees on either one.
type Remedy string

const (
	// RemedyCLI marks `bintrail dump`, whose knob is the --lock-mode flag.
	RemedyCLI Remedy = "cli"
	// RemedyConsole marks the console daemon, which has no flag surface and is
	// configured by environment variable.
	RemedyConsole Remedy = "console"
	// RemedyConsoleSaved marks the console when the lock mode in force was
	// saved in its settings file (#1986). That value wins over the
	// environment variable, so naming the variable would send the operator
	// to a knob that changes nothing; the remedy is the saved setting.
	RemedyConsoleSaved Remedy = "console-saved"
)

// SavedLockModeEndpoint is how the console's saved lock mode is changed or
// cleared (internal/console handleBackupSettingsDaemonUpdate).
const SavedLockModeEndpoint = "PUT /api/backup-settings/daemon/lock_mode"

// forMode renders this surface's way of selecting a specific mode.
func (r Remedy) forMode(m baseline.LockMode) string {
	switch r {
	case RemedyCLI:
		return "pass --lock-mode " + string(m)
	case RemedyConsoleSaved:
		return "save lock mode " + string(m) + " instead (" + SavedLockModeEndpoint + ` with {"value":"` + string(m) +
			`"}; {"use_startup":true} clears the saved one)`
	}
	return "set BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=" + string(m)
}

// noCheckModes names the modes that skip this gate entirely. It is the escape
// hatch offered when the CHECK ITSELF could not run — suggesting lock-all there
// would be useless, since lock-all is verified by this same query. Without it,
// an operator whose source cannot answer SHOW GRANTS gets a hard stop on the
// default path with nothing to try.
func (r Remedy) noCheckModes() string {
	return " — to proceed without this check, " + r.forMode(baseline.LockModeSafeNoLock) +
		", which needs no privilege but ABORTS rather than write a snapshot stitched from" +
		" several instants; or " + r.forMode(baseline.LockModeNoLock) + " to accept such a snapshot"
}

// ErrFTWRLPrivilegesMissing marks the refusal of lock mode ftwrl because the
// user lacks RELOAD (or FLUSH_TABLES), BACKUP_ADMIN, or both (#1986). The
// console's automatic lock mode retries such a snapshot once with lock-all,
// which managed MySQL accepts where BACKUP_ADMIN cannot be granted. Only the
// three missing-privilege refusals carry it: a source that cannot be reached,
// grants or a version that cannot be read, and every lock-all refusal do not,
// because retrying on those would hide their real cause.
var ErrFTWRLPrivilegesMissing = errors.New("lock mode ftwrl: RELOAD/FLUSH_TABLES or BACKUP_ADMIN missing")

// MissingPrivilegesError is a refusal because the user lacks privileges a
// lock mode needs, with what is missing as data (#1986): the console shows the
// exact GRANT to run without reading the message. The message is the
// refusal's own words, unchanged. Only ftwrl's refusals answer
// errors.Is(err, ErrFTWRLPrivilegesMissing), the marker execute retries on.
//
// Made only where the statement is certain: the privileges ftwrl or lock-all
// need, granted ON *.*. A partial REVOKE, unreadable grants or an unreadable
// version never make one, because a GRANT that names the wrong thing sends
// the operator to fix the wrong thing.
type MissingPrivilegesError struct {
	// Mode is the lock mode the check was for.
	Mode baseline.LockMode
	// Missing is the privileges to grant ON *.*, in the order the statement
	// names them.
	Missing []string
	// Account is the connecting account as SHOW GRANTS names it (`u`@`h`,
	// or 'u'@'h' on MySQL 5.7 and MariaDB), quotes included; "" when no line
	// could be read for it.
	Account string
	msg     string
}

func (e *MissingPrivilegesError) Error() string { return e.msg }
func (e *MissingPrivilegesError) Is(target error) bool {
	return target == ErrFTWRLPrivilegesMissing && e.Mode == baseline.LockModeFTWRL
}

// Grant is the statement that gives the account what the check found
// missing, naming the account exactly as SHOW GRANTS did. "" when no account
// was read: the source DSN names the server's host, not the account's, so
// "user@'%'" would be a guess, and a GRANT for an account that does not exist
// (or, on MariaDB, one that creates a second account) sends the operator to
// fix the wrong thing.
func (e *MissingPrivilegesError) Grant() string {
	if len(e.Missing) == 0 || e.Account == "" {
		return ""
	}
	return "GRANT " + strings.Join(e.Missing, ", ") + " ON *.* TO " + e.Account + ";"
}

// missingPrivileges builds the refusal. SHOW VIEW joins the statement when
// the account holds it nowhere: mydumper needs it for any view in the dump,
// and the Connect screen's grant asks for it beside these.
func missingPrivileges(g *grantSet, mode baseline.LockMode, missing []string, format string, args ...any) error {
	if !g.grantedAnywhere("SHOW VIEW") {
		missing = append(missing, "SHOW VIEW")
	}
	return &MissingPrivilegesError{Mode: mode, Missing: missing, Account: g.account, msg: fmt.Sprintf(format, args...)}
}

// CheckPrivileges queries the source for the privileges the requested
// point-consistent lock mode needs and fails loudly, before mydumper ever runs,
// unless they are present. This is a hard gate, not an advisory warning:
// unlike the no-lock skew warning in consoleapp, a query failure here aborts the
// dump rather than being swallowed, because the alternative — letting mydumper
// attempt FTWRL half-privileged — is a segfault, not a clean error (#800).
// Never silently falls back to a mode that is not point-consistent.
func CheckPrivileges(ctx context.Context, sourceDSN string, mode baseline.LockMode, remedy Remedy, schemas []string) error {
	db, err := config.Connect(sourceDSN)
	if err != nil {
		// Deliberately NO alternatives clause. An unreachable source is not a
		// privilege problem: mydumper cannot dump in ANY mode, so naming the
		// weaker ones buys nothing and actively misleads — on the console the
		// knob is a daemon environment variable, so an operator who sets
		// no-lock to get past a transient blip silently degrades EVERY future
		// baseline, with nothing to expire it or prompt a revert.
		return fmt.Errorf("point-consistent baseline: cannot connect to source to verify privileges: %w", err)
	}
	defer db.Close()
	return checkPrivilegesDB(ctx, db, mode, remedy, schemas)
}

// grantSet is what SHOW GRANTS FOR CURRENT_USER() told us, split by SCOPE.
// The distinction is load-bearing: FTWRL's privileges are global-only, while
// LOCK_ALL's LOCK TABLES is grantable per schema and is routinely held that
// way by a least-privilege account.
type grantSet struct {
	// global holds privileges granted ON *.*.
	global map[string]bool
	// scoped maps a privilege to the objects it was granted on (`db`.*,
	// `db`.`tbl`). Kept as the raw object text so a refusal can quote back
	// what the operator's own SHOW GRANTS shows.
	scoped map[string][]string
	// revoked maps a privilege to the objects a PARTIAL REVOKE took it back on.
	// MySQL 8.0.16+ renders these as separate REVOKE lines in SHOW GRANTS, and
	// reading only the GRANT lines reports a privilege the user does not have —
	// verified on MySQL 8.0.46 with partial_revokes=ON: a user holding
	// `GRANT LOCK TABLES ON *.*` with `REVOKE LOCK TABLES ON \`appdb\`.*` gets
	// "ERROR 1044 Access denied ... to database 'appdb'" on LOCK TABLES there.
	// That is the FALSE-PASS direction: mydumper launches and dies partway,
	// leaving a partial dump, which is exactly what this preflight exists to
	// prevent. partial_revokes defaults OFF, but it is a one-way switch.
	revoked map[string][]string
	// unparsed counts grant lines this parser did not recognise as either a
	// privilege grant or a role membership. It exists so a misparse is
	// DEBUGGABLE: the failure this whole package is fixing was a check that
	// refused confidently while reading the wrong source, and the operator had
	// no way to tell a real refusal from a blind one.
	unparsed int
	// account is the connecting account as the first privilege line names it
	// after TO, quotes included and nothing after it (MariaDB appends the
	// password hash there); "" when no line could be read for it.
	account string
}

// grantedGlobally reports whether p was granted ON *.*, directly or via
// ALL PRIVILEGES. This is the right question for RELOAD, FLUSH_TABLES and
// BACKUP_ADMIN: MySQL only accepts those at global scope.
func (g *grantSet) grantedGlobally(p string) bool {
	return g.global[p] || g.global["ALL PRIVILEGES"]
}

// grantedAnywhere reports whether p was granted at ANY scope. This is the
// right question for LOCK TABLES under LOCK_ALL — see checkPrivilegesDB.
func (g *grantSet) grantedAnywhere(p string) bool {
	return g.grantedGlobally(p) || len(g.scoped[p]) > 0 || len(g.scoped["ALL PRIVILEGES"]) > 0
}

// scopesFor renders the objects p was granted on, for a message.
func (g *grantSet) scopesFor(p string) string {
	s := append([]string{}, g.scoped[p]...)
	s = append(s, g.scoped["ALL PRIVILEGES"]...)
	sort.Strings(s)
	return strings.Join(s, ", ")
}

// add parses one SHOW GRANTS line into the set.
func (g *grantSet) add(line string) {
	if strings.HasPrefix(line, "REVOKE ") {
		g.addRevoke(line)
		return
	}
	if !strings.HasPrefix(line, "GRANT ") {
		g.unparsed++
		return
	}
	rest := line[len("GRANT "):]
	oi := strings.Index(rest, " ON ")
	if oi < 0 {
		// A role membership: "GRANT `r`@`%` TO `u`@`%`". Not a privilege grant,
		// and nothing is lost by skipping it — MySQL expands an ACTIVE role's
		// privileges into their own ON *.* lines in this same result set
		// (measured on MySQL 8.0: a user whose RELOAD and FLUSH_TABLES came
		// only from a default-activated role saw both listed inline, attributed
		// to the user). Not counted as unparsed: it is a normal line.
		return
	}
	obj := rest[oi+len(" ON "):]
	ti := strings.Index(obj, " TO ")
	if ti < 0 {
		g.unparsed++
		return
	}
	names := splitPrivileges(rest[:oi])
	if len(names) == 0 {
		g.unparsed++
		return
	}
	if g.account == "" {
		g.account = grantAccount(line)
	}
	if obj = strings.TrimSpace(obj[:ti]); obj == "*.*" {
		for _, n := range names {
			g.global[n] = true
		}
		return
	}
	for _, n := range names {
		g.scoped[n] = append(g.scoped[n], obj)
	}
}

// grantAccount is the account a privilege line grants to, verbatim: the
// quoted user, @, the quoted host, and nothing after (WITH GRANT OPTION,
// REQUIRE, MariaDB's IDENTIFIED BY PASSWORD '<hash>'). "" for a line that has
// no ON clause (a role membership) or an account this cannot read whole.
func grantAccount(line string) string {
	oi := strings.Index(line, " ON ")
	if oi < 0 {
		return ""
	}
	rest := line[oi+len(" ON "):]
	ti := strings.Index(rest, " TO ")
	if ti < 0 {
		return ""
	}
	s := rest[ti+len(" TO "):]
	user := quotedLen(s)
	if user == 0 || user >= len(s) || s[user] != '@' {
		return ""
	}
	host := quotedLen(s[user+1:])
	if host == 0 {
		return ""
	}
	return s[:user+1+host]
}

// quotedLen is the length of the quoted identifier or string s starts with,
// closing quote included: `...` with “ inside, or '...' with ” or \'
// inside. 0 when s does not start with one or it never closes.
func quotedLen(s string) int {
	if s == "" || (s[0] != '`' && s[0] != '\'') {
		return 0
	}
	q := s[0]
	for i := 1; i < len(s); i++ {
		switch {
		case q == '\'' && s[i] == '\\':
			i++
		case s[i] == q && i+1 < len(s) && s[i+1] == q:
			i++
		case s[i] == q:
			return i + 1
		}
	}
	return 0
}

// addRevoke parses "REVOKE <privs> ON <object> FROM <user>".
func (g *grantSet) addRevoke(line string) {
	rest := line[len("REVOKE "):]
	oi := strings.Index(rest, " ON ")
	if oi < 0 {
		g.unparsed++
		return
	}
	obj := rest[oi+len(" ON "):]
	fi := strings.Index(obj, " FROM ")
	if fi < 0 {
		g.unparsed++
		return
	}
	names := splitPrivileges(rest[:oi])
	if len(names) == 0 {
		g.unparsed++
		return
	}
	obj = strings.TrimSpace(obj[:fi])
	for _, n := range names {
		g.revoked[n] = append(g.revoked[n], obj)
	}
}

// revokeSchema extracts the schema component of a grant/revoke object
// (`appdb`.* → appdb). Returns ok=false for a shape it does not recognise —
// which is treated as "cannot evaluate", never as "does not apply".
func revokeSchema(obj string) (string, bool) {
	dot := strings.Index(obj, ".")
	if dot <= 0 {
		return "", false
	}
	return strings.Trim(obj[:dot], "`\""), true
}

// revokesAffecting splits the partial revokes of p into those that PROVABLY
// apply to a dump of these schemas and those that cannot be decided.
//
// The split matters because the two directions have opposite costs. Refusing a
// dump whose schemas a revoke does not touch is a false refusal — the defect
// #1381 was filed for. Passing one it does touch lets mydumper die partway.
// So only a LITERAL schema-name match counts as blocking: a MySQL grant pattern
// may contain `%`/`_` wildcards, and a pattern equal to the schema name matches
// it whatever the wildcard rules say, while a non-equal pattern containing a
// wildcard MIGHT match and is reported rather than acted on.
//
// An empty schema list means "dump every non-system schema", so any revoke is
// blocking: whatever it names is inside the dump.
func (g *grantSet) revokesAffecting(p string, schemas []string) (blocking, undecidable []string) {
	var objs []string
	objs = append(objs, g.revoked[p]...)
	objs = append(objs, g.revoked["ALL PRIVILEGES"]...)
	for _, obj := range objs {
		if len(schemas) == 0 {
			blocking = append(blocking, obj)
			continue
		}
		name, ok := revokeSchema(obj)
		if !ok {
			undecidable = append(undecidable, obj)
			continue
		}
		matched := false
		for _, s := range schemas {
			if strings.EqualFold(name, s) {
				matched = true
				break
			}
		}
		switch {
		case matched:
			blocking = append(blocking, obj)
		case strings.ContainsAny(name, "%_"):
			undecidable = append(undecidable, obj)
		}
	}
	return blocking, undecidable
}

// splitPrivileges turns "SELECT (a, b), LOCK TABLES" into the privilege names
// alone. Column lists are stripped BEFORE the comma split — splitting first
// would parse the column names as privileges.
func splitPrivileges(s string) []string {
	var b strings.Builder
	depth := 0
	for _, r := range s {
		switch {
		case r == '(':
			depth++
		case r == ')':
			if depth > 0 {
				depth--
			}
		case depth == 0:
			b.WriteRune(r)
		}
	}
	var out []string
	for _, p := range strings.Split(b.String(), ",") {
		if p = strings.ToUpper(strings.TrimSpace(p)); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseGrants reads the connecting user's privileges.
//
// It parses SHOW GRANTS FOR CURRENT_USER() rather than reading
// information_schema.USER_PRIVILEGES, which was the source until this was
// measured against RDS: that view exposes only the rows the connecting user
// can SEE in mysql.user, and a managed master user cannot see its own. On RDS
// MySQL 8.4 it returned exactly one row — USAGE — for an account whose
// SHOW GRANTS listed RELOAD and FLUSH_TABLES as direct grants. Reading it made
// this check fail CLOSED on every managed source, which since #1377 made the
// point-consistent default unreachable there. SHOW GRANTS needs no privilege
// to run for the current user and expands whatever the session actually has,
// including the privileges of active roles.
func parseGrants(ctx context.Context, db *sql.DB) (*grantSet, error) {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		return nil, fmt.Errorf("point-consistent baseline: cannot verify source privileges: %w", err)
	}
	defer rows.Close()

	g := &grantSet{global: map[string]bool{}, scoped: map[string][]string{}, revoked: map[string][]string{}}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, fmt.Errorf("point-consistent baseline: cannot read source privileges: %w", err)
		}
		g.add(line)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("point-consistent baseline: cannot read source privileges: %w", err)
	}
	// The parsed verdict is the input to a refusal an operator reads mid-incident.
	// Log what we actually read so a misparse is diagnosable from the daemon log
	// instead of being indistinguishable from a genuine refusal.
	names := make([]string, 0, len(g.global))
	for n := range g.global {
		names = append(names, n)
	}
	sort.Strings(names)
	slog.Debug("read source privileges for baseline lock mode",
		"global", names, "scoped", g.scoped, "unrecognized_lines", g.unparsed)
	return g, nil
}

// checkPrivilegesDB is CheckPrivileges' core logic against an already-open
// *sql.DB, split out so it is unit-testable with sqlmock. Requirements are
// PER MODE, because they differ in kind:
//
//   - FTWRL needs RELOAD or FLUSH_TABLES on every flavor, plus BACKUP_ADMIN on
//     MySQL/Percona 8.0+ (it issues LOCK INSTANCE FOR BACKUP first). Granting
//     BACKUP_ADMIN WITHOUT RELOAD does not fail cleanly — the pinned build
//     SEGFAULTS — which is why this check exists at all. All three are
//     global-only privileges, so only an ON *.* grant can satisfy them.
//   - LOCK_ALL needs LOCK TABLES, at ANY scope. mydumper's own help names it as
//     one of the two modes it supports on RDS/Aurora ("We support LOCK_ALL and
//     SAFE_NO_LOCK modes for RDS/Aurora", string in the pinned v1.0.3-1 binary),
//     and it locks the EXPORTED TABLES rather than the instance — so a
//     least-privilege account holding LOCK TABLES on just the dumped schema is
//     enough. Verified: a dump ran to completion with only
//     `GRANT SELECT, LOCK TABLES, SHOW VIEW ON \`appdb\`.*`. Requiring it
//     globally would refuse a configuration that demonstrably works, which is
//     the exact defect (#1381) this mode was added to fix.
//   - The low-privilege modes never reach here.
//
// ALL PRIVILEGES satisfies any of them at the matching scope.
func checkPrivilegesDB(ctx context.Context, db *sql.DB, mode baseline.LockMode, remedy Remedy, schemas []string) error {
	g, err := parseGrants(ctx, db)
	if err != nil {
		// The hatch belongs HERE, not on the connect path: the source answered,
		// so a dump can genuinely still succeed — it is only this check that
		// could not run.
		return fmt.Errorf("%w%s", err, remedy.noCheckModes())
	}

	// SHOW GRANTS expands the privileges of a session's ACTIVE roles (measured
	// on MySQL 8.0). The remedy is therefore server-side: bintrail opens this
	// connection itself and mydumper opens its own, so there is no point at
	// which an operator could issue SET ROLE for either.
	const roleCaveat = " (if these are granted through a role, make it active on connect —" +
		" SET DEFAULT ROLE ALL TO '<user>'@'<host>', or activate_all_roles_on_login=ON —" +
		" since neither this check nor mydumper can issue SET ROLE on its own connection)"

	if mode == baseline.LockModeLockAll {
		if g.grantedAnywhere("LOCK TABLES") {
			blocking, undecidable := g.revokesAffecting("LOCK TABLES", schemas)
			if len(undecidable) > 0 {
				slog.Warn("lock-all: a partial revoke of LOCK TABLES may cover a schema in this dump;"+
					" if mydumper fails with \"Access denied\", this is why",
					"revokes", strings.Join(undecidable, ", "), "schemas", schemas)
			}
			if len(blocking) > 0 {
				return fmt.Errorf("lock-all baseline mode requires LOCK TABLES on the dumped schemas, and a partial"+
					" REVOKE takes it back on %s — the global grant does not apply there (verified: LOCK TABLES then"+
					" fails with \"ERROR 1044 Access denied\"). Restore it, e.g."+
					" GRANT LOCK TABLES ON <schema>.* TO '<user>'@'%%', or exclude that schema from the dump%s",
					strings.Join(blocking, ", "), remedy.noCheckModes())
			}
			if !g.grantedGlobally("LOCK TABLES") {
				// Accepted, but say which objects it covers: if the dump reaches a
				// schema outside them mydumper fails cleanly, and this line is what
				// makes that error make sense. Not a refusal — the grant patterns
				// can carry wildcards, so "does it cover the dump" is not decidable
				// here, and guessing wrong is the false refusal #1381 was about.
				slog.Info("lock-all: LOCK TABLES is granted per object, not globally;"+
					" the dump must stay within these", "scopes", g.scopesFor("LOCK TABLES"),
					"schemas", schemas)
			}
			return nil
		}
		// Name ftwrl when the user could actually run it: an operator who copied
		// an RDS recipe onto a self-hosted source holding global RELOAD would
		// otherwise be steered straight past the point-consistent mode they have.
		// ftwrl also needs BACKUP_ADMIN on MySQL/Percona 8.0+ (#1986): RELOAD
		// alone does not make it available there, and a version that cannot be
		// read makes no claim at all.
		alt := remedy.noCheckModes()
		if g.grantedGlobally("RELOAD") || g.grantedGlobally("FLUSH_TABLES") {
			needBA, verErr := requiresBackupAdmin(ctx, db)
			if verErr != nil {
				slog.Debug("lock-all refusal: the source version could not be read, so it does not say whether ftwrl is available", "error", verErr)
			}
			if verErr == nil && (!needBA || g.grantedGlobally("BACKUP_ADMIN")) {
				alt = " — this user already holds RELOAD/FLUSH_TABLES globally, so " +
					remedy.forMode(baseline.LockModeFTWRL) + " is available and is also point-consistent" + alt
			}
		}
		return missingPrivileges(g, mode, []string{"LOCK TABLES"}, "lock-all baseline mode requires the LOCK TABLES privilege, which the current user does not"+
			" have at any scope — grant it, e.g. GRANT LOCK TABLES ON *.* TO '<user>'@'%%' (a grant on just the dumped"+
			" schema also works, since LOCK_ALL locks the exported tables)%s%s", roleCaveat, alt)
	}

	if g.grantedGlobally("ALL PRIVILEGES") {
		return nil
	}

	requireBackupAdmin, err := requiresBackupAdmin(ctx, db)
	if err != nil {
		// Unlike the paths above, the grants ARE known here — so if this user
		// can run lock-all, say so instead of offering only the weaker modes.
		// noCheckModes exists for when the grant query itself failed.
		if g.grantedAnywhere("LOCK TABLES") {
			return fmt.Errorf("%w — this user holds LOCK TABLES, so %s, which is point-consistent and needs no"+
				" version check%s", err, remedy.forMode(baseline.LockModeLockAll), remedy.noCheckModes())
		}
		return fmt.Errorf("%w%s", err, remedy.noCheckModes())
	}
	hasFlush := g.grantedGlobally("RELOAD") || g.grantedGlobally("FLUSH_TABLES")
	// A partial revoke cannot take back a global-only privilege like RELOAD in a
	// way that affects FLUSH TABLES WITH READ LOCK (which is not schema-scoped),
	// so no revoke check here — but say so, or the asymmetry with lock-all above
	// reads as an oversight.
	missingBackupAdmin := requireBackupAdmin && !g.grantedGlobally("BACKUP_ADMIN")
	if hasFlush && !missingBackupAdmin {
		return nil
	}

	// The alternatives clause is the one actionable sentence here, and this
	// gate fires on the DEFAULT path. lock-all comes FIRST because it is the
	// only alternative that is still point-consistent — and on managed MySQL
	// (RDS grants LOCK TABLES but refuses BACKUP_ADMIN outright) it is the
	// only one that can work at all.
	// The managed-MySQL parenthetical is gated on requireBackupAdmin: BACKUP_ADMIN
	// does not exist on MariaDB or MySQL 5.7, and naming it there would send an
	// operator hunting a privilege their server cannot have.
	lockAllNote := ", which is also point-consistent and needs only LOCK TABLES"
	if requireBackupAdmin {
		lockAllNote += " (the mode that works on managed MySQL, where BACKUP_ADMIN cannot be granted at all;" +
			" mydumper's own help names LOCK_ALL and SAFE_NO_LOCK as the modes it supports on RDS/Aurora)"
	}
	alternatives := " — or " + remedy.forMode(baseline.LockModeLockAll) + lockAllNote +
		"; or " + remedy.forMode(baseline.LockModeSafeNoLock) +
		", which needs no privilege but ABORTS rather than write a snapshot stitched from several instants," +
		" so expect it to refuse on a write-active source; or " + remedy.forMode(baseline.LockModeNoLock) +
		" to accept such a snapshot"

	switch {
	case !hasFlush && missingBackupAdmin:
		return missingPrivileges(g, mode, []string{"RELOAD", "BACKUP_ADMIN"},
			"point-consistent baseline mode requires the source DB user to have BOTH the"+
				" BACKUP_ADMIN and the RELOAD (or FLUSH_TABLES) privilege (MySQL/Percona 8.0+); the current user has"+
				" neither — grant both, e.g. GRANT BACKUP_ADMIN, RELOAD ON *.* TO '<user>'@'%%'%s%s", alternatives, roleCaveat)
	case !hasFlush:
		// Reached when BACKUP_ADMIN is present (or not required) but RELOAD is
		// not. On MySQL 8.0+ that half-grant is the #800 crash input, and it is
		// worth naming: an operator holding BACKUP_ADMIN has already been told
		// once that it was the missing privilege, and needs to know why the
		// remaining half is not merely another clean refusal.
		crashNote := ""
		if requireBackupAdmin {
			crashNote = " — granting BACKUP_ADMIN alone is the dangerous half-grant:" +
				" the pinned mydumper build SEGFAULTS on it rather than failing cleanly, which is why this check runs first"
		}
		return missingPrivileges(g, mode, []string{"RELOAD"}, "point-consistent baseline mode requires the RELOAD (or FLUSH_TABLES) privilege, which the"+
			" current user has at neither name — grant it, e.g. GRANT RELOAD ON *.* TO '<user>'@'%%'%s%s%s",
			crashNote, alternatives, roleCaveat)
	default:
		return missingPrivileges(g, mode, []string{"BACKUP_ADMIN"}, "point-consistent baseline mode requires the BACKUP_ADMIN privilege (MySQL/Percona 8.0+) in"+
			" addition to RELOAD/FLUSH_TABLES, which the current user already has — grant it, e.g."+
			" GRANT BACKUP_ADMIN ON *.* TO '<user>'@'%%'. NOTE: on managed MySQL such as RDS this grant is REFUSED"+
			" outright, so ftwrl cannot work there%s%s", alternatives, roleCaveat)
	}
}
