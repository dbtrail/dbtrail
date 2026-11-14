package console

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sort"
	"strings"
)

// One source family per installation (#2815).
//
// An installation serves MySQL and MariaDB servers, or PostgreSQL servers,
// never both. The capture engine, the copy's rules and the client port are
// each picked per server, so the two families can sit in one registry by
// construction; no test runs them together in one process, and what is not
// tested together is not offered together. The first server fixes the family
// and a server of the other one is refused where servers enter: the
// registry's add (addLocked) and, for the server given on the command line,
// the console's (Server.newServerFamilyRefusal).
//
// This is about SOURCES. The index is MySQL for every source family.
//
// A servers file that already holds both families (it was edited by hand, or
// written before this rule) does not stop anything: every server is still
// served and captured, the mix is logged as an error where the file is read,
// and no server can be ADDED until one family is left (removing one is how it
// is fixed). Editing a server stays allowed, and so does everything the
// program writes about one on its own: an edit cannot change a server's
// family, and refusing it would leave a server whose password was rotated
// with no way to be captured again short of removing a whole family. There
// is no setting that allows the mix.

// The two families. MariaDB is MySQL's: one capture engine, one client port.
const (
	FamilyMySQL    = "mysql"
	FamilyPostgres = "postgres"
)

// FamilyOfFlavor is the family a source flavor belongs to. A flavor that does
// not read as one this build knows is MySQL's, as ServerEntry.SourceFlavor
// has it: an entry that reaches here with one was edited by hand.
func FamilyOfFlavor(flavor string) string {
	if f, err := NormalizeFlavor(flavor); err == nil && f == FlavorPostgres {
		return FamilyPostgres
	}
	return FamilyMySQL
}

// Family is the entry's source family.
func (e ServerEntry) Family() string { return FamilyOfFlavor(e.Flavor) }

// familyWords names a family the way a person reads it.
func familyWords(family string) string {
	if family == FamilyPostgres {
		return "PostgreSQL"
	}
	return "MySQL and MariaDB"
}

// oneOfFamily names one server of a family: "a PostgreSQL server".
func oneOfFamily(family string) string {
	if family == FamilyPostgres {
		return "a PostgreSQL server"
	}
	return "a MySQL or MariaDB server"
}

// ErrOtherFamily is what errors.Is matches for every refusal under this rule.
var ErrOtherFamily = errors.New("this installation serves another database family")

// OtherFamilyError refuses a server because of the families the installation
// already holds. Its text is read in the web interface and in the terminal
// (`dbtrail server add`), so it says, in this order: what is, that nothing
// was changed, what to do, and why.
type OtherFamilyError struct {
	// Installed is the family the installation serves; Asked the one the
	// refused server belongs to. Both set, and different, when Mixed is nil.
	Installed, Asked string
	// Mixed, when set, says the installation already holds both families:
	// the names of its PostgreSQL servers and of its MySQL and MariaDB ones.
	// Nothing can be added then, whatever its family.
	MixedPostgres, MixedMySQL []string
}

func (e *OtherFamilyError) mixed() bool { return len(e.MixedPostgres) > 0 && len(e.MixedMySQL) > 0 }

func (e *OtherFamilyError) Error() string {
	const why = "Each DBTrail installation serves one database family, MySQL and MariaDB or PostgreSQL, because the two are tested and released apart."
	if e.mixed() {
		fix := "Remove the servers of one family, or move them to an installation of their own: until then no server can be added."
		if slices.Contains(e.MixedPostgres, bootServerWords) || slices.Contains(e.MixedMySQL, bootServerWords) {
			// It is not in the list of servers: nothing in the web
			// interface removes it.
			fix = "One of them is the server this was started with (--index-dsn or --source-dsn), which only starting DBTrail without it removes; or remove the servers of the other family. Until then no server can be added."
		}
		return fmt.Sprintf("This installation holds servers of both database families (%s: %s; %s: %s), which is not supported. Nothing was changed. %s %s",
			familyWords(FamilyPostgres), someNames(e.MixedPostgres), familyWords(FamilyMySQL), someNames(e.MixedMySQL), fix, why)
	}
	return fmt.Sprintf("This installation serves %s servers, so %s cannot be added to it. Nothing was changed. "+
		"To use DBTrail with %s, run a second installation for it. %s",
		familyWords(e.Installed), oneOfFamily(e.Asked), familyWords(e.Asked), why)
}

func (e *OtherFamilyError) Is(target error) bool { return target == ErrOtherFamily }

// someNames lists names for a message a person reads in one line: the first
// few, and how many more there are.
func someNames(names []string) string {
	const show = 4
	if len(names) <= show {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:show], ", "), len(names)-show)
}

// familiesOf sorts servers' names by family.
func familiesOf(servers []ServerEntry) (postgres, mysql []string) {
	for _, e := range servers {
		name := e.Name
		if name == "" {
			name = e.ID
		}
		if e.Family() == FamilyPostgres {
			postgres = append(postgres, name)
		} else {
			mysql = append(mysql, name)
		}
	}
	sort.Strings(postgres)
	sort.Strings(mysql)
	return postgres, mysql
}

// bootServerWords names, in a refusal, the server given on the command line.
const bootServerWords = "the server this was started with"

// familyRefusal says why a server of flavor cannot join servers, nil when it
// can: there are none, or they are all of its family. boot, when not "", is
// the family of the server given on the command line, which counts as one
// more server of that family (named "the server this was started with").
func familyRefusal(servers []ServerEntry, boot, flavor string) error {
	postgres, mysql := familiesOf(servers)
	switch boot {
	case FamilyPostgres:
		postgres = append(postgres, bootServerWords)
	case FamilyMySQL:
		mysql = append(mysql, bootServerWords)
	}
	if len(postgres) > 0 && len(mysql) > 0 {
		return &OtherFamilyError{MixedPostgres: postgres, MixedMySQL: mysql}
	}
	asked := FamilyOfFlavor(flavor)
	switch {
	case len(postgres) > 0 && asked != FamilyPostgres:
		return &OtherFamilyError{Installed: FamilyPostgres, Asked: asked}
	case len(mysql) > 0 && asked != FamilyMySQL:
		return &OtherFamilyError{Installed: FamilyMySQL, Asked: asked}
	}
	return nil
}

// MixedFamilies reports whether the registry holds servers of both families,
// with their names: the state a hand-edited file can be in.
func (r *Registry) MixedFamilies() (postgres, mysql []string, mixed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	postgres, mysql = familiesOf(r.file.Servers)
	return postgres, mysql, len(postgres) > 0 && len(mysql) > 0
}

// warnFamilies says, where a servers file is read, the two things about its
// families that the file's owner has to know and nothing else would tell
// them: that it holds both, and that an entry's flavor is not one this build
// knows (it is then read as MySQL, which is what puts a PostgreSQL server
// written "pgsql" among the MySQL ones). Errors in the log, not refusals:
// every server in the file is still served.
//
// It returns what it said, and says nothing when that is what it said last
// time (said): a process that follows the file reads it again on every change
// the daemon makes to it, and the same sentence on each would bury the rest.
func warnFamilies(path string, servers []ServerEntry, said string) string {
	var unknown []string
	for _, e := range servers {
		if _, err := NormalizeFlavor(e.Flavor); err != nil {
			unknown = append(unknown, fmt.Sprintf("%s (flavor %q)", e.Name, e.Flavor))
		}
	}
	postgres, mysql := familiesOf(servers)
	mixed := len(postgres) > 0 && len(mysql) > 0
	now := ""
	if mixed || len(unknown) > 0 {
		now = strings.Join(unknown, ",") + "|" + strings.Join(postgres, ",") + "|" + strings.Join(mysql, ",")
		if !mixed {
			now = strings.Join(unknown, ",") + "||"
		}
	}
	if now == said {
		return now
	}
	if len(unknown) > 0 {
		slog.Error("the servers file names a source flavor this version does not know, and reads those servers as MySQL: write mysql, mariadb or postgres",
			"file", path, "servers", strings.Join(unknown, ", "))
	}
	if mixed {
		slog.Error("the servers file holds servers of both database families, which is not supported: each installation serves MySQL and MariaDB, or PostgreSQL. "+
			"Every server is still served and captured; no server can be added until one family is left",
			"file", path, "postgresql", strings.Join(postgres, ", "), "mysql_and_mariadb", strings.Join(mysql, ", "),
			"fix", "remove the servers of one family, or move them to an installation of their own")
	}
	return now
}

// bootFamily is the family of the server given on the command line, "" when
// there is none to count (a daemon started with no source keeps a hidden one
// that captures nothing).
func (s *Server) bootFamily() string {
	if !s.cm.bootSelectable() {
		return ""
	}
	if b, _ := s.cm.bootInfo(); b != nil && b.pgSource {
		return FamilyPostgres
	}
	return FamilyMySQL
}

// newServerFamilyRefusal is the rule for a server about to be added through
// the console, with the server given on the command line counted: the
// registry alone (addLocked) cannot see that one.
func (s *Server) newServerFamilyRefusal(flavor string) error {
	return familyRefusal(s.cm.reg.List(), s.bootFamily(), flavor)
}

// warnBootFamily says at start, in the log, that the server given on the
// command line and the servers in the file are of different families: the
// file alone holds one family then, so reading it says nothing
// (warnFamilies), and the first sign would be a refused add.
func (s *Server) warnBootFamily() {
	boot := s.bootFamily()
	if boot == "" || s.cm.reg == nil {
		return
	}
	postgres, mysql := familiesOf(s.cm.reg.List())
	other := mysql
	if boot == FamilyMySQL {
		other = postgres
	}
	if len(other) == 0 {
		return
	}
	slog.Error("the server this was started with and the servers in the servers file are of different database families, which is not supported: each installation serves MySQL and MariaDB, or PostgreSQL. "+
		"Every server is still served and captured; no server can be added until one family is left",
		"started_with", familyWords(boot), "servers_of_the_other_family", strings.Join(other, ", "),
		"fix", "start DBTrail without --index-dsn or --source-dsn, or remove those servers")
}

// OtherFamilyCode is the "code" of the refusal's JSON (writeJSONErrorCode): a
// page tells this refusal from one a person can fix and try again.
const OtherFamilyCode = "other_family"

// writeNewServerError answers a refused add: the other-family refusal with
// its code, anything else as it always was.
func writeNewServerError(w http.ResponseWriter, status int, err error) {
	if errors.Is(err, ErrOtherFamily) {
		writeJSONErrorCode(w, status, err.Error(), OtherFamilyCode)
		return
	}
	writeJSONError(w, status, err.Error())
}

// familiesDTO is the two families' servers, by name, in the servers list of
// an installation that holds both.
type familiesDTO struct {
	Postgres []string `json:"postgres"`
	MySQL    []string `json:"mysql"`
}

// installationFamily is what the page is told about the families: the one
// this installation serves ("" while it has no server), and, when it holds
// both, the servers of each. The server given on the command line counts, as
// it does for an add (newServerFamilyRefusal): the two must never disagree
// about what an add will be answered.
func (s *Server) installationFamily() (family string, mixed *familiesDTO) {
	var servers []ServerEntry
	if s.cm.reg != nil {
		servers = s.cm.reg.List()
	}
	postgres, mysql := familiesOf(servers)
	switch s.bootFamily() {
	case FamilyPostgres:
		postgres = append(postgres, bootServerWords)
	case FamilyMySQL:
		mysql = append(mysql, bootServerWords)
	}
	switch {
	case len(postgres) > 0 && len(mysql) > 0:
		return "", &familiesDTO{Postgres: postgres, MySQL: mysql}
	case len(postgres) > 0:
		return FamilyPostgres, nil
	case len(mysql) > 0:
		return FamilyMySQL, nil
	}
	return "", nil
}
