package console

import (
	"errors"
	"fmt"
	"log/slog"
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
// and no server can be added or edited by a person until one family is left
// (removing one is how it is fixed). Nothing that the
// program writes about a server on its own (a kept-snapshot mark, a schedule,
// MySQL corrected to MariaDB) is refused: that would stop the capture this
// rule leaves running. There is no setting that allows the mix.

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
		return fmt.Sprintf("This installation holds servers of both database families (%s: %s; %s: %s), which is not supported. Nothing was changed. "+
			"Remove the servers of one family, or move them to an installation of their own: until then no server can be added or edited. %s",
			familyWords(FamilyPostgres), strings.Join(e.MixedPostgres, ", "), familyWords(FamilyMySQL), strings.Join(e.MixedMySQL, ", "), why)
	}
	return fmt.Sprintf("This installation serves %s servers, so %s cannot be added to it. Nothing was changed. "+
		"To use DBTrail with %s, run a second installation for it. %s",
		familyWords(e.Installed), oneOfFamily(e.Asked), familyWords(e.Asked), why)
}

func (e *OtherFamilyError) Is(target error) bool { return target == ErrOtherFamily }

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

// familyRefusal says why a server of flavor cannot join servers, nil when it
// can: there are none, or they are all of its family. boot, when not "", is
// the family of the server given on the command line, which counts as one
// more server of that family (named "the server this was started with").
func familyRefusal(servers []ServerEntry, boot, flavor string) error {
	postgres, mysql := familiesOf(servers)
	const bootName = "the server this was started with"
	switch boot {
	case FamilyPostgres:
		postgres = append(postgres, bootName)
	case FamilyMySQL:
		mysql = append(mysql, bootName)
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
// with their names: the state a hand-edited file can be in. The web interface
// says so while it lasts.
func (r *Registry) MixedFamilies() (postgres, mysql []string, mixed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	postgres, mysql = familiesOf(r.file.Servers)
	return postgres, mysql, len(postgres) > 0 && len(mysql) > 0
}

// MixedFamiliesRefusal is the refusal for a change a person asks for while
// the registry holds both families, nil when it does not.
func (r *Registry) MixedFamiliesRefusal() error {
	postgres, mysql, mixed := r.MixedFamilies()
	if !mixed {
		return nil
	}
	return &OtherFamilyError{MixedPostgres: postgres, MixedMySQL: mysql}
}

// warnMixedFamilies logs, where a servers file is read, that it holds both
// families. An error, not a refusal: every server in it is still served.
func warnMixedFamilies(path string, servers []ServerEntry) {
	postgres, mysql := familiesOf(servers)
	if len(postgres) == 0 || len(mysql) == 0 {
		return
	}
	slog.Error("the servers file holds servers of both database families, which is not supported: each installation serves MySQL and MariaDB, or PostgreSQL. "+
		"Every server is still served and captured; no server can be added until one family is left",
		"file", path, "postgresql", strings.Join(postgres, ", "), "mysql_and_mariadb", strings.Join(mysql, ", "),
		"fix", "remove the servers of one family, or move them to an installation of their own")
}

// newServerFamilyRefusal is the rule for a server about to be added through
// the console, with the server given on the command line counted: the
// registry alone (addLocked) cannot see that one.
func (s *Server) newServerFamilyRefusal(flavor string) error {
	boot := ""
	if s.cm.bootSelectable() {
		boot = FamilyMySQL
		if b, _ := s.cm.bootInfo(); b != nil && b.pgSource {
			boot = FamilyPostgres
		}
	}
	return familyRefusal(s.cm.reg.List(), boot, flavor)
}
