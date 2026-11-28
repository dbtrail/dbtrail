package installer

import (
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The promise of an installation for PostgreSQL servers: word for word the
// headline of dbtrail.com/postgresql (#3319).
const pgPromise = "Heavy PostgreSQL reports, on a copy of your tables."

// The start page of the PostgreSQL path.
const pgStartPage = "https://www.dbtrail.com/docs/postgres/"

// otherDatabaseRE finds a database other than PostgreSQL named in the
// installer's own text. People who come for PostgreSQL leave when its
// installer talks about MySQL, so nothing it prints on that path may name
// one, not even the store that keeps DBTrail's history ("DBTrail's history
// store"). Docker's own progress lines (`compose pull`, `compose up`) name
// the stack's services and images, index-mysql among them; the stand-in
// Docker prints none, so they are not checked here.
var otherDatabaseRE = regexp.MustCompile(`(?i)mysql|mariadb|percona|binlog`)

// pgRun runs install.sh for PostgreSQL servers and checks it finished.
func pgRun(t *testing.T, env ...string) run {
	t.Helper()
	r := install(t, append([]string{"DBTRAIL_FOR=postgres"}, env...)...)
	if r.failed || !strings.Contains(r.out, "DBTrail is up.") {
		t.Fatalf("the install for PostgreSQL did not finish:\n%s", r.out)
	}
	return r
}

// otherDatabaseLines returns every printed line that names another database,
// with the stack's own folder left out (a temporary path decides nothing).
func otherDatabaseLines(out, dir string) []string {
	var hits []string
	for _, line := range strings.Split(out, "\n") {
		if otherDatabaseRE.MatchString(strings.ReplaceAll(line, dir, "")) {
			hits = append(hits, line)
		}
	}
	return hits
}

// Every successful path the stand-ins reach: the default, a port chosen by
// hand, 9090 taken (metrics move), 3309 taken (no other port is published,
// and nothing is said about it) and 5433 taken (the PostgreSQL port is not
// published, and the steps say how to publish another).
func TestInstallerPostgres_namesNoOtherDatabase(t *testing.T) {
	for _, env := range [][]string{nil, {"DBTRAIL_PORT=8091"}, {"BUSY_PORTS=9090"}, {"BUSY_PORTS=3309"}, {"BUSY_PORTS=5433"}} {
		t.Run(strings.Join(append([]string{"postgres"}, env...), " "), func(t *testing.T) {
			r := pgRun(t, env...)
			for _, h := range otherDatabaseLines(r.out, r.dir) {
				t.Errorf("the install for PostgreSQL names another database: %q", h)
			}
			for _, h := range sentenceHits(t, r.out, r.dir) {
				t.Errorf("banned in a sentence the installer prints: %s", h)
			}
			if strings.Contains(r.out, promise) {
				t.Errorf("the install for PostgreSQL makes the MySQL promise:\n%s", r.out)
			}
			// 3309 taken: said nowhere, so the compose file shows the branch ran.
			if strings.Contains(strings.Join(env, " "), "3309") && strings.Contains(r.compose(t), `"127.0.0.1:3309:3309"`) {
				t.Errorf("with 3309 taken its line is still in docker-compose.yml")
			}
		})
	}
}

// What the end text says on the PostgreSQL path: its promise, its start
// page, what to prepare, the five steps ending on the heavy report through
// the PostgreSQL port, and the forwarding box (without it the port does not
// answer for the server, #3254).
func TestInstallerPostgres_saysItsOwnSteps(t *testing.T) {
	r := pgRun(t)
	for _, want := range []string{
		pgPromise,
		pgStartPage,
		"Every step in detail: ",
		"host and port",
		"a PostgreSQL login that can create roles",
		"wal_level = logical",
		"REPLICA IDENTITY FULL",
		"Open the full form, and pick PostgreSQL",
		"Leave ticked the box",
		"Read database now",
		"4. PostgreSQL port.",
		"PostgreSQL clients",
		"psql line",
		"5. Heavy report.",
		"server add --flavor postgres",
		"Docker volumes",
		"docker compose down",
	} {
		if !strings.Contains(r.out, want) {
			t.Errorf("the install for PostgreSQL does not say %q:\n%s", want, r.out)
		}
	}
	if i, j := strings.Index(r.out, "5. Heavy report."), strings.Index(r.out, "4. PostgreSQL port."); j < 0 || i < j {
		t.Errorf("the heavy report is not the step after the PostgreSQL port:\n%s", r.out)
	}
	for _, gone := range []string{"  6. ", startPage, "INDEX_DSN", "A PostgreSQL server instead?", "3309"} {
		if strings.Contains(r.out, gone) {
			t.Errorf("the install for PostgreSQL still says %q:\n%s", gone, r.out)
		}
	}
}

// 5433 taken: the step says how to publish another port for psql.
func TestInstallerPostgres_aTakenPortSaysHowToPublishAnother(t *testing.T) {
	r := pgRun(t, "BUSY_PORTS=5433")
	if !strings.Contains(r.out, "127.0.0.1:5434:5433") || !strings.Contains(r.out, "port= in the psql line") {
		t.Errorf("with 5433 taken the steps do not say how to publish another port:\n%s", r.out)
	}
}

// The stack is the same: the PostgreSQL path writes the compose file the
// MySQL path writes, and starts it once.
func TestInstallerPostgres_installsTheSameStack(t *testing.T) {
	my := install(t)
	pg := pgRun(t)
	if my.compose(t) != pg.compose(t) {
		t.Errorf("the PostgreSQL path wrote another docker-compose.yml than the default one")
	}
	if pg.upCalls != 1 {
		t.Errorf("compose up ran %d times on the PostgreSQL path, want 1", pg.upCalls)
	}
}

// postgresql is read as postgres; any other value is refused before anything
// is touched.
func TestInstaller_forTakesPostgresOrMySQL(t *testing.T) {
	if r := install(t, "DBTRAIL_FOR=postgresql"); r.failed || !strings.Contains(r.out, pgPromise) {
		t.Errorf("DBTRAIL_FOR=postgresql is not the PostgreSQL path:\n%s", r.out)
	}
	if r := install(t, "DBTRAIL_FOR=mysql"); r.failed || !strings.Contains(r.out, promise) {
		t.Errorf("DBTRAIL_FOR=mysql is not the default path:\n%s", r.out)
	}
	r := install(t, "DBTRAIL_FOR=oracle")
	if !r.failed || !strings.Contains(r.out, `DBTRAIL_FOR is "oracle"; it takes postgres or mysql.`) {
		t.Errorf("DBTRAIL_FOR=oracle is not refused with its reason:\n%s", r.out)
	}
	if r.upCalls != 0 || strings.Contains(r.calls, "compose") {
		t.Errorf("DBTRAIL_FOR=oracle reached Docker:\n%s", r.calls)
	}
}

// install-postgres.sh is install.sh on the PostgreSQL path: run against the
// checkout's install.sh, it prints the PostgreSQL promise and names no other
// database.
func TestInstallPostgresScript_runsTheInstallerForPostgreSQL(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	r := installScript(t, "install-postgres.sh", nil, "DBTRAIL_INSTALLER="+filepath.Join(root, "install.sh"))
	if r.failed || !strings.Contains(r.out, "DBTrail is up.") || !strings.Contains(r.out, pgPromise) {
		t.Fatalf("install-postgres.sh did not run the installer for PostgreSQL:\n%s", r.out)
	}
	for _, h := range otherDatabaseLines(r.out, r.dir) {
		t.Errorf("install-postgres.sh names another database: %q", h)
	}
}

// A refusal on the PostgreSQL path suggests a command that comes back
// through the same door: install-postgres.sh, with the knobs on `sh`.
func TestInstallerPostgres_aRefusalSuggestsTheSameDoor(t *testing.T) {
	r := install(t, "DBTRAIL_FOR=postgres", "BUSY_PORTS=8090 8091")
	if !r.failed || r.upCalls != 0 {
		t.Fatalf("the installer went on with port 8090 taken:\n%s", r.out)
	}
	want := "curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/main/install-postgres.sh | DBTRAIL_DIR=" + r.dir + " DBTRAIL_PORT=8092 sh"
	if !strings.Contains(r.out, want) {
		t.Errorf("a taken port does not suggest the PostgreSQL door.\nwant: %s\ngot:\n%s", want, r.out)
	}
	r = install(t, "DBTRAIL_FOR=postgres", "STUB_RELEASE_FAIL=1")
	if !r.failed || !strings.Contains(r.out, "main/install-postgres.sh | DBTRAIL_DIR="+r.dir+" DBTRAIL_REF=vX.Y.Z sh") {
		t.Errorf("a release not found does not suggest the PostgreSQL door:\n%s", r.out)
	}
	for _, rr := range []run{r} {
		if strings.Contains(rr.out, "/install.sh |") {
			t.Errorf("the PostgreSQL path suggests install.sh:\n%s", rr.out)
		}
	}
}

// install-postgres.sh downloads install.sh from main and runs it for
// PostgreSQL.
func TestInstallPostgresScript_downloadsTheInstaller(t *testing.T) {
	r := installScript(t, "install-postgres.sh", nil)
	if r.failed || !strings.Contains(r.out, "DBTrail is up.") || !strings.Contains(r.out, pgPromise) {
		t.Fatalf("install-postgres.sh did not download and run the installer for PostgreSQL:\n%s", r.out)
	}
	if !strings.Contains(r.urls, "https://raw.githubusercontent.com/dbtrail/dbtrail/main/install.sh") {
		t.Errorf("install-postgres.sh did not download main's install.sh:\n%s", r.urls)
	}
}

// What comes back is checked before it runs: an empty answer or a page (a
// proxy, a sign-in portal) is refused, and nothing reaches Docker.
func TestInstallPostgresScript_refusesWhatIsNotTheInstaller(t *testing.T) {
	for _, body := range []string{"\n", "<html>sign in</html>", "#!/bin/sh\necho hi\n"} {
		r := installScript(t, "install-postgres.sh", nil, "STUB_INSTALLER_BODY="+body)
		if !r.failed || !strings.Contains(r.out, "is not the DBTrail installer") {
			t.Errorf("body %q: not refused with its reason:\n%s", body, r.out)
		}
		if strings.Contains(r.calls, "compose") {
			t.Errorf("body %q: Docker was reached:\n%s", body, r.calls)
		}
	}
}
