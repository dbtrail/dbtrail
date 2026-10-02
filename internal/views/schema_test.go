package views

import (
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// TestValidateDatabaseName is the list of names written down BEFORE the
// validator, one per way a value typed at a flag can go wrong.
//
// Every refusal is checked for its REASON, not only for being a refusal: a
// validator that refused everything would pass a test that asked for errors
// alone, and the accepted half below is what keeps it honest in the other
// direction.
func TestValidateDatabaseName(t *testing.T) {
	for _, name := range []string{
		"wp", "a", "b", "rds_prod", "server1", "_x", "a1_b2", "wp_posts",
		// A keyword DuckDB takes bare. Refusing every keyword would refuse
		// names an operator reasonably picks (`prod`, `data`, `index`).
		"data", "index", "user",
		strings.Repeat("a", maxSchemaNameLen),
	} {
		if err := ValidateDatabaseName(name); err != nil {
			t.Errorf("%q was refused, and it is a usable name: %v", name, err)
		}
	}

	for _, tc := range []struct {
		name, in, want string
	}{
		{"empty", "", "empty"},
		{"only spaces", "   ", "letters, digits and underscore"},
		{"inner space", "my server", "letters, digits and underscore"},
		{"leading space", " wp", "letters, digits and underscore"},
		{"trailing space", "wp ", "letters, digits and underscore"},
		{"tab", "wp\tx", "letters, digits and underscore"},
		{"newline", "wp\nx", "letters, digits and underscore"},
		{"dot", "wp.prod", "letters, digits and underscore"},
		{"double quote", `wp"x`, "letters, digits and underscore"},
		{"single quote", "wp'x", "letters, digits and underscore"},
		{"semicolon", "wp;x", "letters, digits and underscore"},
		{"dash", "wp-prod", "letters, digits and underscore"},
		{"slash", "wp/prod", "letters, digits and underscore"},
		{"backslash", `wp\prod`, "letters, digits and underscore"},
		{"comment opener", "wp--x", "letters, digits and underscore"},
		{"unicode letter", "señor", "letters, digits and underscore"},
		{"unicode that lowercases to ascii", "Kelvin", "letters, digits and underscore"},
		{"starts with a digit", "1wp", "start with a letter or an underscore"},
		{"only digits", "2026", "start with a letter or an underscore"},
		{"uppercase", "WP", "lowercase"},
		{"mixed case", "Wp", "lowercase"},
		{"one past the longest", strings.Repeat("a", maxSchemaNameLen+1), "too long"},
		{"very long", strings.Repeat("a", 5000), "too long"},
		{"main", "main", "already has"},
		{"temp", "temp", "already has"},
		{"system", "system", "already has"},
		{"memory", "memory", "already has"},
		{"information_schema", "information_schema", "already has"},
		{"pg_catalog", "pg_catalog", "already has"},
		// Refused as uppercase first, which is the point: there is no spelling
		// of the default schema that gets through.
		{"MAIN", "MAIN", "lowercase"},
		{"Main", "Main", "lowercase"},
		{"reserved word select", "select", "SQL keyword"},
		{"reserved word order", "order", "SQL keyword"},
		{"keyword that is not reserved but will not parse bare", "left", "SQL keyword"},
		{"the alias of another server's index", "wp_live", "_live"},
		{"the suffix alone", "_live", "_live"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDatabaseName(tc.in)
			if err == nil {
				t.Fatalf("%q was accepted", tc.in)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal of %q does not say %q: %v", tc.in, tc.want, err)
			}
			// The refusal is printed on a terminal. A 5000-character or
			// multi-line value echoed back whole buries the reason.
			if len(err.Error()) > 400 || strings.ContainsAny(err.Error(), "\n\t") {
				t.Errorf("the refusal of %q would not fit a terminal line: %q", tc.in, err.Error())
			}
			for _, dash := range []string{"—", "–"} {
				if strings.Contains(err.Error(), dash) {
					t.Errorf("the refusal of %q contains %q", tc.in, dash)
				}
			}
		})
	}
}

// schemaInputs is every shape of file that names something a second file
// could also name: the pinned golden layout, both following modes, a table
// with a delta, and each of those with the live leg.
func schemaInputs() map[string]Input {
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	snap := time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC)
	live := func() *LiveIndex {
		return &LiveIndex{Host: "db.internal", Port: 3306, Database: "idx", User: "reader", BintrailID: "x"}
	}

	pinned := goldenInput()

	pointer := Input{
		GeneratedAt: at, Version: "test", BaselineSource: "/data/baselines", BaselineSnapshot: snap,
		Follow: FollowPointer,
		Baselines: []BaselineTable{
			{Schema: "shop", Table: "orders", Path: "/data/baselines/current/shop/orders.parquet", Rel: "shop/orders.parquet"},
			{Schema: "shop", Table: "lines", Path: "/data/baselines/current/shop/lines.parquet", Rel: "shop/lines.parquet", Delta: true},
		},
		ArchiveSources: []string{"/data/archives/bintrail_id=x"},
	}

	newest := Input{
		GeneratedAt: at, Version: "test", BaselineSource: "s3://b/baselines/", BaselineSnapshot: snap,
		Follow: FollowNewest,
		Baselines: []BaselineTable{
			{Schema: "shop", Table: "orders", Path: "s3://b/baselines/2026-04-30T03-00-00Z/shop/orders.parquet", Rel: "shop/orders.parquet",
				SchemaKnown: true, Decimals: []DecimalColumn{{Name: "total", Precision: 10, Scale: 2}}},
			{Schema: "shop", Table: "lines", Path: "s3://b/baselines/2026-04-30T03-00-00Z/shop/lines.parquet", Rel: "shop/lines.parquet", Delta: true},
			{Schema: "shop", Table: "old", Path: "s3://b/baselines/2026-04-30T03-00-00Z/shop/old.parquet", Rel: "shop/old.parquet", Delta: true, DeltaLegacy: true},
		},
		ArchiveSources: []string{"s3://b/archives/bintrail_id=x"},
		BucketStores:   map[string]storage.BucketStore{},
	}

	grouped := Input{
		GeneratedAt: at, Version: "test",
		ArchiveSources: []string{"/data/archives/bintrail_id=x"},
		ArchiveGroups: []ArchiveGroup{
			{Columns: []string{"event_id", "event_timestamp"}, Files: []string{"/data/archives/bintrail_id=x/event_date=2026-05-01/event_hour=03/a.parquet"}},
			{Columns: []string{"event_id", "event_timestamp", "gtid"}, Files: []string{"/data/archives/bintrail_id=x/event_date=2026-05-01/event_hour=04/a.parquet"}},
		},
	}

	liveOnly := Input{GeneratedAt: at, Version: "test", LiveIndex: live()}

	out := map[string]Input{
		"pinned": pinned, "pointer": pointer, "newest": newest, "grouped": grouped, "live only": liveOnly,
	}
	for _, name := range []string{"pinned", "pointer", "newest", "grouped"} {
		in := out[name]
		in.LiveIndex = live()
		out[name+" with the live leg"] = in
	}
	return out
}

var (
	createViewRE  = regexp.MustCompile(`(?m)^CREATE OR REPLACE VIEW (.+) AS$`)
	setVariableRE = regexp.MustCompile(`(?m)^SET VARIABLE (\w+) = `)
	getVariableRE = regexp.MustCompile(`getvariable\('(\w+)'\)`)
	attachRE      = regexp.MustCompile(`(?m)^ATTACH '' AS ("[^"]+") `)
	attachSecret  = regexp.MustCompile(`(?m)^ATTACH '' AS "[^"]+" \(TYPE mysql, SECRET ("[^"]+"), READ_ONLY\);$`)
	createSecret  = regexp.MustCompile(`(?m)^CREATE OR REPLACE SECRET (\S+) \(`)
	liveFromRE    = regexp.MustCompile(`FROM ("[^"]+")\."binlog_events"`)
	liveProseRE   = regexp.MustCompile("(\"[^\"]+\"|`[^`]+`)\\.\"binlog_events\"")
	createSchema  = regexp.MustCompile(`(?m)^CREATE SCHEMA IF NOT EXISTS (.+);$`)
	attachDBRE    = regexp.MustCompile(`(?m)^ATTACH IF NOT EXISTS ':memory:' AS (.+);$`)
)

func captures(re *regexp.Regexp, s string) []string {
	var out []string
	for _, m := range re.FindAllStringSubmatch(s, -1) {
		out = append(out, m[1])
	}
	return out
}

// sharedAcrossServers are the names two files are ALLOWED to have in common,
// each for a stated reason. Everything else a file creates has to differ.
//
// The S3 secret is the one entry. It holds no server's identity: it is "use
// the credential chain", and the same statement from two files is the same
// secret. Two servers whose buckets live in DIFFERENT stores or regions do
// still overwrite each other's copy of it, which a schema cannot fix (a secret
// has no schema) and the documentation states.
var sharedAcrossServers = []string{"bintrail_s3_chain"}

// TestSchema_nothingAFileCreatesIsSharedWithAnotherServers is the generic
// half: it does not list the names this change derived, it reads the file for
// EVERYTHING it creates and requires two servers' files to have none of it in
// common.
//
// That is what makes it a guard for the next name as well as for these. A
// session variable, a secret or an alias added later is created by a
// statement one of these patterns matches, and it fails here until it is
// derived from the schema too.
func TestSchema_nothingAFileCreatesIsSharedWithAnotherServers(t *testing.T) {
	for name, in := range schemaInputs() {
		t.Run(name, func(t *testing.T) {
			created := func(schema string) []string {
				in := in
				in.Database = schema
				out := Generate(in)
				var all []string
				for _, re := range []*regexp.Regexp{createViewRE, setVariableRE, attachRE, attachDBRE, createSecret} {
					all = append(all, captures(re, out)...)
				}
				if len(all) == 0 {
					t.Fatalf("the file creates nothing this test can see, so it would pass on anything:\n%s", out)
				}
				return all
			}
			a, b := created("aa"), created("bb")
			for _, n := range a {
				if slices.Contains(b, n) && !slices.Contains(sharedAcrossServers, strings.Trim(n, `"`)) {
					t.Errorf("both servers' files create %s, so the second one loaded replaces the first", n)
				}
			}
		})
	}
}

// TestSchema_everyReferenceStaysInsideTheFile is the half about what the
// views READ, which is where an overwrite hides best: the view has its own
// qualified name, is listed under its own schema, and returns another server's
// rows.
func TestSchema_everyReferenceStaysInsideTheFile(t *testing.T) {
	for name, in := range schemaInputs() {
		t.Run(name, func(t *testing.T) {
			in.Database = "wp"
			out := Generate(in)

			// Every view is created in the schema, quoted.
			views := captures(createViewRE, out)
			for _, v := range views {
				if !strings.HasPrefix(v, `"wp"."`) {
					t.Errorf("view %s is not created in the schema", v)
				}
			}
			if len(views) == 0 {
				t.Fatalf("no view in the file:\n%s", out)
			}

			// The schema exists before the first view that needs it, once.
			dbs := captures(attachDBRE, out)
			if len(dbs) != 1 || dbs[0] != `"wp"` {
				t.Fatalf("databases attached = %v, want exactly one for \"wp\"", dbs)
			}
			if strings.Index(out, "ATTACH IF NOT EXISTS") > strings.Index(out, "CREATE OR REPLACE VIEW") {
				t.Error("the database is attached after a view that lives in it")
			}
			for _, s := range captures(createSchema, out) {
				if !strings.HasPrefix(s, `"wp"."`) {
					t.Errorf("schema %s is not created in the database", s)
				}
			}

			// A variable read is a variable this file set.
			set := captures(setVariableRE, out)
			for _, v := range captures(getVariableRE, out) {
				if !slices.Contains(set, v) {
					t.Errorf("the file reads session variable %s and never sets it", v)
				}
			}
			for _, v := range set {
				if !strings.HasSuffix(v, "_wp") {
					t.Errorf("session variable %s does not carry the schema, so another server's file sets the same one", v)
				}
			}

			// The index leg reads the catalog this file attached, through the
			// secret this file created.
			attached := captures(attachRE, out)
			if in.LiveIndex == nil {
				if len(attached) != 0 {
					t.Errorf("an ATTACH in a file with no live leg: %v", attached)
				}
				return
			}
			if len(attached) != 1 || attached[0] != `"wp_live"` {
				t.Fatalf("attached catalogs = %v, want exactly \"wp_live\"", attached)
			}
			from := captures(liveFromRE, out)
			if len(from) == 0 {
				t.Fatal("the events view has no index leg")
			}
			for _, c := range from {
				if c != attached[0] {
					t.Errorf("the index leg reads catalog %s, and this file attached %s", c, attached[0])
				}
			}
			// The comments tell the reader which catalog to query by hand. A
			// stale name there sends them to another server's index.
			for _, c := range captures(liveProseRE, out) {
				if strings.Trim(c, "\"`") != "wp_live" {
					t.Errorf("the file names catalog %s for the reader to query, and attached \"wp_live\"", c)
				}
			}
			secrets := captures(attachSecret, out)
			if len(secrets) != 1 || !slices.Contains(captures(createSecret, out), secrets[0]) {
				t.Errorf("the ATTACH reads secret %v, which this file does not create", secrets)
			}
			for _, stale := range []string{"bintrail_live", `"bintrail_index"`} {
				if strings.Contains(out, stale) {
					t.Errorf("the file still names %s, which every server's file would share", stale)
				}
			}
		})
	}
}

// TestSchema_emptyIsTheFileAsItWas states the default in the one way the
// golden files cannot: they pin five inputs, and this holds for every shape
// above. A file generated with no schema names none.
func TestSchema_emptyIsTheFileAsItWas(t *testing.T) {
	for name, in := range schemaInputs() {
		t.Run(name, func(t *testing.T) {
			out := Generate(in)
			if strings.Contains(out, "ATTACH IF NOT EXISTS") {
				t.Error("a file generated with no database attaches one")
			}
			// Two parts at most: "schema"."table", or "events" alone.
			quotedPart := regexp.MustCompile(`^"(?:[^"]|"")*"(?:\."(?:[^"]|"")*")?$`)
			for _, v := range captures(createViewRE, out) {
				if !quotedPart.MatchString(v) {
					t.Errorf("view %s is qualified with a database in a file generated with none", v)
				}
			}
			if in.LiveIndex != nil && !strings.Contains(out, `ATTACH '' AS "bintrail_live" (TYPE mysql, SECRET "bintrail_index", READ_ONLY);`) {
				t.Error("the ATTACH of a file generated with no schema changed")
			}
			for _, v := range captures(setVariableRE, out) {
				if !slices.Contains([]string{newestVar, missingVar, checkVar}, v) {
					t.Errorf("session variable %s is not one the file set before", v)
				}
			}
		})
	}
}

// TestSchema_isQuotedWhereverItIsWritten: the validator is the producer's
// job, and the generator does not rely on it. A name that reached Generate
// unvalidated must still not be able to end its identifier.
func TestSchema_isQuotedWhereverItIsWritten(t *testing.T) {
	in := schemaInputs()["pinned with the live leg"]
	in.Database = `x"; DROP VIEW y; --`
	out := Generate(in)
	// With every quoted identifier and string literal taken out of a line,
	// what is left is the SQL the engine would parse as statements.
	quoted := regexp.MustCompile(`"(?:[^"]|"")*"|'(?:[^']|'')*'`)
	seen := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		if strings.Contains(line, "DROP") {
			seen++
		}
		if strings.Contains(quoted.ReplaceAllString(line, ""), "DROP") {
			t.Errorf("the schema name left its quotes: %s", line)
		}
	}
	if seen < 3 {
		t.Errorf("the name was written on %d statement line(s); this input has a schema, a view and an ATTACH to write it on", seen)
	}
	if !strings.Contains(out, `ATTACH IF NOT EXISTS ':memory:' AS "x""; DROP VIEW y; --";`) {
		t.Errorf("the database is not attached under its quoted name:\n%s", out)
	}
}

// TestGenerateViews_schema: the entry point a caller EXECUTES gets the schema
// statement with its views and an empty string without them, as before.
func TestGenerateViews_schema(t *testing.T) {
	in := goldenInput()
	in.Database = "wp"
	out := GenerateViews(in)
	if strings.Index(out, `ATTACH IF NOT EXISTS ':memory:' AS "wp";`) != 0 {
		t.Errorf("the database is not the first statement:\n%s", out)
	}
	if !strings.Contains(out, `CREATE OR REPLACE VIEW "wp"."main"."events" AS`) {
		t.Errorf("the events view is not qualified:\n%s", out)
	}
	in.OnlyViews = ViewSet{}
	if got := GenerateViews(in); got != "" {
		t.Errorf("a render that selected no view is not empty: %q", got)
	}
}
