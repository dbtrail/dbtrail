package console

import (
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// RefusedTablesCap is how many refused tables one run keeps by name. The rest
// are counted (RefusedTablesOmitted), never dropped silently: the run history
// holds BaselineRunHistoryCap runs per server in one file, and a server with
// thousands of tables refused at every slot would otherwise grow it without
// limit.
const RefusedTablesCap = 20

// refusedReasonCap and refusedNameCap bound one entry, in runes. A reason is
// an error text and can run to several lines; a name comes from the snapshot
// listing, and MySQL's own limit is 64 per identifier, so the cap only ever
// cuts a name that did not come from a server.
const (
	refusedReasonCap = 300
	refusedNameCap   = 200
)

// RefusedTable is one table that stopped a refresh or a restore, with the
// verdict the command line prints for it (reconstruct.RefreshOutcomes) and the
// reason. Publication is all-or-nothing, so each of these stopped the copy of
// every table, not only its own.
//
// Verdict is kept as written: a file from a newer version may carry one this
// version does not know, and the page shows it as it is.
type RefusedTable struct {
	Name    string `json:"name"`
	Verdict string `json:"verdict"`
	Reason  string `json:"reason,omitempty"`
}

// The three shapes a credential takes in an error text. Each errs toward
// removing too much: a reason with a word missing is still a reason.
var (
	// user:password@ in a URL. The password may hold an @ (it runs to the
	// last one before the path), not a slash, which a URL has to encode.
	urlCredentials = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s:@]+:[^/\s]*@`)
	// user:password@ in a MySQL DSN, which has no scheme:
	// user:password@tcp(host)/db, user:password@unix(path)/db, user:password@/db.
	dsnCredentials = regexp.MustCompile(`[^\s:@/()]+:\S*?@(tcp\(|unix\(|/)`)
	// password=secret, the key and value form.
	keyedPassword = regexp.MustCompile(`(?i)\b(password|passwd|pwd)=\S+`)
)

// barePasswordMin is the shortest password removed wherever it appears on
// its own. A shorter one is often a word ("test", "orders"), and removing a
// word everywhere can turn a reason into one about another table. Inside a
// connection string it is removed whatever its length.
const barePasswordMin = 8

// RefusedTablesOf keeps the refused tables of a run, in the order the run
// listed them, up to RefusedTablesCap, and counts the ones left out. Tables
// that were refreshed, unchanged or skipped are not kept: they did not stop
// anything.
//
// secrets are the connection strings the run used. Each one, and the password
// inside it, is removed from every reason, and so is the credentials part of
// any URL or DSN: the reason is an error text written far from here, and it
// reaches a browser and a file on disk.
func RefusedTablesOf(outcomes []reconstruct.RefreshOutcome, secrets ...string) (kept []RefusedTable, omitted int) {
	for _, o := range outcomes {
		if !o.Refused() {
			continue
		}
		if len(kept) >= RefusedTablesCap {
			omitted++
			continue
		}
		kept = append(kept, RefusedTable{
			Name:    clipRunes(oneLine(o.Table), refusedNameCap),
			Verdict: o.Verdict,
			Reason:  clipRunes(oneLine(scrubSecrets(o.Detail, secrets)), refusedReasonCap),
		})
	}
	return kept, omitted
}

// scrubSecrets removes each connection string and its password from msg, then
// the credentials of any URL or DSN left in it.
func scrubSecrets(msg string, secrets []string) string {
	for _, dsn := range secrets {
		if dsn == "" {
			continue
		}
		msg = strings.ReplaceAll(msg, dsn, "<dsn>")
		if cfg, err := mysql.ParseDSN(dsn); err == nil && len(cfg.Passwd) >= barePasswordMin {
			msg = strings.ReplaceAll(msg, cfg.Passwd, "***")
		}
	}
	msg = urlCredentials.ReplaceAllString(msg, "${1}***@")
	msg = dsnCredentials.ReplaceAllString(msg, "***@${1}")
	return keyedPassword.ReplaceAllString(msg, "${1}=***")
}

// oneLine folds every run of whitespace, line breaks included, into one space
// and drops bytes that are not text.
func oneLine(s string) string {
	return strings.Join(strings.Fields(strings.ToValidUTF8(s, "?")), " ")
}

// clipRunes cuts s to at most n runes, ending in "..." when it cut.
func clipRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n-3]) + "..."
}

// withholdRefusedTables drops the refused tables' names and reasons from st
// for a session that carries a data profile, keeping the count. The snapshot
// listing is refused outright to such a session, because nothing in it is
// filtered by the profile; the statuses served outside that listing follow the
// same rule for the one part of them that names tables one by one.
func withholdRefusedTables(r *http.Request, st BaselineStatus) BaselineStatus {
	if sessionRestricted(r) {
		st.RefusedTables, st.RefusedTablesOmitted = nil, 0
	}
	return st
}

// withholdScheduleTables is withholdRefusedTables for the schedule's state,
// which carries the list on its last run and on its last fallback.
func withholdScheduleTables(r *http.Request, dto *backupScheduleDTO) *backupScheduleDTO {
	if dto == nil || !sessionRestricted(r) {
		return dto
	}
	if dto.LastRun != nil {
		dto.LastRun.RefusedTables, dto.LastRun.RefusedTablesOmitted = nil, 0
		// The names go, the count stays: "3 tables are not in your copy
		// yet" names nothing a profile could deny.
		dto.LastRun.NewTables, dto.LastRun.NewTablesOmitted = nil, dto.LastRun.NewTablesOmitted+len(dto.LastRun.NewTables)
		dto.LastRun.NewTablesUnchecked = withheldUnchecked(dto.LastRun.NewTablesUnchecked)
	}
	if dto.LastFallback != nil {
		dto.LastFallback.RefusedTables, dto.LastFallback.RefusedTablesOmitted = nil, 0
	}
	return dto
}

// NewTablesOf keeps the names of the tables a published update left out
// (#1993), sorted as given, up to RefusedTablesCap, and counts the rest: the
// same bound and the same reason as RefusedTablesOf (a migration that creates
// hundreds of tables must not grow the run history without limit).
func NewTablesOf(names []string) (kept []string, omitted int) {
	for _, n := range names {
		if len(kept) >= RefusedTablesCap {
			omitted++
			continue
		}
		kept = append(kept, clipRunes(oneLine(n), refusedNameCap))
	}
	return kept, omitted
}

// ScrubReason is the treatment RefusedTablesOf gives a reason, for an error
// text that is not a table's: one line, at most refusedReasonCap runes, and
// each connection string in secrets, its password and any credentials left
// in a URL or DSN removed. It reaches a browser and a file on disk.
func ScrubReason(msg string, secrets ...string) string {
	return clipRunes(oneLine(scrubSecrets(msg, secrets)), refusedReasonCap)
}

// withheldUnchecked keeps the fact that the new-tables check did not run and
// drops the driver's error, which can name the source's account and host.
func withheldUnchecked(reason string) string {
	if reason == "" {
		return ""
	}
	return "the source could not be asked"
}
