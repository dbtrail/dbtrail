package verify

import (
	"regexp"
	"slices"
	"strings"
)

// A foreign key that cascades is applied by the server itself: on MariaDB and
// on MySQL before 9.6 the change to the CHILD rows is not written to the binary
// log. A copy built from the recorded changes therefore never sees it, and a
// later read of the database differs from it on exactly those rows. The check
// reports that as the mismatch it is; this says why, so it is not read as a
// broken table.

// fkLine is one foreign key of SHOW CREATE TABLE output, as the backup stores
// it: a constraint line of its own, the parent's name in backticks (with its
// schema in front when it lives in another one).
var fkLine = regexp.MustCompile("(?i)^CONSTRAINT\\s+`(?:[^`]|``)+`\\s+FOREIGN KEY\\s*\\([^)]*\\)\\s*REFERENCES\\s+((?:`(?:[^`]|``)+`\\.)?`(?:[^`]|``)+`)\\s*\\([^)]*\\)(.*)$")

// fkRule is one referential action that changes child rows.
var fkRule = regexp.MustCompile(`(?i)\bON\s+(DELETE|UPDATE)\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT)\b`)

// cascadeNote is the sentence for a table whose CREATE TABLE text (a
// baseline's footer) declares foreign keys that change its rows when the
// parent changes; "" when it declares none, or the text is not MySQL's.
func cascadeNote(createSQL string) string {
	var rules []string
	for _, line := range strings.Split(createSQL, "\n") {
		m := fkLine.FindStringSubmatch(strings.TrimSuffix(strings.TrimSpace(line), ","))
		if m == nil {
			continue
		}
		parent := strings.ReplaceAll(strings.ReplaceAll(m[1], "``", "\x00"), "`", "")
		parent = strings.ReplaceAll(parent, "\x00", "`")
		for _, r := range fkRule.FindAllStringSubmatch(m[2], -1) {
			rule := parent + " (ON " + strings.ToUpper(r[1]) + " " + strings.ToUpper(strings.Join(strings.Fields(r[2]), " ")) + ")"
			if !slices.Contains(rules, rule) {
				rules = append(rules, rule)
			}
		}
	}
	if len(rules) == 0 {
		return ""
	}
	return "Likely cause: this table has a foreign key the server applies by itself: " + strings.Join(rules, ", ") + ". " +
		"MariaDB and MySQL before 9.6 do not write those changes to the binary log, so a copy updated from the recorded changes never sees them. " +
		"Your database is not affected; a full read of it brings the copy back in line"
}

// withCascadeNote adds the pair's note to a mismatch. Only to a mismatch: a
// match needs no cause, and an inconclusive or failed table was not compared.
func withCascadeNote(res *TableResult, p BaselinePair) {
	if res.Status == StatusMismatch && p.NewCascadeNote != "" {
		res.Detail += ". " + p.NewCascadeNote
	}
}
