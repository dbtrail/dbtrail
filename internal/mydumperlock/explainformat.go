package mydumperlock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ExplainFormatSection is the option-file section that makes mydumper run its
// sessions with explain_format=TRADITIONAL (#2211). mydumper estimates each
// table's size with a plain EXPLAIN SELECT and reads the "rows" column of the
// result. Under explain_format=TREE (the default on Percona Server 9.7) or
// JSON, EXPLAIN returns a single column, and mydumper crashes with a
// segmentation fault before it dumps anything (mydumper/mydumper#2307).
// Measured against MySQL 9.4 with explain_format=TREE: 0.16.1, 0.16.9 and
// 1.0.3-1 (both builds of the console image) exit 139 without this section
// and 0 with it.
//
// The setting is a SESSION variable in mydumper's own connections: DBTrail
// never changes the source server's global configuration.
const ExplainFormatSection = "[mydumper_session_variables]\nexplain_format = TRADITIONAL\n"

// SessionVariablesFloor is the oldest mydumper build measured to take
// --defaults-extra-file and apply its [mydumper_session_variables] section
// (0.16.1-3 and 0.16.9-1 images, against MySQL 9.4 with explain_format=TREE).
// Older builds were not measured: no published image of them runs on arm64,
// and passing a flag a build does not know would fail a dump that may work.
var SessionVariablesFloor = Version{0, 16, 1}

// TakesSessionVariables reports whether this build is at or above
// SessionVariablesFloor.
func (v Version) TakesSessionVariables() bool { return !v.Less(SessionVariablesFloor) }

// SourceExplainFormat reads the session value of explain_format on the source.
// present is false when the server has no such variable (MariaDB, MySQL before
// 8.0.32): there mydumper must NOT be given the section, because a SET SESSION
// of an unknown variable makes mydumper exit 1 (measured with 1.0.3-1 against
// MariaDB 11.4 and MySQL 8.0.31), which would fail a dump that works today.
func SourceExplainFormat(ctx context.Context, db *sql.DB) (value string, present bool, err error) {
	var name string
	err = db.QueryRowContext(ctx, "SHOW SESSION VARIABLES WHERE Variable_name = 'explain_format'").Scan(&name, &value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read explain_format: %w", err)
	}
	return value, true, nil
}

// ExplainFormatPlan is what one dump does about explain_format.
type ExplainFormatPlan struct {
	// Send is true when the dump must pass ExplainFormatSection to mydumper.
	Send bool
	// Warning, when not empty, says why the section could not be sent to a
	// source whose format makes mydumper crash.
	Warning string
}

// PlanExplainFormat decides from what the source answered and what is known of
// the mydumper build. The section is sent whenever the variable exists and the
// build takes it, whatever the value: on TRADITIONAL it changes nothing, and
// any other value (TREE, JSON) makes EXPLAIN return no "rows" column.
// buildKnown is false when the build's version could not be read, in which
// case nothing is sent, as with a build below SessionVariablesFloor.
func PlanExplainFormat(present bool, value string, buildKnown bool, build Version) ExplainFormatPlan {
	if !present {
		return ExplainFormatPlan{}
	}
	if buildKnown && build.TakesSessionVariables() {
		return ExplainFormatPlan{Send: true}
	}
	if strings.EqualFold(strings.TrimSpace(value), "TRADITIONAL") {
		return ExplainFormatPlan{}
	}
	which := "this mydumper's version could not be read"
	if buildKnown {
		which = "this mydumper (" + build.String() + ") is older than " + SessionVariablesFloor.String()
	}
	return ExplainFormatPlan{Warning: fmt.Sprintf("the source runs with explain_format=%s, which makes mydumper crash "+
		"(segmentation fault) while it sizes tables, and %s, so it cannot be told to use TRADITIONAL for its own "+
		"sessions. Install mydumper %s or newer", strings.TrimSpace(value), which, SessionVariablesFloor)}
}
