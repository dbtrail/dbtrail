package doctor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// MariaDBVersionCheckName is the doctor row that grades a MariaDB source's
// version against the supported minimum.
const MariaDBVersionCheckName = "MariaDB version"

// The oldest MariaDB release DBTrail supports as a source. 10.6 reached end
// of life on 2026-07-06; 10.11, 11.4, 11.8 and 12.3 are the versions CI
// tests. Keep this and the CI matrix in step.
const (
	mariaDBMinMajor = 10
	mariaDBMinMinor = 11
)

// checkMariaDBVersion grades a VERSION() string the source connection check
// already read, so it costs no query. ok is false for anything that is not a
// MariaDB, and then the report has no row: MySQL output stays as it was.
//
// An older MariaDB gets a WARN, never a FAIL. No MySQL version is refused
// either, and capture on 10.6 worked when it was tested; what changes is that
// nothing proves it any more.
func checkMariaDBVersion(version string) (c CheckResult, ok bool) {
	if metadata.ClassifyVersion(version) != metadata.FlavorMariaDB {
		return CheckResult{}, false
	}
	v := strings.TrimSpace(version)
	// Old clients hide MariaDB behind a fake "5.5.5-" MySQL version.
	v = strings.TrimPrefix(v, "5.5.5-")
	major, minor, parsed := majorMinor(v)
	if !parsed {
		return CheckResult{
			Name:   MariaDBVersionCheckName,
			Status: StatusSkip,
			Detail: fmt.Sprintf("could not read a version number from VERSION() = %q", strings.TrimSpace(version)),
		}, true
	}
	number := v
	if i := strings.IndexByte(v, '-'); i > 0 {
		number = v[:i]
	}
	if major > mariaDBMinMajor || (major == mariaDBMinMajor && minor >= mariaDBMinMinor) {
		return CheckResult{Name: MariaDBVersionCheckName, Status: StatusPass, Detail: "MariaDB " + number}, true
	}
	return CheckResult{
		Name:   MariaDBVersionCheckName,
		Status: StatusWarn,
		Detail: fmt.Sprintf("MariaDB %s is older than %d.%d, the oldest version DBTrail supports",
			number, mariaDBMinMajor, mariaDBMinMinor),
		Remediation: "Capture may still work, but no test covers this version.\n" +
			"Upgrade the source to MariaDB 10.11 or newer. DBTrail tests 10.11, 11.4, 11.8 and 12.3.",
	}, true
}

// majorMinor reads the leading "major.minor" of a version like
// "10.11.8-MariaDB-log". The minor may be followed by anything that is not a
// digit.
func majorMinor(v string) (major, minor int, ok bool) {
	head, rest, found := strings.Cut(v, ".")
	if !found {
		return 0, 0, false
	}
	major, err := strconv.Atoi(head)
	if err != nil {
		return 0, 0, false
	}
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	minor, err = strconv.Atoi(rest[:end]) // an empty minor fails here

	if err != nil {
		return 0, 0, false
	}
	return major, minor, true
}
