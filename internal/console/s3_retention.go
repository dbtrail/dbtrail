package console

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/cliutil"
)

// S3RetainMin is the shortest age a server's S3 retention can be (#2596).
// The newest snapshot of each table is kept whatever the age, so a shorter
// one would not empty the bucket, but it would leave it holding only the
// present.
const S3RetainMin = 24 * time.Hour

// ParseS3Retain reads ServerEntry.BaselineS3Retain: empty is off.
func ParseS3Retain(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	d, err := cliutil.ParseRetain(raw)
	if err != nil {
		return 0, fmt.Errorf("delete from S3 after: %w", err)
	}
	if d < S3RetainMin {
		return 0, fmt.Errorf("delete from S3 after: %s is shorter than a day; set 1d or more", raw)
	}
	return d, nil
}

func normS3(u string) string { return strings.TrimSuffix(strings.TrimSpace(u), "/") }

// S3RetentionFor is the age past which the daemon removes snapshots from the
// S3 destination dest, and the IDs of the servers whose destination it is,
// sorted. It is on only when every one of them has an age set:
//
//   - Servers sharing a destination share its snapshots, and a snapshot does
//     not record which of them wrote it, so the longest age decides, and one
//     with none keeps them all.
//   - The daemon's own destination (globalS3, its startup flags) is never
//     pruned: what writes there has no age to read.
//   - The age is never shorter than localRetain, the local "Delete by age".
//     The local prune removes a local snapshot only once its copy is in S3;
//     one removed from S3 first would stay on this disk for good, and be
//     sent again.
//
// A value this build cannot read is off: a file edited by hand must not
// remove anything.
func S3RetentionFor(entries []ServerEntry, globalS3, dest string, localRetain time.Duration) (time.Duration, []string, bool) {
	dest = normS3(dest)
	if dest == "" {
		return 0, nil, false
	}
	var ids []string
	var age time.Duration
	on := true
	for _, e := range entries {
		if normS3(e.BaselineS3) != dest {
			continue
		}
		ids = append(ids, e.ID)
		d, err := ParseS3Retain(e.BaselineS3Retain)
		if err != nil || d <= 0 {
			on = false
		}
		age = max(age, d)
	}
	slices.Sort(ids)
	if len(ids) == 0 || !on || normS3(globalS3) == dest {
		return 0, ids, false
	}
	return max(age, localRetain), ids, true
}

// keepS3Retain is the S3 age an edit that rebuilds the entry carries over:
// the stored one while the destination stays the same, none once it moves.
func keepS3Retain(old ServerEntry, newS3 string) string {
	if normS3(newS3) != normS3(old.BaselineS3) {
		return ""
	}
	return old.BaselineS3Retain
}

// s3AgeLowered reports whether replacing the registry's entries `before`
// with `after` turns the S3 cleanup on, or shortens its age, at any
// destination either names (#2596). The age of a destination is derived
// from every server that shares it, so a change to one server (moving it
// away, deleting it) can switch another's on: that is a removal from the
// bucket, and asks for what setting the age asks for.
func s3AgeLowered(before, after []ServerEntry) bool {
	dests := map[string]bool{}
	for _, list := range [][]ServerEntry{before, after} {
		for _, e := range list {
			if d := normS3(e.BaselineS3); d != "" {
				dests[d] = true
			}
		}
	}
	for d := range dests {
		was, _, wasOn := S3RetentionFor(before, "", d, 0)
		now, _, nowOn := S3RetentionFor(after, "", d, 0)
		if nowOn && (!wasOn || now < was) {
			return true
		}
	}
	return false
}

// withEntry is entries with e in place of the entry of the same ID, or
// without it when e is nil (a deletion of id).
func withEntry(entries []ServerEntry, id string, e *ServerEntry) []ServerEntry {
	out := make([]ServerEntry, 0, len(entries))
	for _, x := range entries {
		if x.ID != id {
			out = append(out, x)
		} else if e != nil {
			out = append(out, *e)
		}
	}
	return out
}

// s3AgeMu makes the s3AgeLowered check and the registry write it guards one
// step: two edits checked against the same registry (deleting two servers
// that each kept a shared destination's cleanup off) would each pass alone
// and together switch it on. Every handler that can move a destination or
// delete a server holds it from its List to its write. This process is the
// registry's only web writer, so a process lock is the whole of it.
var s3AgeMu sync.Mutex

// mayLowerS3Age refuses (403) a change that s3AgeLowered finds, for a session
// without settings:write. True when the change may go ahead.
func mayLowerS3Age(w http.ResponseWriter, r *http.Request, before, after []ServerEntry) bool {
	pol := policyFrom(r.Context())
	if pol == nil || pol.Allows(ext.PermSettingsWrite) || !s3AgeLowered(before, after) {
		return true
	}
	recordConsoleDeny(r, "authz.denied", string(ext.PermSettingsWrite), map[string]string{"field": "baseline_s3_retain"})
	writeJSONError(w, http.StatusForbidden, "forbidden: this change starts removing snapshots from an S3 destination another server shares, or removes them sooner, which needs the "+string(ext.PermSettingsWrite)+" permission")
	return false
}
