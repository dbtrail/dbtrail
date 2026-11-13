package baseline

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// LastPruneFile records, beside the snapshots it is about, the last prune that
// removed any (#1681). The Snapshots page reads it to say why copies are gone:
// a copy vanishing from the list with no sentence saying why is the silent
// failure retention must not introduce. It lives in the snapshot root rather
// than in the console's registry because the fact is about this directory's
// contents, it has to survive a restart, and `bintrail baseline
// --baseline-retain` removes copies there too (after its upload). Dot-prefixed, a regular file, and not a timestamp, so
// discovery skips it; the upload names it explicitly (isPruneArtifact).
const LastPruneFile = ".last-prune.json"

// LastPrune is the record: when the last prune that removed anything ran, and
// how many snapshots it removed.
type LastPrune struct {
	At      time.Time `json:"at"`
	Removed int       `json:"removed"`
}

// ReadLastPrune reads dir's record. ok is false when there is none (never
// pruned, or no such directory). An unreadable or malformed record is an
// error, never "never pruned": that would drop the one sentence explaining
// why copies are gone.
func ReadLastPrune(dir string) (rec LastPrune, ok bool, err error) {
	b, err := os.ReadFile(filepath.Join(dir, LastPruneFile))
	if errors.Is(err, os.ErrNotExist) {
		return LastPrune{}, false, nil
	}
	if err != nil {
		return LastPrune{}, false, fmt.Errorf("read the last prune record: %w", err)
	}
	if err := json.Unmarshal(b, &rec); err != nil {
		return LastPrune{}, false, fmt.Errorf("read the last prune record %s: %w", filepath.Join(dir, LastPruneFile), err)
	}
	if rec.At.IsZero() || rec.Removed <= 0 {
		return LastPrune{}, false, fmt.Errorf("the last prune record %s is incomplete", filepath.Join(dir, LastPruneFile))
	}
	return rec, true, nil
}

// writeRecord, writeFailureRecord and removeRecord are indirected only so a
// test can fail the write and see the stale record go.
var (
	writeRecord          = writeLastPrune
	writeFailureRecord   = writePruneFailure
	writeNotInDestRecord = writeNotInDestination
	removeRecord         = os.Remove
)

// LastPruneFailureFile records, beside the snapshots, the last prune attempt
// that FAILED (#1681): when, and why. A successful attempt removes it. Without
// it a folder that stops shrinking is visible only in the daemon's log, while
// the page goes on saying how many snapshots it keeps.
const LastPruneFailureFile = ".last-prune-failure.json"

// PruneFailure is that record.
type PruneFailure struct {
	At     time.Time `json:"at"`
	Reason string    `json:"reason"`
}

// ReadLastPruneFailure reads dir's failure record; ok is false when the last
// attempt did not fail. An unreadable record is an error, never "no failure".
func ReadLastPruneFailure(dir string) (rec PruneFailure, ok bool, err error) {
	b, err := os.ReadFile(filepath.Join(dir, LastPruneFailureFile))
	if errors.Is(err, os.ErrNotExist) {
		return PruneFailure{}, false, nil
	}
	if err != nil {
		return PruneFailure{}, false, fmt.Errorf("read the prune failure record: %w", err)
	}
	if err := json.Unmarshal(b, &rec); err != nil || rec.At.IsZero() || rec.Reason == "" {
		return PruneFailure{}, false, fmt.Errorf("the prune failure record %s cannot be read", filepath.Join(dir, LastPruneFailureFile))
	}
	return rec, true, nil
}

// NotInDestinationFile records, beside the snapshots, how many snapshots the
// last prune attempt kept because the S3 destination answered that it does
// not have them (2026-10-01). They are old enough to go, but a local snapshot
// with no S3 copy is the only copy, so it stays, and before this the only
// place that said so was the daemon's log. Snapshots that could not be
// CHECKED are not counted here: that attempt failed, and LastPruneFailureFile
// says it. Written by every completed attempt against an S3 destination,
// removed when the count is zero or the folder has no destination any more.
const NotInDestinationFile = ".not-in-destination.json"

// NotInDestination is that record.
type NotInDestination struct {
	At    time.Time `json:"at"`
	Count int       `json:"count"`
	// Names are the first of them (notInDestinationNamesKept), oldest
	// first, so the page can say which; absent in a record an older build
	// wrote.
	Names []string `json:"names,omitempty"`
	// Resending says this daemon sends them again after each cleanup
	// (#2563); false, or absent, when nothing here will, and the page says
	// to send or remove them by hand.
	Resending bool `json:"resending,omitempty"`
	// NotAsked names those among them the destination could not be ASKED
	// about: not known to be missing (the last restore's result whose
	// question failed, a snapshot S3 refused, #2689).
	NotAsked []string `json:"not_asked,omitempty"`
	// Unasked is how a build before #2689 named the one such snapshot it
	// could have (the last restore's result). Written as the first of
	// NotAsked, so a record an older build reads after a downgrade still
	// names one; read back into NotAsked by ReadNotInDestination.
	Unasked string `json:"unasked,omitempty"`
	// ReadRefused names those among them that were sent again, which S3
	// accepted, and that S3 then refused to read back (#2696): a destination
	// that takes uploads without s3:GetObject. They are not sent again.
	ReadRefused []string `json:"read_refused,omitempty"`
	// NotAskedCount and ReadRefusedCount are how many NotAsked and
	// ReadRefused name in all (#2701): the lists are cut to
	// notInDestinationNamesKept like Names, and the page counts with these.
	// Zero in a record an older build wrote: the lists are whole there.
	NotAskedCount    int `json:"not_asked_count,omitempty"`
	ReadRefusedCount int `json:"read_refused_count,omitempty"`
	// NeedsListBucket says the last send was refused because S3 would not
	// say whether its own cleanup is removing the snapshot's folder: with
	// "Delete from S3 after" set, that question needs s3:ListBucket (#2701).
	NeedsListBucket bool `json:"needs_list_bucket,omitempty"`
	// WaitsForAnswer says NextSend is the wait after sending again snapshots
	// S3 could not be asked about (#2696), not after a send that failed.
	WaitsForAnswer bool `json:"waits_for_answer,omitempty"`
	// NextSend, when set, is when the next send is tried: an hour after a
	// round in which one failed (#2678). Absent: at the next cleanup.
	NextSend *time.Time `json:"next_send,omitempty"`
}

// RewriteNotInDestination replaces dir's record after the cleanup's resends
// (#2563): with nothing left it is removed, so the page stops saying a
// snapshot S3 now has is missing until the next cleanup.
func RewriteNotInDestination(dir string, rec NotInDestination) error {
	if rec.Count <= 0 {
		if err := removeRecord(filepath.Join(dir, NotInDestinationFile)); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	if len(rec.Names) > notInDestinationNamesKept {
		rec.Names = rec.Names[:notInDestinationNamesKept]
	}
	return writeNotInDestRecord(dir, rec)
}

// notInDestinationNamesKept bounds Names: the page names a few, the count
// says how many.
const notInDestinationNamesKept = 5

// ReadNotInDestination reads dir's record; ok is false when the last attempt
// found every old snapshot at the destination. An unreadable record is an
// error, never "all of them are there".
func ReadNotInDestination(dir string) (rec NotInDestination, ok bool, err error) {
	b, err := os.ReadFile(filepath.Join(dir, NotInDestinationFile))
	if errors.Is(err, os.ErrNotExist) {
		return NotInDestination{}, false, nil
	}
	if err != nil {
		return NotInDestination{}, false, fmt.Errorf("read the record of snapshots not at the destination: %w", err)
	}
	if err := json.Unmarshal(b, &rec); err != nil || rec.At.IsZero() || rec.Count <= 0 {
		return NotInDestination{}, false, fmt.Errorf("the record of snapshots not at the destination %s cannot be read", filepath.Join(dir, NotInDestinationFile))
	}
	if rec.Unasked != "" && len(rec.NotAsked) == 0 {
		rec.NotAsked = []string{rec.Unasked}
	}
	rec.Unasked = ""
	return rec, true, nil
}

func writeNotInDestination(dir string, rec NotInDestination) error {
	rec.At = rec.At.UTC()
	trimNotInDestination(&rec)
	rec.Unasked = ""
	if len(rec.NotAsked) > 0 {
		rec.Unasked = rec.NotAsked[0]
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return writeFileAtomic0600(dir, NotInDestinationFile, b)
}

func writePruneFailure(dir string, rec PruneFailure) error {
	rec.At = rec.At.UTC()
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return writeFileAtomic0600(dir, LastPruneFailureFile, b)
}

// writeLastPrune replaces dir's record atomically (temp file, fsync, rename),
// 0600 like the rest of the state DBTrail writes.
func writeLastPrune(dir string, rec LastPrune) error {
	rec.At = rec.At.UTC()
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return writeFileAtomic0600(dir, LastPruneFile, b)
}

func writeFileAtomic0600(dir, name string, b []byte) error {
	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(dir, name))
}

// isPruneArtifact reports whether path is the prune's own bookkeeping directly
// under the root: the record, its temp files, or the lock. Regular files, so
// the upload walk would otherwise publish them as snapshot data.
func isPruneArtifact(root, path string) bool {
	if filepath.Dir(path) != filepath.Clean(root) {
		return false
	}
	name := filepath.Base(path)
	for _, rec := range []string{LastPruneFile, LastPruneFailureFile, NotInDestinationFile, CleanupWaitingFile, ResentFile} {
		if name == rec || strings.HasPrefix(name, rec+".tmp-") {
			return true
		}
	}
	return name == pruneLockName
}

// CountLocalSnapshots counts the snapshot folders directly under dir,
// complete or not (#1681). A missing dir holds none. The console uses it to
// refuse turning on a keep-newest count over snapshots nobody chose to prune.
func CountLocalSnapshots(dir string) (int, error) {
	snaps, err := enumerateLocalSnapshots(dir)
	return len(snaps), err
}

// CleanupWaitingFile records, beside the snapshots, that the cleanup of this
// folder is standing still for a job that reads its older snapshots (#2678):
// which job, when that job started, and when a cleanup last found it
// running. Removed by the first cleanup that runs. Without it a folder over
// its count while a long restore or check runs is unexplained on the page.
const CleanupWaitingFile = ".cleanup-waiting.json"

// CleanupWaiting is that record.
type CleanupWaiting struct {
	// Since is when the job named started (its registration, not the first
	// cleanup that waited); At is when a cleanup last found it running.
	Since time.Time `json:"since"`
	At    time.Time `json:"at"`
	What  string    `json:"what"`
	// ValidUntil is until when the record speaks for now: twice the
	// periodic cleanup's interval after it was written. A daemon that
	// waits rewrites it at every cleanup, and removes it at the first one
	// that runs; one past this was left by a process that stopped, or is
	// read by a console whose own process prunes nothing.
	ValidUntil time.Time `json:"valid_until"`
}

// cleanupWaitingFresh is how long a record with no ValidUntil of its own is
// believed after it was written: two of the default hourly cleanups.
const cleanupWaitingFresh = 2 * time.Hour

// ReadCleanupWaiting reads dir's record; ok is false when the last cleanup
// ran, or the record is too old to speak for now. An unreadable record
// reads as none: it explains, it decides nothing.
func ReadCleanupWaiting(dir string) (rec CleanupWaiting, ok bool) {
	b, err := os.ReadFile(filepath.Join(dir, CleanupWaitingFile))
	if err != nil {
		return CleanupWaiting{}, false
	}
	if err := json.Unmarshal(b, &rec); err != nil || rec.Since.IsZero() || rec.At.IsZero() || rec.What == "" {
		return CleanupWaiting{}, false
	}
	until := rec.ValidUntil
	if until.IsZero() {
		until = rec.At.Add(cleanupWaitingFresh)
	}
	if time.Now().After(until) {
		return CleanupWaiting{}, false
	}
	return rec, true
}

// noteCleanupWaiting keeps the record in step with one cleanup: rewritten
// with the job it waits for now and that job's own start when it stood
// still for what, removed when it ran.
func noteCleanupWaiting(dir, what string, every time.Duration) {
	path := filepath.Join(dir, CleanupWaitingFile)
	if what == "" {
		if err := removeRecord(path); err != nil && !os.IsNotExist(err) {
			slog.Warn("baseline prune: the cleanup ran, but the record saying it was waiting could not be removed; the page may go on saying it waits", "dir", dir, "error", err)
		}
		return
	}
	now := time.Now().UTC()
	rec := CleanupWaiting{Since: now, At: now, What: what}
	// The job's own start. Asked again here: the job may have ended since
	// the cleanup saw it, and then the record is not written at all.
	cur, since, _ := readingNow(dir, time.Now())
	if cur == "" {
		_ = removeRecord(path)
		return
	}
	rec.What, rec.Since = cur, since.UTC()
	if every <= 0 {
		every = time.Hour
	}
	rec.ValidUntil = now.Add(2 * every)
	b, err := json.Marshal(rec)
	if err == nil {
		err = writeFileAtomic0600(dir, CleanupWaitingFile, b)
	}
	if err != nil {
		slog.Warn("baseline prune: the cleanup is waiting for a job, and the record of it could not be written; the page will not say why the folder is over its count", "dir", dir, "error", err)
	}
}

// ResentFile records, beside the snapshots, the ones the cleanup sent again
// whose copy S3 has not confirmed since (#2696), and when: name -> time. A
// snapshot on it that S3 then refuses to read is the evidence of a
// destination that accepts uploads and refuses reads (no s3:GetObject), so
// it is not sent again; without the record, each cleanup would send it again,
// S3 would take it, and the next question would be refused again, for ever.
// Cleared for a snapshot S3 answers about, and for one no longer on disk.
const ResentFile = ".resent-unconfirmed.json"

// MarkResent notes that name was sent again just now. Called by the cleanup's
// resend after a send S3 accepted. Not under the prune's lock: a prune that
// rewrites the record meanwhile may drop the mark, which costs one more send
// (the hour's wait after sending a snapshot S3 could not be asked about
// bounds it), never a removal.
func MarkResent(dir, name string, at time.Time) error {
	marks := resentMarks(dir)
	marks[name] = at.UTC()
	return writeResent(dir, marks)
}

// forgetResent removes name from that record: S3 answered about it. A
// failure is logged; a mark left behind costs at most one send not made.
func forgetResent(dir, name string) {
	marks := resentMarks(dir)
	if _, ok := marks[name]; !ok {
		return
	}
	delete(marks, name)
	if err := writeResent(dir, marks); err != nil {
		slog.Warn("baseline prune: the record of snapshots sent again could not be updated", "dir", dir, "error", err)
	}
}

// ResentUnconfirmed lists the snapshots on that record, sorted.
func ResentUnconfirmed(dir string) []string {
	var out []string
	for name := range resentMarks(dir) {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// resentMarks reads the record; a missing or unreadable one is empty (an
// unreadable one is logged): the evidence is lost, and the hour's wait still
// bounds the sends.
func resentMarks(dir string) map[string]time.Time {
	marks := map[string]time.Time{}
	b, err := os.ReadFile(filepath.Join(dir, ResentFile))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("baseline prune: the record of snapshots sent again could not be read", "dir", dir, "error", err)
		}
		return marks
	}
	if err := json.Unmarshal(b, &marks); err != nil {
		slog.Warn("baseline prune: the record of snapshots sent again cannot be read; it is replaced", "dir", dir, "error", err)
		return map[string]time.Time{}
	}
	return marks
}

func writeResent(dir string, marks map[string]time.Time) error {
	if len(marks) == 0 {
		if err := removeRecord(filepath.Join(dir, ResentFile)); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	b, err := json.Marshal(marks)
	if err != nil {
		return err
	}
	return writeFileAtomic0600(dir, ResentFile, b)
}

// trimNotInDestination cuts Names, NotAsked and ReadRefused to
// notInDestinationNamesKept each (#2701), with the counts of the two
// subsets kept whole. The ones also in Names go first, so a name the page
// shows is never said to be missing because its "could not be asked" entry
// was the one cut. Every writer goes through it.
func trimNotInDestination(rec *NotInDestination) {
	if rec.NotAskedCount < len(rec.NotAsked) {
		rec.NotAskedCount = len(rec.NotAsked)
	}
	if rec.ReadRefusedCount < len(rec.ReadRefused) {
		rec.ReadRefusedCount = len(rec.ReadRefused)
	}
	if len(rec.Names) > notInDestinationNamesKept {
		rec.Names = rec.Names[:notInDestinationNamesKept]
	}
	shown := map[string]bool{}
	for _, n := range rec.Names {
		shown[n] = true
	}
	cut := func(l []string) []string {
		if len(l) <= notInDestinationNamesKept {
			return l
		}
		out := make([]string, 0, notInDestinationNamesKept)
		for _, pass := range []bool{true, false} {
			for _, n := range l {
				if shown[n] == pass && len(out) < notInDestinationNamesKept {
					out = append(out, n)
				}
			}
		}
		return out
	}
	rec.NotAsked, rec.ReadRefused = cut(rec.NotAsked), cut(rec.ReadRefused)
}
