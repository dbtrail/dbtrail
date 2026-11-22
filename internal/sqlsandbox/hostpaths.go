package sqlsandbox

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

// hiddenPathWord stands in an error a client reads where the engine named a
// folder of the host (#3096).
const hiddenPathWord = "<hidden>"

// hideHostPaths replaces every folder of roots named in msg with
// hiddenPathWord. A folder counts only as a whole path: it starts the
// message or follows a character that cannot be part of a path, and it ends
// the message, a path component or the path, so /tmp does not match
// /tmpfiles or /mnt/tmp. Longer folders go first, so one inside another is
// replaced whole. Each folder is also hidden in the form its links resolve
// to. The root folder and empty entries are skipped: they would match every
// path.
func hideHostPaths(msg string, roots []string) string {
	if msg == "" {
		return msg
	}
	clean := make([]string, 0, 2*len(roots))
	add := func(r string) {
		r = filepath.Clean(r)
		if r == "/" || r == "." || !strings.HasPrefix(r, "/") || slices.Contains(clean, r) {
			return
		}
		clean = append(clean, r)
	}
	for _, r := range roots {
		if r == "" {
			continue
		}
		add(r)
		// The engine may name a folder by the path its links resolve to
		// (a temporary folder that is a link, a data folder mounted
		// through one): that form is hidden too. A folder that cannot be
		// resolved (gone, unreadable) is hidden as configured only.
		if resolved, err := filepath.EvalSymlinks(r); err == nil {
			add(resolved)
		}
	}
	if len(clean) == 0 {
		return msg
	}
	slices.SortFunc(clean, func(a, b string) int { return len(b) - len(a) })
	// One pass over the message as it came, every boundary read from it and
	// never from what was written out: a marker already in the text (a
	// client can type one, and the engine repeats what it was given) says
	// nothing about the path after it. A folder inside one just hidden
	// (/tmp/tmp, /data/copy/tmp) follows a letter of the hidden one, so it
	// is not a folder of its own. An equals sign or an at sign before it
	// starts a value (x=/data, user@/data), not a longer path.
	var b strings.Builder
	for i := 0; i < len(msg); {
		if i == 0 || !pathByte(msg[i-1]) || msg[i-1] == '=' || msg[i-1] == '@' {
			if n := wholePathAt(msg, i, clean); n > 0 {
				b.WriteString(hiddenPathWord)
				i += n
				continue
			}
		}
		b.WriteByte(msg[i])
		i++
	}
	return b.String()
}

// wholePathAt returns the length of the first of roots (longest first) that
// msg holds at i as a whole path, or 0. It ends the message, a path
// component, or is followed by a character that cannot be part of a path, or
// by a period that ends the sentence (one followed by the end of the text, a
// space or a new line, so /data/copy.parquet is not taken for it). A
// URL keeps its path: after "scheme://host" a folder follows a letter of the
// host or a slash, both of which can be part of a path, so the caller never
// asks here.
func wholePathAt(msg string, i int, roots []string) int {
	for _, r := range roots {
		if !strings.HasPrefix(msg[i:], r) {
			continue
		}
		end := i + len(r)
		if end == len(msg) || msg[end] == '/' || !pathByte(msg[end]) {
			return len(r)
		}
		if msg[end] == '.' && (end+1 == len(msg) || msg[end+1] == ' ' || msg[end+1] == '\n') {
			return len(r)
		}
	}
	return 0
}

// pathByte reports whether c can be part of a path name next to a folder.
func pathByte(c byte) bool {
	switch {
	case c == 0:
		return false
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	return strings.IndexByte("/._-~+=@", c) >= 0
}

// hostRoots are the folders an error of this statement must not name: the
// copy's directories, the spill directory of the statement and the folder
// all of them are made in, the result file's, and the engine's folder under
// the home of the user DBTrail runs as (secrets and extensions), and that
// home.
func (r *Runner) hostRoots(job Job, spill spillSpec, spoolDir string) []string {
	roots := append([]string{}, job.CopyDirs...)
	roots = append(roots, spill.Dir, spoolDir, r.spillRoot)
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		roots = append(roots, filepath.Join(home, ".duckdb"), home)
	}
	return roots
}

// hideHostPathsInError rewrites the two errors whose text a client reads
// from the engine, a refusal and an engine error, with the host's folders
// replaced. The original text goes to the log for the operator: at info
// level once a minute, at debug level the rest, so a client that sends such
// statements in a loop does not fill the log. Every other error is returned
// as is: the ports replace a WorkerError's text before a client sees it, and
// the rest are this package's own words.
func hideHostPathsInError(err error, roots []string) error {
	var re *RefusedError
	var qe *QueryError
	switch {
	case errors.As(err, &re):
		if hidden := hideHostPaths(re.Reason, roots); hidden != re.Reason {
			logHiddenPath("a refusal", re.Reason)
			return &RefusedError{Reason: hidden}
		}
	case errors.As(err, &qe):
		if hidden := hideHostPaths(qe.Message, roots); hidden != qe.Message {
			logHiddenPath("an engine error", qe.Message)
			return &QueryError{Message: hidden}
		}
	}
	return err
}

// hiddenPathLoggedAt is when an original text was last logged at info
// level, in Unix nanoseconds.
var hiddenPathLoggedAt atomic.Int64

func logHiddenPath(what, original string) {
	now := time.Now().UnixNano()
	last := hiddenPathLoggedAt.Load()
	if now-last >= int64(time.Minute) && hiddenPathLoggedAt.CompareAndSwap(last, now) {
		slog.Info("sql on the copy: "+what+" named a folder of the host; the client read it with the folder hidden (more of these this minute are logged at debug level)", "text", original)
		return
	}
	slog.Debug("sql on the copy: "+what+" named a folder of the host; the client read it with the folder hidden", "text", original)
}
