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
	slices.SortFunc(clean, func(a, b string) int { return len(b) - len(a) })
	for _, r := range clean {
		msg = replaceWholePath(msg, r)
	}
	return msg
}

func replaceWholePath(msg, root string) string {
	var b strings.Builder
	rest := msg
	for {
		i := strings.Index(rest, root)
		if i < 0 {
			b.WriteString(rest)
			return b.String()
		}
		end := i + len(root)
		before := byte(0)
		if i > 0 {
			before = rest[i-1]
		} else if b.Len() > 0 {
			before = b.String()[b.Len()-1]
		}
		// Right after a folder already hidden, the match is a folder inside
		// it (/tmp/tmp, /data/copy/tmp), not a folder of its own.
		afterHidden := strings.HasSuffix(b.String()+rest[:i], hiddenPathWord)
		after := byte(0)
		if end < len(rest) {
			after = rest[end]
		}
		// A URL keeps its path: after "scheme://host" the folder follows a
		// letter of the host or a slash, both of which can be part of a
		// path, so it is not taken for a folder of the host.
		if !pathByte(before) && !afterHidden && (after == 0 || after == '/' || !pathByte(after)) {
			b.WriteString(rest[:i])
			b.WriteString(hiddenPathWord)
		} else {
			b.WriteString(rest[:end])
		}
		rest = rest[end:]
	}
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
