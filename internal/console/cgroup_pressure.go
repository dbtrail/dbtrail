package console

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// PressureSample is one reading of this process's cgroup stall counters
// (Linux PSI): the "some" total of cpu.pressure and io.pressure, the wall
// time during which at least one task of the cgroup was ready to run but
// waiting for a core, or waiting on storage. Cumulative since the cgroup was
// created, so only the difference between two samples means anything.
//
// The cgroup is the whole process's, and with `watch` that is capture, the
// console and every baseline job together: a reading says the process
// waited, not which part of it did. Nor does it see a process it waits ON:
// a fold blocked on a starved index MySQL reads as time on a socket here.
//
// CPUErr and IOErr say why a counter could not be read ("unknown: ..."), ""
// when it was. An unread counter is never zero: a zero would read as a
// measured "it never waited".
type PressureSample struct {
	CPU, IO       time.Duration
	CPUErr, IOErr string
}

// ReadPressure samples this process's own cgroup.
func ReadPressure() PressureSample { return readPressure("/", os.Getpid()) }

func readPressure(root string, pid int) PressureSample {
	var s PressureSample
	s.CPU, s.CPUErr = readStallTotal(root, "cpu.pressure", pid)
	s.IO, s.IOErr = readStallTotal(root, "io.pressure", pid)
	return s
}

// readStallTotal reads the "some" total of one pressure file of this
// process's cgroup v2, found the way cpuPriorityOf finds cpu.weight
// (ownCgroupFile). PSI exists only under cgroup v2: a v1-only layout has
// nowhere to read it from.
//
// The cgroup it reads must list pid in its cgroup.procs. A root cgroup's
// pressure files are the whole machine's, and two layouts reach one with
// nothing in the path to say so: the host's /sys/fs/cgroup mounted into a
// container that has its own namespace ("0::/" then names the host's root),
// and the fallback to the mount root. Without the check either would print
// the machine's stall as this process's.
func readStallTotal(root, file string, pid int) (time.Duration, string) {
	b, err := os.ReadFile(filepath.Join(root, "proc/self/cgroup"))
	if err != nil {
		return 0, "unknown: /proc/self/cgroup could not be read"
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		parts := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(parts) != 3 || parts[0] != "0" || parts[1] != "" {
			continue
		}
		path, body, err := ownCgroupFile(root, filepath.Join(root, "sys/fs/cgroup"), parts[2], file)
		if err != nil {
			return 0, "unknown: " + path + " " + readFailure(err)
		}
		if !listsPID(filepath.Join(root, filepath.Dir(path), "cgroup.procs"), pid) {
			return 0, fmt.Sprintf("unknown: %s is not this process's own cgroup (its cgroup.procs does not list it), "+
				"so its counter would count other processes", filepath.Dir(path))
		}
		total, ok := someTotal(body)
		if !ok {
			return 0, fmt.Sprintf("unknown: %s holds no \"some\" total", path)
		}
		return total, ""
	}
	return 0, "unknown: the stall counter needs cgroup v2, and /proc/self/cgroup has no v2 line"
}

// readFailure words why a cgroup file did not read: absent is what a
// kernel without pressure stall information, or with it switched off for the
// cgroup, looks like; a refusal is the account's, not the kernel's.
func readFailure(err error) string {
	switch {
	case errors.Is(err, errCgroupOutside):
		return "is outside this process's cgroup namespace"
	case errors.Is(err, fs.ErrNotExist):
		return "is not present"
	case errors.Is(err, fs.ErrPermission):
		return "could not be read: permission denied"
	default:
		return "could not be read"
	}
}

// listsPID reports whether a cgroup.procs file lists pid.
func listsPID(procs string, pid int) bool {
	b, err := os.ReadFile(procs)
	if err != nil {
		return false
	}
	want := strconv.Itoa(pid)
	for _, f := range strings.Fields(string(b)) {
		if f == want {
			return true
		}
	}
	return false
}

// errCgroupOutside: /proc/self/cgroup names a path above the namespace's
// root ("/../x"), which cleaning would turn into some OTHER cgroup below it.
var errCgroupOutside = errors.New("cgroup path outside the namespace")

// ownCgroupFile reads file in the cgroup /proc/self/cgroup names (rel, below
// base), and when that cgroup's DIRECTORY does not exist, at base itself:
// inside a container that shares the host's cgroup namespace the path names
// the host's view, while the container's own cgroup is mounted at the root.
// Only a missing directory falls back. A directory without the file is the
// right cgroup without that counter, and the root's file would be another
// cgroup's. The path it returns is the one it read or failed on, with root
// trimmed off.
func ownCgroupFile(root, base, rel, file string) (string, []byte, error) {
	trim := func(p string) string {
		return "/" + strings.TrimPrefix(strings.TrimPrefix(p, filepath.Clean(root)), "/")
	}
	if slices.Contains(strings.Split(rel, "/"), "..") {
		return trim(filepath.Join(base, file)), nil, errCgroupOutside
	}
	dir := filepath.Join(base, filepath.Clean("/"+rel))
	if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		dir = base
	}
	own := filepath.Join(dir, file)
	b, err := os.ReadFile(own)
	return trim(own), b, err
}

// someTotal parses the total= field, in microseconds, of the "some" line.
func someTotal(b []byte) (time.Duration, bool) {
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "some" {
			continue
		}
		for _, f := range fields[1:] {
			if v, ok := strings.CutPrefix(f, "total="); ok {
				us, err := strconv.ParseUint(v, 10, 63)
				if err != nil {
					return 0, false
				}
				return time.Duration(us) * time.Microsecond, true
			}
		}
	}
	return 0, false
}

// StallWords says, for each counter, what share of a run of took the process
// spent stalled between two samples: "31% of the run (7m26s of 24m0s)", or
// the reason it cannot be told. Each counter is worded on its own, so one
// unreadable file does not hide the other.
func StallWords(before, after PressureSample, took time.Duration) (cpu, io string) {
	return stallShare(before.CPU, after.CPU, before.CPUErr, after.CPUErr, took),
		stallShare(before.IO, after.IO, before.IOErr, after.IOErr, took)
}

// stallSkew is how far past the run a difference may go and still be read
// as the two reads' skew, capped at the whole run.
const stallSkew = 100 * time.Millisecond

func stallShare(before, after time.Duration, beforeErr, afterErr string, took time.Duration) string {
	switch {
	case beforeErr != "":
		return beforeErr
	case afterErr != "":
		return afterErr
	case after < before:
		return "unknown: the counter went backwards"
	case took <= 0:
		return "unknown: the run took no measurable time"
	}
	// "some" is wall time with at least one task stalled, so it cannot pass
	// the run by more than the two reads' own skew (microseconds). More than
	// that is two different counters, or a broken one, not a measurement.
	if after-before > took+stallSkew {
		return "unknown: the counter moved more than the run lasted"
	}
	stalled := min(after-before, took)
	unit := time.Millisecond
	if took >= time.Minute {
		unit = time.Second
	}
	pct := math.Round(100 * float64(stalled) / float64(took))
	return fmt.Sprintf("%d%% of the run (%s of %s)", int(pct), stalled.Round(unit), took.Round(unit))
}
