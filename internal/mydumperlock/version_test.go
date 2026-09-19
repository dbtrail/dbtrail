package mydumperlock

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseVersion(t *testing.T) {
	cases := []struct {
		name                            string
		output                          string
		wantMajor, wantMinor, wantPatch int
		wantErr                         bool
		// wantErrContains pins text the refusal must quote back. Without it
		// the error branch only asserts non-nil, and the deliberate choice to
		// quote the RAW field rather than the post-strip value is unpinned —
		// a refactor reporting "ersion" for a "version" field stays green.
		wantErrContains string
	}{
		{
			name:      "standard_0.10.0",
			output:    "mydumper 0.10.0, built against MySQL 8.0.36\n",
			wantMajor: 0, wantMinor: 10, wantPatch: 0,
		},
		{
			name:      "standard_0.11.5",
			output:    "mydumper 0.11.5, built against MySQL 8.0.37\n",
			wantMajor: 0, wantMinor: 11, wantPatch: 5,
		},
		{
			// Captured from `mydumper --version` in the mydumper/mydumper
			// v0.16.3-6 image. It used to read "mydumper 0.16.3-6, built
			// against MySQL 8.4.3" — a shape no binary prints, which made the
			// "v is a 1.x thing" story look measured when it was not. The real
			// string carries the prefix, so 0.16.3 was ALSO unreadable before
			// this fix; the outcome happened to be unchanged there, because
			// 0.16 is below the flag floor either way.
			name:      "v_prefixed_0.16.3_with_suffix",
			output:    "mydumper v0.16.3-6, built against MySQL 8.4.1 with SSL support\n",
			wantMajor: 0, wantMinor: 16, wantPatch: 3,
		},
		{
			name:      "future_major_1",
			output:    "mydumper 1.0.0, built against MySQL 9.0.0\n",
			wantMajor: 1, wantMinor: 0, wantPatch: 0,
		},
		{
			// The shape current releases actually print (#1686). No 1.x build
			// ever shipped the bare "1.0.0" above, so that case passed all
			// along while every real 1.x install failed to parse — and a failed
			// parse is read as "very old mydumper", which skips the privilege
			// preflight, refuses an explicit --lock-mode and dumps under
			// heavier locks than the operator asked for.
			name:      "v_prefixed_1.0.5_with_package_suffix",
			output:    "mydumper v1.0.5-1, built against MariaDB 10.8.8 with SSL support\n",
			wantMajor: 1, wantMinor: 0, wantPatch: 5,
		},
		{
			// The build internal/baseline/lockmode.go records its measured
			// lock-mode findings against, so it has to be readable here.
			name:      "v_prefixed_pinned_1.0.3",
			output:    "mydumper v1.0.3-1, built against MySQL 8.4.9 with SSL support\n",
			wantMajor: 1, wantMinor: 0, wantPatch: 3,
		},
		{
			// The exact floor SupportsLockMode gates on: no 0.18.0 was
			// ever released, so 0.18.1 is the first build accepting the flags.
			name:      "floor_0.18.1",
			output:    "mydumper 0.18.1, built against MySQL 8.0.36\n",
			wantMajor: 0, wantMinor: 18, wantPatch: 1,
		},
		{
			// Stripping the "v" must not turn a version-less field into 0.0.0:
			// SupportsLockMode would read that as a pre-0.18 build and
			// proceed against it silently, instead of reporting it unreadable.
			name:    "bare_v_no_digits",
			output:  "mydumper v\n",
			wantErr: true,
		},
		{
			// An unrecognised SHAPE (version not in the second field) must stay
			// an error rather than be guessed at. runDump (cliapp) and the console's
			// runMydumper both run the privilege preflight on exactly this outcome.
			name:            "version_not_in_second_field",
			output:          "mydumper version v1.0.5-1\n",
			wantErr:         true,
			wantErrContains: `"version"`,
		},
		{
			name:    "empty_output",
			output:  "",
			wantErr: true,
		},
		{
			name:    "garbage",
			output:  "not a version string at all\n",
			wantErr: true,
		},
		{
			name:    "single_word",
			output:  "mydumper\n",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := ParseVersion(tc.output)
			major, minor, patch := v.Major, v.Minor, v.Patch
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error but got %d.%d.%d", major, minor, patch)
				}
				if tc.wantErrContains != "" && !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Errorf("error = %q, want it to quote %s — the refusal must name what mydumper actually printed",
						err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if major != tc.wantMajor || minor != tc.wantMinor || patch != tc.wantPatch {
				t.Errorf("got %d.%d.%d, want %d.%d.%d", major, minor, patch, tc.wantMajor, tc.wantMinor, tc.wantPatch)
			}
		})
	}
}

// writeScript writes an executable bash script and returns its path. The body
// uses only builtins (printf, exit) so the test does not depend on PATH.
func writeScript(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "mydumper")
	if err := os.WriteFile(p, []byte("#!/bin/bash\n"+body), 0o755); err != nil {
		t.Fatalf("write fake mydumper: %v", err)
	}
	return p
}

// TestProbeVersionSeparatesNotRunnableFromUnreadable pins #1699: a binary that
// did not run and a binary that printed something unreadable are different
// facts with different remedies, so only the first may wrap ErrNotRunnable.
func TestProbeVersionSeparatesNotRunnableFromUnreadable(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-mydumper")

	notExec := filepath.Join(t.TempDir(), "mydumper")
	if err := os.WriteFile(notExec, []byte("#!/bin/bash\nexit 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// What a mydumper whose dynamic linker cannot resolve a library does: the
	// loader prints to stderr and the process exits 127 before main runs.
	loader := writeScript(t, "printf 'mydumper: error while loading shared libraries: libmysqlclient.so.21: cannot open shared object file\n' >&2\nexit 127\n")

	// Printed a real version, then exited non-zero: the version still counts.
	versionThenFail := writeScript(t, "printf 'mydumper 0.10.0, built against MySQL 8.0.36\n'\nexit 1\n")

	// Ran fine, printed something that is not a version: UNKNOWN, not broken.
	garbage := writeScript(t, "printf 'mydumper built from source\n'\nexit 0\n")

	// Measured from Ubuntu 24.04's package (0.10.1-1ubuntu3).
	distro := writeScript(t, "printf 'mydumper 0.10.0, built against MySQL 8.0.36\n'\nexit 0\n")

	cases := []struct {
		name            string
		path            string
		wantNotRunnable bool
		wantVersion     Version
		wantErr         bool
		wantErrContains string
	}{
		{name: "missing binary", path: missing, wantNotRunnable: true, wantErr: true, wantErrContains: missing},
		{name: "not executable", path: notExec, wantNotRunnable: true, wantErr: true, wantErrContains: notExec},
		{name: "loader failure", path: loader, wantNotRunnable: true, wantErr: true, wantErrContains: "libmysqlclient.so.21"},
		{name: "version then non-zero exit", path: versionThenFail, wantVersion: Version{0, 10, 0}},
		{name: "unreadable output", path: garbage, wantErr: true, wantErrContains: "built from source"},
		{name: "distribution 0.10", path: distro, wantVersion: Version{0, 10, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := ProbeVersion(tc.path)
			if got := errors.Is(err, ErrNotRunnable); got != tc.wantNotRunnable {
				t.Fatalf("errors.Is(err, ErrNotRunnable) = %v, want %v (err: %v)", got, tc.wantNotRunnable, err)
			}
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want an error, got version %s", v)
				}
				if !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Errorf("error %q does not name %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v != tc.wantVersion {
				t.Errorf("version = %s, want %s", v, tc.wantVersion)
			}
		})
	}
}

// TestSupportsLockModeFloor pins the floor at 0.18: the 0.18 series starts at
// 0.18.1, 0.17 and the 0.10 distribution builds reject the flags, and every 1.x
// accepts them.
func TestSupportsLockModeFloor(t *testing.T) {
	for _, tc := range []struct {
		v    Version
		want bool
	}{
		{Version{0, 10, 0}, false}, // Ubuntu 24.04 / Debian bookworm packages (self-reports 0.10.0)
		{Version{0, 11, 5}, false}, // had --no-locks/--trx-consistency-only, NOT these flags
		{Version{0, 16, 3}, false},
		{Version{0, 17, 9}, false}, // last minor before the flags landed
		{Version{0, 18, 1}, true},  // --sync-thread-lock-mode/--trx-tables introduced
		{Version{0, 21, 0}, true},
		{Version{1, 0, 3}, true}, // the build the console image bundles
	} {
		if got := tc.v.SupportsLockMode(); got != tc.want {
			t.Errorf("%s.SupportsLockMode() = %v, want %v", tc.v, got, tc.want)
		}
	}
	if LockModeFloor != "0.18.1" {
		t.Errorf("LockModeFloor = %q; the refusals quote it, so it must name the first build that accepts the flags", LockModeFloor)
	}
}
