package consoleapp

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// Every image recipe that installs the mydumper package must also install
// libmariadb3 (#1876).
//
// The arm64 mydumper package is linked against MariaDB Connector/C, which loads
// caching_sha2_password from a plugin directory that only libmariadb3 ships.
// Without it mydumper cannot log in on arm64 as a user created with the MySQL
// 8.0+ default, and nothing fails until someone takes a snapshot.
//
// This guard is TEMPORARY and it is the cheap kind: it reads the recipes, it
// does not build them. The real test is build/smoke-console-base.sh, which runs
// mydumper from the built image against real servers, but today it only covers
// build/Dockerfile.console-base. The console recipes still carry their own copy
// of the install block and no CI job builds them. Until they are built FROM the
// base image, this is the only thing that fails if someone drops the package
// from one of them. Delete it in the change that moves the last recipe onto the
// base image.
//
// What it cannot see: a package that is listed and does nothing (a rename in a
// future Debian release, a different client library in a future mydumper
// build). Only the smoke test sees those.

const mydumperClientLibrary = "libmariadb3"

// A download or an install of a mydumper .deb, in any spelling of the version.
var reMydumperDeb = regexp.MustCompile(`mydumper[_-][^\s"']*\.deb`)

// recipeInstructions returns a Dockerfile's instructions the way Docker reads
// them: comment lines dropped first (also inside a continued RUN), then lines
// ending in a backslash joined with the next one.
func recipeInstructions(text string) []string {
	var kept []string
	for _, line := range strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		kept = append(kept, line)
	}
	var out []string
	var cur strings.Builder
	for _, line := range kept {
		trimmed := strings.TrimRight(line, " \t")
		if strings.HasSuffix(trimmed, `\`) {
			cur.WriteString(strings.TrimSuffix(trimmed, `\`))
			cur.WriteString(" ")
			continue
		}
		cur.WriteString(line)
		if s := strings.TrimSpace(cur.String()); s != "" {
			out = append(out, s)
		}
		cur.Reset()
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}
	return out
}

// aptInstalls reports whether some `apt-get install` command in the recipe
// names the package as a whole word. A mention in a comment, in an echo or as
// part of a longer package name does not count.
func aptInstalls(text, pkg string) bool {
	for _, instruction := range recipeInstructions(text) {
		for _, command := range regexp.MustCompile(`&&|\|\||;|\|`).Split(instruction, -1) {
			fields := strings.Fields(command)
			if len(fields) > 0 && strings.EqualFold(fields[0], "RUN") {
				fields = fields[1:]
			}
			at := slices.Index(fields, "apt-get")
			if at != 0 || !slices.Contains(fields, "install") {
				continue
			}
			if slices.Contains(fields[at+1:], pkg) {
				return true
			}
		}
	}
	return false
}

// installsMydumperDeb reports whether the recipe fetches or installs a
// mydumper .deb. A comment that names one does not count.
func installsMydumperDeb(text string) bool {
	for _, instruction := range recipeInstructions(text) {
		if reMydumperDeb.MatchString(instruction) {
			return true
		}
	}
	return false
}

func TestMydumperClientLibraryRule(t *testing.T) {
	const good = "FROM debian:bookworm-slim\n" +
		"RUN apt-get update && \\\n" +
		"    apt-get install -y --no-install-recommends \\\n" +
		"        ca-certificates wget zstd libmariadb3 && \\\n" +
		"    wget -q \"https://example.test/mydumper_1.0.3-1.bookworm_${TARGETARCH}.deb\" -O /tmp/mydumper.deb && \\\n" +
		"    dpkg -i /tmp/mydumper.deb\n"

	cases := []struct {
		name         string
		text         string
		wantMydumper bool
		wantLibrary  bool
	}{
		{"installs both", good, true, true},
		{"semicolons instead of &&", strings.ReplaceAll(good, " && \\\n", "; \\\n"), true, true},
		{"library dropped", strings.Replace(good, " libmariadb3", "", 1), true, false},
		{"library only in a comment", strings.Replace(good, " libmariadb3 &&", " && \\\n    # libmariadb3 goes here", 1), true, false},
		{"library only in a comment above", "# needs libmariadb3\n" + strings.Replace(good, " libmariadb3", "", 1), true, false},
		{"a longer package name", strings.Replace(good, " libmariadb3", " libmariadb3-dev", 1), true, false},
		{"library only in an echo", strings.Replace(good, " libmariadb3 &&", " && echo apt-get install libmariadb3 &&", 1), true, false},
		{"library removed, not installed", strings.Replace(good, " libmariadb3 &&", " && apt-get remove -y libmariadb3 &&", 1), true, false},
		{"library in a second apt-get install", strings.Replace(good, " libmariadb3 &&", " && apt-get install -y libmariadb3 &&", 1), true, true},
		{"windows line endings", strings.ReplaceAll(good, "\n", "\r\n"), true, true},
		{"no mydumper at all", "FROM debian:bookworm-slim\nRUN apt-get update && apt-get install -y zstd\n", false, false},
		{"mydumper named in a comment only", "FROM debian:bookworm-slim\n# wget mydumper_1.0.3-1.bookworm_amd64.deb\nRUN apt-get install -y zstd\n", false, false},
		{"empty file", "", false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := installsMydumperDeb(c.text); got != c.wantMydumper {
				t.Errorf("installsMydumperDeb = %v, want %v", got, c.wantMydumper)
			}
			if got := aptInstalls(c.text, mydumperClientLibrary); got != c.wantLibrary {
				t.Errorf("aptInstalls(%s) = %v, want %v", mydumperClientLibrary, got, c.wantLibrary)
			}
		})
	}
}

func TestRecipesThatInstallMydumperInstallItsClientLibrary(t *testing.T) {
	root := ".."
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repository root not found at %s: %v", root, err)
	}

	var withMydumper []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); name == ".git" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasPrefix(d.Name(), "Dockerfile") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(data)
		if !installsMydumperDeb(text) {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		withMydumper = append(withMydumper, filepath.ToSlash(rel))
		if !aptInstalls(text, mydumperClientLibrary) {
			t.Errorf("%s installs the mydumper package and does not apt-get install %s: on arm64 mydumper cannot log in as a user created with the MySQL 8 default (#1876)",
				rel, mydumperClientLibrary)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the repository: %v", err)
	}

	// The recipes this guard exists for. If the walk stops finding one, the
	// guard is looking in the wrong place or for the wrong text, and a pass
	// would mean nothing. When a recipe moves onto the base image, take it out
	// of this list in the same change.
	for _, want := range []string{
		"build/Dockerfile.bintrail-console",
		"build/Dockerfile.bintrail-console.goreleaser",
		"build/Dockerfile.console-base",
	} {
		if !slices.Contains(withMydumper, want) {
			t.Errorf("%s was not seen installing the mydumper package; recipes seen: %v", want, withMydumper)
		}
	}
}
