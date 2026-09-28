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
// The console recipes do not install mydumper themselves: they are built FROM
// the console base image, the one place it is installed and the one
// build/smoke-console-base.sh tests against real servers. This guard reads the
// recipes, it does not build them. It fails if a recipe installs the package
// without the library, and TestConsoleRecipesAreBuiltFromTheBaseImage fails if a
// console recipe stops using the base image or names a tag other than the one
// in build/console-base.tag.
//
// What it cannot see: a package that is listed and does nothing (a rename in a
// future Debian release, a different client library in a future mydumper
// build). Only the smoke test sees those.

const mydumperClientLibrary = "libmariadb3"

// A download or an install of a mydumper .deb, in any spelling of the version.
var reMydumperDeb = regexp.MustCompile(`mydumper[_-][^\s"']*\.deb`)

// recipeInstructions returns the instructions of a Dockerfile's LAST stage, the
// one that becomes the image: everything after the last FROM. A package
// installed in an earlier stage (a builder) is not in the image.
func recipeInstructions(text string) []string {
	all := allInstructions(text)
	last := -1
	for i, instruction := range all {
		if fields := strings.Fields(instruction); len(fields) > 0 && strings.EqualFold(fields[0], "FROM") {
			last = i
		}
	}
	return all[last+1:]
}

// allInstructions returns a Dockerfile's instructions the way Docker reads
// them: comment lines dropped first (also inside a continued RUN), then lines
// ending in a backslash joined with the next one.
func allInstructions(text string) []string {
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
		{"library only in the builder stage", "FROM golang:1.25 AS builder\nRUN apt-get update && apt-get install -y libmariadb3\n" + strings.Replace(good, " libmariadb3", "", 1), true, false},
		{"mydumper only in the builder stage", good + "FROM debian:bookworm-slim\nRUN apt-get update && apt-get install -y zstd\n", false, false},
		{"both in the last of two stages", "FROM golang:1.25 AS builder\nRUN go build ./...\n" + good, true, true},
		{"lowercase from", "from golang:1.25 as builder\nRUN apt-get install -y libmariadb3\nfrom debian:bookworm-slim\n" + strings.TrimPrefix(strings.Replace(good, " libmariadb3", "", 1), "FROM debian:bookworm-slim\n"), true, false},
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

	// The recipe this guard exists for. If the walk stops finding it, the guard
	// is looking in the wrong place or for the wrong text, and a pass would mean
	// nothing.
	const base = "build/Dockerfile.console-base"
	if !slices.Contains(withMydumper, base) {
		t.Errorf("%s was not seen installing the mydumper package; recipes seen: %v", base, withMydumper)
	}
	// A console recipe that installs the package again is a second copy nothing
	// tests.
	for _, recipe := range consoleRecipes {
		if slices.Contains(withMydumper, recipe) {
			t.Errorf("%s installs the mydumper package itself; it must come from the base image", recipe)
		}
	}
}

// The recipes that become the console image: one built from source, one that
// takes the binary the release already compiled.
var consoleRecipes = []string{
	"build/Dockerfile.bintrail-console",
	"build/Dockerfile.bintrail-console.goreleaser",
}

// lastFrom returns the image the recipe's last stage starts from, the one that
// becomes the image, and whether the recipe has a FROM at all.
func lastFrom(text string) (string, bool) {
	image, found := "", false
	for _, instruction := range allInstructions(text) {
		fields := strings.Fields(instruction)
		if len(fields) >= 2 && strings.EqualFold(fields[0], "FROM") {
			image, found = fields[1], true
			// FROM --platform=... image
			for _, f := range fields[1:] {
				if !strings.HasPrefix(f, "--") {
					image = f
					break
				}
			}
		}
	}
	return image, found
}

func TestLastFrom(t *testing.T) {
	cases := []struct {
		name, text, want string
		found            bool
	}{
		{"one stage", "FROM a:1\nRUN true\n", "a:1", true},
		{"two stages", "FROM golang:1.25 AS builder\nRUN true\nFROM b:2\nCOPY x y\n", "b:2", true},
		{"lowercase", "from a:1 as x\nfrom b:2\n", "b:2", true},
		{"platform flag", "FROM --platform=linux/amd64 b:2\n", "b:2", true},
		{"a FROM in a comment", "FROM a:1\n# FROM b:2\n", "a:1", true},
		{"empty file", "", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, found := lastFrom(c.text)
			if got != c.want || found != c.found {
				t.Errorf("lastFrom = %q, %v; want %q, %v", got, found, c.want, c.found)
			}
		})
	}
}

func TestConsoleRecipesAreBuiltFromTheBaseImage(t *testing.T) {
	root := ".."
	raw, err := os.ReadFile(filepath.Join(root, "build", "console-base.tag"))
	if err != nil {
		t.Fatalf("reading the base image tag: %v", err)
	}
	tag := strings.TrimSpace(string(raw))
	if tag == "" || strings.ContainsAny(tag, " \t\n:/@") {
		t.Fatalf("build/console-base.tag holds %q, not one tag", tag)
	}
	want := "ghcr.io/dbtrail/bintrail-console-base:" + tag

	for _, recipe := range consoleRecipes {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(recipe)))
		if err != nil {
			t.Errorf("reading %s: %v", recipe, err)
			continue
		}
		got, found := lastFrom(string(data))
		if !found {
			t.Errorf("%s has no FROM", recipe)
			continue
		}
		if got != want {
			t.Errorf("%s is built FROM %s, want %s (the tag in build/console-base.tag)", recipe, got, want)
		}
	}
}
