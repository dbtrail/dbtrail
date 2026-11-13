package installer

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// #2638: the installer installs ONE release and writes it down. The compose
// file and the dbtrail command come from that release's git tag, and a fresh
// install writes BINTRAIL_TAG to .env, so a re-run or a `docker compose pull`
// no longer jumps over every release since the install.

const (
	newest      = "v0.130.0" // the tag releaseBody names
	newestImage = "0.130.0"  // the image tag it maps to: no v
)

func (r run) fetched(path string) bool {
	for _, u := range strings.Split(r.urls, "\n") {
		if strings.HasSuffix(u, path) {
			return true
		}
	}
	return false
}

func (r run) askedGitHub() bool {
	return strings.Contains(r.urls, "api.github.com")
}

func writeFile(t *testing.T, path, body string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
}

func withEnv(body string) func(string) {
	return func(dir string) { writeFileNoT(filepath.Join(dir, ".env"), body) }
}

func writeFileNoT(path, body string) {
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		panic(err)
	}
}

func noBannedWords(t *testing.T, r run) {
	t.Helper()
	for _, h := range sentenceHits(t, r.out, r.dir) {
		t.Errorf("banned in a sentence the installer prints: %s", h)
	}
}

// A fresh install takes the compose file and the dbtrail command from the
// newest release's tag, writes the image tag (without the v) to .env before
// it pulls, and its end text names the version and how to move off it.
func TestVersion_aFreshInstallPinsTheNewestRelease(t *testing.T) {
	r := install(t)
	if r.failed || r.upCalls != 1 {
		t.Fatalf("install failed:\n%s", r.out)
	}
	for _, p := range []string{"/" + newest + "/docker-compose.yml", "/" + newest + "/dbtrail.sh"} {
		if !r.fetched(p) {
			t.Errorf("did not fetch %s; asked for:\n%s", p, r.urls)
		}
	}
	if strings.Contains(r.urls, "/main/") {
		t.Errorf("fetched something from main:\n%s", r.urls)
	}
	if got := envFile(t, r); got != "BINTRAIL_TAG="+newestImage+"\n" {
		t.Errorf(".env = %q, want the image tag of %s", got, newest)
	}
	if !strings.Contains(r.calls, "pull sees BINTRAIL_TAG="+newestImage) {
		t.Errorf("the pull ran before BINTRAIL_TAG was in .env:\n%s", r.calls)
	}
	for _, want := range []string{
		"This is version " + newestImage,
		"set by BINTRAIL_TAG in " + r.dir + "/.env",
		"To upgrade, set BINTRAIL_TAG to the new version, then run docker compose pull && docker compose up -d",
		"docs/upgrade.md",
	} {
		if !strings.Contains(r.out, want) {
			t.Errorf("the output does not say %q:\n%s", want, r.out)
		}
	}
	noBannedWords(t, r)
}

// The answer is read with sed: it must not depend on how the JSON is laid
// out, and only a plain release tag is taken. Anything else stops a fresh
// install before it creates anything, and never falls back to main.
func TestVersion_theReleaseLookup(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		fail       bool   // curl -f failing, as on a 403 rate limit
		want       string // the version found; "" = the install stops
		why        string // what the refusal says
	}{
		{"indented, as the API sends it", releaseBody, false, "0.130.0", ""},
		{"on one line", `{"url":"x","tag_name":"v0.131.2","name":"v0.131.2","body":"a, b"}`, false, "0.131.2", ""},
		{"CRLF line ends", "{\r\n  \"tag_name\": \"v0.131.3\",\r\n  \"draft\": false\r\n}", false, "0.131.3", ""},
		{"a 403 or no network", "", true, "", "60 lookups an hour"},
		{"an empty answer", "", false, "", "did not name a release"},
		{"a proxy's HTML page", "<html><body>Sign in to continue</body></html>", false, "", "did not name a release"},
		{"JSON with no tag_name", `{"message":"Not Found"}`, false, "", "did not name a release"},
		{"a prerelease tag", `{"tag_name": "v0.131.0-rc1"}`, false, "", "did not name a release"},
		{"a tag without the v", `{"tag_name": "0.131.0"}`, false, "", "did not name a release"},
		{"an empty tag", `{"tag_name": ""}`, false, "", "did not name a release"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := []string{"STUB_RELEASE_BODY=" + tc.body}
			if tc.fail {
				env = append(env, "STUB_RELEASE_FAIL=1")
			}
			r := install(t, env...)
			if !r.askedGitHub() {
				t.Fatalf("GitHub was never asked:\n%s", r.urls)
			}
			if tc.want != "" {
				if r.failed {
					t.Fatalf("install failed:\n%s", r.out)
				}
				if got := envFile(t, r); got != "BINTRAIL_TAG="+tc.want+"\n" {
					t.Errorf(".env = %q, want BINTRAIL_TAG=%s", got, tc.want)
				}
				if !r.fetched("/v" + tc.want + "/docker-compose.yml") {
					t.Errorf("the compose file did not come from v%s:\n%s", tc.want, r.urls)
				}
				return
			}
			if !r.failed || r.upCalls != 0 {
				t.Fatalf("the install went on without a release (failed=%v up=%d):\n%s", r.failed, r.upCalls, r.out)
			}
			if _, err := os.Stat(r.dir); err == nil {
				t.Error("the stack directory was created before the install stopped")
			}
			if strings.Contains(r.urls, "docker-compose.yml") {
				t.Errorf("a compose file was fetched anyway:\n%s", r.urls)
			}
			for _, want := range []string{"Couldn't find the newest DBTrail release, so nothing was installed", tc.why,
				"DBTRAIL_DIR=" + r.dir + " DBTRAIL_REF=vX.Y.Z sh"} {
				if !strings.Contains(r.out, want) {
					t.Errorf("the refusal does not say %q:\n%s", want, r.out)
				}
			}
			noBannedWords(t, r)
		})
	}
}

// The command a failed lookup suggests keeps the ports the operator chose.
func TestVersion_aFailedLookupKeepsTheChosenPortsInItsCommand(t *testing.T) {
	r := install(t, "STUB_RELEASE_FAIL=1", "DBTRAIL_PORT=8095", "DBTRAIL_METRICS_PORT=9095")
	want := "install.sh | DBTRAIL_DIR=" + r.dir + " DBTRAIL_REF=vX.Y.Z DBTRAIL_PORT=8095 DBTRAIL_METRICS_PORT=9095 sh"
	if !r.failed || !strings.Contains(r.out, want) {
		t.Errorf("want %q in the refusal:\n%s", want, r.out)
	}
}

// A BINTRAIL_TAG the operator already set is theirs: .env is never changed,
// the compose file and the dbtrail command come from that version, and
// GitHub is not asked, so this also works with no network to its API.
func TestVersion_aVersionAlreadyInEnvIsKeptAndUsed(t *testing.T) {
	for _, tc := range []struct{ name, env string }{
		{"plain", "BINTRAIL_TAG=0.128.0\n"},
		{"with other lines", "SCHEMAS=shop\nBINTRAIL_TAG=0.128.0\nINDEX_BUFFER_POOL=2G\n"},
		{"export", "export BINTRAIL_TAG=0.128.0\n"},
		{"colon form", "BINTRAIL_TAG: 0.128.0\n"},
		{"double quotes", "BINTRAIL_TAG=\"0.128.0\"\n"},
		{"single quotes", "BINTRAIL_TAG='0.128.0'\n"},
		{"spaces and a comment", "  BINTRAIL_TAG = 0.128.0   # chosen by hand\n"},
		{"CRLF", "BINTRAIL_TAG=0.128.0\r\n"},
		{"no newline at the end", "BINTRAIL_TAG=0.128.0"},
		{"twice, the last wins", "BINTRAIL_TAG=0.127.0\nBINTRAIL_TAG=0.128.0\n"},
		{"a prerelease", "BINTRAIL_TAG=0.128.0-rc1\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := "0.128.0"
			if strings.Contains(tc.env, "-rc1") {
				want = "0.128.0-rc1"
			}
			r := installWith(t, withEnv(tc.env), "STUB_RELEASE_FAIL=1")
			if r.failed {
				t.Fatalf("install failed:\n%s", r.out)
			}
			if got := envFile(t, r); got != tc.env {
				t.Errorf(".env changed from %q to %q", tc.env, got)
			}
			if r.askedGitHub() {
				t.Errorf("GitHub was asked although the version is set:\n%s", r.urls)
			}
			for _, p := range []string{"/v" + want + "/docker-compose.yml", "/v" + want + "/dbtrail.sh"} {
				if !r.fetched(p) {
					t.Errorf("did not fetch %s:\n%s", p, r.urls)
				}
			}
			if !strings.Contains(r.out, "This is version "+want) || !strings.Contains(r.out, "already in .env") {
				t.Errorf("the kept version is not said:\n%s", r.out)
			}
			noBannedWords(t, r)
		})
	}
}

// A BINTRAIL_TAG that is set but names no version (empty, latest, a tag of
// one's own, a bare key) is left as it is; the compose file comes from the
// newest release, and nothing claims a version.
func TestVersion_aTagThatIsNotAVersionIsLeftAlone(t *testing.T) {
	for _, tc := range []struct{ name, env, says string }{
		{"empty", "BINTRAIL_TAG=\n", "set but empty"},
		{"bare key", "BINTRAIL_TAG\n", "set but empty"},
		{"latest", "BINTRAIL_TAG=latest\n", "is not a version number"},
		{"a tag of one's own", "BINTRAIL_TAG=dev\n", "is not a version number"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := installWith(t, withEnv(tc.env))
			if r.failed {
				t.Fatalf("install failed:\n%s", r.out)
			}
			if got := envFile(t, r); got != tc.env {
				t.Errorf(".env changed from %q to %q", tc.env, got)
			}
			if !r.fetched("/" + newest + "/docker-compose.yml") {
				t.Errorf("the compose file did not come from the newest release:\n%s", r.urls)
			}
			if !strings.Contains(r.out, tc.says) || strings.Contains(r.out, "This is version") {
				t.Errorf("wrong words for BINTRAIL_TAG %q:\n%s", tc.env, r.out)
			}
			noBannedWords(t, r)
		})
	}
}

// Images are tagged without the v, so a BINTRAIL_TAG with one names an image
// that does not exist: the install stops and says what to write instead,
// without changing the file.
func TestVersion_aTagWithAVStops(t *testing.T) {
	r := installWith(t, withEnv("BINTRAIL_TAG=v0.128.0\n"))
	if !r.failed || r.upCalls != 0 {
		t.Fatalf("the install went on with a v in BINTRAIL_TAG:\n%s", r.out)
	}
	if !strings.Contains(r.out, "Change it to BINTRAIL_TAG=0.128.0") {
		t.Errorf("the fix is not said:\n%s", r.out)
	}
	if got := envFile(t, r); got != "BINTRAIL_TAG=v0.128.0\n" {
		t.Errorf(".env changed to %q", got)
	}
}

// A fresh install only ever ADDS its line, on a line of its own.
func TestVersion_theLineIsAddedToAnExistingEnv(t *testing.T) {
	line := "BINTRAIL_TAG=" + newestImage + "\n"
	for _, tc := range []struct{ name, before, after string }{
		{"empty file", "", line},
		{"other lines", "SCHEMAS=shop\n", "SCHEMAS=shop\n" + line},
		{"no newline at the end", "SCHEMAS=shop", "SCHEMAS=shop\n" + line},
		{"commented out", "# BINTRAIL_TAG=0.1.0\n", "# BINTRAIL_TAG=0.1.0\n" + line},
		{"a similar name", "BINTRAIL_TAG_OLD=0.1.0\n", "BINTRAIL_TAG_OLD=0.1.0\n" + line},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := installWith(t, withEnv(tc.before))
			if r.failed {
				t.Fatalf("install failed:\n%s", r.out)
			}
			if got := envFile(t, r); got != tc.after {
				t.Errorf(".env = %q, want %q", got, tc.after)
			}
		})
	}
}

// A value in the installing shell beats .env in compose, so .env is left
// alone and the run says the value lasts for this run only.
func TestVersion_aTagInTheShellIsLeftToTheShell(t *testing.T) {
	r := install(t, "BINTRAIL_TAG=0.128.0")
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	if got := envFile(t, r); got != "" {
		t.Errorf(".env = %q, want none", got)
	}
	if r.askedGitHub() || !r.fetched("/v0.128.0/docker-compose.yml") {
		t.Errorf("want the compose file of v0.128.0 and no lookup:\n%s", r.urls)
	}
	for _, want := range []string{"set in this shell", "This is version 0.128.0", "for this run only"} {
		if !strings.Contains(r.out, want) {
			t.Errorf("the output does not say %q:\n%s", want, r.out)
		}
	}
	noBannedWords(t, r)
}

// DBTRAIL_REF still chooses: a release tag is that version (written without
// the v, no lookup); any other ref is not a release, so nothing is written
// and the run says the images are latest.
func TestVersion_DBTRAIL_REF(t *testing.T) {
	for _, tc := range []struct {
		ref, tag string // tag: the BINTRAIL_TAG written; "" = none
	}{
		{"v0.129.0", "0.129.0"},
		{"v0.129.0-rc1", "0.129.0-rc1"},
		{"main", ""},
		{"some-branch", ""},
	} {
		t.Run(tc.ref, func(t *testing.T) {
			r := install(t, "DBTRAIL_REF="+tc.ref, "STUB_RELEASE_FAIL=1")
			if r.failed {
				t.Fatalf("install failed:\n%s", r.out)
			}
			if r.askedGitHub() {
				t.Errorf("GitHub was asked although DBTRAIL_REF chose:\n%s", r.urls)
			}
			for _, p := range []string{"/" + tc.ref + "/docker-compose.yml", "/" + tc.ref + "/dbtrail.sh"} {
				if !r.fetched(p) {
					t.Errorf("did not fetch %s:\n%s", p, r.urls)
				}
			}
			want := ""
			if tc.tag != "" {
				want = "BINTRAIL_TAG=" + tc.tag + "\n"
			}
			if got := envFile(t, r); got != want {
				t.Errorf(".env = %q, want %q", got, want)
			}
			if tc.tag == "" {
				if !strings.Contains(r.out, "DBTRAIL_REF="+tc.ref+" is not a release") || !strings.Contains(r.out, "no version is set") {
					t.Errorf("a ref that is not a release is not said:\n%s", r.out)
				}
			} else if !strings.Contains(r.out, "This is version "+tc.tag) {
				t.Errorf("the version is not named:\n%s", r.out)
			}
			noBannedWords(t, r)
		})
	}
}

// DBTRAIL_REF and a BINTRAIL_TAG already in .env that name different
// versions: .env still wins for the images, and the mismatch is said.
func TestVersion_DBTRAIL_REFAgainstAKeptTagIsSaid(t *testing.T) {
	r := installWith(t, withEnv("BINTRAIL_TAG=0.128.0\n"), "DBTRAIL_REF=v0.129.0")
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	if got := envFile(t, r); got != "BINTRAIL_TAG=0.128.0\n" {
		t.Errorf(".env changed to %q", got)
	}
	if !r.fetched("/v0.129.0/docker-compose.yml") || !strings.Contains(r.out, "Make the two name the same version") {
		t.Errorf("the mismatch is not said:\n%s\n%s", r.out, r.urls)
	}
	noBannedWords(t, r)
}

// A re-run over a stack installed before versions were written reuses its
// compose file and does not write .env (as for the buffer pool, a new line
// would restart the stack in the middle of capture); it says how to pin.
// Its lookup is only for the dbtrail command, so a failed one keeps the
// command already there instead of stopping the run.
func TestVersion_aReRunOverAnUnpinnedStack(t *testing.T) {
	compose, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	prep := func(dir string) {
		writeFileNoT(filepath.Join(dir, "docker-compose.yml"), string(compose))
		writeFileNoT(filepath.Join(dir, ".env"), "SCHEMAS=shop\n")
		if err := os.WriteFile(filepath.Join(dir, "dbtrail"), []byte("#!/bin/sh\necho old\n"), 0o755); err != nil {
			panic(err)
		}
	}
	t.Run("lookup answers", func(t *testing.T) {
		r := installWith(t, prep)
		if r.failed {
			t.Fatalf("re-run failed:\n%s", r.out)
		}
		if got := envFile(t, r); got != "SCHEMAS=shop\n" {
			t.Errorf("a re-run changed .env to %q", got)
		}
		if r.fetched("docker-compose.yml") {
			t.Errorf("the existing compose file was fetched again:\n%s", r.urls)
		}
		if !r.fetched("/" + newest + "/dbtrail.sh") {
			t.Errorf("the dbtrail command did not come from the newest release:\n%s", r.urls)
		}
		if !strings.Contains(r.out, "add BINTRAIL_TAG="+newestImage+" to") || !strings.Contains(r.out, "no version is set") {
			t.Errorf("how to pin is not said:\n%s", r.out)
		}
		noBannedWords(t, r)
	})
	t.Run("lookup fails", func(t *testing.T) {
		r := installWith(t, prep, "STUB_RELEASE_FAIL=1")
		if r.failed || r.upCalls != 1 {
			t.Fatalf("a failed lookup stopped a re-run:\n%s", r.out)
		}
		got, _ := os.ReadFile(filepath.Join(r.dir, "dbtrail"))
		if !strings.Contains(string(got), "echo old") || strings.Contains(r.urls, "dbtrail.sh") {
			t.Errorf("the dbtrail command was not simply kept:\n%s", r.urls)
		}
		for _, want := range []string{"Couldn't find the newest DBTrail release", "kept the dbtrail command", "add BINTRAIL_TAG=<version>"} {
			if !strings.Contains(r.out, want) {
				t.Errorf("the output does not say %q:\n%s", want, r.out)
			}
		}
		noBannedWords(t, r)
	})
}

// A compose file that does not read BINTRAIL_TAG (an older DBTRAIL_REF)
// gets no line: it would claim a version the stack does not run.
func TestVersion_aComposeThatDoesNotReadTheTagGetsNoLine(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	// The file's comments still name BINTRAIL_TAG, as an edited copy of the
	// current file would: only the image: lines decide.
	all := strings.ReplaceAll(string(b), "${BINTRAIL_TAG:-latest}", "latest")
	one := strings.Replace(string(b), "bintrail-console:${BINTRAIL_TAG:-latest}", "bintrail-console:0.120.0", 1)
	if !strings.Contains(all, "BINTRAIL_TAG") || one == string(b) {
		t.Fatal("could not build the edited compose files")
	}
	for name, body := range map[string]string{"no image reads it": all, "one image fixed by hand": one} {
		t.Run(name, func(t *testing.T) {
			src := filepath.Join(t.TempDir(), "docker-compose.yml")
			writeFile(t, src, body, 0o644)
			r := install(t, "COMPOSE_SRC="+src)
			if r.failed {
				t.Fatalf("install failed:\n%s", r.out)
			}
			if got := envFile(t, r); got != "" {
				t.Errorf(".env = %q, want none", got)
			}
			if !strings.Contains(r.out, "does not take its image versions from BINTRAIL_TAG") || strings.Contains(r.out, "This is version") {
				t.Errorf("wrong words for a compose file whose images ignore BINTRAIL_TAG:\n%s", r.out)
			}
			noBannedWords(t, r)

			// A version already in .env is not claimed either: this file ignores it.
			r = installWith(t, withEnv("BINTRAIL_TAG=0.128.0\n"), "COMPOSE_SRC="+src)
			if r.failed {
				t.Fatalf("install failed:\n%s", r.out)
			}
			if got := envFile(t, r); got != "BINTRAIL_TAG=0.128.0\n" {
				t.Errorf(".env changed to %q", got)
			}
			if strings.Contains(r.out, "This is version") {
				t.Errorf("a version this compose file ignores is claimed:\n%s", r.out)
			}
		})
	}
}

// A BINTRAIL_TAG shaped like a version that no release has (a typo) stops
// with the cause named, not only a failed download.
func TestVersion_aKeptVersionWithNoReleaseSaysSo(t *testing.T) {
	r := installWith(t, withEnv("BINTRAIL_TAG=0.13.0\n"), "STUB_COMPOSE_FAIL=1")
	if !r.failed || r.upCalls != 0 {
		t.Fatalf("the install went on:\n%s", r.out)
	}
	if !strings.Contains(r.out, "BINTRAIL_TAG=0.13.0 (in .env) names no DBTrail release") {
		t.Errorf("the cause is not named:\n%s", r.out)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "docker-compose.yml")); err == nil {
		t.Error("a failed download left a docker-compose.yml behind")
	}
	if got := envFile(t, r); got != "BINTRAIL_TAG=0.13.0\n" {
		t.Errorf(".env changed to %q", got)
	}
	noBannedWords(t, r)
}

// What the line the installer writes rests on: every DBTrail image in the
// compose file reads BINTRAIL_TAG, and the release publishes the images
// under the version WITHOUT the v (GoReleaser's {{ .Version }}). If either
// changes, the written line would pin nothing or name an image that does
// not exist.
func TestVersion_theWrittenTagNamesAPublishedImage(t *testing.T) {
	compose, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	imageRE := regexp.MustCompile(`(?m)^\s*image:\s*(ghcr\.io/dbtrail/\S+)`)
	images := imageRE.FindAllStringSubmatch(string(compose), -1)
	if len(images) < 3 {
		t.Fatalf("found %d DBTrail images in docker-compose.yml", len(images))
	}
	for _, m := range images {
		if !strings.HasSuffix(m[1], ":${BINTRAIL_TAG:-latest}") {
			t.Errorf("%s does not read BINTRAIL_TAG", m[1])
		}
	}
	gr, err := os.ReadFile(filepath.Join("..", "..", "build", ".goreleaser.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, img := range []string{"bintrail", "bintrail-console"} {
		want := `name_template: "ghcr.io/dbtrail/` + img + `:{{ .Version }}"`
		if !strings.Contains(string(gr), want) {
			t.Errorf("build/.goreleaser.yaml no longer publishes %s as %s", img, want)
		}
	}
	if strings.Contains(string(gr), ":v{{") || strings.Contains(string(gr), "{{ .Tag }}") {
		t.Error("build/.goreleaser.yaml tags an image with the git tag's v; the installer writes BINTRAIL_TAG without it")
	}
}
