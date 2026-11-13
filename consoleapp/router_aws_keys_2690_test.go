package consoleapp

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
)

// #2690: the compose router can be given S3 keys of its own (ROUTER_AWS_* in
// .env), so the process that serves the MySQL port reads with read-only keys
// while the capture service keeps the keys that write and delete. Compose
// falls each variable back to the shared one, and it cannot express "the
// router's token, or none, when the router has its own pair": the router
// settles that at start from the marker the compose file hands it, which
// names the ROUTER_AWS_* variables that were set and never their values.

func setRouterAWSEnv(t *testing.T, marker, key, secret, token string) {
	t.Helper()
	t.Setenv(routerOwnAWSKeysEnv, marker)
	t.Setenv("AWS_ACCESS_KEY_ID", key)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secret)
	t.Setenv("AWS_SESSION_TOKEN", token)
}

func TestRouterAWSKeys_noRouterKeysChangesNothing(t *testing.T) {
	// Exactly today's behaviour: the shared keys and their token stay.
	for _, marker := range []string{"", "   ", ","} {
		setRouterAWSEnv(t, marker, "shared-id", "shared-secret", "shared-token")
		line, err := applyRouterAWSKeys()
		if err != nil {
			t.Fatalf("marker %q: %v", marker, err)
		}
		if got := os.Getenv("AWS_SESSION_TOKEN"); got != "shared-token" {
			t.Errorf("marker %q: AWS_SESSION_TOKEN = %q, want the shared token kept", marker, got)
		}
		if os.Getenv("AWS_ACCESS_KEY_ID") != "shared-id" || os.Getenv("AWS_SECRET_ACCESS_KEY") != "shared-secret" {
			t.Errorf("marker %q: the shared keys were changed", marker)
		}
		if !strings.Contains(line, "AWS_ACCESS_KEY_ID") || !strings.Contains(line, "ROUTER_AWS_ACCESS_KEY_ID") {
			t.Errorf("marker %q: line %q should say the shared keys are used and how to give the router its own", marker, line)
		}
	}
}

func TestRouterAWSKeys_noKeysAtAllSaysNothing(t *testing.T) {
	// An instance role or a mounted ~/.aws: nothing in the environment, and
	// nothing to say about keys the router does not have.
	setRouterAWSEnv(t, "", "", "", "")
	line, err := applyRouterAWSKeys()
	if err != nil || line != "" {
		t.Fatalf("line %q err %v, want nothing", line, err)
	}
	if _, set := os.LookupEnv("AWS_SESSION_TOKEN"); set {
		t.Error("an empty AWS_SESSION_TOKEN is left set; it is dropped so no reader has to treat empty as none")
	}
}

func TestRouterAWSKeys_emptySharedTokenIsDroppedBesideSharedKeys(t *testing.T) {
	// The systemd recipe: the router's own environment file sets
	// AWS_SESSION_TOKEN= to clear a token the shared file sets.
	setRouterAWSEnv(t, "", "router-id", "router-secret", "")
	if _, err := applyRouterAWSKeys(); err != nil {
		t.Fatal(err)
	}
	if _, set := os.LookupEnv("AWS_SESSION_TOKEN"); set {
		t.Error("the empty AWS_SESSION_TOKEN is still set")
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") != "router-id" {
		t.Error("the keys were changed")
	}
}

func TestRouterAWSKeys_ownPairWithoutTokenDropsTheSharedToken(t *testing.T) {
	// Compose filled AWS_SESSION_TOKEN from the shared one: it belongs to the
	// shared pair, and beside the router's long-term keys S3 refuses it.
	for _, marker := range []string{"access_key_id,secret_access_key,", "secret_access_key,access_key_id", " access_key_id , secret_access_key ", "ACCESS_KEY_ID,SECRET_ACCESS_KEY,"} {
		setRouterAWSEnv(t, marker, "router-id", "router-secret", "shared-token")
		line, err := applyRouterAWSKeys()
		if err != nil {
			t.Fatalf("marker %q: %v", marker, err)
		}
		if v, set := os.LookupEnv("AWS_SESSION_TOKEN"); set {
			t.Errorf("marker %q: AWS_SESSION_TOKEN is still %q; the router's own pair must not carry the shared token", marker, v)
		}
		if os.Getenv("AWS_ACCESS_KEY_ID") != "router-id" || os.Getenv("AWS_SECRET_ACCESS_KEY") != "router-secret" {
			t.Errorf("marker %q: the router's keys were changed", marker)
		}
		if !strings.Contains(line, "router's own keys") || !strings.Contains(line, "no session token") {
			t.Errorf("marker %q: line %q", marker, line)
		}
		for _, v := range []string{"router-id", "router-secret", "shared-token"} {
			if strings.Contains(line, v) {
				t.Errorf("marker %q: the line shows a credential value: %q", marker, line)
			}
		}
	}
}

func TestRouterAWSKeys_ownPairWithOwnTokenKeepsIt(t *testing.T) {
	setRouterAWSEnv(t, "access_key_id,secret_access_key,session_token,", "router-id", "router-secret", "router-token")
	line, err := applyRouterAWSKeys()
	if err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("AWS_SESSION_TOKEN"); got != "router-token" {
		t.Errorf("AWS_SESSION_TOKEN = %q, want the router's own token", got)
	}
	if !strings.Contains(line, "ROUTER_AWS_SESSION_TOKEN") || strings.Contains(line, "router-token") {
		t.Errorf("line %q", line)
	}
}

func TestRouterAWSKeys_halfAPairRefusesToStart(t *testing.T) {
	// Compose fills the missing half from the shared keys: a key id of one
	// pair with the secret of another, which S3 refuses on every request.
	// Refused at start with what to change, never a value.
	cases := []struct {
		marker string
		want   []string
	}{
		{"access_key_id,", []string{"ROUTER_AWS_ACCESS_KEY_ID is set", "ROUTER_AWS_SECRET_ACCESS_KEY is not"}},
		{"secret_access_key,", []string{"ROUTER_AWS_SECRET_ACCESS_KEY is set", "ROUTER_AWS_ACCESS_KEY_ID is not"}},
		{"access_key_id,session_token,", []string{"ROUTER_AWS_ACCESS_KEY_ID is set", "ROUTER_AWS_SECRET_ACCESS_KEY is not"}},
		{"session_token,", []string{"ROUTER_AWS_SESSION_TOKEN is set", "ROUTER_AWS_ACCESS_KEY_ID and ROUTER_AWS_SECRET_ACCESS_KEY"}},
	}
	for _, c := range cases {
		setRouterAWSEnv(t, c.marker, "mixed-id", "mixed-secret", "mixed-token")
		line, err := applyRouterAWSKeys()
		if err == nil {
			t.Fatalf("marker %q: started (line %q); a mixed pair must refuse", c.marker, line)
		}
		for _, w := range c.want {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("marker %q: error %q does not say %q", c.marker, err, w)
			}
		}
		for _, v := range []string{"mixed-id", "mixed-secret", "mixed-token"} {
			if strings.Contains(err.Error(), v) {
				t.Errorf("marker %q: the error shows a credential value: %q", c.marker, err)
			}
		}
	}
}

func TestRouterAWSKeys_unknownMarkerWordRefuses(t *testing.T) {
	setRouterAWSEnv(t, "access_key_id,secret_access_key,region,", "a", "b", "")
	if _, err := applyRouterAWSKeys(); err == nil || !strings.Contains(err.Error(), `"region"`) {
		t.Fatalf("err %v, want a refusal naming the word it does not know", err)
	}
}

// The compose file: each credential of the router falls back to the shared
// variable, the marker names only which ROUTER_AWS_* are set, and the
// capture service is given none of it.
func TestComposeRouter_ownAWSKeysFallBackToTheShared(t *testing.T) {
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc composeRouterFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	router := doc.Services["router"].Environment
	want := map[string]string{
		"AWS_ACCESS_KEY_ID":     "${ROUTER_AWS_ACCESS_KEY_ID:-${AWS_ACCESS_KEY_ID:-}}",
		"AWS_SECRET_ACCESS_KEY": "${ROUTER_AWS_SECRET_ACCESS_KEY:-${AWS_SECRET_ACCESS_KEY:-}}",
		"AWS_SESSION_TOKEN":     "${ROUTER_AWS_SESSION_TOKEN:-${AWS_SESSION_TOKEN:-}}",
		routerOwnAWSKeysEnv:     "${ROUTER_AWS_ACCESS_KEY_ID:+access_key_id,}${ROUTER_AWS_SECRET_ACCESS_KEY:+secret_access_key,}${ROUTER_AWS_SESSION_TOKEN:+session_token,}",
		// The store is the same for both processes: one region, one endpoint.
		"AWS_REGION":             "${AWS_REGION:-}",
		"BINTRAIL_S3_ENDPOINT":   "${BINTRAIL_S3_ENDPOINT:-}",
		"BINTRAIL_S3_PATH_STYLE": "${BINTRAIL_S3_PATH_STYLE:-}",
	}
	for k, v := range want {
		if got, _ := router[k].(string); got != v {
			t.Errorf("router %s = %q, want %q", k, got, v)
		}
	}
	for name, svc := range doc.Services {
		if name == "router" {
			continue
		}
		for k, v := range svc.Environment {
			s, _ := v.(string)
			if k == routerOwnAWSKeysEnv || strings.Contains(s, "ROUTER_AWS_") {
				t.Errorf("service %s is given %s=%v: the router's keys are the router's alone", name, k, v)
			}
		}
	}
	// The shared keys of the capture service are untouched.
	capture := doc.Services[composeService].Environment
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		if got, _ := capture[k].(string); got != "${"+k+":-}" {
			t.Errorf("%s %s = %q, want ${%s:-}", composeService, k, got, k)
		}
	}
}

// The same rules through the real interpolation: `docker compose config`
// renders the router's environment from a .env, and that environment is
// what applyRouterAWSKeys then settles. Skipped where Docker Compose is not
// installed (it is on the CI runners).
func TestComposeRouter_renderedAWSKeys(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	if out, err := exec.Command("docker", "compose", "version").CombinedOutput(); err != nil {
		t.Skipf("docker compose is not available: %v %s", err, out)
	}
	abs, err := filepath.Abs(composePath)
	if err != nil {
		t.Fatal(err)
	}
	render := func(t *testing.T, dotenv string) map[string]*string {
		t.Helper()
		dir := t.TempDir()
		envFile := filepath.Join(dir, "router.env")
		if err := os.WriteFile(envFile, []byte(dotenv), 0o600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("docker", "compose", "--project-name", "rt2690", "--project-directory", dir,
			"--env-file", envFile, "-f", abs, "config", "--format", "json")
		// Only what the CLI needs: an AWS_* of the machine running the test
		// would otherwise be interpolated too.
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
		out, err := cmd.Output()
		if err != nil {
			var stderr []byte
			if ee, ok := err.(*exec.ExitError); ok {
				stderr = ee.Stderr
			}
			t.Fatalf("docker compose config: %v\n%s", err, stderr)
		}
		var doc struct {
			Services map[string]struct {
				Environment map[string]*string `json:"environment"`
			} `json:"services"`
		}
		if err := json.Unmarshal(out, &doc); err != nil {
			t.Fatal(err)
		}
		if _, ok := doc.Services["router"]; !ok {
			t.Fatal("no router service in the rendered file")
		}
		for name, svc := range doc.Services {
			if name == "router" {
				continue
			}
			for k, v := range svc.Environment {
				if v != nil && strings.Contains(*v, "ROUTER-") {
					t.Errorf("service %s is given the router's %s", name, k)
				}
			}
		}
		return doc.Services["router"].Environment
	}
	str := func(v *string) string {
		if v == nil {
			return "<unset>"
		}
		return *v
	}
	shared := "AWS_ACCESS_KEY_ID=SHARED-ID\nAWS_SECRET_ACCESS_KEY=SHARED-SECRET\nAWS_SESSION_TOKEN=SHARED-TOKEN\n"
	cases := []struct {
		name, dotenv string
		// What the container is given, and what the router runs with after
		// applyRouterAWSKeys ("<unset>" for none); refuse: it stops at start.
		key, secret, token, runToken string
		refuse                       bool
	}{
		{name: "nothing set", dotenv: "", key: "", secret: "", token: "", runToken: "<unset>"},
		{name: "shared keys only, as before", dotenv: shared, key: "SHARED-ID", secret: "SHARED-SECRET", token: "SHARED-TOKEN", runToken: "SHARED-TOKEN"},
		{name: "router pair beside a shared token", dotenv: shared + "ROUTER_AWS_ACCESS_KEY_ID=ROUTER-ID\nROUTER_AWS_SECRET_ACCESS_KEY=ROUTER-SECRET\n",
			key: "ROUTER-ID", secret: "ROUTER-SECRET", token: "SHARED-TOKEN", runToken: "<unset>"},
		{name: "router pair and token", dotenv: shared + "ROUTER_AWS_ACCESS_KEY_ID=ROUTER-ID\nROUTER_AWS_SECRET_ACCESS_KEY=ROUTER-SECRET\nROUTER_AWS_SESSION_TOKEN=ROUTER-TOKEN\n",
			key: "ROUTER-ID", secret: "ROUTER-SECRET", token: "ROUTER-TOKEN", runToken: "ROUTER-TOKEN"},
		{name: "router pair, no shared keys", dotenv: "ROUTER_AWS_ACCESS_KEY_ID=ROUTER-ID\nROUTER_AWS_SECRET_ACCESS_KEY=ROUTER-SECRET\n",
			key: "ROUTER-ID", secret: "ROUTER-SECRET", token: "", runToken: "<unset>"},
		{name: "router variables present but empty", dotenv: shared + "ROUTER_AWS_ACCESS_KEY_ID=\nROUTER_AWS_SECRET_ACCESS_KEY=\nROUTER_AWS_SESSION_TOKEN=\n",
			key: "SHARED-ID", secret: "SHARED-SECRET", token: "SHARED-TOKEN", runToken: "SHARED-TOKEN"},
		{name: "router key id only", dotenv: shared + "ROUTER_AWS_ACCESS_KEY_ID=ROUTER-ID\n",
			key: "ROUTER-ID", secret: "SHARED-SECRET", token: "SHARED-TOKEN", refuse: true},
		{name: "router token only", dotenv: shared + "ROUTER_AWS_SESSION_TOKEN=ROUTER-TOKEN\n",
			key: "SHARED-ID", secret: "SHARED-SECRET", token: "ROUTER-TOKEN", refuse: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := render(t, c.dotenv)
			for k, want := range map[string]string{"AWS_ACCESS_KEY_ID": c.key, "AWS_SECRET_ACCESS_KEY": c.secret, "AWS_SESSION_TOKEN": c.token} {
				if got := str(env[k]); got != want {
					t.Errorf("rendered %s = %q, want %q", k, got, want)
				}
			}
			setRouterAWSEnv(t, str(env[routerOwnAWSKeysEnv]), str(env["AWS_ACCESS_KEY_ID"]), str(env["AWS_SECRET_ACCESS_KEY"]), str(env["AWS_SESSION_TOKEN"]))
			_, err := applyRouterAWSKeys()
			if c.refuse {
				if err == nil {
					t.Fatal("the router started with a mixed pair")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := "<unset>"
			if v, set := os.LookupEnv("AWS_SESSION_TOKEN"); set {
				got = v
			}
			if got != c.runToken {
				t.Errorf("the router runs with AWS_SESSION_TOKEN %q, want %q", got, c.runToken)
			}
			if os.Getenv("AWS_ACCESS_KEY_ID") != c.key {
				t.Errorf("the router runs with AWS_ACCESS_KEY_ID %q, want %q", os.Getenv("AWS_ACCESS_KEY_ID"), c.key)
			}
		})
	}
}
