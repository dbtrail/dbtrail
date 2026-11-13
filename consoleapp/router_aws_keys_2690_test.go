package consoleapp

import (
	"os"
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
