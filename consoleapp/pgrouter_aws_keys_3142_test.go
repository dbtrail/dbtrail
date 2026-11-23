package consoleapp

import (
	"os"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
)

// #3142: the PostgreSQL port binds the time-travel engine the MySQL port
// binds, whose snapshot lookup reads the server's S3 destination when the
// local folder holds no snapshot old enough. So the pg-router service is
// given the S3 variables the router is given, and the same rule for keys of
// its own (PG_ROUTER_AWS_* in .env): the shared token never rides with its
// own pair, and half a pair refuses the start.

func setPGRouterAWSEnv(t *testing.T, marker, key, secret, token string) {
	t.Helper()
	t.Setenv(pgRouterOwnAWSKeysEnv, marker)
	// The MySQL router's marker must decide nothing here.
	t.Setenv(routerOwnAWSKeysEnv, "access_key_id,")
	t.Setenv("AWS_ACCESS_KEY_ID", key)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secret)
	t.Setenv("AWS_SESSION_TOKEN", token)
}

func awsTokenNow() string {
	if v, set := os.LookupEnv("AWS_SESSION_TOKEN"); set {
		return v
	}
	return "<unset>"
}

func TestPGRouterAWSKeys(t *testing.T) {
	cases := []struct {
		name                       string
		marker, key, secret, token string
		// wantToken is AWS_SESSION_TOKEN after, "<unset>" for none.
		wantToken string
		// wantLine are words the startup line has; none: the line is "".
		wantLine []string
		// refuse are words the refusal has; set: the start is refused.
		refuse []string
	}{
		{name: "no keys at all says nothing and drops the empty token", wantToken: "<unset>"},
		{name: "shared keys and token stay", key: "shared-id", secret: "shared-secret", token: "shared-token",
			wantToken: "shared-token", wantLine: []string{"PostgreSQL router:", "AWS_ACCESS_KEY_ID", "PG_ROUTER_AWS_ACCESS_KEY_ID"}},
		{name: "blank marker is no marker", marker: " , ", key: "shared-id", secret: "shared-secret", token: "shared-token",
			wantToken: "shared-token", wantLine: []string{"AWS_ACCESS_KEY_ID"}},
		{name: "shared keys with an empty token: the systemd recipe", key: "own-id", secret: "own-secret",
			wantToken: "<unset>", wantLine: []string{"AWS_ACCESS_KEY_ID"}},
		{name: "own pair drops the shared token", marker: "access_key_id,secret_access_key,", key: "own-id", secret: "own-secret", token: "shared-token",
			wantToken: "<unset>", wantLine: []string{"this service's own keys", "no session token", "PG_ROUTER_AWS_SESSION_TOKEN"}},
		{name: "own pair, any order, case and spaces", marker: " SECRET_ACCESS_KEY , access_key_id ", key: "own-id", secret: "own-secret", token: "shared-token",
			wantToken: "<unset>", wantLine: []string{"this service's own keys"}},
		{name: "own pair with its own token keeps it", marker: "access_key_id,secret_access_key,session_token,", key: "own-id", secret: "own-secret", token: "own-token",
			wantToken: "own-token", wantLine: []string{"this service's own keys", "PG_ROUTER_AWS_SESSION_TOKEN"}},
		{name: "key id only", marker: "access_key_id,", key: "mixed-id", secret: "mixed-secret", token: "mixed-token",
			refuse: []string{"PG_ROUTER_AWS_ACCESS_KEY_ID is set", "PG_ROUTER_AWS_SECRET_ACCESS_KEY is not", "docker compose up -d pg-router"}},
		{name: "secret only", marker: "secret_access_key,", key: "mixed-id", secret: "mixed-secret", token: "mixed-token",
			refuse: []string{"PG_ROUTER_AWS_SECRET_ACCESS_KEY is set", "PG_ROUTER_AWS_ACCESS_KEY_ID is not", "docker compose up -d pg-router"}},
		{name: "key id and token, no secret", marker: "access_key_id,session_token,", key: "mixed-id", secret: "mixed-secret", token: "mixed-token",
			refuse: []string{"PG_ROUTER_AWS_ACCESS_KEY_ID is set", "PG_ROUTER_AWS_SECRET_ACCESS_KEY is not"}},
		{name: "token only", marker: "session_token,", key: "mixed-id", secret: "mixed-secret", token: "mixed-token",
			refuse: []string{"PG_ROUTER_AWS_SESSION_TOKEN is set", "PG_ROUTER_AWS_ACCESS_KEY_ID and PG_ROUTER_AWS_SECRET_ACCESS_KEY", "docker compose up -d pg-router"}},
		{name: "a word it does not know", marker: "access_key_id,secret_access_key,region,", key: "a-id", secret: "a-secret",
			refuse: []string{`"region"`, pgRouterOwnAWSKeysEnv, "pg-router"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setPGRouterAWSEnv(t, c.marker, c.key, c.secret, c.token)
			line, err := applyPGRouterAWSKeys()
			said := line
			if err != nil {
				said = err.Error()
			}
			for _, v := range []string{c.key, c.secret, c.token} {
				if v != "" && strings.Contains(said, v) {
					t.Errorf("a credential value is shown: %q", said)
				}
			}
			if len(c.refuse) > 0 {
				if err == nil {
					t.Fatalf("started (line %q); want a refusal", line)
				}
				for _, w := range c.refuse {
					if !strings.Contains(err.Error(), w) {
						t.Errorf("refusal %q does not say %q", err, w)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := awsTokenNow(); got != c.wantToken {
				t.Errorf("AWS_SESSION_TOKEN is %q after, want %q", got, c.wantToken)
			}
			if os.Getenv("AWS_ACCESS_KEY_ID") != c.key || os.Getenv("AWS_SECRET_ACCESS_KEY") != c.secret {
				t.Error("the keys were changed")
			}
			if len(c.wantLine) == 0 && line != "" {
				t.Errorf("line %q, want none", line)
			}
			for _, w := range c.wantLine {
				if !strings.Contains(line, w) {
					t.Errorf("line %q does not say %q", line, w)
				}
			}
			if line != "" && (!strings.HasPrefix(line, "PostgreSQL router: ") || !strings.HasSuffix(line, "\n")) {
				t.Errorf("line %q is not one startup line of this service", line)
			}
		})
	}
}

// The start itself: a mixed pair stops pg-router before it reads a setting
// or opens anything.
func TestRunPGRouter_refusesHalfAPairOfItsOwnKeys(t *testing.T) {
	clearPGRouterEnv(t)
	setPGRouterAWSEnv(t, "access_key_id,", "mixed-id", "shared-secret", "")
	// A setting that would refuse the start on its own, after the keys.
	t.Setenv("BINTRAIL_PG_ROUTER_WORKERS", "not-a-number")
	err := runPGRouter(pgRouterTestCmd(t), nil)
	if err == nil || !strings.Contains(err.Error(), "PG_ROUTER_AWS_SECRET_ACCESS_KEY is not") {
		t.Fatalf("err %v, want the refusal of half a pair of its own keys", err)
	}
}

// The compose file: each credential of pg-router falls back to the shared
// variable, its marker names only which PG_ROUTER_AWS_* are set, the store
// is the one the other services read, and neither router is given the
// other's keys.
func TestComposePGRouter_isGivenS3AccessAndItsOwnKeys(t *testing.T) {
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc composeRouterFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	svc, ok := doc.Services["pg-router"]
	if !ok {
		t.Fatalf("no pg-router service in %s", composePath)
	}
	want := map[string]string{
		"AWS_ACCESS_KEY_ID":     "${PG_ROUTER_AWS_ACCESS_KEY_ID:-${AWS_ACCESS_KEY_ID:-}}",
		"AWS_SECRET_ACCESS_KEY": "${PG_ROUTER_AWS_SECRET_ACCESS_KEY:-${AWS_SECRET_ACCESS_KEY:-}}",
		"AWS_SESSION_TOKEN":     "${PG_ROUTER_AWS_SESSION_TOKEN:-${AWS_SESSION_TOKEN:-}}",
		pgRouterOwnAWSKeysEnv:   "${PG_ROUTER_AWS_ACCESS_KEY_ID:+access_key_id,}${PG_ROUTER_AWS_SECRET_ACCESS_KEY:+secret_access_key,}${PG_ROUTER_AWS_SESSION_TOKEN:+session_token,}",
	}
	for k, v := range want {
		if got, _ := svc.Environment[k].(string); got != v {
			t.Errorf("pg-router %s = %q, want %q", k, got, v)
		}
	}
	// One store for every process that reads it: what the router is given.
	router := doc.Services["router"].Environment
	for _, k := range []string{"AWS_REGION", "BINTRAIL_S3_ENDPOINT", "BINTRAIL_S3_PATH_STYLE"} {
		got, _ := svc.Environment[k].(string)
		if w, _ := router[k].(string); got == "" || got != w {
			t.Errorf("pg-router %s = %q, want the router's %q", k, got, w)
		}
	}
	if _, has := svc.Environment[routerOwnAWSKeysEnv]; has {
		t.Errorf("pg-router is given %s, the MySQL router's marker", routerOwnAWSKeysEnv)
	}
	for name, other := range doc.Services {
		if name == "pg-router" {
			continue
		}
		for k, v := range other.Environment {
			if s, _ := v.(string); k == pgRouterOwnAWSKeysEnv || strings.Contains(s, "PG_ROUTER_AWS_") {
				t.Errorf("service %s is given %s=%v: pg-router's keys are its own", name, k, v)
			}
		}
	}
}

// What the compose file renders is what applyPGRouterAWSKeys settles: the
// marker's three words, as compose writes them for each .env, through the
// start. (docker compose's own interpolation of this syntax is exercised
// for the router in TestComposeRouter_renderedAWSKeys.)
func TestComposePGRouter_markerWordsAreTheOnesTheStartReads(t *testing.T) {
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc composeRouterFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	marker, _ := doc.Services["pg-router"].Environment[pgRouterOwnAWSKeysEnv].(string)
	// Every set variable writes its word: the marker of a full .env.
	full := marker
	for _, v := range []string{"PG_ROUTER_AWS_ACCESS_KEY_ID", "PG_ROUTER_AWS_SECRET_ACCESS_KEY", "PG_ROUTER_AWS_SESSION_TOKEN"} {
		open := "${" + v + ":+"
		i := strings.Index(full, open)
		if i < 0 {
			t.Fatalf("the marker %q does not test %s", marker, v)
		}
		j := strings.Index(full[i:], "}")
		full = full[:i] + full[i+len(open):i+j] + full[i+j+1:]
	}
	if strings.ContainsAny(full, "${}") {
		t.Fatalf("the marker %q has more in it than the three variables: %q", marker, full)
	}
	setPGRouterAWSEnv(t, full, "own-id", "own-secret", "own-token")
	line, err := applyPGRouterAWSKeys()
	if err != nil {
		t.Fatalf("the marker of a .env with all three set (%q) refuses the start: %v", full, err)
	}
	if !strings.Contains(line, "PG_ROUTER_AWS_SESSION_TOKEN") || awsTokenNow() != "own-token" {
		t.Errorf("marker %q: line %q, token %q; want the service's own pair with its own token", full, line, awsTokenNow())
	}
}
