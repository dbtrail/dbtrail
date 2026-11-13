package consoleapp

import (
	"fmt"
	"os"
	"strings"
)

// routerOwnAWSKeysEnv names which of the compose file's ROUTER_AWS_*
// variables were set (#2690), as a list of words: access_key_id,
// secret_access_key, session_token. Never their values: those reach the
// router as AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN,
// each falling back to the shared variable of the same name.
//
// The fallback is per variable because that is all compose interpolation can
// say. Two things it cannot say are settled here, at start, before anything
// reads the environment:
//
//   - The router's own pair without a token of its own must run with NO
//     token. Compose filled AWS_SESSION_TOKEN from the shared one, which
//     belongs to the shared pair; beside long-term keys S3 refuses it.
//   - Half a pair is refused. Compose filled the other half from the shared
//     keys, a key id of one pair with the secret of another, which S3 refuses
//     on every request, and an operator who set it meant the router to read
//     with keys of its own.
const routerOwnAWSKeysEnv = "BINTRAIL_ROUTER_OWN_AWS_KEYS"

// applyRouterAWSKeys settles the router's S3 credentials from the marker and
// returns the startup line that says which keys it reads with ("" with no
// keys in the environment: an instance role or a mounted ~/.aws, which this
// does not see). An error refuses the start. Neither shows a value.
//
// Unsetting is how the token is dropped, never an empty value. The Go readers
// treat an empty variable as unset (the AWS SDK's environment loader reads
// the token only beside a non-empty pair, and duckdbutil's secrets skip an
// empty one), but DuckDB's own credential chain reads the C environment and
// an unset variable needs no assumption about it. The process runs with cgo,
// so os.Unsetenv reaches that C environment too.
func applyRouterAWSKeys() (string, error) {
	var key, secret, token bool
	for _, w := range strings.Split(os.Getenv(routerOwnAWSKeysEnv), ",") {
		switch strings.ToLower(strings.TrimSpace(w)) {
		case "":
		case "access_key_id":
			key = true
		case "secret_access_key":
			secret = true
		case "session_token":
			token = true
		default:
			return "", fmt.Errorf("%s names %q, which is not one of access_key_id, secret_access_key, session_token. The compose file sets it from the ROUTER_AWS_* variables in .env; take the router service's lines from the current docker-compose.yml", routerOwnAWSKeysEnv, strings.TrimSpace(w))
		}
	}
	switch {
	case key && !secret:
		return "", fmt.Errorf("ROUTER_AWS_ACCESS_KEY_ID is set and ROUTER_AWS_SECRET_ACCESS_KEY is not: the router would sign S3 requests with its own key id and the shared secret, which S3 refuses. Set both in .env, or neither to read with the shared keys, then docker compose up -d router")
	case secret && !key:
		return "", fmt.Errorf("ROUTER_AWS_SECRET_ACCESS_KEY is set and ROUTER_AWS_ACCESS_KEY_ID is not: the router would sign S3 requests with the shared key id and its own secret, which S3 refuses. Set both in .env, or neither to read with the shared keys, then docker compose up -d router")
	case token && !key:
		return "", fmt.Errorf("ROUTER_AWS_SESSION_TOKEN is set without ROUTER_AWS_ACCESS_KEY_ID and ROUTER_AWS_SECRET_ACCESS_KEY: a session token belongs to the key pair it was issued with, and the shared keys are another pair. Set the router's pair beside it in .env, or remove it, then docker compose up -d router")
	case key && token:
		return "Router: S3 is read with the router's own keys (ROUTER_AWS_ACCESS_KEY_ID, ROUTER_AWS_SECRET_ACCESS_KEY and ROUTER_AWS_SESSION_TOKEN), not the capture service's.\n", nil
	case key:
		if err := os.Unsetenv("AWS_SESSION_TOKEN"); err != nil {
			return "", fmt.Errorf("drop the shared AWS_SESSION_TOKEN beside the router's own keys: %w", err)
		}
		return "Router: S3 is read with the router's own keys (ROUTER_AWS_ACCESS_KEY_ID and ROUTER_AWS_SECRET_ACCESS_KEY), not the capture service's, with no session token: the shared AWS_SESSION_TOKEN belongs to the shared keys. Set ROUTER_AWS_SESSION_TOKEN when the router's keys are temporary.\n", nil
	}
	// The shared keys. An empty token is what compose hands over when none
	// is set, and what a systemd unit's own environment file says to clear a
	// shared one: dropped, so no reader has to treat empty as none.
	if v, set := os.LookupEnv("AWS_SESSION_TOKEN"); set && v == "" {
		if err := os.Unsetenv("AWS_SESSION_TOKEN"); err != nil {
			return "", fmt.Errorf("drop the empty AWS_SESSION_TOKEN: %w", err)
		}
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		return "", nil
	}
	return "Router: S3 is read with the keys in AWS_ACCESS_KEY_ID. In the compose stack those are the capture service's, which can also write and delete; ROUTER_AWS_ACCESS_KEY_ID and ROUTER_AWS_SECRET_ACCESS_KEY in .env give the router read-only keys of its own (docs/s3-iam-policy.md).\n", nil
}
