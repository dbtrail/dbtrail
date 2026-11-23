package consoleapp

import (
	"fmt"
	"os"
	"strings"
)

// pgRouterOwnAWSKeysEnv names which of the compose file's PG_ROUTER_AWS_*
// variables were set (#3142), as a list of words: access_key_id,
// secret_access_key, session_token. Never their values: those reach
// pg-router as AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and
// AWS_SESSION_TOKEN, each falling back to the shared variable of the same
// name.
//
// This is the MySQL router's rule (router_aws_keys.go, #2690) for the
// PostgreSQL router, in a file of its own with this service's names. The
// PostgreSQL port binds the same time-travel engine, whose snapshot lookup
// reads the server's S3 destination when the local folder has no snapshot
// old enough, so this process reads S3 too and never writes it.
const pgRouterOwnAWSKeysEnv = "BINTRAIL_PG_ROUTER_OWN_AWS_KEYS"

// applyPGRouterAWSKeys settles pg-router's S3 credentials from the marker
// and returns the startup line that says which keys it reads with ("" with
// no keys in the environment: an instance role or a mounted ~/.aws). An
// error refuses the start. Neither shows a value.
//
// Compose can only fall each variable back to the shared one. Two things it
// cannot say are settled here, before anything reads the environment: the
// service's own pair without a token of its own runs with NO token (the
// shared token belongs to the shared pair), and half a pair is refused (a
// key id of one pair with the secret of another, which S3 refuses on every
// request). The token is dropped by unsetting it, which reaches the C
// environment DuckDB's credential chain reads.
func applyPGRouterAWSKeys() (string, error) {
	const apply = "then docker compose up -d pg-router"
	var key, secret, token bool
	for _, w := range strings.Split(os.Getenv(pgRouterOwnAWSKeysEnv), ",") {
		switch strings.ToLower(strings.TrimSpace(w)) {
		case "":
		case "access_key_id":
			key = true
		case "secret_access_key":
			secret = true
		case "session_token":
			token = true
		default:
			return "", fmt.Errorf("%s names %q, which is not one of access_key_id, secret_access_key, session_token. The compose file sets it from the PG_ROUTER_AWS_* variables in .env; take the pg-router service's lines from the current docker-compose.yml", pgRouterOwnAWSKeysEnv, strings.TrimSpace(w))
		}
	}
	switch {
	case key && !secret:
		return "", fmt.Errorf("PG_ROUTER_AWS_ACCESS_KEY_ID is set and PG_ROUTER_AWS_SECRET_ACCESS_KEY is not: the PostgreSQL router would sign S3 requests with its own key id and the shared secret, which S3 refuses. Set both in .env, or neither to read with the shared keys, %s", apply)
	case secret && !key:
		return "", fmt.Errorf("PG_ROUTER_AWS_SECRET_ACCESS_KEY is set and PG_ROUTER_AWS_ACCESS_KEY_ID is not: the PostgreSQL router would sign S3 requests with the shared key id and its own secret, which S3 refuses. Set both in .env, or neither to read with the shared keys, %s", apply)
	case token && !key:
		return "", fmt.Errorf("PG_ROUTER_AWS_SESSION_TOKEN is set without PG_ROUTER_AWS_ACCESS_KEY_ID and PG_ROUTER_AWS_SECRET_ACCESS_KEY: a session token belongs to the key pair it was issued with, and the shared keys are another pair. Set the PostgreSQL router's pair beside it in .env, or remove it, %s", apply)
	case key && token:
		return "PostgreSQL router: S3 is read with this service's own keys (PG_ROUTER_AWS_ACCESS_KEY_ID, PG_ROUTER_AWS_SECRET_ACCESS_KEY and PG_ROUTER_AWS_SESSION_TOKEN), not the capture service's.\n", nil
	case key:
		if err := os.Unsetenv("AWS_SESSION_TOKEN"); err != nil {
			return "", fmt.Errorf("drop the shared AWS_SESSION_TOKEN beside the PostgreSQL router's own keys: %w", err)
		}
		return "PostgreSQL router: S3 is read with this service's own keys (PG_ROUTER_AWS_ACCESS_KEY_ID and PG_ROUTER_AWS_SECRET_ACCESS_KEY), not the capture service's, with no session token: the shared AWS_SESSION_TOKEN belongs to the shared keys. Set PG_ROUTER_AWS_SESSION_TOKEN when this service's keys are temporary.\n", nil
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
	return "PostgreSQL router: S3 is read with the keys in AWS_ACCESS_KEY_ID. In the compose stack those are the capture service's, which can also write and delete; PG_ROUTER_AWS_ACCESS_KEY_ID and PG_ROUTER_AWS_SECRET_ACCESS_KEY in .env give this service read-only keys of its own (docs/s3-iam-policy.md).\n", nil
}
