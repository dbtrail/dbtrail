package duckdbutil

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// Per-bucket keys on the DuckDB half (#1575, second slice).

func keyedStore(t *testing.T, endpoint, id, secret string) storage.BucketStore {
	t.Helper()
	st, err := store(t, endpoint, "", "").WithKeys(id, secret)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// A bucket with keys of its own gets them, and only them: never a
// credential_chain secret first (the chain would win with the daemon's
// credentials), never the environment's keys or session token.
func TestApplyBucketStoreSecrets_keyedBucketUsesItsOwnKeys(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAENVIRONMENT")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "envsecret")
	t.Setenv("AWS_SESSION_TOKEN", "envtoken")
	stores := map[string]storage.BucketStore{
		"keyed": keyedStore(t, "http://minio:9000", "AKIASTORE", "st'oresecret"),
		"plain": store(t, "http://minio:9000", "", ""),
	}
	for _, tryChain := range []bool{true, false} {
		var ran []string
		exec := func(_ context.Context, stmt string) error { ran = append(ran, stmt); return nil }
		if err := applyBucketStoreSecrets(context.Background(), exec, stores, tryChain); err != nil {
			t.Fatal(err)
		}
		var keyed []string
		for _, stmt := range ran {
			if strings.Contains(stmt, "'s3://keyed/'") {
				keyed = append(keyed, stmt)
			}
		}
		if len(keyed) != 1 {
			t.Fatalf("tryChain=%v: keyed bucket got %d statements, want exactly one:\n%s", tryChain, len(keyed), strings.Join(ran, "\n"))
		}
		want := "PROVIDER config, KEY_ID 'AKIASTORE', SECRET 'st''oresecret', SCOPE 's3://keyed/'"
		if !strings.Contains(keyed[0], want) {
			t.Errorf("tryChain=%v: keyed statement lacks %q:\n%s", tryChain, want, keyed[0])
		}
		for _, leak := range []string{"credential_chain", "AKIAENVIRONMENT", "envsecret", "envtoken", "SESSION_TOKEN"} {
			if strings.Contains(keyed[0], leak) {
				t.Errorf("tryChain=%v: keyed statement carries %s:\n%s", tryChain, leak, keyed[0])
			}
		}
		if tryChain && !strings.Contains(strings.Join(ran, "\n"), "PROVIDER credential_chain, SCOPE 's3://plain/'") {
			t.Errorf("a keyless bucket lost its credential_chain secret:\n%s", strings.Join(ran, "\n"))
		}
	}
}

// The environment holds NO keys here, so a withhold that only counts the
// environment's keys would pass DuckDB's echo of the statement straight
// through, the store's secret with it.
func TestApplyBucketStoreSecrets_keyedErrorWithheldWithEmptyEnvironment(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SESSION_TOKEN", "")
	stores := map[string]storage.BucketStore{"keyed": keyedStore(t, "http://minio:9000", "AKIASTORE", "st'oresecret")}
	echo := func(_ context.Context, stmt string) error {
		return fmt.Errorf("Parser Error: syntax error at or near %q", stmt)
	}
	err := applyBucketStoreSecrets(context.Background(), echo, stores, true)
	if err == nil {
		t.Fatal("a keyed secret that cannot be created must fail the session")
	}
	t.Log(err)
	for _, leak := range []string{"oresecret", "st''ore"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("the error carries the store's secret (%s): %v", leak, err)
		}
	}
	if !strings.Contains(err.Error(), `"keyed"`) || !strings.Contains(err.Error(), "S3 store's keys") {
		t.Errorf("the error must name the bucket and say whose keys the statement carries: %v", err)
	}
	if strings.Contains(err.Error(), "environment") {
		t.Errorf("the error blames the environment for keys that came from the store: %v", err)
	}
	// A plain failure that echoes nothing is the diagnostic, and stays.
	plain := func(context.Context, string) error { return errors.New("Catalog Error: httpfs is not loaded") }
	if err := applyBucketStoreSecrets(context.Background(), plain, stores, true); err == nil || !strings.Contains(err.Error(), "httpfs is not loaded") {
		t.Errorf("a plain message must be kept: %v", err)
	}
}

// views.sql leaves the process: its scoped secrets stay credential_chain and
// never carry a store's keys.
func TestBucketStoreSecretStatements_neverCarryStoreKeys(t *testing.T) {
	stmts := BucketStoreSecretStatements(map[string]storage.BucketStore{"keyed": keyedStore(t, "http://minio:9000", "AKIASTORE", "storesecret")})
	got := strings.Join(stmts, "\n")
	if len(stmts) != 1 || !strings.Contains(got, "PROVIDER credential_chain, SCOPE 's3://keyed/'") {
		t.Errorf("keyed bucket in views.sql: %s", got)
	}
	for _, leak := range []string{"AKIASTORE", "storesecret", "KEY_ID", "SECRET '"} {
		if strings.Contains(got, leak) {
			t.Errorf("views.sql statement carries %s: %s", leak, got)
		}
	}
}
