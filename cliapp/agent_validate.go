package cliapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// runAgentValidate runs pre-flight checks and prints pass/fail for each.
// Checks MySQL, S3, dbtrail API, and WebSocket connectivity depending on
// which flags are configured.
func runAgentValidate(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var failed int

	// ── MySQL checks ────────────────────────────────────────────────────
	if agtSourceDSN != "" {
		db, err := config.Connect(agtSourceDSN)
		if err != nil {
			printCheck("MySQL connection", "", fmt.Errorf("connect: %w", err))
			failed++
			printSkip("Replication privileges", "MySQL connection failed")
			printSkip("Schema snapshot", "MySQL connection failed")
		} else {
			defer db.Close()

			detail, err := checkMySQL(ctx, db)
			printCheck("MySQL connection", detail, err)
			if err != nil {
				failed++
			}

			detail, err = checkReplPrivileges(ctx, db)
			printCheck("Replication privileges", detail, err)
			if err != nil {
				failed++
			}

			detail, err = checkSchemaSnapshot(ctx, db)
			printCheck("Schema snapshot", detail, err)
			if err != nil {
				failed++
			}
		}
	} else {
		printSkip("MySQL connection", "--source-dsn not set")
		printSkip("Replication privileges", "--source-dsn not set")
		printSkip("Schema snapshot", "--source-dsn not set")
	}

	// ── S3 check ────────────────────────────────────────────────────────
	if agtS3Bucket != "" {
		detail, err := checkS3(ctx)
		printCheck("S3 bucket", detail, err)
		if err != nil {
			failed++
		}
	} else if agtSourceDSN != "" && agtServerID != 0 {
		// Mirror the runAgent invariant: BYOS mode (#289) requires
		// --s3-bucket. If --validate said "skipped" while the real run
		// would refuse to start, the operator would clear --validate,
		// re-run, and hit the fail-fast — a smaller version of the same
		// silent-misconfiguration UX the original bug filed against.
		printCheck("BYOS flush sink", "",
			fmt.Errorf("--s3-bucket (or BINTRAIL_S3_BUCKET) is required in BYOS mode (--source-dsn + --server-id set); without it the agent buffers events in memory and drops them on restart"))
		failed++
	} else {
		printSkip("S3 bucket", "--s3-bucket not set")
	}

	// ── dbtrail API check ───────────────────────────────────────────────
	detail, err := checkAPI(ctx)
	printCheck("DBTrail API", detail, err)
	if err != nil {
		failed++
	}

	// ── WebSocket check ─────────────────────────────────────────────────
	detail, err = checkWebSocket(ctx)
	printCheck("WebSocket channel", detail, err)
	if err != nil {
		failed++
	}

	fmt.Println()
	if failed > 0 {
		return fmt.Errorf("%d check(s) failed", failed)
	}

	fmt.Println("Ready to stream. Remove --validate to start the agent.")
	return nil
}

func printCheck(name, detail string, err error) {
	if err != nil {
		fmt.Printf("✗ %s: %v\n", name, err)
	} else if detail != "" {
		fmt.Printf("✓ %s OK (%s)\n", name, detail)
	} else {
		fmt.Printf("✓ %s OK\n", name)
	}
}

func printSkip(name, reason string) {
	fmt.Printf("- %s skipped (%s)\n", name, reason)
}

// ── Individual checks ──────────────────────────────────────────────────────

// checkMySQL verifies MySQL connectivity, version, binlog_format, and
// binlog_row_image.
func checkMySQL(ctx context.Context, db *sql.DB) (string, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return "", fmt.Errorf("query version: %w", err)
	}

	if err := metadata.ValidateBinlogFormat(db); err != nil {
		return "", err
	}
	if err := metadata.ValidateBinlogRowImage(db); err != nil {
		return "", err
	}

	return fmt.Sprintf("%s, ROW format, FULL image", version), nil
}

// checkReplPrivileges verifies that the MySQL user has REPLICATION SLAVE
// and REPLICATION CLIENT privileges.
func checkReplPrivileges(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return "", fmt.Errorf("SHOW GRANTS: %w", err)
	}
	defer rows.Close()

	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return "", fmt.Errorf("scan grant: %w", err)
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate grants: %w", err)
	}

	hasSlave, hasClient := metadata.HasReplPrivileges(grants)

	var missing []string
	if !hasSlave {
		missing = append(missing, "REPLICATION SLAVE")
	}
	if !hasClient {
		missing = append(missing, "REPLICATION CLIENT")
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("missing privileges: %s", strings.Join(missing, ", "))
	}

	return "REPLICATION SLAVE, REPLICATION CLIENT", nil
}

// checkSchemaSnapshot builds a resolver from the source MySQL's
// information_schema and reports the table and schema counts.
func checkSchemaSnapshot(ctx context.Context, db *sql.DB) (string, error) {
	schemas := cliutil.ParseSchemaList(agtSchemas)
	resolver, err := buildResolverFromSource(db, schemas)
	if err != nil {
		return "", err
	}

	var schemaCount int
	if len(schemas) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		q := fmt.Sprintf(`SELECT COUNT(DISTINCT TABLE_SCHEMA) FROM information_schema.TABLES
			WHERE TABLE_SCHEMA IN (%s) AND TABLE_TYPE = 'BASE TABLE'`, placeholders)
		var args []any
		for _, s := range schemas {
			args = append(args, s)
		}
		if err := db.QueryRowContext(ctx, q, args...).Scan(&schemaCount); err != nil {
			slog.Warn("schema count query failed", "error", err)
		}
	} else {
		if err := db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT TABLE_SCHEMA) FROM information_schema.TABLES
			WHERE TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')
			AND TABLE_TYPE = 'BASE TABLE'`).Scan(&schemaCount); err != nil {
			slog.Warn("schema count query failed", "error", err)
		}
	}

	if schemaCount > 0 {
		return fmt.Sprintf("%d tables across %d schemas", resolver.TableCount(), schemaCount), nil
	}
	return fmt.Sprintf("%d tables", resolver.TableCount()), nil
}

// checkS3 verifies S3 connectivity by writing, reading, and deleting a
// test object in the configured bucket.
func checkS3(ctx context.Context) (string, error) {
	backend, err := storage.NewS3Backend(ctx, storage.S3Config{
		Bucket: agtS3Bucket,
		Region: agtS3Region,
		Prefix: agtS3Prefix,
	})
	if err != nil {
		return "", fmt.Errorf("initialize: %w", err)
	}

	testKey := "_bintrail_validate"
	if err := backend.Put(ctx, testKey, strings.NewReader("ok")); err != nil {
		return "", fmt.Errorf("write test object: %w", err)
	}
	// Best-effort cleanup — ensures the test object is removed even if
	// Get fails below.
	defer backend.Delete(ctx, testKey)

	rc, err := backend.Get(ctx, testKey)
	if err != nil {
		return "", fmt.Errorf("read test object: %w", err)
	}
	rc.Close()

	region := agtS3Region
	if region == "" {
		region = "default"
	}
	return fmt.Sprintf("%s, %s", agtS3Bucket, region), nil
}

// checkAPI verifies connectivity to the dbtrail API by sending an
// authenticated request to the validation endpoint.
func checkAPI(ctx context.Context) (string, error) {
	baseURL := wsEndpointToHTTP(agtEndpoint)
	url := baseURL + "/v1/agent/validate"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+agtAPIKey)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return "", fmt.Errorf("authentication failed (HTTP %d)", resp.StatusCode)
	case resp.StatusCode == http.StatusNotFound:
		// The /v1/agent/validate endpoint may not exist on older API versions.
		// A 404 proves connectivity but NOT that the API key is valid (the
		// server has no reason to check auth for an unknown path).
		return "reachable, auth not verified", nil
	case resp.StatusCode >= 400:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}

	var result struct {
		TenantID string `json:"tenant_id"`
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if json.Unmarshal(body, &result) == nil && result.TenantID != "" {
		return fmt.Sprintf("tenant: %s", result.TenantID), nil
	}

	return "authenticated", nil
}

// checkWebSocket verifies that the agent can establish a WebSocket
// connection to the dbtrail endpoint with the configured API key.
func checkWebSocket(ctx context.Context) (string, error) {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+agtAPIKey)

	conn, resp, err := websocket.Dial(ctx, agtEndpoint, &websocket.DialOptions{
		HTTPHeader: header,
	})
	if err != nil {
		if resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			return "", fmt.Errorf("authentication failed (HTTP %d)", resp.StatusCode)
		}
		return "", fmt.Errorf("dial: %w", err)
	}
	conn.CloseNow()

	return "connected", nil
}
