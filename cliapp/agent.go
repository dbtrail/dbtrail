package cliapp

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/agent"
	"github.com/dbtrail/dbtrail/internal/buffer"
	"github.com/dbtrail/dbtrail/internal/byos"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/storage"
)

var agentCmd = &cobra.Command{
	Use:   "agent",
	Short: "Connect to DBTrail and listen for commands",
	Long: `Start an outbound agent channel to the DBTrail service. The agent opens a
WebSocket connection to DBTrail, authenticates with its API key, and listens
for commands (resolve_pk, recover, forensics_query). No inbound ports are
required; all communication is initiated by the agent.

The connection auto-reconnects with exponential backoff on failure and sends
periodic heartbeats to report agent status.

In BYOS mode (when --source-dsn and --server-id are provided), the agent also
reads binlogs from the customer MySQL (or MariaDB, with --source-flavor
mariadb) and keeps recent events in an in-memory buffer. Recovery and pk
resolution queries check the buffer first (fastest, recent data), then fall
back to S3 Parquet archives.

Examples:
  # Start agent with index database
  bintrail agent --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
    --index-dsn "user:pass@tcp(host:3306)/binlog_index"

  # Start agent with Parquet archives on S3
  bintrail agent --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
    --archive-s3 "s3://my-bucket/archives/"

  # BYOS mode: stream + buffer
  bintrail agent --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
    --source-dsn "user:pass@tcp(host:3306)/mydb" \
    --server-id 99999 --buffer-retain "6h"`,
	RunE: runAgent,
}

var (
	agtAPIKey               string
	agtEndpoint             string
	agtIndexDSN             string
	agtSourceDSN            string
	agtFlavor               string
	agtArchiveDir           string
	agtArchiveS3            string
	agtBufferRetain         string
	agtServerID             uint32
	agtServerUUID           string
	agtBatchSize            int
	agtSchemas              string
	agtTables               string
	agtStartGTID            string
	agtS3Bucket             string
	agtS3Region             string
	agtS3Prefix             string
	agtFlushInterval        string
	agtBufferMaxEvents      int
	agtBufferMaxBytes       string
	agtValidate             bool
	agtMaxReconnectAttempts int
)

func init() {
	agentCmd.Flags().StringVar(&agtAPIKey, "api-key", "", "API key for DBTrail authentication (required)")
	agentCmd.Flags().StringVar(&agtEndpoint, "endpoint", "", "DBTrail WebSocket endpoint URL (required)")
	agentCmd.Flags().StringVar(&agtIndexDSN, "index-dsn", "", "DSN for the index MySQL database")
	agentCmd.Flags().StringVar(&agtSourceDSN, "source-dsn", "", "DSN for the source MySQL database (enables forensics queries; required for BYOS streaming)")
	agentCmd.Flags().StringVar(&agtFlavor, "source-flavor", "mysql", "Source database flavor for BYOS streaming: mysql or mariadb (MariaDB source support is alpha)")
	agentCmd.Flags().StringVar(&agtArchiveDir, "archive-dir", "", "Local directory containing Parquet archives")
	agentCmd.Flags().StringVar(&agtArchiveS3, "archive-s3", "", "S3 path to Parquet archives (e.g. s3://bucket/prefix/)")
	agentCmd.Flags().StringVar(&agtBufferRetain, "buffer-retain", "6h", "How long to retain events in the in-memory buffer (e.g. 6h, 24h)")
	agentCmd.Flags().Uint32Var(&agtServerID, "server-id", 0, "MySQL server ID for replication, numeric uint32 (required for BYOS streaming). If you have a pre-registered UUID from the DBTrail dashboard, pass it to --server-uuid instead; this flag does NOT accept UUIDs and will reject them with strconv.ParseUint.")
	agentCmd.Flags().StringVar(&agtServerUUID, "server-uuid", "", "UUID of a pre-registered BYOS server (POST /api/v1/servers). When set, the SaaS reconciles this agent's WebSocket connection to that record; when empty, the SaaS auto-creates a new byos-<server-id> record (back-compat). A UUID that doesn't match a pre-registered server (typo, stale config, cross-tenant) is logged server-side as a WARNING with the UUID + tenant ID; the SaaS will NOT bind your agent to any record in that case. Verify in the dashboard that the expected pre-registered server is showing the connection.")
	agentCmd.Flags().IntVar(&agtBatchSize, "batch-size", 1000, "Number of events per batch flush")
	agentCmd.Flags().StringVar(&agtSchemas, "schemas", "", "Comma-separated list of schemas to index (empty = all)")
	agentCmd.Flags().StringVar(&agtTables, "tables", "", "Comma-separated list of tables to index (empty = all)")
	agentCmd.Flags().StringVar(&agtStartGTID, "start-gtid", "", "GTID set to start streaming from (first run only)")
	agentCmd.Flags().StringVar(&agtS3Bucket, "s3-bucket", "", "S3 bucket for BYOS payload storage")
	agentCmd.Flags().StringVar(&agtS3Region, "s3-region", "", "AWS region for the S3 bucket")
	agentCmd.Flags().StringVar(&agtS3Prefix, "s3-prefix", "bintrail/", "Key prefix within the S3 bucket")
	agentCmd.Flags().StringVar(&agtFlushInterval, "flush-interval", "5s", "Max time between metadata/payload flushes (e.g. 5s, 10s)")
	agentCmd.Flags().IntVar(&agtBufferMaxEvents, "buffer-max-events", 0, "Max events in the in-memory buffer (0 = unlimited)")
	agentCmd.Flags().StringVar(&agtBufferMaxBytes, "buffer-max-bytes", "0", "Max approximate buffer size, e.g. 256MB, 1GB (0 = unlimited)")
	agentCmd.Flags().BoolVar(&agtValidate, "validate", false, "Run pre-flight checks and exit without starting the agent")
	agentCmd.Flags().IntVar(&agtMaxReconnectAttempts, "max-reconnect-attempts", 10, "Exit (non-zero) after this many consecutive WebSocket reconnect failures so a process supervisor (e.g. systemd Restart=on-failure) can respawn the agent. The counter resets whenever a connection stays up longer than the heartbeat interval, so transient drops on a healthy long-running agent never trip the limit. Use 0 for unlimited retries.")
	_ = agentCmd.MarkFlagRequired("api-key")
	_ = agentCmd.MarkFlagRequired("endpoint")
	bindCommandEnv(agentCmd)

	rootCmd.AddCommand(agentCmd)
}

func runAgent(cmd *cobra.Command, args []string) error {
	flavor, err := normalizeAgentFlavor(agtFlavor)
	if err != nil {
		return err
	}
	agtFlavor = flavor

	if agtValidate {
		return runAgentValidate(cmd.Context())
	}
	// Reject negative --max-reconnect-attempts: the underlying ChannelConfig
	// helper coerces negatives to 0 (= unlimited), which would silently
	// re-enable the exact failure mode #191 fixes if a user typo'd `-1`
	// expecting "give up fast". Make zero an explicit, intentional opt-in.
	if agtMaxReconnectAttempts < 0 {
		return fmt.Errorf("invalid --max-reconnect-attempts %d: must be >= 0 (use 0 explicitly for unlimited retries)", agtMaxReconnectAttempts)
	}

	// Validate and canonicalize --server-uuid before any side effects.
	// Empty preserves the legacy auto-create-on-connect behavior
	// (back-compat). See #317. Canonicalization (lowercase, hyphenated,
	// no braces, no urn prefix) closes the silent-divergence footgun
	// where uppercase / braced / urn-form copy-paste sources would send
	// different header values for the same logical UUID. See #329.
	canonical, err := validateServerUUID(agtServerUUID)
	if err != nil {
		return err
	}
	agtServerUUID = canonical
	// Surface the effective UUID at startup so an env-file typo or accidental
	// re-export is visible in the log right next to the connect line. The
	// SaaS does not currently echo back the reconciled record, so this is the
	// only place an operator can confirm what the agent will send. See #317
	// silent-failure findings C2/C3.
	if agtServerUUID != "" {
		slog.Info("agent using --server-uuid", "server_uuid", agtServerUUID, "hint", "verify in DBTrail dashboard that the expected pre-registered server is showing this connection; a UUID mismatch will be logged server-side as a WARNING and the SaaS will not bind to any record")
	}

	start := time.Now()

	// Build archive sources list.
	var archiveSources []string
	if agtArchiveDir != "" {
		archiveSources = append(archiveSources, agtArchiveDir)
	}
	if agtArchiveS3 != "" {
		archiveSources = append(archiveSources, agtArchiveS3)
	}

	// Determine if BYOS streaming mode is requested.
	byosMode := agtSourceDSN != "" && agtServerID != 0

	// At least one data source must be configured.
	if agtIndexDSN == "" && len(archiveSources) == 0 && !byosMode {
		return fmt.Errorf("at least one data source required: --index-dsn, --archive-dir, --archive-s3, or BYOS mode (--source-dsn + --server-id)")
	}

	// BYOS mode requires a flush sink (S3 bucket) so events are durably
	// persisted. Without one, the in-memory buffer accumulates events
	// with no durable destination and drops everything on restart — the
	// agent looks healthy on the WebSocket channel while the SaaS sees
	// zero data. Refuse to start so the misconfiguration surfaces at
	// startup rather than as silent zero-data drift on the SaaS side.
	// See issue #289.
	if err := validateBYOSFlushConfig(byosMode, agtS3Bucket); err != nil {
		return err
	}

	handler := &agent.DefaultHandler{
		ArchiveSources: archiveSources,
		Logger:         slog.Default(),
	}

	// Connect to index database if provided.
	var indexDB *sql.DB
	if agtIndexDSN != "" {
		db, err := config.Connect(agtIndexDSN)
		if err != nil {
			return fmt.Errorf("connect to index database: %w", err)
		}
		defer db.Close()
		if err := indexer.EnsureSchema(db); err != nil {
			return fmt.Errorf("schema migration: %w", err)
		}
		indexDB = db
		handler.IndexDB = db
	}

	// Connect to source database if provided (for forensics queries + BYOS streaming).
	var sourceDB *sql.DB
	if agtSourceDSN != "" {
		db, err := config.Connect(agtSourceDSN)
		if err != nil {
			return fmt.Errorf("connect to source database: %w", err)
		}
		defer db.Close()
		sourceDB = db
		handler.SourceDB = db
		// Carry the resolved source host so extension-registered agent
		// commands receive it via ext.AgentDeps.
		if host, _, _, _, perr := config.ParseSourceDSN(agtSourceDSN); perr == nil {
			handler.SourceHost = host
		}
	}

	// Capture source server identity (architecture §22.11) for BYOS metadata
	// records. Independent of indexDB so it works in fully stateless BYOS.
	// Hard-fail on capture failure: silently emitting events with empty
	// server_uuid would degrade dbtrail's identity resolution to the legacy
	// NULL-bintrail_id path for the entire agent lifetime, with no operator
	// signal until queries return wrong results.
	var sourceIdent byos.SourceIdentity
	if sourceDB != nil {
		ident, err := byos.LoadSourceIdentity(cmd.Context(), sourceDB, agtSourceDSN)
		if err != nil {
			return fmt.Errorf("capture source identity for BYOS metadata: %w", err)
		}
		sourceIdent = ident
	}

	// Resolve bintrail_id — the stable server identifier used for metadata
	// records and WebSocket heartbeats. Requires both source and index DBs.
	var bintrailID string
	if sourceDB != nil && indexDB != nil {
		id, err := byos.ResolveServerIdentity(cmd.Context(), sourceDB, indexDB, agtSourceDSN)
		if err != nil {
			if errors.Is(err, serverid.ErrConflict) {
				return fmt.Errorf("cannot start agent: %w", err)
			}
			// In BYOS+S3 mode, falling back to numeric server-id would
			// create S3 partitions that cannot be correlated with future
			// runs that resolve a proper bintrail_id.
			if byosMode && agtS3Bucket != "" {
				return fmt.Errorf("server identity resolution required for BYOS with S3: %w", err)
			}
			slog.Warn("server identity resolution failed; proceeding without bintrail_id", "error", err)
		} else {
			bintrailID = id
			slog.Info("server identity resolved", "bintrail_id", bintrailID)
		}
	} else if byosMode {
		// BYOS without --index-dsn: the SaaS now resolves a stable
		// bintrail_id server-side from the @@server_uuid + host/port/user
		// fields that `byos.LoadSourceIdentity` captured above and that
		// `SplitEvent` stamps on every metadata record (architecture §22.11,
		// implemented in nethalo/dbtrail#1179). The customer agent no
		// longer needs a local index DB — the SaaS bt_<prefix>.bintrail_servers
		// table is the canonical identity record for both hosted and BYOS.
		//
		// The local bintrailID stays empty in this path. Downstream call
		// sites that previously consumed it (the S3 partition key at
		// `NewPayloadWriter`, the WebSocket heartbeat label at
		// `ChannelConfig.BintrailID`) now fall back to the numeric
		// --server-id via `cmp.Or(bintrailID, fmt.Sprint(agtServerID))`.
		// These are customer-local identifiers, intentionally decoupled
		// from the SaaS-resolved bintrail_id: the SaaS correlates the
		// connection by API key + the source identity on metadata
		// records, not by the heartbeat label.
		slog.Info("BYOS without --index-dsn; SaaS will resolve bintrail_id via source identity propagation",
			"server_uuid", sourceIdent.ServerUUID,
			"local_server_id", fmt.Sprint(agtServerID))

		// Log the chosen S3 partition key so it shows in the startup banner
		// regardless of whether the marker check that follows passes.
		// EnsurePartitionKey refuses to start the agent if a marker already
		// exists under a different server_id (issue #198), so contrary to
		// what an older CLI version would do, prior objects cannot be
		// silently orphaned here — the agent will fail fast and surface a
		// migration message instead.
		if agtS3Bucket != "" {
			slog.Info("BYOS+S3 without --index-dsn: S3 partition key set to numeric --server-id",
				"partition_key", fmt.Sprint(agtServerID))
		}
	}

	// BYOS streaming: start buffer + streaming goroutine.
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	// See runStream: a long-lived process needs its own drain, off the
	// heartbeat and reconnect paths.
	go tel.Client().RunDaemon(ctx, cmd.Name())

	var flushState *flushPipelineState

	if byosMode {
		retain, err := cliutil.ParseRetain(agtBufferRetain)
		if err != nil {
			return fmt.Errorf("invalid --buffer-retain: %w", err)
		}
		if agtBufferMaxEvents < 0 {
			return fmt.Errorf("invalid --buffer-max-events %d: must be >= 0", agtBufferMaxEvents)
		}
		maxBytes, err := cliutil.ParseByteSize(agtBufferMaxBytes)
		if err != nil {
			return fmt.Errorf("invalid --buffer-max-bytes: %w", err)
		}

		buf := buffer.New(buffer.Config{
			MaxAge:    retain,
			MaxEvents: agtBufferMaxEvents,
			MaxBytes:  maxBytes,
			Logger:    slog.Default(),
		})
		handler.Buffer = buf

		// Use bintrail_id as the server identifier when available, falling
		// back to the numeric @@server_id.
		serverIDStr := cmp.Or(bintrailID, fmt.Sprint(agtServerID))

		slog.Info("BYOS mode enabled",
			"source_dsn", maskDSN(agtSourceDSN),
			"server_id", serverIDStr,
			"buffer_retain", retain.String(),
			"buffer_max_events", agtBufferMaxEvents,
			"buffer_max_bytes", agtBufferMaxBytes)

		// Initialize flush sinks if S3 bucket is configured.
		var metaClient *byos.MetadataClient
		var payloadWriter *byos.PayloadWriter

		if agtS3Bucket != "" {
			// Pass the canonicalized --server-uuid so the metadata client sets
			// X-Bintrail-Server-UUID on every /v1/events POST, mirroring the
			// WS-dial reconcile path (issue #317). Empty preserves legacy
			// behavior. See dbtrail/bintrail#341 + dbtrail/dbtrail#1495.
			metaClient = byos.NewMetadataClient(wsEndpointToHTTP(agtEndpoint), agtAPIKey, agtServerUUID)

			s3Backend, err := storage.NewS3Backend(ctx, storage.S3Config{
				Bucket: agtS3Bucket,
				Region: agtS3Region,
				Prefix: agtS3Prefix,
			})
			if err != nil {
				return fmt.Errorf("initialize S3 backend: %w", err)
			}

			// Detect the silent partition-key cutover described in #198:
			// upgrading from BYOS+S3+--index-dsn (UUID partition key) to
			// BYOS+S3 without --index-dsn (numeric partition key) would
			// split objects across two prefixes with no operator signal.
			// The marker file is written on first run and validated on
			// every subsequent run; a mismatch hard-fails with guidance.
			if err := byos.EnsurePartitionKey(ctx, s3Backend, serverIDStr); err != nil {
				return err
			}

			payloadWriter = byos.NewPayloadWriter(s3Backend, serverIDStr)

			slog.Info("BYOS flush pipeline initialized",
				"s3_bucket", agtS3Bucket,
				"s3_region", agtS3Region,
				"s3_prefix", agtS3Prefix)
		}

		flushInterval, err := time.ParseDuration(agtFlushInterval)
		if err != nil {
			return fmt.Errorf("invalid --flush-interval: %w", err)
		}

		flushState = &flushPipelineState{
			metadataStatus: "ok",
			payloadStatus:  "ok",
		}

		identPtr := &atomic.Pointer[byos.SourceIdentity]{}
		identPtr.Store(&sourceIdent)

		// Extension source jobs (ext.RegisterSourceJob) run alongside the BYOS
		// stream, same contract as under `stream`/`up`: daemon-scoped
		// secondary work, never fatal to capture, no-op in the stock binary.
		// Only in BYOS mode — without --source-dsn there is no live source to
		// observe — and only with an index, since a source job's persistence
		// target is the index database (stateless BYOS keeps nothing local).
		if src, ok := agentSourceJobInfo(); ok {
			ext.RunSourceJobs(ctx, src)
		}

		streamErrCh := make(chan error, 1)
		go func() {
			streamErrCh <- runBYOSStream(ctx, handler.SourceDB, buf, &byosFlushConfig{
				metaClient:    metaClient,
				payloadWriter: payloadWriter,
				serverID:      serverIDStr,
				sourceIdent:   identPtr,
				sourceDB:      handler.SourceDB,
				flushInterval: flushInterval,
				state:         flushState,
			})
		}()

		// Wait briefly for fast setup failures (bad credentials, wrong
		// binlog_format, etc.) before starting the agent channel. If the
		// stream survives setup, monitor for runtime failures in background.
		select {
		case err := <-streamErrCh:
			if err != nil {
				return fmt.Errorf("BYOS stream failed: %w", err)
			}
		case <-time.After(3 * time.Second):
			// Stream survived setup — monitor for runtime failures.
			go func() {
				if err := <-streamErrCh; err != nil && ctx.Err() == nil {
					slog.Error("BYOS stream stopped unexpectedly", "error", err)
					cancel()
				}
			}()
		}
	}

	cfg := agent.ChannelConfig{
		Endpoint: agtEndpoint,
		APIKey:   agtAPIKey,
		Version:  Version,
		// Heartbeat label — fall back to the numeric --server-id when
		// bintrailID is empty (BYOS without --index-dsn path, see above).
		// The SaaS keys connections by APIKey, not by this field, so an
		// empty string would technically work but degrades dashboard
		// display. cmp.Or keeps the label stable and operator-recognizable.
		BintrailID:           cmp.Or(bintrailID, fmt.Sprint(agtServerID)),
		ServerUUID:           agtServerUUID,
		MaxReconnectAttempts: agtMaxReconnectAttempts,
	}

	var statusFn func() *agent.FlushStatus
	if flushState != nil {
		statusFn = flushState.toFlushStatus
	}
	ch := agent.NewChannel(cfg, handler, nil, statusFn)
	// Connection handles for extension-registered commands
	// (ext.RegisterAgentCommand), built once here and passed by the dispatch
	// loop to every registry handler. Fields are nil/empty for whatever this
	// agent wasn't configured with — handlers nil-check.
	ch.ExtDeps = ext.AgentDeps{
		IndexDB:    indexDB,
		SourceDB:   sourceDB,
		SourceDSN:  agtSourceDSN,
		SourceHost: handler.SourceHost,
	}

	slog.Info("starting agent",
		"endpoint", agtEndpoint,
		"has_index", agtIndexDSN != "",
		"has_source", agtSourceDSN != "",
		"has_buffer", byosMode,
		"archives", len(archiveSources))

	err = ch.Run(ctx)

	slog.Info("agent stopped",
		"duration", time.Since(start).Truncate(time.Second).String(),
		"error", err)

	// Return the error — if it's a *agent.FatalCloseError, main() will
	// map it to a distinct process exit code (64/65) AFTER all deferred
	// cleanup in this function runs (buffer flush, S3 writers, source
	// DB close, etc.). Calling os.Exit here would skip those defers.
	// See issue #201.
	return err
}

// wsEndpointToHTTP converts a WebSocket agent endpoint URL to an HTTP base
// URL suitable for the metadata API. For example:
//
//	"wss://api.dbtrail.io/v1/agent" → "https://api.dbtrail.io"
//	"ws://localhost:8080/v1/agent"  → "http://localhost:8080"
func wsEndpointToHTTP(endpoint string) string {
	// Convert scheme.
	s := strings.Replace(endpoint, "wss://", "https://", 1)
	s = strings.Replace(s, "ws://", "http://", 1)
	// Strip the path — MetadataClient appends /v1/events itself.
	if i := strings.Index(s, "://"); i != -1 {
		rest := s[i+3:]
		if j := strings.Index(rest, "/"); j != -1 {
			s = s[:i+3+j]
		}
	}
	return s
}

// maskDSN redacts the password from a DSN for logging.
func maskDSN(dsn string) string {
	for i := range dsn {
		if dsn[i] == ':' {
			for j := i + 1; j < len(dsn); j++ {
				if dsn[j] == '@' {
					return dsn[:i+1] + "***" + dsn[j:]
				}
			}
		}
	}
	return dsn
}

// ─── BYOS flush pipeline state ─────────────────────────────────────────────

// byosFlushConfig holds the sinks and settings for the BYOS flush pipeline.
// All fields are optional — when metaClient and payloadWriter are nil,
// the stream loop runs in buffer-only mode (hosted mode).
//
// sourceIdent is an atomic pointer so the per-flush load and the periodic
// re-capture in byosStreamLoop never observe a torn SourceIdentity value.
// Today both operations share the stream-loop goroutine, but the atomic
// keeps the invariant intact if flushing moves to its own goroutine later.
type byosFlushConfig struct {
	metaClient       *byos.MetadataClient
	payloadWriter    *byos.PayloadWriter
	serverID         string
	sourceIdent      *atomic.Pointer[byos.SourceIdentity]
	sourceDB         *sql.DB       // for periodic @@server_uuid re-capture
	identityInterval time.Duration // re-identity cadence; 0 => default 60s
	flushInterval    time.Duration
	state            *flushPipelineState
}

// flushPipelineState tracks the health of metadata/payload flushes.
// Written by byosStreamLoop, read by the heartbeat's StatusProvider.
type flushPipelineState struct {
	mu                sync.Mutex
	bufferEvents      int
	bufferBytes       int64
	sizeEvictions     int64
	metadataStatus    string // "ok" or "degraded"
	payloadStatus     string // "ok" or "degraded"
	lastMetadataFlush *time.Time
	lastPayloadFlush  *time.Time

	// Cumulative counts of events/batches permanently dropped after the
	// in-process retries were exhausted. There is no on-disk spool yet (see
	// the follow-up), so a batch that fails all retries is truncated and lost;
	// these counters are the only durable, monotonic signal that this
	// happened. The metadataStatus/payloadStatus bools flip back to "ok" on
	// the next successful flush and erase the memory of the outage — the
	// counters never reset. Metadata and payload are tracked separately
	// because the two sinks fail independently: a nonzero skew between them
	// means the hosted index and the client bucket now disagree about which
	// row images exist.
	metadataLostEvents  int64
	metadataLostBatches int64
	payloadLostEvents   int64
	payloadLostBatches  int64
}

// flushLossTotals is a snapshot of the cumulative lost-event/lost-batch
// counters, returned by updateFlush so the caller can log the running total
// without re-acquiring the lock.
type flushLossTotals struct {
	metadataLostEvents  int64
	metadataLostBatches int64
	payloadLostEvents   int64
	payloadLostBatches  int64
}

// updateFlush records the outcome of a sink flush. metaEvents/payloadEvents
// are the batch sizes that were attempted; when a sink fails they are added to
// its cumulative lost-event counter (the batch is dropped without a spool). It
// returns a snapshot of the cumulative totals so the caller can log the
// running total on a drop.
func (s *flushPipelineState) updateFlush(metaOK, payloadOK bool, metaEvents, payloadEvents int) flushLossTotals {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if metaOK {
		s.metadataStatus = "ok"
		s.lastMetadataFlush = &now
	} else {
		s.metadataStatus = "degraded"
		s.metadataLostEvents += int64(metaEvents)
		s.metadataLostBatches++
	}
	if payloadOK {
		s.payloadStatus = "ok"
		s.lastPayloadFlush = &now
	} else {
		s.payloadStatus = "degraded"
		s.payloadLostEvents += int64(payloadEvents)
		s.payloadLostBatches++
	}
	return flushLossTotals{
		metadataLostEvents:  s.metadataLostEvents,
		metadataLostBatches: s.metadataLostBatches,
		payloadLostEvents:   s.payloadLostEvents,
		payloadLostBatches:  s.payloadLostBatches,
	}
}

func (s *flushPipelineState) setBufferStats(events int, bytes int64, evictions int64) {
	s.mu.Lock()
	s.bufferEvents = events
	s.bufferBytes = bytes
	s.sizeEvictions = evictions
	s.mu.Unlock()
}

func (s *flushPipelineState) toFlushStatus() *agent.FlushStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.bufferEvents
	b := s.bufferBytes
	e := s.sizeEvictions
	return &agent.FlushStatus{
		BufferEvents:        &n,
		BufferBytes:         &b,
		SizeEvictions:       &e,
		MetadataStatus:      s.metadataStatus,
		PayloadStatus:       s.payloadStatus,
		LastMetadataFlush:   s.lastMetadataFlush,
		LastPayloadFlush:    s.lastPayloadFlush,
		MetadataLostEvents:  s.metadataLostEvents,
		MetadataLostBatches: s.metadataLostBatches,
		PayloadLostEvents:   s.payloadLostEvents,
		PayloadLostBatches:  s.payloadLostBatches,
	}
}

// agentSourceJobInfo describes the agent's capture source for the extension
// source-job seam, and reports whether jobs should run at all. Extracted from
// runAgent so the decision is unit-testable without a daemon (mirrors
// streamSourceJobInfo).
//
// Both DSNs are required: --source-dsn is the live server a job observes, and
// the index is the only place a job can persist what it observes — a stateless
// BYOS agent (no --index-dsn) has neither a local destination nor a schema to
// write into. Flavor is the agent's --source-flavor (normalized at the top of
// runAgent), so a flavor-gated source job sees the flavor the BYOS stream
// actually runs with. The cmp.Or default only matters when the globals are set
// without going through cobra (tests): a job must never be told an empty
// flavor, which a flavor-gated job would silently skip on.
func agentSourceJobInfo() (ext.SourceJobInfo, bool) {
	if agtSourceDSN == "" || agtIndexDSN == "" {
		return ext.SourceJobInfo{}, false
	}
	return ext.SourceJobInfo{
		SourceDSN: agtSourceDSN,
		IndexDSN:  agtIndexDSN,
		Flavor:    cmp.Or(agtFlavor, gomysql.MySQLFlavor),
	}, true
}

// normalizeAgentFlavor validates --source-flavor and maps the empty string to
// the default. Mirrors internal/streamrun's normalizeFlavor (same accepted
// literals, same defaulting) so the agent and `bintrail stream` reject and
// accept identically. postgres is deliberately not accepted: the BYOS stream
// is a binlog reader.
func normalizeAgentFlavor(flavor string) (string, error) {
	switch flavor {
	case "":
		return gomysql.MySQLFlavor, nil
	case gomysql.MySQLFlavor, gomysql.MariaDBFlavor:
		return flavor, nil
	default:
		return "", fmt.Errorf("invalid --source-flavor %q: must be %q or %q", flavor, gomysql.MySQLFlavor, gomysql.MariaDBFlavor)
	}
}

// ─── BYOS streaming ────────────────────────────────────────────────────────

// runBYOSStream reads binlogs from the source MySQL (or MariaDB, under
// --source-flavor mariadb) and writes events to the in-memory buffer, and
// optionally flushes metadata/payload to sinks.
func runBYOSStream(ctx context.Context, sourceDB *sql.DB, buf *buffer.Buffer, fc *byosFlushConfig) error {
	// Validate binlog settings.
	if err := metadata.ValidateBinlogFormat(sourceDB); err != nil {
		return err
	}
	if err := metadata.ValidateBinlogRowImage(sourceDB); err != nil {
		return err
	}

	slog.Info("BYOS stream: binlog_format=ROW, binlog_row_image=FULL validated")

	// Build schema resolver from the source DB's information_schema.
	// The resolver maps column indices to names so the parser can produce
	// named column maps (RowBefore, RowAfter) and identify PK columns.
	resolver, err := buildResolverFromSource(sourceDB, cliutil.ParseSchemaList(agtSchemas))
	if err != nil {
		return fmt.Errorf("build schema resolver: %w", err)
	}
	slog.Info("BYOS schema resolver built", "tables", resolver.TableCount())

	// Build filters.
	filters := cliutil.BuildIndexFilters(agtSchemas, agtTables)

	sp := parser.NewStreamParser(resolver, filters, nil)

	// Parse source DSN for BinlogSyncer.
	host, port, user, password, err := config.ParseSourceDSN(agtSourceDSN)
	if err != nil {
		return err
	}

	syncer := replication.NewBinlogSyncer(byosSyncerConfig(agtServerID, agtFlavor, host, port, user, password))
	defer syncer.Close()

	// Determine start position.
	streamer, err := startBYOSSyncer(sourceDB, syncer, agtFlavor, agtStartGTID)
	if err != nil {
		return err
	}

	slog.Info("BYOS stream started", "start_gtid", agtStartGTID)

	// Run event loop.
	events := make(chan parser.Event, 1000)
	parseErrCh := make(chan error, 1)

	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	err = byosStreamLoop(ctx, events, buf, agtBatchSize, fc)

	parseErr := <-parseErrCh
	if parseErr != nil && ctx.Err() == nil {
		return fmt.Errorf("parser error: %w", parseErr)
	}
	return err
}

// byosSyncerConfig builds the BYOS binlog syncer's config. Pure — extracted
// from runBYOSStream so the flavor fan-out is unit-testable without a live
// source (mirrors consoleapp's sourceStreamConfig extraction).
func byosSyncerConfig(serverID uint32, flavor, host string, port uint16, user, password string) replication.BinlogSyncerConfig {
	cfg := replication.BinlogSyncerConfig{
		ServerID:                serverID,
		Flavor:                  flavor,
		Host:                    host,
		Port:                    port,
		User:                    user,
		Password:                password,
		HeartbeatPeriod:         30 * time.Second,
		MaxReconnectAttempts:    0,
		TimestampStringLocation: time.UTC, // see internal/parser/parser.go (#757)
	}
	if flavor == gomysql.MariaDBFlavor {
		// Ask the MariaDB source to send ANNOTATE_ROWS events — MariaDB only
		// forwards them to a replica that set this dump flag (#699) — and
		// compensate the MariaDB 11.4+ zero-LogPos events (#1117). Both
		// mirror the streamrun syncer's MariaDB branch; see the full
		// rationale there.
		cfg.DumpCommandFlag |= replication.BINLOG_SEND_ANNOTATE_ROWS_EVENT
		cfg.FillZeroLogPos = true
	}
	return cfg
}

// startBYOSSyncer starts the binlog syncer from the given GTID set or
// from the server's current binlog position.
func startBYOSSyncer(sourceDB *sql.DB, syncer *replication.BinlogSyncer, flavor, startGTID string) (*replication.BinlogStreamer, error) {
	if startGTID != "" {
		gset, err := parseBYOSStartGTID(flavor, startGTID)
		if err != nil {
			return nil, err
		}
		s, err := syncer.StartSyncGTID(gset)
		return s, parser.WrapReplicationError(err)
	}
	// No start GTID — query current position from source and start there.
	file, pos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		return nil, err
	}
	slog.Info("starting from current binlog position", "file", file, "pos", pos)
	s, err := syncer.StartSync(gomysql.Position{Name: file, Pos: pos})
	return s, parser.WrapReplicationError(err)
}

// parseBYOSStartGTID parses --start-gtid with the parser for the configured
// source flavor. A MariaDB GTID set ("0-2-71") is not parseable as a MySQL
// set, so the hardwired "mysql" literal this replaces made --start-gtid
// unusable against a MariaDB source.
func parseBYOSStartGTID(flavor, startGTID string) (gomysql.GTIDSet, error) {
	gset, err := gomysql.ParseGTIDSet(flavor, startGTID)
	if err != nil {
		return nil, fmt.Errorf("parse start GTID set: %w", err)
	}
	return gset, nil
}

// byosStreamLoop reads events from the parser channel and writes them to
// the buffer. When flush sinks are configured (fc.metaClient / fc.payloadWriter),
// it also splits events and flushes metadata to dbtrail and payload to S3.
func byosStreamLoop(ctx context.Context, events <-chan parser.Event, buf *buffer.Buffer, batchSize int, fc *byosFlushConfig) error {
	if batchSize <= 0 {
		batchSize = 1000
	}

	batch := make([]parser.Event, 0, batchSize)
	evictTicker := time.NewTicker(5 * time.Minute)
	defer evictTicker.Stop()

	flushInterval := 5 * time.Second
	if fc != nil && fc.flushInterval > 0 {
		flushInterval = fc.flushInterval
	}
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	// Periodic source-identity re-capture (issue #196).
	identityInterval := 60 * time.Second
	if fc != nil && fc.identityInterval > 0 {
		identityInterval = fc.identityInterval
	}
	var identityTickerC <-chan time.Time
	if fc != nil && fc.sourceDB != nil && fc.sourceIdent != nil {
		t := time.NewTicker(identityInterval)
		defer t.Stop()
		identityTickerC = t.C
	}
	identityFailures := 0

	// flushBatch drains the pending batch to the in-memory buffer and, if
	// sinks are configured, to the metadata/payload destinations.
	//
	// Returns an error only for programmer errors detected during the sink
	// flush (nil sourceIdent pointer). Transient sink failures are logged
	// inside flushToSinks and surfaced via flushPipelineState.
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		buf.Insert(batch)
		slog.Debug("BYOS batch flushed to buffer", "events", len(batch), "buffer_size", buf.Len())

		// Split and flush to sinks if configured. Skip on shutdown —
		// the sinks would fail immediately with context.Canceled and
		// produce misleading error logs.
		var flushErr error
		if fc != nil && (fc.metaClient != nil || fc.payloadWriter != nil) && ctx.Err() == nil {
			flushErr = flushToSinks(ctx, batch, fc)
		}
		if fc != nil && fc.state != nil {
			fc.state.setBufferStats(buf.Len(), buf.ApproxBytes(), buf.SizeEvictions())
		}

		batch = batch[:0]
		return flushErr
	}

	for {
		select {
		case <-ctx.Done():
			_ = flushBatch()
			return nil

		case <-evictTicker.C:
			n := buf.Evict()
			if n > 0 {
				slog.Info("BYOS buffer eviction", "evicted", n, "remaining", buf.Len())
			}
			if fc != nil && fc.state != nil {
				fc.state.setBufferStats(buf.Len(), buf.ApproxBytes(), buf.SizeEvictions())
			}

		case <-flushTicker.C:
			if err := flushBatch(); err != nil {
				return err
			}

		case <-identityTickerC:
			if err := checkSourceIdentity(ctx, fc.sourceDB, fc.sourceIdent, &identityFailures); err != nil {
				_ = flushBatch()
				return err
			}

		case ev, ok := <-events:
			if !ok {
				_ = flushBatch()
				return nil
			}

			// Skip non-row events (GTID tracking, DDL, commit boundaries).
			if ev.EventType == parser.EventGTID || ev.EventType == parser.EventDDL || ev.EventType == parser.EventCommit {
				continue
			}

			batch = append(batch, ev)
			if len(batch) >= batchSize {
				if err := flushBatch(); err != nil {
					return err
				}
			}
		}
	}
}

// flushToSinks splits events via byos.SplitEvent and sends metadata to
// dbtrail and payload to S3. Retries each sink up to 3 times with
// exponential backoff. Transient sink failures are logged but never block
// the stream. Returns an error only for programmer-error conditions (nil
// sourceIdent pointer) so the caller can tear down the stream rather than
// run forever with a degraded heartbeat — consistent with how
// checkSourceIdentity handles its "BUG" case.
func flushToSinks(ctx context.Context, batch []parser.Event, fc *byosFlushConfig) error {
	if fc.sourceIdent == nil {
		return fmt.Errorf("BUG: sourceIdent pointer is nil; refusing to emit metadata with empty server_uuid (batch_size=%d)", len(batch))
	}
	p := fc.sourceIdent.Load()
	if p == nil {
		return fmt.Errorf("BUG: sourceIdent pointer not initialized; refusing to emit metadata with empty server_uuid (batch_size=%d)", len(batch))
	}
	ident := *p

	var metaBatch []byos.MetadataRecord
	var payloadBatch []byos.PayloadRecord

	for i := range batch {
		meta, payload, err := byos.SplitEvent(batch[i], fc.serverID, ident)
		if err != nil {
			slog.Warn("BYOS split failed, skipping event",
				"error", err,
				"schema", batch[i].Schema,
				"table", batch[i].Table,
				"event_type", batch[i].EventType)
			continue
		}
		metaBatch = append(metaBatch, meta)
		payloadBatch = append(payloadBatch, payload)
	}

	if len(metaBatch) == 0 {
		return nil
	}

	metaOK := true
	payloadOK := true

	// Flush metadata to dbtrail API.
	if fc.metaClient != nil {
		if err := retryFlush(ctx, 3, func() error {
			return fc.metaClient.Send(ctx, metaBatch)
		}); err != nil {
			slog.Error("BYOS metadata flush failed after retries",
				"events", len(metaBatch), "error", err)
			metaOK = false
		}
	}

	// Flush payload to customer S3.
	if fc.payloadWriter != nil {
		if err := retryFlush(ctx, 3, func() error {
			return fc.payloadWriter.WriteRecords(ctx, payloadBatch)
		}); err != nil {
			slog.Error("BYOS payload flush failed after retries",
				"events", len(payloadBatch), "error", err)
			payloadOK = false
		}
	}

	if fc.state != nil {
		totals := fc.state.updateFlush(metaOK, payloadOK, len(metaBatch), len(payloadBatch))
		if !metaOK || !payloadOK {
			// A batch failed all retries and was dropped without a spool —
			// permanent, irrecoverable loss. Escalate above the per-sink
			// errors above with the running cumulative totals so the loss is
			// an observable, monotonic signal rather than a single lost line.
			slog.Error("BYOS batch dropped after retries — events permanently lost (no on-disk spool)",
				"metadata_dropped", !metaOK,
				"payload_dropped", !payloadOK,
				"batch_events", len(metaBatch),
				"cumulative_metadata_lost_events", totals.metadataLostEvents,
				"cumulative_metadata_lost_batches", totals.metadataLostBatches,
				"cumulative_payload_lost_events", totals.payloadLostEvents,
				"cumulative_payload_lost_batches", totals.payloadLostBatches,
				"sink_skew_events", totals.metadataLostEvents-totals.payloadLostEvents)
		}
	}
	return nil
}

// maxIdentityFailures is the number of consecutive failed re-captures that
// will be tolerated before tearing down the stream. At the default 60s
// cadence this is ~10 minutes — long enough to ride out a network blip
// but short enough to surface permanent failures (revoked credentials,
// dropped user) instead of logging WARN forever.
const maxIdentityFailures = 10

// checkSourceIdentity re-reads @@server_uuid from sourceDB and compares it to
// the identity currently stored in prev.
//
// Host/port/user are not re-derived here: they come from the startup DSN
// flag and cannot change without an agent restart, so parsing the DSN on
// every tick would only hide real DSN-misconfiguration errors as transient.
//
// Return values:
//   - UUID unchanged: nil, failures counter reset.
//   - UUID changed:   error naming both UUIDs (operator intervention required).
//   - DB query fails: nil for the first maxIdentityFailures consecutive calls
//     (logged as warning); after that, returns the error so the stream tears
//     down rather than running forever with a possibly-stale identity.
//   - context.Canceled: nil, no log (the loop is shutting down).
//
// The failure counter tracks STRICT consecutive failures — a single successful
// query resets it. A source that flaps (succeeds at least once per
// maxIdentityFailures ticks) will log repeated warnings but never trip the
// threshold. Permanent failures (revoked creds, dropped user) still do.
// Rolling-window detection is out of scope; if flapping becomes a real
// failure mode, revisit this function.
func checkSourceIdentity(ctx context.Context, sourceDB *sql.DB, prev *atomic.Pointer[byos.SourceIdentity], failures *int) error {
	var uuid string
	err := sourceDB.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		*failures++
		slog.Warn("BYOS source identity re-capture failed",
			"error", err, "consecutive_failures", *failures, "max", maxIdentityFailures)
		if *failures >= maxIdentityFailures {
			return fmt.Errorf(
				"source identity re-capture failed %d consecutive times: %w. "+
					"This usually indicates revoked credentials, a dropped user, or a "+
					"permanently unreachable source. The agent cannot confirm identity "+
					"stability and is aborting rather than silently stamping metadata "+
					"with a possibly-stale @@server_uuid",
				*failures, err)
		}
		return nil
	}
	*failures = 0

	current := prev.Load()
	if current == nil {
		// Programmer error: the pointer must be seeded before the stream
		// goroutine dispatches. Tear down rather than silently stamping
		// metadata with an empty server_uuid.
		return fmt.Errorf("BUG: sourceIdent pointer was not initialized at agent startup")
	}
	if uuid != current.ServerUUID {
		return fmt.Errorf(
			"source server identity changed: @@server_uuid was %s, now %s. "+
				"The source MySQL was restarted with a regenerated auto.cnf, failed "+
				"over behind a VIP, or the DSN host now resolves to a different "+
				"instance. Continuing would silently misattribute events to the prior "+
				"server's bintrail_id. Verify the source and restart the agent",
			current.ServerUUID, uuid,
		)
	}
	return nil
}

// retryFlush retries fn up to maxAttempts times with exponential backoff
// (1s, 2s, 4s). Returns the last error on persistent failure.
func retryFlush(ctx context.Context, maxAttempts int, fn func() error) error {
	var err error
	delay := time.Second
	for attempt := range maxAttempts {
		err = fn()
		if err == nil {
			return nil
		}
		if attempt == maxAttempts-1 {
			break
		}
		slog.Warn("BYOS flush attempt failed, retrying",
			"attempt", attempt+1, "delay", delay.String(), "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	return err
}

// ─── Schema resolver ────────────────────────────────────────────────────────

// buildResolverFromSource queries information_schema.COLUMNS on the source
// MySQL and builds an in-memory Resolver. This avoids requiring a MySQL index
// database for schema snapshots in BYOS mode.
func buildResolverFromSource(sourceDB *sql.DB, schemas []string) (*metadata.Resolver, error) {
	var q string
	var args []any

	if len(schemas) == 0 {
		q = `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
		            ORDINAL_POSITION, COLUMN_KEY, DATA_TYPE, EXTRA,
		            COALESCE(CHARACTER_SET_NAME, '')
		     FROM information_schema.COLUMNS
		     WHERE TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')
		     ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		q = fmt.Sprintf(`SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
		                         ORDINAL_POSITION, COLUMN_KEY, DATA_TYPE, EXTRA,
		                         COALESCE(CHARACTER_SET_NAME, '')
		                  FROM information_schema.COLUMNS
		                  WHERE TABLE_SCHEMA IN (%s)
		                  ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`, placeholders)
		for _, s := range schemas {
			args = append(args, s)
		}
	}

	rows, err := sourceDB.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("query information_schema.COLUMNS: %w", err)
	}
	defer rows.Close()

	tables := make(map[string]*metadata.TableMeta)
	for rows.Next() {
		var schema, table, column, colKey, dataType, extra, characterSet string
		var ordinal int
		if err := rows.Scan(&schema, &table, &column, &ordinal, &colKey, &dataType, &extra, &characterSet); err != nil {
			return nil, fmt.Errorf("scan column row: %w", err)
		}

		key := schema + "." + table
		tm, ok := tables[key]
		if !ok {
			tm = &metadata.TableMeta{Schema: schema, Table: table}
			tables[key] = tm
		}

		isGenerated := strings.Contains(extra, "STORED GENERATED") || strings.Contains(extra, "VIRTUAL GENERATED")
		isPK := colKey == "PRI"

		tm.Columns = append(tm.Columns, metadata.ColumnMeta{
			Name:            column,
			OrdinalPosition: ordinal,
			IsPK:            isPK,
			DataType:        dataType,
			IsGenerated:     isGenerated,
			// CharacterSet (#756) lets metadata.MapRow transcode an
			// invalid-UTF-8 latin1 CHAR/VARCHAR value instead of failing loud
			// on it — without this, every BYOS/agent-captured row from a
			// legacy latin1 table would be silently dropped (warn-and-skip)
			// even though the same table snapshotted via `bintrail snapshot`
			// would transcode successfully.
			CharacterSet: characterSet,
		})
		if isPK {
			tm.PKColumns = append(tm.PKColumns, column)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns: %w", err)
	}

	// #1272: implicitly system-versioned MariaDB tables hide row_start/row_end
	// from information_schema.COLUMNS while their row images carry them — the
	// column-count-mismatch guard would silently skip every event of the
	// table. Same synthesis `bintrail snapshot` applies; no-op on MySQL.
	if err := metadata.AddImplicitPeriodColumns(sourceDB, schemas, tables); err != nil {
		return nil, fmt.Errorf("detect system-versioned tables: %w", err)
	}

	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables found; check --schemas and source server permissions")
	}

	// Ensure columns are sorted by ordinal position (they should be from
	// ORDER BY, but be defensive).
	for _, tm := range tables {
		slices.SortFunc(tm.Columns, func(a, b metadata.ColumnMeta) int {
			return cmp.Compare(a.OrdinalPosition, b.OrdinalPosition)
		})
	}

	// The resolver reflects the LIVE schema read moments ago, so its
	// creation time is exactly now. Without it the #700 drift guard runs in
	// zero-time strict mode: an agent catching up through backlog written
	// before a column rename would hard-error on every restart, with a
	// remediation (`bintrail snapshot`) that does nothing for this
	// resolver — it never reads schema_snapshots.
	return metadata.NewResolverFromTablesAt(0, time.Now().UTC(), tables), nil
}

// validateServerUUID enforces that --server-uuid (when supplied) is a
// well-formed UUID and returns its canonical (lowercase, hyphenated, no
// braces, no urn prefix) form. An empty string is allowed and preserves
// the legacy auto-create-on-connect SaaS behavior (back-compat); a
// malformed value is rejected up-front so the operator gets a clear error
// instead of a silent mis-association on the SaaS side. See issue #317.
//
// Canonicalization closes the silent-divergence footgun documented in
// issue #329: uuid.Parse accepts uppercase, mixed-case, braced {…}, and
// urn:uuid:… forms, so two operators registering the same logical server
// from different copy-paste sources would otherwise send divergent
// X-Bintrail-Server-UUID header values. The SaaS should also normalize
// on receipt as defense-in-depth, but normalizing in the agent — closer
// to the operator's input — is the right primary fix.
func validateServerUUID(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	parsed, err := uuid.Parse(s)
	if err != nil {
		return "", fmt.Errorf("invalid --server-uuid %q: must be a valid UUID (e.g. 550e8400-e29b-41d4-a716-446655440000) or empty for back-compat: %w", s, err)
	}
	return parsed.String(), nil
}

// validateBYOSFlushConfig enforces that a BYOS-mode agent has a configured
// flush sink. Returns an error if BYOS mode is enabled but s3Bucket is empty.
//
// Rationale (issue #289): when --source-dsn and --server-id are set the agent
// reads binlogs into an in-memory buffer. The buffer is flushed to the SaaS
// (metadata) and customer S3 (payload) only when --s3-bucket is set. Without a
// flush sink, the agent looks healthy on the WebSocket channel while every
// event accumulates in memory and is dropped on restart, with no operator
// signal — the SaaS Explorer / recover flows return empty.
//
// We fail fast at startup rather than warn-and-continue: most operators don't
// read agent logs proactively, and the steady-state symptom (empty Explorer)
// is exactly the silent-failure mode the project explicitly rejects (cf #262,
// #277). A future --saas-managed-storage flag could default the S3 target to a
// dbtrail-resolved location once the SaaS endpoint to resolve it exists; until
// then, an explicit --s3-bucket is mandatory.
func validateBYOSFlushConfig(byosMode bool, s3Bucket string) error {
	if !byosMode {
		return nil
	}
	if s3Bucket == "" {
		return fmt.Errorf("BYOS mode (--source-dsn + --server-id) requires --s3-bucket (or BINTRAIL_S3_BUCKET) to flush events to durable storage; agent refuses to start to prevent silent data loss (the in-memory buffer would accumulate events and drop them on restart with no operator signal). See issue #289")
	}
	return nil
}
