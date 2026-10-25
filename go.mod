module github.com/dbtrail/dbtrail

go 1.25.11

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/aws/aws-sdk-go-v2 v1.41.7
	github.com/aws/aws-sdk-go-v2/config v1.32.17
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.23
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.21.1
	github.com/aws/aws-sdk-go-v2/service/s3 v1.101.0
	github.com/aws/smithy-go v1.25.1
	github.com/coder/websocket v1.8.14
	// PINNED to the DuckDB 1.4 "Andium" LTS line. v2.5.6 == DuckDB 1.4.5.
	// Patch bumps WITHIN the line (v2.5.7, ...) are expected and welcome — they
	// are where LTS fixes land. What is forbidden is any v2.1xxxx.x, which is
	// DuckDB >= 1.5: duckdb-go changed versioning at 1.5 and now encodes the
	// engine version in the minor component (DuckDB 1.5.5 => v2.10505.0), so
	// under semver v2.10505.0 > v2.5.6 and a plain `go get -u` (or Dependabot,
	// if it is ever enabled here) walks the build off LTS with no import-path
	// change to notice. The six duckdb-go-bindings/lib/* modules use the same
	// encoding and move with this one.
	//
	// This comment cannot enforce itself — `go get -u` rewrites the version and
	// leaves the text byte-identical. The guard that actually fails is
	// TestDuckDBEngineOnLTSLine in internal/duckdbutil; it also carries the
	// pre-flight checklist for the day we do move off.
	//
	// Nothing in 1.5 is reachable from here. Our surface is read-side only:
	// parquet_scan (hive_partitioning/union_by_name), glob, parquet metadata
	// functions, one COPY TO PARQUET, INSTALL/LOAD httpfs+aws, CREATE SECRET
	// credential_chain, and SET threads/memory_limit/temp_directory/s3_region/
	// home_directory. Every handle is in-memory (sql.Open("duckdb", "")) and our
	// durable format is Parquet written by parquet-go, not DuckDB, so 1.5's
	// storage and type headliners cannot reach us. What 1.5 DOES change is
	// httpfs's backend, httplib -> curl, landing on the S3-direct scan (#511),
	// credential_chain secrets, cross-region resolution and the $HOME/.duckdb
	// extension dir (#610) — none of which CI exercises against real S3.
	//
	// 1.4 LTS support ENDS 2026-09-16. Past that date this pin is a known
	// liability rather than a preference: either move to the next LTS or record
	// an explicit decision to run an unsupported engine. There is no 1.6 on the
	// release calendar and DuckDB's stated policy is "every other release is
	// LTS", so the next LTS is EXPECTED to be 2.0.0 (Fall 2026, tentative) —
	// but 2.0.0 carries no LTS marker yet, so confirm before relying on it.
	github.com/duckdb/duckdb-go/v2 v2.5.6
	// >= v1.15.0 is a data-safety floor, not a preference: through v1.14.0,
	// MariadbGTIDSet.AddSet stored the EVENT'S OWN *MariadbGTID pointer into
	// the syncer's accumulated set on a domain's first appearance, and the
	// next event of that domain forward()ed — mutated — it while the consumer
	// goroutine could still be reading the delivered event (data race; torn
	// GTIDs could reach the checkpoint). v1.15.0 refactored AddSet to store a
	// Clone, removing the shared write entirely. Never downgrade below it.
	github.com/go-mysql-org/go-mysql v1.16.0
	github.com/go-sql-driver/mysql v1.9.3
	github.com/google/uuid v1.6.0
	github.com/jackc/pglogrepl v0.0.0-20260401131349-e37c41485510
	github.com/jackc/pgx/v5 v5.10.0
	github.com/modelcontextprotocol/go-sdk v1.3.1
	github.com/parquet-go/parquet-go v0.24.0
	github.com/prometheus/client_golang v1.23.2
	github.com/spf13/cobra v1.10.2
	go.yaml.in/yaml/v2 v2.4.2
	golang.org/x/crypto v0.50.0
	golang.org/x/sync v0.20.0
	golang.org/x/term v0.42.0
	golang.org/x/text v0.36.0
)

require (
	github.com/apache/arrow-go/v18 v18.6.0
	github.com/apache/iceberg-go v0.6.0
	github.com/prometheus/client_model v0.6.2
	github.com/spf13/pflag v1.0.10
	gopkg.in/yaml.v3 v3.0.1
)

require (
	atomicgo.dev/cursor v0.2.0 // indirect
	atomicgo.dev/keyboard v0.2.9 // indirect
	atomicgo.dev/schedule v0.1.0 // indirect
	cel.dev/expr v0.25.1 // indirect
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth v0.20.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.7.0 // indirect
	cloud.google.com/go/monitoring v1.24.3 // indirect
	cloud.google.com/go/storage v1.62.1 // indirect
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.20.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.13.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.4 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.1 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.6.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.31.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.55.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.55.0 // indirect
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/apache/thrift v0.23.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.10 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/glue v1.141.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.42.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/cncf/xds/go v0.0.0-20251210132809-ee656c7534f5 // indirect
	github.com/cockroachdb/apd/v3 v3.2.1 // indirect
	github.com/containerd/console v1.0.5 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/creasty/defaults v1.8.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/duckdb/duckdb-go-bindings v0.3.5 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-amd64 v0.3.5 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-arm64 v0.3.5 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-amd64 v0.3.5 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-arm64 v0.3.5 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/windows-amd64 v0.3.5 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.36.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/goccy/go-yaml v1.17.1 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.0 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/jsonschema-go v0.4.2 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/wire v0.7.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.15 // indirect
	github.com/googleapis/gax-go/v2 v2.22.0 // indirect
	github.com/gookit/color v1.6.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lithammer/fuzzysearch v1.1.8 // indirect
	github.com/mattn/go-runewidth v0.0.20 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/pingcap/errors v0.11.5-0.20260310054046-9c8b3586e4b2 // indirect
	github.com/pingcap/log v1.1.1-0.20260227082333-572e590d08f1 // indirect
	github.com/pingcap/tidb/pkg/parser v0.0.0-20260504140133-511dba1dbe17 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/pterm/pterm v0.12.83 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/segmentio/encoding v0.5.3 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/substrait-io/substrait v0.87.0 // indirect
	github.com/substrait-io/substrait-go/v8 v8.1.0 // indirect
	github.com/substrait-io/substrait-protobuf/go v0.85.0 // indirect
	github.com/twmb/avro v1.7.2 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.39.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.67.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.67.0 // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/sdk v1.43.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	gocloud.dev v0.45.0 // indirect
	golang.org/x/exp v0.0.0-20260218203240-3dfff04db8fa // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/api v0.278.0 // indirect
	google.golang.org/genproto v0.0.0-20260319201613-d00831a3d3e7 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260401024825-9d38bb4040a9 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260427160629-7cedc36a6bc4 // indirect
	google.golang.org/grpc v1.80.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)
