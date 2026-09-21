package cliapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/storage"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Create index tables in the target MySQL database",
	Long: `Creates the binlog_events (partitioned), schema_snapshots, and index_state
tables in the target MySQL database. The database is created if it does not exist.`,
	RunE: runInit,
}

var (
	initIndexDSN   string
	initPartitions int
	initEncrypt    bool
	initS3Bucket   string
	initS3Region   string
	initS3ARN      string
	initFormat     string
)

func init() {
	initCmd.Flags().StringVar(&initIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	initCmd.Flags().IntVar(&initPartitions, "partitions", 48, "Number of hourly partitions to create; partitions span from (N-1) hours ago to the current hour so historical binlog events are properly distributed")
	initCmd.Flags().BoolVar(&initEncrypt, "encrypt", false, "Enable InnoDB tablespace encryption on binlog_events (requires a keyring plugin on the MySQL server; adds ENCRYPTION='Y' to the table DDL)")
	initCmd.Flags().StringVar(&initS3Bucket, "s3-bucket", "", "S3 bucket name to create for archiving (optional; mutually exclusive with --s3-arn; the created bucket gets a 365-day object-expiry lifecycle rule; see docs/capacity.md)")
	initCmd.Flags().StringVar(&initS3Region, "s3-region", "us-east-1", "AWS region for the S3 bucket (required with --s3-bucket; ignored with --s3-arn since the SDK resolves the bucket region automatically)")
	initCmd.Flags().StringVar(&initS3ARN, "s3-arn", "", "ARN of an existing S3 bucket to use for archiving (optional; mutually exclusive with --s3-bucket)")
	initCmd.Flags().StringVar(&initFormat, "format", "text", "Output format: text or json")
	_ = initCmd.MarkFlagRequired("index-dsn")
	bindCommandEnv(initCmd)

	rootCmd.AddCommand(initCmd)
}

func runInit(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(initFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", initFormat)
	}
	if initS3Bucket != "" && initS3ARN != "" {
		return fmt.Errorf("--s3-bucket and --s3-arn are mutually exclusive: use --s3-bucket to create a new bucket, or --s3-arn to use an existing one")
	}

	// Validate the ARN format before doing any database work so that a typo
	// does not leave the user with a half-initialized state and a non-zero exit.
	var s3ARNBucket, s3ARNPartition string
	if initS3ARN != "" {
		var err error
		s3ARNBucket, s3ARNPartition, err = parseS3ARN(initS3ARN)
		if err != nil {
			return fmt.Errorf("invalid --s3-arn: %w", err)
		}
	}

	cfg, err := mysql.ParseDSN(initIndexDSN)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}

	dbName := cfg.DBName
	if dbName == "" {
		return fmt.Errorf("--index-dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
	}

	// Step 1: Connect to the server without a database to CREATE DATABASE IF NOT EXISTS.
	// We avoid using the pooled connection for USE statements — reconnect with the DB
	// name baked into the DSN instead.
	if err := indexer.EnsureDatabase(cfg, dbName, func(s string) { fmt.Println(s) }); err != nil {
		return err
	}

	// Step 2: Reconnect with the database name in the DSN.
	db, err := config.OpenMySQL(cfg)
	if err != nil {
		return fmt.Errorf("failed to open index database connection: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to connect to index database %q: %w", dbName, err)
	}

	// Track created tables for JSON output.
	var tablesCreated []string
	logTable := func(name string) {
		tablesCreated = append(tablesCreated, name)
		if initFormat != "json" {
			fmt.Printf("  \u2713 %s\n", name)
		}
	}

	// Create the full index table set (shared with the control-plane
	// supervisor, which provisions a per-source index DB the same way).
	if err := indexer.CreateIndexTables(cmd.Context(), db, initPartitions, initEncrypt, logTable); err != nil {
		return err
	}

	var s3Result *string
	// s3Err captures a bucket setup/verify failure so it can be surfaced in
	// JSON mode (an s3_error field + a non-zero exit). In text mode it stays
	// non-fatal: a human reads the stderr warning and remediation and acts.
	var s3Err error
	if initS3Bucket != "" {
		if initFormat != "json" {
			fmt.Printf("\nSetting up S3 bucket...\n")
		}
		if err := setupS3Bucket(cmd.Context(), initS3Bucket, initS3Region); err != nil {
			// A malformed S3 endpoint is a configuration fault, not a bucket
			// one: creating the bucket by hand fixes nothing, and the
			// remediation below would send the operator to do exactly that.
			if errors.Is(err, storage.ErrS3EndpointConfig) {
				return err
			}
			s3Err = err
			if initFormat != "json" {
				fmt.Fprintf(os.Stderr, "Warning: could not create S3 bucket %q: %v\n\n", initS3Bucket, err)
				fmt.Fprint(os.Stderr, s3Instructions(initS3Bucket, initS3Region))
			}
		} else {
			s := fmt.Sprintf("%s (region: %s)", initS3Bucket, initS3Region)
			s3Result = &s
			if initFormat != "json" {
				fmt.Printf("  \u2713 S3 bucket: %s\n", s)
			}
		}
	}

	if initS3ARN != "" {
		if initFormat != "json" {
			fmt.Printf("\nVerifying existing S3 bucket...\n")
		}
		if err := verifyS3Bucket(cmd.Context(), s3ARNBucket, initS3Region); err != nil {
			// See above: an IAM policy is not what is wrong here.
			if errors.Is(err, storage.ErrS3EndpointConfig) {
				return err
			}
			s3Err = err
			if initFormat != "json" {
				fmt.Fprintf(os.Stderr, "Warning: could not verify S3 bucket %q: %v\n\n", s3ARNBucket, err)
				fmt.Fprint(os.Stderr, s3IAMInstructions(s3ARNBucket, s3ARNPartition))
			}
		} else {
			s := fmt.Sprintf("%s (ARN: %s)", s3ARNBucket, initS3ARN)
			s3Result = &s
			if initFormat != "json" {
				fmt.Printf("  \u2713 S3 bucket: %s\n", s)
			}
		}
	}

	if initFormat == "json" {
		result := buildInitResult(dbName, tablesCreated, s3Result, s3Err)
		if err := cliutil.OutputJSON(result); err != nil {
			return err
		}
		// Fail hard in JSON mode so exit-code-only automation gets a signal:
		// a zero exit with a silently-omitted bucket would make a pipeline
		// believe archiving is provisioned when it is not (issue #810). Text
		// mode stays non-fatal by design — the warning above is for a human.
		if s3Err != nil {
			return fmt.Errorf("S3 bucket setup failed: %w", s3Err)
		}
		return nil
	}

	fmt.Printf("\nInitialization complete. Index database: %s\n", dbName)
	return nil
}

// initJSONResult is the --format json payload for the init command.
type initJSONResult struct {
	Database      string   `json:"database"`
	TablesCreated []string `json:"tables_created"`
	S3Bucket      *string  `json:"s3_bucket,omitempty"`
	S3Error       *string  `json:"s3_error,omitempty"`
}

// buildInitResult assembles the JSON payload. A non-nil s3Err is surfaced in
// the s3_error field so JSON consumers never see a silently-omitted bucket
// (issue #810). s3Result and s3Err are mutually exclusive per invocation.
func buildInitResult(dbName string, tables []string, s3Result *string, s3Err error) initJSONResult {
	r := initJSONResult{
		Database:      dbName,
		TablesCreated: tables,
		S3Bucket:      s3Result,
	}
	if s3Err != nil {
		msg := s3Err.Error()
		r.S3Error = &msg
	}
	return r
}

// setupS3Bucket creates a private S3 bucket with a 1-year lifecycle expiry policy.
// It loads AWS credentials from the environment, ~/.aws/credentials, or the EC2 instance
// metadata service — whichever is available. If creation fails, the caller should print
// s3Instructions so the user can set up the bucket manually.
func setupS3Bucket(ctx context.Context, bucket, region string) error {
	client, err := storage.NewS3Client(ctx, region)
	if err != nil {
		return err
	}

	// Create the bucket. us-east-1 rejects CreateBucketConfiguration; all other
	// regions require it. BucketAlreadyOwnedByYou is treated as success so that
	// re-running bintrail init is idempotent (mirrors CREATE TABLE IF NOT EXISTS).
	createInput := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if region != "us-east-1" {
		createInput.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}
	if _, err := client.CreateBucket(ctx, createInput); err != nil {
		var alreadyOwned *types.BucketAlreadyOwnedByYou
		if !errors.As(err, &alreadyOwned) {
			return fmt.Errorf("create bucket: %w", err)
		}
	}

	// Block all public access.
	if _, err := client.PutPublicAccessBlock(ctx, &s3.PutPublicAccessBlockInput{
		Bucket: aws.String(bucket),
		PublicAccessBlockConfiguration: &types.PublicAccessBlockConfiguration{
			BlockPublicAcls:       aws.Bool(true),
			IgnorePublicAcls:      aws.Bool(true),
			BlockPublicPolicy:     aws.Bool(true),
			RestrictPublicBuckets: aws.Bool(true),
		},
	}); err != nil {
		return fmt.Errorf("block public access: %w", err)
	}

	// Add a lifecycle rule: expire all objects after 365 days.
	if _, err := client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("bintrail-1yr-expiry"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
					Expiration: &types.LifecycleExpiration{
						Days: aws.Int32(365),
					},
				},
			},
		},
	}); err != nil {
		return fmt.Errorf("set lifecycle policy: %w", err)
	}

	return nil
}

// parseS3ARN extracts the bucket name and partition from an S3 bucket ARN.
// S3 bucket ARNs have the form arn:partition:s3:::bucket-name where partition
// is "aws", "aws-cn", or "aws-us-gov". Both region (parts[3]) and account-id
// (parts[4]) are empty for bucket-level ARNs. Object ARNs contain a "/" in the
// resource field and are rejected to avoid a confusing HeadBucket error.
func parseS3ARN(arn string) (bucket, partition string, err error) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 {
		return "", "", fmt.Errorf("expected 6 colon-separated fields, got %d (want arn:partition:s3:::bucket-name)", len(parts))
	}
	if parts[0] != "arn" {
		return "", "", fmt.Errorf("must start with \"arn:\", got %q", parts[0])
	}
	if parts[2] != "s3" {
		return "", "", fmt.Errorf("service must be \"s3\", got %q", parts[2])
	}
	if parts[3] != "" || parts[4] != "" {
		return "", "", fmt.Errorf("S3 bucket ARNs have empty region and account fields (got %q and %q); this looks like an object or access-point ARN", parts[3], parts[4])
	}
	bucket = parts[5]
	if bucket == "" {
		return "", "", fmt.Errorf("bucket name is empty in ARN %q", arn)
	}
	if strings.Contains(bucket, "/") {
		return "", "", fmt.Errorf("bucket name %q contains \"/\"; pass a bucket ARN (arn:aws:s3:::bucket-name), not an object ARN", bucket)
	}
	return bucket, parts[1], nil
}

// verifyS3Bucket checks that an S3 bucket exists and is accessible by the
// current AWS credentials using a HeadBucket request.
func verifyS3Bucket(ctx context.Context, bucket, region string) error {
	client, err := storage.NewS3Client(ctx, region)
	if err != nil {
		return err
	}
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return fmt.Errorf("head bucket: %w", err)
	}
	return nil
}

// s3IAMInstructions returns a human-readable IAM policy snippet the caller
// needs to attach to the IAM role or user that runs bintrail, so it can read
// and write Parquet archives to the bucket. partition must be the ARN partition
// string ("aws", "aws-cn", or "aws-us-gov") so the resource ARNs are correct.
func s3IAMInstructions(bucket, partition string) string {
	return fmt.Sprintf(`To allow bintrail to read and write archives in this bucket, attach the
following IAM policy to the role or user running bintrail:

  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "BintrailS3Access",
        "Effect": "Allow",
        "Action": [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload"
        ],
        "Resource": [
          "arn:%s:s3:::%s",
          "arn:%s:s3:::%s/*"
        ]
      }
    ]
  }

Minimum permissions explained:
  s3:PutObject             — write Parquet archive files
  s3:GetObject             — read back archive files
  s3:ListBucket            — enumerate archived partitions
  s3:DeleteObject          — remove superseded archives (optional but recommended)
  s3:AbortMultipartUpload  — clean up failed/interrupted large uploads (orphaned parts are billed)

`, partition, bucket, partition, bucket)
}

// s3Instructions returns a human-readable string with AWS CLI and console steps
// to manually create and configure the S3 bucket.
func s3Instructions(bucket, region string) string {
	var sb strings.Builder

	sb.WriteString("To create the bucket manually, run the following AWS CLI commands:\n\n")

	// Create bucket.
	sb.WriteString("  aws s3api create-bucket \\\n")
	fmt.Fprintf(&sb, "    --bucket %s \\\n", bucket)
	fmt.Fprintf(&sb, "    --region %s", region)
	if region != "us-east-1" {
		fmt.Fprintf(&sb, " \\\n    --create-bucket-configuration LocationConstraint=%s", region)
	}
	sb.WriteString("\n\n")

	// Block public access.
	sb.WriteString("  aws s3api put-public-access-block \\\n")
	fmt.Fprintf(&sb, "    --bucket %s \\\n", bucket)
	sb.WriteString("    --public-access-block-configuration \"BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true\"\n\n")

	// Lifecycle policy.
	sb.WriteString("  aws s3api put-bucket-lifecycle-configuration \\\n")
	fmt.Fprintf(&sb, "    --bucket %s \\\n", bucket)
	sb.WriteString("    --lifecycle-configuration '{\"Rules\":[{\"ID\":\"bintrail-1yr-expiry\",\"Status\":\"Enabled\",\"Filter\":{\"Prefix\":\"\"},\"Expiration\":{\"Days\":365}}]}'\n\n")

	// Console instructions.
	sb.WriteString("Or via the AWS Console (https://s3.console.aws.amazon.com/s3/bucket/create):\n")
	fmt.Fprintf(&sb, "  Bucket name             : %s\n", bucket)
	fmt.Fprintf(&sb, "  Region                  : %s\n", region)
	sb.WriteString("  Block all public access : enabled\n")
	sb.WriteString("  Lifecycle rule          : expire all objects after 365 days\n\n")

	return sb.String()
}
