package doctor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// The Object Lock check reports the ransomware posture of the S3 archive
// bucket (#1197). After rotation drops a partition from the live index, its
// Parquet archive is the ONLY copy — a credential that can delete S3 objects
// can erase the recovery safety net. S3 Object Lock with a default retention
// rule makes archived objects immutable for the retention period, and this
// check tells the operator whether that protection is actually in force.
//
// The check is ADVISORY (WARN/SKIP only, never FAIL): running without Object
// Lock is a legitimate posture, and doctor must stay safe as a CI smoke test.

// ObjectLockCheckName is the check's display name.
const ObjectLockCheckName = "Archive S3 Object Lock"

// objectLockBudget bounds the S3 round trip so an unreachable endpoint cannot
// stall the report (same rationale as doctor.Build's internal budget).
const objectLockBudget = 10 * time.Second

// objectLockState is the fetched bucket posture, separated from the verdict
// so the WARN thresholds are unit-testable without a live bucket (the
// capacity.go pattern).
type objectLockState struct {
	queryErr   error         // the configuration could not be read (network/permission)
	notEnabled bool          // the bucket has Object Lock disabled
	mode       string        // default-retention mode (GOVERNANCE/COMPLIANCE); "" = no default rule
	retention  time.Duration // default-retention period; 0 = no default rule
}

// objectLockAPI is the one S3 operation the check performs — an interface so
// the error classification is testable with a mock.
type objectLockAPI interface {
	GetObjectLockConfiguration(ctx context.Context, params *s3.GetObjectLockConfigurationInput, optFns ...func(*s3.Options)) (*s3.GetObjectLockConfigurationOutput, error)
}

// fetchObjectLockState reads the bucket's Object Lock configuration and
// classifies the outcome. A disabled bucket surfaces as the API error
// ObjectLockConfigurationNotFoundError, not as an empty configuration.
func fetchObjectLockState(ctx context.Context, client objectLockAPI, bucket string) objectLockState {
	out, err := client.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "ObjectLockConfigurationNotFoundError" {
			return objectLockState{notEnabled: true}
		}
		return objectLockState{queryErr: err}
	}
	cfg := out.ObjectLockConfiguration
	if cfg == nil || cfg.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		return objectLockState{notEnabled: true}
	}
	var st objectLockState
	if cfg.Rule != nil && cfg.Rule.DefaultRetention != nil {
		dr := cfg.Rule.DefaultRetention
		st.mode = string(dr.Mode)
		// Years are approximated as 365d (S3 adds calendar years, so a leap year
		// can differ by a day — acceptable for an advisory check whose failure
		// direction is a spurious WARN). AWS forbids setting both Days and Years;
		// if a non-AWS store ever returns both, Days wins.
		if dr.Years != nil {
			st.retention = time.Duration(*dr.Years) * 365 * 24 * time.Hour
		}
		if dr.Days != nil {
			st.retention = time.Duration(*dr.Days) * 24 * time.Hour
		}
	}
	return st
}

// objectLockVerdict turns the fetched posture into the check outcome:
//   - configuration unreadable: SKIP — posture UNKNOWN, never reported as
//     either safe or unsafe (the anti-cry-wolf rule).
//   - lock disabled: WARN — archives are deletable; remediation carries the
//     enable-on-existing-bucket steps (AWS supports that since Nov 2023;
//     existing objects are NOT retroactively locked).
//   - lock enabled but no default retention rule: WARN — bintrail uploads
//     set no per-object retention, so new archives land UNLOCKED.
//   - default retention shorter than the index retention window: WARN —
//     rotation archives a partition at the moment it drops it, so data the
//     operator evidently still relies on (it was worth keeping live for the
//     whole --retain window) would lose deletion protection sooner than
//     that same window once the archive becomes the only copy.
//   - otherwise PASS, with the mode and period in the detail (GOVERNANCE
//     notes its s3:BypassGovernanceRetention escape hatch).
func objectLockVerdict(bucket string, st objectLockState, retain time.Duration) CheckResult {
	if st.queryErr != nil {
		return CheckResult{
			Name:   ObjectLockCheckName,
			Status: StatusSkip,
			Detail: fmt.Sprintf("could not read the Object Lock configuration of bucket %q: %v", bucket, st.queryErr),
			Remediation: "The check needs the s3:GetBucketObjectLockConfiguration permission on the bucket.\n" +
				"A PermanentRedirect means the bucket lives in a different region — pass --archive-s3-region.\n" +
				"If the endpoint was unreachable, retry once before investigating further.",
		}
	}
	if st.notEnabled {
		return CheckResult{
			Name:   ObjectLockCheckName,
			Status: StatusWarn,
			Detail: fmt.Sprintf("bucket %q does not have S3 Object Lock enabled — after rotation the Parquet archive is the ONLY copy of a partition, and any credential that can delete S3 objects can erase it (ransomware / compromised key)", bucket),
			Remediation: "On AWS, Object Lock can be enabled on an EXISTING bucket (since Nov 2023): enable\n" +
				"versioning first, then apply the lock configuration. Objects already in the bucket\n" +
				"are NOT retroactively locked — only new writes get the default retention. (Some\n" +
				"S3-compatible stores still require enabling the lock at bucket creation.)\n" +
				"  aws s3api put-bucket-versioning --bucket " + bucket + " --versioning-configuration Status=Enabled\n" +
				"  aws s3api put-object-lock-configuration --bucket " + bucket + " \\\n" +
				"    --object-lock-configuration 'ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=COMPLIANCE,Days=<retention>}}'\n" +
				"Recipe and threat model: docs/object-lock.md",
		}
	}
	if st.retention <= 0 {
		return CheckResult{
			Name:   ObjectLockCheckName,
			Status: StatusWarn,
			Detail: fmt.Sprintf("bucket %q has Object Lock enabled but NO default retention rule — bintrail uploads set no per-object retention, so new archives are written unlocked", bucket),
			Remediation: "Set a default retention rule so every uploaded archive is locked automatically:\n" +
				"  aws s3api put-object-lock-configuration --bucket " + bucket + " \\\n" +
				"    --object-lock-configuration 'ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=COMPLIANCE,Days=<retention>}}'\n" +
				"Pick Days to cover at least your index retention window (rotate --retain) plus the window you want deletion protection for. See docs/object-lock.md.",
		}
	}
	if retain > 0 && st.retention < retain {
		mode := st.mode
		if mode == "" {
			mode = "COMPLIANCE" // non-AWS stores can omit it; never emit a broken Mode=
		}
		return CheckResult{
			Name:   ObjectLockCheckName,
			Status: StatusWarn,
			Detail: fmt.Sprintf("bucket %q default retention %s is SHORTER than the index retention window %s (doctor's --retain — pass your real rotation window if you haven't) — rotation archives a partition at the moment it drops it, so data you kept live for %s stays deletion-protected for only %s once the archive becomes the only copy", bucket, fmtLockDuration(st.retention), fmtLockDuration(retain), fmtLockDuration(retain), fmtLockDuration(st.retention)),
			Remediation: "Raise the default retention to at least the rotation window:\n" +
				"  aws s3api put-object-lock-configuration --bucket " + bucket + " \\\n" +
				"    --object-lock-configuration 'ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=" + mode + ",Days=" + fmt.Sprintf("%d", int(retain.Hours()/24)+1) + "}}'",
		}
	}
	detail := fmt.Sprintf("bucket %q: Object Lock enabled, default retention %s %s", bucket, st.mode, fmtLockDuration(st.retention))
	if st.mode == string(types.ObjectLockRetentionModeGovernance) {
		detail += " (GOVERNANCE mode: principals holding s3:BypassGovernanceRetention can still delete; COMPLIANCE binds every principal including root)"
	}
	return CheckResult{Name: ObjectLockCheckName, Status: StatusPass, Detail: detail}
}

// fmtLockDuration renders whole days as "Nd" and falls back to the standard
// duration string otherwise.
func fmtLockDuration(d time.Duration) string {
	if d > 0 && d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
	}
	return d.String()
}

// CheckArchiveObjectLock reports the Object Lock posture of the --archive-s3
// bucket. retain is doctor's --retain window (0 = no rotation configured —
// the retention comparison is skipped, the posture checks still run).
func CheckArchiveObjectLock(ctx context.Context, archiveS3, region string, retain time.Duration) CheckResult {
	bucket, _, err := storage.ParseS3URL(archiveS3)
	if err != nil {
		return CheckResult{
			Name:        ObjectLockCheckName,
			Status:      StatusWarn,
			Detail:      fmt.Sprintf("invalid --archive-s3 URL %q: %v", archiveS3, err),
			Remediation: "Pass an s3:// URL, e.g. s3://my-bucket/archives/",
		}
	}
	ctx, cancel := context.WithTimeout(ctx, objectLockBudget)
	defer cancel()
	client, err := storage.NewS3ClientForBucket(ctx, bucket, region)
	if err != nil {
		return objectLockVerdict(bucket, objectLockState{queryErr: err}, retain)
	}
	return objectLockVerdict(bucket, fetchObjectLockState(ctx, client, bucket), retain)
}
