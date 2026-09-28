package doctor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// The snapshot expiry check reads whether the bucket rule that expires old
// snapshots is actually there (#1680). bintrail never deletes a snapshot
// from S3 and never sets a rule on a bucket (#1622): the console generates
// the rule and the operator applies it. Until this check, a bucket where
// the rule was applied and one where it was copied and forgotten looked the
// same everywhere, while one of them grew without a ceiling.
//
// It is a read and a sentence. The answer is one of three states, and the
// third is its own: a configuration that could not be read is never
// reported as "no rule" (a missing permission would raise an alarm on a
// bucket that is fine) and never as silence (which would hide a real one).
//
// The check is ADVISORY (WARN/SKIP/PASS, never FAIL), like the Object Lock
// check beside it: keeping every snapshot forever is a legitimate choice.

// SnapshotExpiryCheckName is the check's display name.
const SnapshotExpiryCheckName = "Snapshot S3 expiry rule"

// snapshotExpiryBudget bounds the S3 round trip so an unreachable endpoint
// cannot stall the report.
const snapshotExpiryBudget = 10 * time.Second

// ExpiryState is which of the three answers the read gave.
type ExpiryState string

const (
	// ExpiryInForce: an enabled rule whose scope covers the snapshot prefix
	// expires objects, by age (Days) or on a fixed date (ExpiresOn).
	ExpiryInForce ExpiryState = "in_force"
	// ExpiryNone: the rules were read and none of them does.
	ExpiryNone ExpiryState = "none"
	// ExpiryUnreadable: the rules could not be read. Nothing is known.
	ExpiryUnreadable ExpiryState = "unreadable"
)

// Why a configuration could not be read, for a screen to pick its words.
const (
	ReasonDenied      = "denied"      // the credentials may not read it
	ReasonUnsupported = "unsupported" // the store does not implement the call
	ReasonError       = "error"       // anything else: network, deadline, a bucket that is gone
)

// LifecycleRule is one bucket rule reduced to what the verdict reads,
// separated from the SDK types so the verdict is testable without a bucket.
type LifecycleRule struct {
	ID      string
	Enabled bool
	// Prefix is the rule's scope exactly as the bucket holds it; "" is the
	// whole bucket.
	Prefix string
	// Conditional: the rule also filters by tag or by object size, so it
	// applies to some objects under Prefix and not to others.
	Conditional bool
	// Days is the age current objects expire at; 0 when the rule has none
	// (it moves objects to another class, expires old versions only, or
	// aborts unfinished uploads).
	Days int
	// Date is a fixed expiry date; zero when the rule has none.
	Date time.Time
	// NoncurrentDays is the age OLD VERSIONS expire at; 0 when the rule
	// leaves them. On a bucket that keeps versions, Days alone only writes
	// a delete marker and every byte stays.
	NoncurrentDays int
}

// SnapshotExpiry is the verdict, and the body of the console's answer.
type SnapshotExpiry struct {
	State  ExpiryState `json:"state"`
	Bucket string      `json:"bucket"`
	// Prefix is the snapshot prefix as the daemon builds keys under it: one
	// trailing slash trimmed, nothing else tidied.
	Prefix string `json:"prefix"`
	// Days and RuleID name the covering rule with the SHORTEST age: with
	// several rules on one object, that is the one that deletes it.
	Days   int    `json:"days,omitempty"`
	RuleID string `json:"rule_id,omitempty"`
	// WholeBucket: that rule's scope is the whole bucket, so it expires
	// everything else in it at the same age.
	WholeBucket bool `json:"whole_bucket,omitempty"`
	// OldVersionsStay: that rule expires current objects only. Whether the
	// bucket keeps versions is not read (it is another permission), so this
	// is said as a condition, never as a finding.
	OldVersionsStay bool `json:"old_versions_stay,omitempty"`
	// ExpiresOn is the earliest fixed date (YYYY-MM-DD, UTC) a covering rule
	// expires on, and DatePassed whether that day has come. Such a rule has
	// no age: from that date on it expires every object in its scope.
	ExpiresOn  string `json:"expires_on,omitempty"`
	DateRuleID string `json:"date_rule_id,omitempty"`
	DatePassed bool   `json:"date_passed,omitempty"`
	// Conditional counts the enabled expiring rules on this prefix that
	// apply by tag or size only. They are NOT counted as protection:
	// bintrail sets no tag on what it uploads, and a size limit leaves part
	// of every snapshot behind. They are counted here so the answer can say
	// why a rule the operator sees in the bucket was not taken.
	Conditional int `json:"conditional,omitempty"`
	// Reason and Error are set on ExpiryUnreadable only.
	Reason string `json:"reason,omitempty"`
	Error  string `json:"error,omitempty"`
}

// s3URLParts splits s3://bucket/prefix the way the console page does
// (s3Parts in app.js) and the daemon builds keys: one trailing slash
// trimmed, no spaces trimmed, no slashes collapsed. A line break is refused
// as the page's pattern refuses it.
func s3URLParts(url string) (bucket, prefix string, ok bool) {
	if strings.ContainsAny(url, "\n\r  ") {
		return "", "", false
	}
	bucket, prefix, err := storage.ParseS3URL(url)
	if err != nil {
		return "", "", false
	}
	return bucket, strings.TrimSuffix(prefix, "/"), true
}

// S3PrefixCovers reports whether a rule on outer (s3://bucket/prefix) also
// expires objects under inner: same bucket, and inner's prefix equals or
// sits under outer's, compared with the slash so that "dbtrail" does not
// cover "dbtrail-archives". It is the covering test the console page
// generates its rule with (s3PrefixCovers in app.js); a test in
// internal/console runs both over one table so they cannot drift.
func S3PrefixCovers(outer, inner string) bool {
	ob, op, ok := s3URLParts(outer)
	if !ok {
		return false
	}
	ib, ip, ok := s3URLParts(inner)
	if !ok || ob != ib {
		return false
	}
	return strings.HasPrefix(ip+"/", op+"/")
}

// RetentionTooShort: an age rule no longer than the schedule interval
// expires the newest snapshot before the next one lands. Equal counts. An
// unknown interval never warns. The same rule as retentionTooShort in
// app.js, pinned against it by the same test.
func RetentionTooShort(everyMinutes, days int) bool {
	return everyMinutes > 0 && days*1440 <= everyMinutes
}

// ruleCovers reports whether a rule with this prefix expires the snapshots
// under s3://bucket/prefix. The covering test is S3PrefixCovers. Two shapes
// it does not see count too, because S3 matches a rule prefix against the
// key by CHARACTERS: a rule with an empty prefix is on the whole bucket
// (S3PrefixCovers compares folders, and at the root it only answers for
// snapshots at the root), and a rule on "back" does expire the keys under
// "backups/", though it names no folder. Leaving either out would report
// "no rule" over a rule that is deleting snapshots.
func ruleCovers(bucket, prefix, rulePrefix string) bool {
	if S3PrefixCovers("s3://"+bucket+"/"+rulePrefix, "s3://"+bucket+"/"+prefix) {
		return true
	}
	keys := ""
	if prefix != "" {
		keys = prefix + "/"
	}
	return strings.HasPrefix(keys, rulePrefix)
}

// SnapshotExpiryVerdict turns the rules of a bucket into the answer for one
// snapshot destination. Pure: no network, and the clock is passed in.
//
//   - a disabled rule protects nothing;
//   - a rule that does not cover the prefix protects nothing;
//   - a rule limited by tag or size is counted apart, never as protection;
//   - of the rules left, the shortest age wins, since it deletes first.
func SnapshotExpiryVerdict(snapshotS3 string, rules []LifecycleRule, now time.Time) SnapshotExpiry {
	bucket, prefix, _ := s3URLParts(snapshotS3)
	v := SnapshotExpiry{State: ExpiryNone, Bucket: bucket, Prefix: prefix}
	var earliest time.Time
	for _, r := range rules {
		if !r.Enabled || (r.Days <= 0 && r.Date.IsZero()) || !ruleCovers(bucket, prefix, r.Prefix) {
			continue
		}
		if r.Conditional {
			v.Conditional++
			continue
		}
		v.State = ExpiryInForce
		if r.Days > 0 && (v.Days == 0 || r.Days < v.Days) {
			v.Days, v.RuleID, v.WholeBucket = r.Days, r.ID, r.Prefix == ""
			v.OldVersionsStay = r.NoncurrentDays <= 0
		}
		if !r.Date.IsZero() && (earliest.IsZero() || r.Date.Before(earliest)) {
			earliest = r.Date
			v.ExpiresOn, v.DateRuleID = r.Date.UTC().Format("2006-01-02"), r.ID
			v.DatePassed = !r.Date.After(now)
		}
	}
	return v
}

// lifecycleRulesFromSDK reduces the bucket's answer to LifecycleRule. The
// scope is the rule's Filter, or the Prefix field rules written before
// filters existed still carry; neither means the whole bucket.
func lifecycleRulesFromSDK(in []types.LifecycleRule) []LifecycleRule {
	out := make([]LifecycleRule, 0, len(in))
	for _, r := range in {
		lr := LifecycleRule{
			ID:      aws.ToString(r.ID),
			Enabled: r.Status == types.ExpirationStatusEnabled,
			Prefix:  aws.ToString(r.Prefix),
		}
		if f := r.Filter; f != nil {
			lr.Prefix = aws.ToString(f.Prefix)
			lr.Conditional = f.Tag != nil || f.ObjectSizeGreaterThan != nil || f.ObjectSizeLessThan != nil
			if a := f.And; a != nil {
				lr.Prefix = aws.ToString(a.Prefix)
				lr.Conditional = lr.Conditional || len(a.Tags) > 0 || a.ObjectSizeGreaterThan != nil || a.ObjectSizeLessThan != nil
			}
		}
		if n := r.NoncurrentVersionExpiration; n != nil {
			lr.NoncurrentDays = int(aws.ToInt32(n.NoncurrentDays))
		}
		if e := r.Expiration; e != nil {
			lr.Days = int(aws.ToInt32(e.Days))
			lr.Date = aws.ToTime(e.Date)
		}
		out = append(out, lr)
	}
	return out
}

// lifecycleAPI is the one S3 operation the check performs, an interface so
// the classification is testable with a mock.
type lifecycleAPI interface {
	GetBucketLifecycleConfiguration(ctx context.Context, params *s3.GetBucketLifecycleConfigurationInput, optFns ...func(*s3.Options)) (*s3.GetBucketLifecycleConfigurationOutput, error)
}

// readSnapshotExpiry reads the bucket's rules and answers for snapshotS3. A
// bucket with no lifecycle configuration answers with the API error
// NoSuchLifecycleConfiguration, not with an empty list: that is a read that
// worked, and its answer is "no rule". Every other error is unreadable.
func readSnapshotExpiry(ctx context.Context, client lifecycleAPI, snapshotS3 string, now time.Time) SnapshotExpiry {
	bucket, prefix, _ := s3URLParts(snapshotS3)
	unreadable := func(reason string, err error) SnapshotExpiry {
		return SnapshotExpiry{State: ExpiryUnreadable, Bucket: bucket, Prefix: prefix, Reason: reason, Error: err.Error()}
	}
	out, err := client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			switch ae.ErrorCode() {
			case "NoSuchLifecycleConfiguration":
				return SnapshotExpiryVerdict(snapshotS3, nil, now)
			case "AccessDenied", "AccessDeniedException", "AllAccessDisabled":
				return unreadable(ReasonDenied, err)
			case "NotImplemented", "XNotImplemented", "MethodNotAllowed":
				return unreadable(ReasonUnsupported, err)
			}
		}
		return unreadable(ReasonError, err)
	}
	if out == nil {
		return unreadable(ReasonError, errors.New("the store answered with nothing"))
	}
	return SnapshotExpiryVerdict(snapshotS3, lifecycleRulesFromSDK(out.Rules), now)
}

// ReadSnapshotExpiry answers for one s3:// snapshot destination, with the
// credentials and bucket routing every other S3 surface uses. The caller
// bounds ctx. A destination that is not an s3:// URL, or a client that
// cannot be built, is unreadable: nothing was read.
func ReadSnapshotExpiry(ctx context.Context, snapshotS3, region string) SnapshotExpiry {
	bucket, prefix, ok := s3URLParts(snapshotS3)
	if !ok {
		return SnapshotExpiry{State: ExpiryUnreadable, Reason: ReasonError,
			Error: fmt.Sprintf("%q is not an s3://bucket/prefix destination", snapshotS3)}
	}
	client, err := storage.NewS3ClientForBucket(ctx, bucket, region)
	if err != nil {
		return SnapshotExpiry{State: ExpiryUnreadable, Bucket: bucket, Prefix: prefix, Reason: ReasonError, Error: err.Error()}
	}
	return readSnapshotExpiry(ctx, client, snapshotS3, time.Now())
}

// snapshotExpiryCheck turns the verdict into the check outcome:
//   - unreadable, or a state this build does not know: SKIP. Nothing is
//     claimed either way.
//   - no rule: WARN. The bucket grows without limit.
//   - a rule whose age is no longer than the schedule interval, or whose
//     fixed date has come: WARN. Snapshots expire before the next exists.
//   - otherwise PASS, with the rule and its age in the detail.
//
// every is how often snapshots are taken (0 = not given: the age is not
// compared, the rest still runs).
func snapshotExpiryCheck(v SnapshotExpiry, every time.Duration) CheckResult {
	where := "under " + v.Prefix + "/"
	if v.Prefix == "" {
		where = "at the root of the bucket"
	}
	const readFix = "The check needs the s3:GetBucketLifecycleConfiguration permission on the bucket; it is optional,\n" +
		"and nothing else needs it (docs/s3-iam-policy.md).\n" +
		"A PermanentRedirect means the bucket lives in a different region: pass --baseline-s3-region.\n" +
		"If the endpoint was unreachable, retry once before investigating further."
	switch v.State {
	case ExpiryInForce, ExpiryNone:
	case ExpiryUnreadable:
		if v.Reason == ReasonUnsupported {
			return CheckResult{
				Name:   SnapshotExpiryCheckName,
				Status: StatusSkip,
				Detail: fmt.Sprintf("the store of bucket %q does not answer the request for its lifecycle rules, so it is unknown whether old snapshots expire: %s", v.Bucket, v.Error),
				Remediation: "Some S3-compatible stores have no lifecycle rules. Check in the store's own tools\n" +
					"whether it removes old objects; bintrail never does.",
			}
		}
		return CheckResult{
			Name:        SnapshotExpiryCheckName,
			Status:      StatusSkip,
			Detail:      fmt.Sprintf("could not read the lifecycle rules of bucket %q, so it is unknown whether old snapshots expire: %s", v.Bucket, v.Error),
			Remediation: readFix,
		}
	default:
		return CheckResult{
			Name:        SnapshotExpiryCheckName,
			Status:      StatusSkip,
			Detail:      fmt.Sprintf("could not tell whether old snapshots in bucket %q expire (answer %q)", v.Bucket, v.State),
			Remediation: readFix,
		}
	}

	if v.State == ExpiryNone {
		detail := fmt.Sprintf("no rule in bucket %q expires the snapshots %s: bintrail never removes one, so the bucket grows without limit", v.Bucket, where)
		if every > 0 {
			if n := int64(30 * 24 * time.Hour / every); n > 0 {
				detail += fmt.Sprintf(", by about %d snapshots every 30 days", n)
			}
		}
		if v.Conditional > 0 {
			detail += fmt.Sprintf(" (%s, and %s not counted)", plural(v.Conditional, "rule on this prefix applies", "rules on this prefix apply")+
				" only to objects with a tag or a size limit", isAre(v.Conditional))
		}
		return CheckResult{
			Name:   SnapshotExpiryCheckName,
			Status: StatusWarn,
			Detail: detail,
			Remediation: "bintrail never deletes from S3 and never sets a bucket rule: the rule is yours to apply.\n" +
				"The Snapshots settings page of the web console writes it for this prefix, with the\n" +
				"commands to apply it. Merge it into the rules the bucket already has:\n" +
				"  aws s3api get-bucket-lifecycle-configuration --bucket " + v.Bucket + "\n" +
				"Keeping every snapshot is a valid choice; this line is then the reminder of what it costs.",
		}
	}

	var parts []string
	status, fix := StatusPass, ""
	if v.Days > 0 {
		s := fmt.Sprintf("%s in bucket %q expires the snapshots %s after %s", ruleName(v.RuleID), v.Bucket, where, plural(v.Days, "day", "days"))
		if v.WholeBucket {
			s += " (the rule is on the whole bucket, so everything else in it expires at that age too)"
		}
		if v.OldVersionsStay {
			s += " (current objects only: if the bucket keeps versions, the old ones stay and the bucket still grows)"
		}
		parts = append(parts, s)
		if minutes := int(every / time.Minute); RetentionTooShort(minutes, v.Days) {
			status = StatusWarn
			parts = append(parts, fmt.Sprintf("with a snapshot every %s the newest one expires before the next exists: there are moments with no snapshot in S3 at all", fmtEvery(every)))
			fix = fmt.Sprintf("Raise the rule's age to at least %s, or take snapshots more often.\n", plural(minutes/1440+1, "day", "days")) +
				"bintrail never changes a bucket rule: edit it in the bucket."
		}
	}
	if v.ExpiresOn != "" {
		if v.DatePassed {
			status = StatusWarn
			parts = append(parts, fmt.Sprintf("%s has been expiring every snapshot %s since %s, whatever its age: a snapshot is removed soon after it arrives", ruleName(v.DateRuleID), where, v.ExpiresOn))
			if fix == "" {
				fix = "A rule with a fixed date keeps expiring after that date. Replace it with a rule by age.\n" +
					"bintrail never changes a bucket rule: edit it in the bucket."
			}
		} else {
			parts = append(parts, fmt.Sprintf("%s expires every snapshot %s on %s, whatever its age", ruleName(v.DateRuleID), where, v.ExpiresOn))
		}
	}
	if len(parts) == 0 {
		// An answer that names no age and no date claims nothing.
		return CheckResult{
			Name:        SnapshotExpiryCheckName,
			Status:      StatusSkip,
			Detail:      fmt.Sprintf("could not tell whether old snapshots in bucket %q expire: the answer names no age and no date", v.Bucket),
			Remediation: readFix,
		}
	}
	return CheckResult{Name: SnapshotExpiryCheckName, Status: status, Detail: strings.Join(parts, "; "), Remediation: fix}
}

// ruleName names a rule by its ID; S3 allows a rule without one.
func ruleName(id string) string {
	if id == "" {
		return "a rule"
	}
	return fmt.Sprintf("rule %q", id)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

// fmtEvery renders a schedule interval in its largest whole unit.
func fmtEvery(d time.Duration) string {
	switch {
	case d%(24*time.Hour) == 0:
		return plural(int(d/(24*time.Hour)), "day", "days")
	case d%time.Hour == 0:
		return plural(int(d/time.Hour), "hour", "hours")
	}
	return plural(int(d/time.Minute), "minute", "minutes")
}

func isAre(n int) string {
	if n == 1 {
		return "is"
	}
	return "are"
}

// CheckSnapshotExpiry reports whether the bucket behind an s3:// snapshot
// destination expires old snapshots. every is how often snapshots are taken
// (0 = unknown). A destination that is not S3 has no bucket rule to read:
// SKIP, the check does not apply.
func CheckSnapshotExpiry(ctx context.Context, snapshotS3, region string, every time.Duration) CheckResult {
	if _, _, ok := s3URLParts(snapshotS3); !ok {
		return CheckResult{
			Name:        SnapshotExpiryCheckName,
			Status:      StatusSkip,
			Detail:      fmt.Sprintf("%q is not an s3://bucket/prefix destination, so there is no bucket rule to read", snapshotS3),
			Remediation: "Pass an s3:// URL, e.g. s3://my-bucket/backups/",
		}
	}
	ctx, cancel := context.WithTimeout(ctx, snapshotExpiryBudget)
	defer cancel()
	return snapshotExpiryCheck(ReadSnapshotExpiry(ctx, snapshotS3, region), every)
}
