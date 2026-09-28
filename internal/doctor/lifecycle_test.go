package doctor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

var expiryNow = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

func rule(id, prefix string, days int) LifecycleRule {
	return LifecycleRule{ID: id, Enabled: true, Prefix: prefix, Days: days}
}

// TestS3PrefixCovers pins the Go covering test on its own. The page's
// s3PrefixCovers is run over a table of its own beside this function in
// internal/console (assets_s3_expiry_1680_test.go), so the two cannot drift.
func TestS3PrefixCovers(t *testing.T) {
	for _, c := range []struct {
		outer, inner string
		want         bool
	}{
		{"s3://b/dbtrail", "s3://b/dbtrail", true},
		{"s3://b/dbtrail", "s3://b/dbtrail/archives", true},
		{"s3://b/dbtrail/", "s3://b/dbtrail", true},
		{"s3://b/dbtrail", "s3://b/dbtrail/", true},
		// At the root the page's test answers for snapshots at the root
		// only; the whole-bucket rule is ruleCovers' to recognise.
		{"s3://b", "s3://b/dbtrail", false},
		{"s3://b", "s3://b", true},
		{"s3://b/", "s3://b", true},
		{"s3://b/dbtrail", "s3://b/dbtrail-archives", false},
		{"s3://b/dbtrail", "s3://other/dbtrail", false},
		{"s3://b/dbtrail/backups", "s3://b/dbtrail", false},
		{"s3://b/dbtrail", "s3://b", false},
		{"s3://b/Dbtrail", "s3://b/dbtrail", false},
		{"s3://b/x", "not a url", false},
		{"not a url", "s3://b/x", false},
		{"", "", false},
		// A line break is refused, as the page's pattern refuses it.
		{"s3://b/x\ny", "s3://b/x\ny", false},
		{"s3://b/x\n", "s3://b/x\n", false},
		// Spaces and doubled slashes are part of the key, never tidied.
		{"s3://b/x ", "s3://b/x", false},
		{"s3://b/x//", "s3://b/x", false},
		{"s3://b/x//", "s3://b/x//", true},
	} {
		if got := S3PrefixCovers(c.outer, c.inner); got != c.want {
			t.Errorf("S3PrefixCovers(%q, %q) = %v, want %v", c.outer, c.inner, got, c.want)
		}
	}
}

func TestRetentionTooShort(t *testing.T) {
	for _, c := range []struct {
		every, days int
		want        bool
	}{
		{5, 1, false}, {360, 1, false}, {1440, 1, true}, {1440, 2, false},
		{10080, 7, true}, {10080, 8, false}, {0, 1, false}, {-5, 1, false},
	} {
		if got := RetentionTooShort(c.every, c.days); got != c.want {
			t.Errorf("RetentionTooShort(%d, %d) = %v, want %v", c.every, c.days, got, c.want)
		}
	}
}

func TestSnapshotExpiryVerdict(t *testing.T) {
	past := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	future := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		url       string
		rules     []LifecycleRule
		wantState ExpiryState
		wantDays  int
		wantID    string
		check     func(t *testing.T, v SnapshotExpiry)
	}{
		{name: "no rules at all", url: "s3://b/backups", wantState: ExpiryNone},
		{
			name: "the generated rule, prefix with its slash", url: "s3://b/backups",
			rules:     []LifecycleRule{rule("dbtrail-backups-expire-30d", "backups/", 30)},
			wantState: ExpiryInForce, wantDays: 30, wantID: "dbtrail-backups-expire-30d",
		},
		{
			name: "destination written with a trailing slash", url: "s3://b/backups/",
			rules:     []LifecycleRule{rule("r", "backups/", 30)},
			wantState: ExpiryInForce, wantDays: 30, wantID: "r",
		},
		{
			name: "the whole-bucket one-year rule of init covers and is a real rule", url: "s3://b/backups",
			rules:     []LifecycleRule{rule("bintrail-1yr-expiry", "", 365)},
			wantState: ExpiryInForce, wantDays: 365, wantID: "bintrail-1yr-expiry",
			check: func(t *testing.T, v SnapshotExpiry) {
				if !v.WholeBucket {
					t.Error("a rule with an empty prefix was not reported as covering the whole bucket")
				}
			},
		},
		{
			name: "snapshots at the bucket root are covered by a whole-bucket rule only", url: "s3://b",
			rules:     []LifecycleRule{rule("sub", "backups/", 7), rule("all", "", 365)},
			wantState: ExpiryInForce, wantDays: 365, wantID: "all",
		},
		{
			name: "a rule on a parent prefix covers", url: "s3://b/dbtrail/backups",
			rules:     []LifecycleRule{rule("parent", "dbtrail/", 60)},
			wantState: ExpiryInForce, wantDays: 60, wantID: "parent",
		},
		{
			name: "a rule on a child prefix does not cover", url: "s3://b/dbtrail",
			rules:     []LifecycleRule{rule("child", "dbtrail/backups/", 60)},
			wantState: ExpiryNone,
		},
		{
			name: "a rule on a sibling that shares characters does not cover", url: "s3://b/dbtrail",
			rules:     []LifecycleRule{rule("sibling", "dbtrail-archives/", 60)},
			wantState: ExpiryNone,
		},
		{
			name: "keys are case sensitive", url: "s3://b/Backups",
			rules:     []LifecycleRule{rule("lower", "backups/", 30)},
			wantState: ExpiryNone,
		},
		{
			// S3 matches a rule prefix by characters, so this rule does
			// delete the snapshots: saying "no rule" would hide it.
			name: "a rule prefix that stops mid-name still expires the snapshots", url: "s3://b/backups",
			rules:     []LifecycleRule{rule("partial", "back", 3)},
			wantState: ExpiryInForce, wantDays: 3, wantID: "partial",
		},
		{
			name: "a disabled rule protects nothing", url: "s3://b/backups",
			rules:     []LifecycleRule{{ID: "off", Enabled: false, Prefix: "backups/", Days: 30}},
			wantState: ExpiryNone,
		},
		{
			name: "two covering rules: the shorter one deletes first", url: "s3://b/backups",
			rules:     []LifecycleRule{rule("year", "", 365), rule("week", "backups/", 7), rule("month", "backups/", 30)},
			wantState: ExpiryInForce, wantDays: 7, wantID: "week",
		},
		{
			name: "a disabled short rule does not beat an enabled long one", url: "s3://b/backups",
			rules:     []LifecycleRule{{ID: "off", Enabled: false, Prefix: "backups/", Days: 1}, rule("year", "", 365)},
			wantState: ExpiryInForce, wantDays: 365, wantID: "year",
		},
		{
			name: "a rule that expires nothing (transition, old versions, unfinished uploads) is not an expiry", url: "s3://b/backups",
			rules:     []LifecycleRule{{ID: "abort", Enabled: true, Prefix: ""}},
			wantState: ExpiryNone,
		},
		{
			name: "a rule limited by tag or size is not counted, and is mentioned", url: "s3://b/backups",
			rules:     []LifecycleRule{{ID: "tagged", Enabled: true, Prefix: "backups/", Days: 30, Conditional: true}},
			wantState: ExpiryNone,
			check: func(t *testing.T, v SnapshotExpiry) {
				if v.Conditional != 1 {
					t.Errorf("Conditional = %d, want 1", v.Conditional)
				}
			},
		},
		{
			name: "a conditional rule on another prefix is not mentioned", url: "s3://b/backups",
			rules:     []LifecycleRule{{ID: "tagged", Enabled: true, Prefix: "other/", Days: 30, Conditional: true}},
			wantState: ExpiryNone,
			check: func(t *testing.T, v SnapshotExpiry) {
				if v.Conditional != 0 {
					t.Errorf("Conditional = %d, want 0", v.Conditional)
				}
			},
		},
		{
			name: "expiry on a fixed date that has not come", url: "s3://b/backups",
			rules:     []LifecycleRule{{ID: "dated", Enabled: true, Prefix: "backups/", Date: future}},
			wantState: ExpiryInForce, wantDays: 0,
			check: func(t *testing.T, v SnapshotExpiry) {
				if v.ExpiresOn != "2027-01-01" || v.DateRuleID != "dated" || v.DatePassed {
					t.Errorf("got %+v, want the date 2027-01-01 of rule dated, not passed", v)
				}
			},
		},
		{
			name: "expiry on a fixed date already behind", url: "s3://b/backups",
			rules:     []LifecycleRule{{ID: "dated", Enabled: true, Prefix: "backups/", Date: past}},
			wantState: ExpiryInForce,
			check: func(t *testing.T, v SnapshotExpiry) {
				if v.ExpiresOn != "2026-01-01" || !v.DatePassed {
					t.Errorf("got %+v, want the date 2026-01-01, passed", v)
				}
			},
		},
		{
			name: "a rule by age and a rule by date both reported", url: "s3://b/backups",
			rules:     []LifecycleRule{rule("age", "backups/", 30), {ID: "dated", Enabled: true, Prefix: "", Date: future}},
			wantState: ExpiryInForce, wantDays: 30, wantID: "age",
			check: func(t *testing.T, v SnapshotExpiry) {
				if v.ExpiresOn != "2027-01-01" {
					t.Errorf("ExpiresOn = %q, want 2027-01-01", v.ExpiresOn)
				}
			},
		},
		{
			name: "a negative or zero age is not an expiry", url: "s3://b/backups",
			rules:     []LifecycleRule{rule("zero", "backups/", 0), rule("neg", "backups/", -5)},
			wantState: ExpiryNone,
		},
		{
			name: "a rule that leaves old versions in place says so", url: "s3://b/backups",
			rules:     []LifecycleRule{rule("current-only", "backups/", 30)},
			wantState: ExpiryInForce, wantDays: 30, wantID: "current-only",
			check: func(t *testing.T, v SnapshotExpiry) {
				if !v.OldVersionsStay {
					t.Error("a rule with no expiry for old versions was not marked")
				}
			},
		},
		{
			name: "the generated rule expires old versions too", url: "s3://b/backups",
			rules:     []LifecycleRule{{ID: "gen", Enabled: true, Prefix: "backups/", Days: 30, NoncurrentDays: 30}},
			wantState: ExpiryInForce, wantDays: 30, wantID: "gen",
			check: func(t *testing.T, v SnapshotExpiry) {
				if v.OldVersionsStay {
					t.Error("a rule that expires old versions was marked as leaving them")
				}
			},
		},
		{
			name: "the mark follows the rule that wins, not another one", url: "s3://b/backups",
			rules: []LifecycleRule{{ID: "long", Enabled: true, Prefix: "", Days: 365, NoncurrentDays: 365},
				rule("short", "backups/", 7)},
			wantState: ExpiryInForce, wantDays: 7, wantID: "short",
			check: func(t *testing.T, v SnapshotExpiry) {
				if !v.OldVersionsStay {
					t.Error("the winning rule leaves old versions and the verdict does not say so")
				}
			},
		},
		{
			name: "a rule with no ID still counts", url: "s3://b/backups",
			rules:     []LifecycleRule{rule("", "backups/", 14)},
			wantState: ExpiryInForce, wantDays: 14, wantID: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := SnapshotExpiryVerdict(tt.url, tt.rules, expiryNow)
			if v.State != tt.wantState {
				t.Fatalf("State = %q, want %q (%+v)", v.State, tt.wantState, v)
			}
			if v.Days != tt.wantDays || v.RuleID != tt.wantID {
				t.Fatalf("Days/RuleID = %d/%q, want %d/%q", v.Days, v.RuleID, tt.wantDays, tt.wantID)
			}
			if v.Reason != "" || v.Error != "" {
				t.Fatalf("a verdict over rules that were read carries a read failure: %+v", v)
			}
			if tt.check != nil {
				tt.check(t, v)
			}
		})
	}
}

func TestSnapshotExpiryVerdictNamesTheDestination(t *testing.T) {
	v := SnapshotExpiryVerdict("s3://my-bucket/a/b/", nil, expiryNow)
	if v.Bucket != "my-bucket" || v.Prefix != "a/b" {
		t.Fatalf("Bucket/Prefix = %q/%q, want my-bucket and a/b", v.Bucket, v.Prefix)
	}
}

type mockLifecycleAPI struct {
	out *s3.GetBucketLifecycleConfigurationOutput
	err error
}

func (m mockLifecycleAPI) GetBucketLifecycleConfiguration(context.Context, *s3.GetBucketLifecycleConfigurationInput, ...func(*s3.Options)) (*s3.GetBucketLifecycleConfigurationOutput, error) {
	return m.out, m.err
}

func TestReadSnapshotExpiryClassifiesTheRead(t *testing.T) {
	ctx := context.Background()
	const url = "s3://b/backups"

	t.Run("a bucket with no lifecycle configuration has no rule: it was read", func(t *testing.T) {
		v := readSnapshotExpiry(ctx, mockLifecycleAPI{err: &smithy.GenericAPIError{Code: "NoSuchLifecycleConfiguration"}}, url, expiryNow)
		if v.State != ExpiryNone || v.Error != "" || v.Reason != "" {
			t.Fatalf("got %+v, want none with no read failure", v)
		}
	})
	t.Run("an empty answer has no rule", func(t *testing.T) {
		v := readSnapshotExpiry(ctx, mockLifecycleAPI{out: &s3.GetBucketLifecycleConfigurationOutput{}}, url, expiryNow)
		if v.State != ExpiryNone {
			t.Fatalf("got %+v, want none", v)
		}
	})
	t.Run("a nil answer with no error is not a rule-free bucket", func(t *testing.T) {
		v := readSnapshotExpiry(ctx, mockLifecycleAPI{}, url, expiryNow)
		if v.State != ExpiryUnreadable {
			t.Fatalf("got %+v, want unreadable", v)
		}
	})
	for _, code := range []string{"AccessDenied", "AccessDeniedException", "AllAccessDisabled"} {
		t.Run("denied ("+code+") is unreadable, never no rule", func(t *testing.T) {
			v := readSnapshotExpiry(ctx, mockLifecycleAPI{err: &smithy.GenericAPIError{Code: code, Message: "no"}}, url, expiryNow)
			if v.State != ExpiryUnreadable || v.Reason != ReasonDenied || v.Error == "" {
				t.Fatalf("got %+v, want unreadable/denied with the error", v)
			}
		})
	}
	for _, code := range []string{"NotImplemented", "MethodNotAllowed", "XNotImplemented"} {
		t.Run("a store that does not implement the call ("+code+") is unreadable", func(t *testing.T) {
			v := readSnapshotExpiry(ctx, mockLifecycleAPI{err: &smithy.GenericAPIError{Code: code}}, url, expiryNow)
			if v.State != ExpiryUnreadable || v.Reason != ReasonUnsupported {
				t.Fatalf("got %+v, want unreadable/unsupported", v)
			}
		})
	}
	t.Run("a network error is unreadable", func(t *testing.T) {
		v := readSnapshotExpiry(ctx, mockLifecycleAPI{err: errors.New("dial tcp: i/o timeout")}, url, expiryNow)
		if v.State != ExpiryUnreadable || v.Reason != ReasonError || !strings.Contains(v.Error, "i/o timeout") {
			t.Fatalf("got %+v, want unreadable/error", v)
		}
	})
	t.Run("a deadline is unreadable", func(t *testing.T) {
		v := readSnapshotExpiry(ctx, mockLifecycleAPI{err: fmt.Errorf("operation error S3: %w", context.DeadlineExceeded)}, url, expiryNow)
		if v.State != ExpiryUnreadable || v.Reason != ReasonError {
			t.Fatalf("got %+v, want unreadable/error", v)
		}
	})
	t.Run("another API error (the bucket is gone) is unreadable", func(t *testing.T) {
		v := readSnapshotExpiry(ctx, mockLifecycleAPI{err: &smithy.GenericAPIError{Code: "NoSuchBucket"}}, url, expiryNow)
		if v.State != ExpiryUnreadable || v.Reason != ReasonError {
			t.Fatalf("got %+v, want unreadable/error", v)
		}
	})
	t.Run("an unreadable verdict still names the destination", func(t *testing.T) {
		v := readSnapshotExpiry(ctx, mockLifecycleAPI{err: errors.New("boom")}, "s3://b/backups/", expiryNow)
		if v.Bucket != "b" || v.Prefix != "backups" {
			t.Fatalf("got %+v", v)
		}
	})
}

func TestLifecycleRulesFromSDK(t *testing.T) {
	date := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	in := []types.LifecycleRule{
		{ID: aws.String("filter-prefix"), Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{Prefix: aws.String("backups/")}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(30)}},
		{ID: aws.String("legacy-prefix"), Status: types.ExpirationStatusEnabled,
			Prefix: aws.String("old/"), Expiration: &types.LifecycleExpiration{Days: aws.Int32(10)}},
		{ID: aws.String("no-filter"), Status: types.ExpirationStatusEnabled,
			Expiration: &types.LifecycleExpiration{Days: aws.Int32(365)}},
		{ID: aws.String("disabled"), Status: types.ExpirationStatusDisabled,
			Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)}},
		{ID: aws.String("tag"), Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{Tag: &types.Tag{Key: aws.String("k"), Value: aws.String("v")}}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(5)}},
		{ID: aws.String("size"), Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{Prefix: aws.String("backups/"), ObjectSizeGreaterThan: aws.Int64(1 << 20)}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(5)}},
		{ID: aws.String("small"), Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{ObjectSizeLessThan: aws.Int64(10)}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(5)}},
		{ID: aws.String("and-prefix-only"), Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{And: &types.LifecycleRuleAndOperator{Prefix: aws.String("backups/")}}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(20)}},
		{ID: aws.String("and-tags"), Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{And: &types.LifecycleRuleAndOperator{Prefix: aws.String("backups/"),
				Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}}}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(20)}},
		{ID: aws.String("and-size"), Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{And: &types.LifecycleRuleAndOperator{Prefix: aws.String("backups/"),
				ObjectSizeLessThan: aws.Int64(100)}}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(20)}},
		{ID: aws.String("generated"), Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{Prefix: aws.String("backups/")}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(30)},
			NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(30)}},
		{ID: aws.String("noncurrent-only"), Status: types.ExpirationStatusEnabled,
			Filter:                      &types.LifecycleRuleFilter{Prefix: aws.String("")},
			NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(3)}},
		{ID: aws.String("abort-only"), Status: types.ExpirationStatusEnabled,
			Filter:                         &types.LifecycleRuleFilter{Prefix: aws.String("")},
			AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(7)}},
		{ID: aws.String("transition-only"), Status: types.ExpirationStatusEnabled,
			Filter:      &types.LifecycleRuleFilter{Prefix: aws.String("")},
			Transitions: []types.Transition{{Days: aws.Int32(30), StorageClass: types.TransitionStorageClassGlacier}}},
		{ID: aws.String("delete-marker-only"), Status: types.ExpirationStatusEnabled,
			Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("")},
			Expiration: &types.LifecycleExpiration{ExpiredObjectDeleteMarker: aws.Bool(true)}},
		{ID: aws.String("dated"), Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{Prefix: aws.String("backups/")}, Expiration: &types.LifecycleExpiration{Date: aws.Time(date)}},
		{Status: types.ExpirationStatusEnabled, Expiration: &types.LifecycleExpiration{Days: aws.Int32(9)}},
		{ID: aws.String("lowercase-status"), Status: types.ExpirationStatus("enabled"),
			Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(2)}},
	}
	want := []LifecycleRule{
		{ID: "filter-prefix", Enabled: true, Prefix: "backups/", Days: 30},
		{ID: "legacy-prefix", Enabled: true, Prefix: "old/", Days: 10},
		{ID: "no-filter", Enabled: true, Days: 365},
		{ID: "disabled", Enabled: false, Days: 1},
		{ID: "tag", Enabled: true, Days: 5, Conditional: true},
		{ID: "size", Enabled: true, Prefix: "backups/", Days: 5, Conditional: true},
		{ID: "small", Enabled: true, Days: 5, Conditional: true},
		{ID: "and-prefix-only", Enabled: true, Prefix: "backups/", Days: 20},
		{ID: "and-tags", Enabled: true, Prefix: "backups/", Days: 20, Conditional: true},
		{ID: "and-size", Enabled: true, Prefix: "backups/", Days: 20, Conditional: true},
		{ID: "generated", Enabled: true, Prefix: "backups/", Days: 30, NoncurrentDays: 30},
		{ID: "noncurrent-only", Enabled: true, NoncurrentDays: 3},
		{ID: "abort-only", Enabled: true},
		{ID: "transition-only", Enabled: true},
		{ID: "delete-marker-only", Enabled: true},
		{ID: "dated", Enabled: true, Prefix: "backups/", Date: date},
		{Enabled: true, Days: 9},
		// S3 answers "Enabled" exactly; anything else is not read as on.
		{ID: "lowercase-status", Enabled: false, Days: 2},
	}
	got := lifecycleRulesFromSDK(in)
	if len(got) != len(want) {
		t.Fatalf("got %d rules, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("rule %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestSnapshotExpiryCheck(t *testing.T) {
	hours := func(n int) time.Duration { return time.Duration(n) * time.Hour }
	tests := []struct {
		name       string
		v          SnapshotExpiry
		every      time.Duration
		wantStatus CheckStatus
		wantDetail []string
		wantFix    string
	}{
		{
			name:       "unreadable for a missing permission is SKIP and names the permission",
			v:          SnapshotExpiry{State: ExpiryUnreadable, Bucket: "b", Prefix: "backups", Reason: ReasonDenied, Error: "api error AccessDenied"},
			wantStatus: StatusSkip, wantDetail: []string{"could not read the lifecycle rules of bucket \"b\"", "AccessDenied"},
			wantFix: "s3:GetBucketLifecycleConfiguration",
		},
		{
			name:       "unreadable on a store without the call is SKIP",
			v:          SnapshotExpiry{State: ExpiryUnreadable, Bucket: "b", Prefix: "backups", Reason: ReasonUnsupported, Error: "api error NotImplemented"},
			wantStatus: StatusSkip, wantDetail: []string{"does not answer"},
		},
		{
			name:       "no rule warns that the bucket grows without limit",
			v:          SnapshotExpiry{State: ExpiryNone, Bucket: "b", Prefix: "backups"},
			wantStatus: StatusWarn, wantDetail: []string{"no rule in bucket \"b\" expires the snapshots under backups/", "grows without limit"},
			wantFix: "never sets a bucket rule",
		},
		{
			name:       "no rule with a schedule says the rate",
			v:          SnapshotExpiry{State: ExpiryNone, Bucket: "b", Prefix: "backups"},
			every:      hours(6),
			wantStatus: StatusWarn, wantDetail: []string{"about 120 snapshots every 30 days"},
		},
		{
			name:       "no rule mentions the rules it did not count",
			v:          SnapshotExpiry{State: ExpiryNone, Bucket: "b", Prefix: "backups", Conditional: 2},
			wantStatus: StatusWarn, wantDetail: []string{"2 rules on this prefix apply only to objects with a tag or a size limit"},
		},
		{
			name:       "no rule at the bucket root",
			v:          SnapshotExpiry{State: ExpiryNone, Bucket: "b"},
			wantStatus: StatusWarn, wantDetail: []string{"expires the snapshots at the root of the bucket"},
		},
		{
			name:       "a rule in force passes with its age",
			v:          SnapshotExpiry{State: ExpiryInForce, Bucket: "b", Prefix: "backups", Days: 30, RuleID: "dbtrail-backups-expire-30d"},
			every:      hours(24),
			wantStatus: StatusPass, wantDetail: []string{"rule \"dbtrail-backups-expire-30d\"", "after 30 days"},
		},
		{
			name:       "one day reads as one day",
			v:          SnapshotExpiry{State: ExpiryInForce, Bucket: "b", Prefix: "backups", Days: 1, RuleID: "r"},
			wantStatus: StatusPass, wantDetail: []string{"after 1 day"},
		},
		{
			name:       "the whole-bucket rule passes and says its reach",
			v:          SnapshotExpiry{State: ExpiryInForce, Bucket: "b", Prefix: "backups", Days: 365, RuleID: "bintrail-1yr-expiry", WholeBucket: true},
			wantStatus: StatusPass, wantDetail: []string{"after 365 days", "whole bucket"},
		},
		{
			name:       "a rule shorter than the schedule warns",
			v:          SnapshotExpiry{State: ExpiryInForce, Bucket: "b", Prefix: "backups", Days: 1, RuleID: "r"},
			every:      hours(48),
			wantStatus: StatusWarn, wantDetail: []string{"after 1 day", "a snapshot every 2 days", "moments with no snapshot"},
			wantFix: "at least 3 days",
		},
		{
			name:       "a rule equal to the schedule warns",
			v:          SnapshotExpiry{State: ExpiryInForce, Bucket: "b", Prefix: "backups", Days: 7, RuleID: "r"},
			every:      hours(7 * 24),
			wantStatus: StatusWarn, wantDetail: []string{"moments with no snapshot"},
			wantFix: "at least 8 days",
		},
		{
			name:       "with no schedule given the age is not compared",
			v:          SnapshotExpiry{State: ExpiryInForce, Bucket: "b", Prefix: "backups", Days: 1, RuleID: "r"},
			wantStatus: StatusPass, wantDetail: []string{"after 1 day"},
		},
		{
			name:       "a fixed date ahead passes and says the date",
			v:          SnapshotExpiry{State: ExpiryInForce, Bucket: "b", Prefix: "backups", ExpiresOn: "2027-01-01", DateRuleID: "dated"},
			wantStatus: StatusPass, wantDetail: []string{"on 2027-01-01", "whatever its age"},
		},
		{
			name:       "a fixed date behind warns",
			v:          SnapshotExpiry{State: ExpiryInForce, Bucket: "b", Prefix: "backups", ExpiresOn: "2026-01-01", DateRuleID: "dated", DatePassed: true},
			wantStatus: StatusWarn, wantDetail: []string{"since 2026-01-01"},
		},
		{
			name:       "a good age rule beside a passed date still warns",
			v:          SnapshotExpiry{State: ExpiryInForce, Bucket: "b", Prefix: "backups", Days: 30, RuleID: "r", ExpiresOn: "2026-01-01", DateRuleID: "dated", DatePassed: true},
			wantStatus: StatusWarn, wantDetail: []string{"after 30 days", "since 2026-01-01"},
		},
		{
			name:       "a rule that leaves old versions passes and says what it leaves",
			v:          SnapshotExpiry{State: ExpiryInForce, Bucket: "b", Prefix: "backups", Days: 30, RuleID: "r", OldVersionsStay: true},
			wantStatus: StatusPass, wantDetail: []string{"after 30 days", "if the bucket keeps versions, the old ones stay"},
		},
		{
			name:       "in force with no age and no date is SKIP, never a green line with no words",
			v:          SnapshotExpiry{State: ExpiryInForce, Bucket: "b", Prefix: "backups"},
			wantStatus: StatusSkip, wantDetail: []string{"could not tell"},
		},
		{
			name:       "a state this build does not know is SKIP, never a claim",
			v:          SnapshotExpiry{State: "something-new", Bucket: "b"},
			wantStatus: StatusSkip, wantDetail: []string{"could not"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := snapshotExpiryCheck(tt.v, tt.every)
			if got.Name != SnapshotExpiryCheckName {
				t.Fatalf("Name = %q", got.Name)
			}
			if got.Status != tt.wantStatus {
				t.Fatalf("Status = %q, want %q (detail: %s)", got.Status, tt.wantStatus, got.Detail)
			}
			for _, want := range tt.wantDetail {
				if !strings.Contains(got.Detail, want) {
					t.Fatalf("Detail %q does not contain %q", got.Detail, want)
				}
			}
			if !strings.Contains(got.Remediation, tt.wantFix) {
				t.Fatalf("Remediation %q does not contain %q", got.Remediation, tt.wantFix)
			}
			if strings.ContainsRune(got.Detail+got.Remediation, '\u2014') {
				t.Fatalf("the text carries an em dash: %q / %q", got.Detail, got.Remediation)
			}
		})
	}
}

// TestSnapshotExpiryCheckNeverFails pins the advisory contract over a grid:
// no state, age or schedule may return StatusFail. A bucket with no expiry
// rule is a legitimate choice, and doctor must stay safe as a smoke test.
func TestSnapshotExpiryCheckNeverFails(t *testing.T) {
	var states []SnapshotExpiry
	for _, st := range []ExpiryState{ExpiryInForce, ExpiryNone, ExpiryUnreadable, "", "unknown"} {
		for _, days := range []int{0, 1, 30, 365} {
			for _, reason := range []string{"", ReasonDenied, ReasonUnsupported, ReasonError} {
				for _, dated := range []bool{false, true} {
					v := SnapshotExpiry{State: st, Bucket: "b", Prefix: "p", Days: days, Reason: reason, Conditional: days % 2}
					if dated {
						v.ExpiresOn, v.DatePassed = "2026-01-01", true
					}
					states = append(states, v)
				}
			}
		}
	}
	for _, v := range states {
		for _, every := range []time.Duration{0, time.Minute, 24 * time.Hour, 400 * 24 * time.Hour} {
			if got := snapshotExpiryCheck(v, every); got.Status == StatusFail {
				t.Fatalf("state %+v every %v returned StatusFail: the check is advisory", v, every)
			}
		}
	}
}

func TestCheckSnapshotExpiryBadURL(t *testing.T) {
	for _, url := range []string{"not-an-s3-url", "s3://", "/var/backups", ""} {
		got := CheckSnapshotExpiry(context.Background(), url, "", 0)
		if got.Status != StatusSkip || !strings.Contains(got.Detail, "not an s3://") {
			t.Fatalf("%q: got %+v, want SKIP naming the URL shape", url, got)
		}
	}
}
