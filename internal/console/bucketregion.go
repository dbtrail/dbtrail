package console

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/dbtrail/dbtrail/internal/storage"
	"github.com/dbtrail/dbtrail/internal/views"
)

// negativeRegionTTL bounds how long a FAILED detection is remembered. Successes
// never expire (a bucket's region is fixed for its lifetime); a failure is not a
// property of the bucket at all, and the cheap causes are transient — a
// credential refresh, a network blip, or the operator closing the download tab,
// which cancels the request context. Caching those forever would leave the
// daemon and the files it hands out disagreeing until someone restarts it.
const negativeRegionTTL = 5 * time.Minute

type bucketRegionEntry struct {
	region   string
	detected bool
	at       time.Time
}

// archiveRegion resolves the region to pin for the layout in `in` — which is
// the downloadable views.sql, and separately the SQL panel's own DuckDB
// session. Separately, not identically: the download resolves its archives
// portably (S3 wherever registered) and the panel local-first, so on a daemon
// holding local copies the file names a bucket the panel does not, and the
// panel legitimately stays unpinned.
//
// It exists because those two used to pin NOTHING while the daemon's own
// archive reads pin a DETECTED bucket region (#511), so the file described a
// different read than this process performs and a store that checks the signing
// region answered 403 or 301 PermanentRedirect to the recipient.
//
// It pins ONLY what was actually detected, and only when every bucket agrees.
// A guess is worse than silence here, which is the opposite of the read path's
// trade-off: s3:GetBucketLocation is deliberately absent from bintrail's
// documented minimal IAM policy, so the fallback is the COMMON case, and where
// this file pins nothing the reader's own credential chain resolves the right
// region unaided. Pinning an ambient region we never confirmed would override
// a correct configuration on someone else's machine, hours later, with nothing
// pointing back here.
//
// Returns ("", true) only when two DETECTIONS disagree — never when one bucket
// merely could not be asked. One secret and one s3_region cannot name two
// regions, and the caller renders that as a stated fact, so it must be one.
func (s *Server) archiveRegion(ctx context.Context, in views.Input) (region string, ambiguous bool) {
	buckets := layoutBuckets(in)
	if len(buckets) == 0 {
		return "", false
	}
	var (
		cfg    aws.Config
		loaded bool
		seen   string
		missed bool
	)
	for _, b := range buckets {
		// A bucket with its own store (#1575) is signed by its scoped secret,
		// which always names a region, so it has no say in this pin. Asking
		// would go to the ambient endpoint (AWS, for a MinIO bucket), and a
		// cached answer from before the store was set would keep speaking for
		// it.
		if _, routed := storage.BucketStoreFor(b); routed {
			continue
		}
		r, ok := s.cachedBucketRegion(ctx, &cfg, &loaded, b)
		if !ok {
			// Contributes neither a pin nor an ambiguity claim: an
			// undetectable bucket is not evidence that the regions differ.
			missed = true
			continue
		}
		switch {
		case seen == "":
			seen = r
		case r != seen:
			return "", true
		}
	}
	if missed {
		// Partial knowledge is not a basis for pinning: the value would be
		// right for the buckets we asked about and a guess for the rest.
		return "", false
	}
	return seen, false
}

// cachedBucketRegion memoizes detection per bucket. buildViewsInput runs on
// every SQL panel query, so this must not put a network round trip on that path
// in the steady state.
//
// The mutex is held ONLY around the map, never across the RPC: sync.Mutex is
// not context-aware, so a goroutine blocked acquiring it cannot be released by
// the SQL panel's setup deadline, and one hung GetBucketLocation on the
// deadline-free download path would stall unrelated requests while the panel's
// single-flight latch turned that into 429s for everyone. Two callers racing
// the same bucket just make the same call twice, which is harmless.
//
// cfg is loaded lazily and at most once per call, and only when some bucket
// actually needs asking — LoadAWSConfig reads ~/.aws/config from disk and can
// pay an IMDS timeout, which does not belong on a fully cached path.
func (s *Server) cachedBucketRegion(ctx context.Context, cfg *aws.Config, loaded *bool, bucket string) (string, bool) {
	s.bucketRegionMu.Lock()
	e, ok := s.bucketRegions[bucket]
	s.bucketRegionMu.Unlock()
	if ok && (e.detected || time.Since(e.at) < negativeRegionTTL) {
		return e.region, e.detected
	}

	if !*loaded {
		c, err := storage.LoadAWSConfig(ctx, "")
		if err != nil {
			// The file will pin no region. Logged because the file is what
			// leaves the host and the log is what stays: without this, "no
			// REGION clause" has no explanation anywhere, and it is the one
			// case the file itself cannot describe.
			slog.Warn("could not resolve an AWS region for this server's S3 layout; no region will be pinned",
				"bucket", bucket, "error", err)
			// Cached like any other failure, and for the same reason its
			// sibling below is: the reachable cause here is a malformed
			// ~/.aws/config or a missing AWS_PROFILE, which is persistent, and
			// the endpoint-config class already 502s upstream. Re-reading a
			// broken file on every SQL panel query is the cost the
			// memoization exists to avoid; the TTL is what lets a fixed
			// config heal without a restart.
			s.rememberBucketRegion(bucket, "", false)
			return "", false
		}
		*cfg, *loaded = c, true
	}

	r, detected := storage.DetectBucketRegion(ctx, *cfg, bucket)
	if !detected {
		// Once per bucket per TTL, not per request: the operator's remedy is
		// to grant s3:GetBucketLocation or accept that readers resolve their
		// own region, and neither is helped by a line on every download.
		// Names no consumer: both the download and the SQL panel reach this,
		// and telling a panel user about a file they never asked for is worse
		// than saying less.
		slog.Warn("S3 bucket region not detected (grant s3:GetBucketLocation to pin it); no region will be pinned",
			"bucket", bucket)
	}
	s.rememberBucketRegion(bucket, r, detected)
	return r, detected
}

func (s *Server) rememberBucketRegion(bucket, region string, detected bool) {
	s.bucketRegionMu.Lock()
	defer s.bucketRegionMu.Unlock()
	if s.bucketRegions == nil {
		s.bucketRegions = map[string]bucketRegionEntry{}
	}
	s.bucketRegions[bucket] = bucketRegionEntry{region: region, detected: detected, at: time.Now()}
}

// layoutBuckets returns the distinct S3 buckets the rendered file will read, in
// a stable order. Both halves count: archives and baselines can live in
// different buckets, and the views read both.
func layoutBuckets(in views.Input) []string {
	var out []string
	seen := map[string]bool{}
	add := func(p string) {
		bucket, _, err := storage.ParseS3URL(p)
		if err != nil || bucket == "" || seen[bucket] {
			return
		}
		seen[bucket] = true
		out = append(out, bucket)
	}
	for _, src := range in.ArchiveSources {
		add(src)
	}
	for _, b := range in.Baselines {
		add(b.Path)
	}
	return out
}
