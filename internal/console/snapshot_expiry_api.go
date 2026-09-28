package console

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// GET /api/backup-settings/servers/{id}/expiry: whether a rule in the
// bucket expires this server's old snapshots (#1680).
//
// The console generates that rule and the operator applies it (#1622); this
// is the read that says whether it is there. It answers with one of three
// states (doctor.SnapshotExpiry), and "could not read" is its own: it is
// never sent as "no rule".
//
// It is a route of its own, asked for by the settings row AFTER the row is
// drawn, and never a field of GET /api/backup-settings: that listing is
// IO-free on purpose, and one bucket that does not answer must not hold the
// page. Each read is bounded (snapshotExpiryBudget) and its answer kept for
// a short while, so a page that redraws its rows asks S3 once.

// snapshotExpiryNotApplicable is the answer for a server with no S3
// destination of its own: there is no bucket rule to read.
const snapshotExpiryNotApplicable doctor.ExpiryState = "not_applicable"

const (
	// snapshotExpiryBudget bounds one read of a bucket's rules.
	snapshotExpiryBudget = 5 * time.Second
	// snapshotExpiryTTL is how long an answer that WAS read is reused. A
	// rule the operator has just applied shows within this.
	snapshotExpiryTTL = 30 * time.Second
	// snapshotExpiryRetryTTL is the same for a read that failed: short, so
	// a permission just granted shows soon, and long enough that a bucket
	// that hangs is not asked again by every redraw.
	snapshotExpiryRetryTTL = 10 * time.Second
)

// snapshotExpiryCache keeps the last answer per destination. The zero value
// is ready. No lock is held across the read: a bucket that hangs holds up
// its own caller only.
type snapshotExpiryCache struct {
	mu      sync.Mutex
	answers map[string]snapshotExpiryAnswer
	// read and now are replaced by tests; nil means the real ones.
	read func(ctx context.Context, snapshotS3 string) doctor.SnapshotExpiry
	now  func() time.Time
}

type snapshotExpiryAnswer struct {
	v  doctor.SnapshotExpiry
	at time.Time
}

func snapshotExpiryFresh(a snapshotExpiryAnswer, now time.Time) bool {
	ttl := snapshotExpiryTTL
	if a.v.State == doctor.ExpiryUnreadable {
		ttl = snapshotExpiryRetryTTL
	}
	return now.Sub(a.at) < ttl
}

// get answers for one s3:// destination, from the cache while it is fresh.
func (c *snapshotExpiryCache) get(ctx context.Context, snapshotS3 string) doctor.SnapshotExpiry {
	now, read := time.Now, func(ctx context.Context, url string) doctor.SnapshotExpiry {
		return doctor.ReadSnapshotExpiry(ctx, url, "")
	}
	if c.now != nil {
		now = c.now
	}
	if c.read != nil {
		read = c.read
	}
	c.mu.Lock()
	a, ok := c.answers[snapshotS3]
	c.mu.Unlock()
	if ok && snapshotExpiryFresh(a, now()) {
		return a.v
	}
	// Detached from the request: a tab that closes mid-read must not leave
	// "the request was cancelled" in the cache as the state of the bucket.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotExpiryBudget)
	defer cancel()
	v := read(ctx, snapshotS3)
	at := now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.answers == nil {
		c.answers = map[string]snapshotExpiryAnswer{}
	}
	// Destinations come from the registry, so the map stays small; an
	// answer past its time is dropped so an edited destination does not
	// stay behind.
	for k, old := range c.answers {
		if !snapshotExpiryFresh(old, at) {
			delete(c.answers, k)
		}
	}
	c.answers[snapshotS3] = snapshotExpiryAnswer{v: v, at: at}
	return v
}

func (s *Server) handleSnapshotExpiry(w http.ResponseWriter, r *http.Request) {
	if s.cm.reg == nil {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	entry, ok := s.cm.reg.Get(r.PathValue("id"))
	if !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	// The entry's OWN destination, the one the settings row hands a rule
	// out for. The daemon default is shared by every server.
	if entry.BaselineS3 == "" {
		writeJSON(w, http.StatusOK, doctor.SnapshotExpiry{State: snapshotExpiryNotApplicable})
		return
	}
	writeJSON(w, http.StatusOK, s.snapshotExpiry.get(r.Context(), entry.BaselineS3))
}
