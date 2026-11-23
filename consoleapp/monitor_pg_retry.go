package consoleapp

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// pgOutageBackoff is the retry delay while a PostgreSQL source cannot be
// reached: monitorBackoffBase doubling to monitorBackoffCap, as the crash-loop
// policy, and then the cap for as long as the outage lasts. Unlike that policy
// it has no give-up. It holds no clock, so a test can drive it past six hours.
type pgOutageBackoff struct {
	attempt int
	// since is the first failure of the current outage; zero means none.
	since time.Time
}

// failed records one run that ended with the source unreachable, and returns
// how long to wait and how long the outage has lasted. A run that streamed
// longer than monitorHealthyReset ended the previous outage.
func (b *pgOutageBackoff) failed(started, now time.Time) (delay, down time.Duration) {
	if now.Sub(started) > monitorHealthyReset {
		b.attempt, b.since = 0, time.Time{}
	}
	if b.since.IsZero() {
		b.since = now
	}
	down = now.Sub(b.since)
	// Stops shifting at the cap: an unbounded shift overflows into a negative
	// delay (see crashLoopPolicy).
	shifted := monitorBackoffBase << b.attempt
	if shifted <= 0 || shifted >= monitorBackoffCap {
		return monitorBackoffCap, down
	}
	b.attempt++
	return shifted, down
}

// progressIfConnected is the PostgreSQL checkpoint tick. The tick fires while
// the capturer is still connecting, so on its own it would show a source that
// is down as running, clear the error, and end the capture_stopped outage.
func (j *monitorJob) progressIfConnected() {
	if j.reachedSource() {
		j.progress()
	}
}

// reachedSource reports whether this run's stream has opened the source
// connection.
func (j *monitorJob) reachedSource() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.sourceConnected
}

// pgRunThroughOutages wraps one PostgreSQL capture run so that a source that
// cannot be reached is retried without limit (#3117). The MySQL syncer does
// the same for a dropped connection; here each attempt is a whole run, so the
// resume path (position, cleanup, re-delivery) is the one every restart uses.
//
// The supervisor's run loop never sees these failures, so they do not count
// toward monitorGiveUpAfter. Every other error is returned to it unchanged and
// keeps the give-up: credentials, a dropped publication, a lost slot, the index.
// So does an unreachable source that was never captured (no saved position, and
// no run of this job reached it): that can be a mistyped address, which waiting
// does not fix. MySQL likewise reconnects without limit only once it streamed.
//
// The job shows the outage the whole time: failed and retrying between
// attempts, which capture_stopped reads as a capture that is down and alerts
// on, and pending during each attempt, as the supervisor's own retries do.
func pgRunThroughOutages(job *monitorJob, e console.ServerEntry, run func(context.Context) error) func(context.Context) error {
	// Across the supervisor's restarts of this job's stream.
	var reached bool
	return func(ctx context.Context) error {
		var backoff pgOutageBackoff
		for {
			started := time.Now()
			err := run(ctx)
			reached = reached || job.reachedSource()
			if err == nil || ctx.Err() != nil || !pgcapture.IsSourceUnreachable(err) {
				return err
			}
			if !reached && !pgcapture.SourceWasCaptured(err) {
				return err
			}
			delay, down := backoff.failed(started, time.Now())
			scrubbed := config.ScrubDSNError(err, e.SourceDSN, e.DSN)
			slog.Warn("the PostgreSQL source cannot be reached; capture keeps retrying and resumes by itself when it is back",
				"server", e.Name, "entry", e.ID, "down_for", down.Round(time.Second), "delay", delay, "error", scrubbed)
			job.fail(pgOutageMessage(scrubbed, down), "", true)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
			job.set("pending", "")
		}
	}
}

// pgOutageMessage is the server's state during an outage. It ends in
// "(retrying)", as every failure the supervisor will retry does.
//
// Waiting cannot fix a source that moved, so it says what does. Edit works
// between attempts, where saving also starts capture on the new address.
// During an attempt the server refuses a new source until capture is stopped,
// hence the second half.
func pgOutageMessage(scrubbed string, down time.Duration) string {
	lasted := ""
	if down >= time.Minute {
		lasted = " for " + humanDown(down)
	}
	return fmt.Sprintf("the PostgreSQL source cannot be reached%s: %s. Capture resumes by itself, from where it stopped, when the source is back. If its address changed, press Edit on this server under Servers, enter the new Source host and press Save; if Save is refused, press Stop first (retrying)", lasted, scrubbed)
}
