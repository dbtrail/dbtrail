package pgcapture

import "time"

// PartitionsRetryAfter is partitionsRetryAfter, for the tests of the retry.
const PartitionsRetryAfter = partitionsRetryAfter

// SetClockForTest replaces the clock the decoder dates a failed read of a
// relation's partitions by.
func SetClockForTest(d *Decoder, now func() time.Time) { d.now = now }

// SetSlotNoticesForTest shortens how often AnchorOnSlot's own creation of a
// slot says it is still waiting, and how long a wait for another session's
// creation lasts before it is warned of.
func SetSlotNoticesForTest(d time.Duration) (restore func()) {
	prevEvery, prevWarn := slotNoticeEvery, slotWaitWarnAfter
	slotNoticeEvery, slotWaitWarnAfter = d, d
	return func() { slotNoticeEvery, slotWaitWarnAfter = prevEvery, prevWarn }
}

// WithCaptureBelowFull turns off the decoder's gate on tables below REPLICA
// IDENTITY FULL, for the tests of the code that stores an earlier row in
// part, which no table reaches outside them.
var WithCaptureBelowFull = withCaptureBelowFull

// CaptureBelowFull is cfg with the same gate off, for the capturer's tests of
// that code.
func CaptureBelowFull(cfg Config) Config {
	cfg.captureBelowFull = true
	return cfg
}
