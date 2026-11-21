package pgcapture

import "time"

// PartitionsRetryAfter is partitionsRetryAfter, for the tests of the retry.
const PartitionsRetryAfter = partitionsRetryAfter

// SetClockForTest replaces the clock the decoder dates a failed read of a
// relation's partitions by.
func SetClockForTest(d *Decoder, now func() time.Time) { d.now = now }
