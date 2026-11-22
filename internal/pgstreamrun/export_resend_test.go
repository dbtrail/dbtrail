package pgstreamrun

import "time"

// SetMaxTxnRowsForTest shrinks the row cap past which a source transaction
// is made visible in parts (maxTxnRows); restore puts it back.
func SetMaxTxnRowsForTest(n int64) (restore func()) {
	prev := maxTxnRows
	maxTxnRows = n
	return func() { maxTxnRows = prev }
}

// SetResendCheckTimeoutForTest changes resendCheckTimeout; restore puts it
// back.
func SetResendCheckTimeoutForTest(d time.Duration) (restore func()) {
	prev := resendCheckTimeout
	resendCheckTimeout = d
	return func() { resendCheckTimeout = prev }
}
