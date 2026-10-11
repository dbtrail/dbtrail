//go:build !linux

package sqlsandbox

// No current resident size to read here without cgo; 0 is "not known", and a
// worker is then replaced by its count of statements alone.
func residentBytes() int64 { return 0 }
