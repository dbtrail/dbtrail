//go:build !darwin && !linux

package consoleapp

// On a platform where a folder's owner cannot be read this way, nothing is
// proven, so nothing outside the folder in use is removed.
func ownFolder(string) string   { return "its owner cannot be checked on this platform" }
func heldInPlace(string) string { return "" }
