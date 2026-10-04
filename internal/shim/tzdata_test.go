package shim

// The zone database is embedded by consoleapp, the package the console
// binaries are built from. These tests resolve zone names without it, so
// embed it here too: they must not depend on the host's zoneinfo.
import _ "time/tzdata"
