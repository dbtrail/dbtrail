package console

// The console pages a message sends people to, by the name the sidebar
// shows. A message names a page through these, never as typed text: pages
// merge and get renamed, and a name typed in one message and missed in
// another sends people to a page that is not there. One message also trims
// its own page suffix off an error before appending it again
// (CheckBackupSchedule), which only works while both sides spell it the
// same.
const (
	// PageSnapshots is the one page Backups, Verification and Backup
	// settings became (#1573). A message that needs to be more precise
	// than the page names one of its sections in words ("under Set when
	// DBTrail starts"), which is what naming a different page used to do.
	PageSnapshots = "Snapshots"
	// PageConnect is where a reader downloads a DuckDB schema or exports
	// to Iceberg, on every console (#1573).
	PageConnect = "Connect AI"
)

// onPage is the suffix an error carries to say where it is fixed, as in
// " (Snapshots page)".
func onPage(name string) string { return " (" + name + " page)" }
