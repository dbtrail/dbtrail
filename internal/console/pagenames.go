package console

// The console pages a message sends people to, by the name the sidebar
// shows. A message names a page through these, never as typed text: the
// backup pages are about to merge under one name, and a name typed in one
// message and missed in another sends people to a page that is not there.
// One message also trims its own page suffix off an error before appending
// it again (backupScheduleRunnable), which only works while both sides spell
// it the same.
const (
	PageBackups        = "Backups"
	PageBackupSettings = "Backup settings"
)

// onPage is the suffix an error carries to say where it is fixed, as in
// " (Backup settings page)".
func onPage(name string) string { return " (" + name + " page)" }
