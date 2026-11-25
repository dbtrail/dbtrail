package consoleapp

import (
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/console"
)

// warnPGPortNotServedAtBoot says once at boot, per PostgreSQL server, that the
// PostgreSQL port does not serve it (0.140.0: it serves a server only when it
// can forward). A warning and nothing more: capture, snapshots and the web
// interface of that server go on, and an install that upgrades with such a
// server saved starts as before. The client that connects gets the same
// reason at login. It returns how many it named.
func warnPGPortNotServedAtBoot(reg *console.Registry, log *slog.Logger) int {
	if reg == nil {
		return 0
	}
	if log == nil {
		log = slog.Default()
	}
	not := console.PGServersNotServed(reg)
	for _, s := range not {
		log.Warn("PostgreSQL port: this server is not served until it can forward; its capture goes on", "server", s.Name, "id", s.ID, "reason", s.Reason)
	}
	return len(not)
}
