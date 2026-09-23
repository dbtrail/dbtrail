package console

import (
	"net"
	"strconv"
	"strings"
)

// serverNameMaxLen caps the derived part of a name. The name is a label: it is
// shown in the server switcher, written into the registry file and printed in
// every log line about that server, so a 250-character fully-qualified host
// would be unreadable in all three. Two hosts that share the first
// serverNameMaxLen characters therefore derive the SAME name — the uniqueness
// rule below is what keeps their entries apart.
const serverNameMaxLen = 60

// derivedNameFallback is the name for an address that was typed but holds no
// character a name can carry. Only a deliberately odd host reaches it; an
// EMPTY host derives nothing at all, because there is nothing to name.
const derivedNameFallback = "database"

// defaultSourcePortNum is the port that is so ordinary it is left out of a
// derived name, per source family. Every other port is part of the name: two
// databases on one host are usually told apart by it.
func defaultSourcePortNum(flavor string) int {
	if flavor == FlavorPostgres {
		return 5432
	}
	return 3306
}

// DeriveServerName builds the name a new server gets when nobody typed one,
// from the address of the database it reads. It returns "" when there is no
// host to derive from; the caller decides what that means (the registry still
// refuses a nameless entry).
//
// The host box does not always hold a bare hostname, so three shapes are
// unpicked before anything else: a host with the port typed into it
// ("db:3307"), an IPv6 literal in brackets ("[::1]"), and both together
// ("[::1]:3307"). An explicitly typed port box wins over a port found inside
// the host, because it is the field the person aimed at.
//
// What comes out carries only lowercase letters, digits, dot, underscore and
// dash. That is not a security boundary — nothing in the product uses a server
// name as a path or a key, the id does that — it is so the label reads the
// same everywhere it is shown.
func DeriveServerName(host, port, flavor string) string {
	host = strings.TrimSpace(host)
	port = strings.TrimSpace(port)
	if host == "" {
		return ""
	}
	// SplitHostPort handles "db:3307" and "[::1]:3307" and fails on a bare
	// host or a bare IPv6 literal; the bracket strip below covers "[::1]".
	if h, p, err := net.SplitHostPort(host); err == nil {
		host = h
		if port == "" {
			port = p
		}
	} else if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}

	// An IPv6 literal sanitizes down to its hextets ("::1" → "1"), which on its
	// own is not a name anyone would recognize. Say what it is.
	ip := net.ParseIP(host)
	isV6 := ip != nil && ip.To4() == nil

	label := sanitizeNameLabel(host)
	if isV6 {
		// The trim is for "::", which sanitizes to nothing: the name is then
		// "ipv6", not "ipv6-".
		label = strings.Trim("ipv6-"+label, "-")
	}
	// sanitizeNameLabel leaves only ASCII, so cutting by byte cannot split a
	// character. The trim is for the cut itself: it can land right after a dot
	// or a dash.
	if len(label) > serverNameMaxLen {
		label = strings.Trim(label[:serverNameMaxLen], "-.")
	}
	// ONE fallback, after every step that can empty the label — a host with no
	// character a name carries, and a cut that lands on a run of separators.
	// It was written twice before, and a mutation showed the first copy could
	// be deleted with every test still green: the second was doing both jobs.
	if label == "" {
		label = derivedNameFallback
	}

	// A port is appended only when it is a real port and not the ordinary one
	// for this source family. Compared as a NUMBER and re-rendered from it, so
	// a padded "03306" is recognized as the ordinary port rather than becoming
	// part of the name.
	if n, err := strconv.Atoi(port); err == nil && n > 0 && n <= 65535 && n != defaultSourcePortNum(flavor) {
		label += "-" + strconv.Itoa(n)
	}
	return label
}

// sanitizeNameLabel folds a host down to the characters a name carries:
// anything else becomes a dash, runs of dashes collapse into one, and dashes
// and dots are trimmed off both ends (which is also what drops the trailing
// root dot of "db.example.com.").
func sanitizeNameLabel(s string) string {
	var b strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(s) {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-') {
			r = '-'
		}
		if r == '-' {
			if lastDash {
				continue
			}
			lastDash = true
		} else {
			lastDash = false
		}
		b.WriteRune(r)
	}
	return strings.Trim(b.String(), "-.")
}

// uniqueNameLocked returns base, or base-2, base-3 … — the first that no entry
// holds. The reserved name of the command-line server counts as held: the
// registry refuses it outright, and the switcher would otherwise show two
// entries under one label. Callers hold r.mu.
//
// The loop terminates: each turn either returns or finds a name an existing
// entry holds, and there are finitely many entries.
func (r *Registry) uniqueNameLocked(base string) string {
	taken := func(n string) bool {
		if n == bootServerID {
			return true
		}
		for _, e := range r.file.Servers {
			if e.Name == n {
				return true
			}
		}
		return false
	}
	if !taken(base) {
		return base
	}
	for i := 2; ; i++ {
		if n := base + "-" + strconv.Itoa(i); !taken(n) {
			return n
		}
	}
}

// NameFor is the name an add would give an entry right now: typed when it is
// not empty, else the automatic name made unique against the registry, worked
// out under the same lock AddAutoNamed takes. It reserves nothing — another
// add may take the name first — so it describes, and AddAutoNamed decides.
func (r *Registry) NameFor(typed, base string) string {
	if typed != "" || base == "" {
		return typed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.uniqueNameLocked(base)
}
