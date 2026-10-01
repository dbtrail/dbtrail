package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// Replica / duplicate detection (#402): adding a replica of an already-
// monitored server double-indexes the same logical writes silently. GTID
// lineage makes the relationship detectable without touching the other
// sources: a replica's @@gtid_executed contains transactions originated at
// its primary's server_uuid, and each monitored entry's per-source index DB
// already stores that entry's server_uuid (bintrail_servers) and accumulated
// executed set (stream_state.gtid_set). Warn-only per the approved decision —
// an amber card in the add-server doctor flow, never a hard block.
//
// Split for testability: replicaOverlapCheck does the IO (registry walk,
// candidate + peer reads); evaluateReplicaOverlap is the pure card builder.

// replicaOverlapTimeout bounds the whole check — it runs inside the
// interactive doctor flow and must not hang on a dead peer index DB.
const replicaOverlapTimeout = 15 * time.Second

const replicaCheckName = "Replica / duplicate detection"

// replicaOverlapRemediation is the warn card's remediation, MySQL and MariaDB.
const replicaOverlapRemediation = "Monitoring a primary and its replica (or the same server twice) indexes every\n" +
	"row change once per entry: duplicate history, duplicate storage.\n\n" +
	"This is a WARN, not a hard fail: monitoring has already started. If the\n" +
	"overlap is unintentional, press Stop on one of the entries (usually keep\n" +
	"the primary)."

// peerIdentity is one monitored entry's recorded identity, as the pure
// evaluator consumes it.
type peerIdentity struct {
	name string
	uuid string
	// executed is the peer's accumulated GTID set from stream_state; empty
	// when never streamed in GTID mode.
	executed string
	// unreadable marks a peer whose index DB could not be read — counted as
	// unverified, never silently dropped.
	unreadable bool
}

// replicaOverlapCheck compares the candidate entry's source against every
// other monitored registry entry. Returns nil when there is nothing to
// compare (no registry, no peers) — no card is shown then.
func (m *monitorSupervisor) replicaOverlapCheck(ctx context.Context, e console.ServerEntry) *console.DoctorCheck {
	// GTID-lineage overlap is a MySQL/MariaDB concept, read over a MySQL
	// connection (@@gtid_mode / @@server_uuid); a PostgreSQL source has no analog
	// and config.Connect would fail on its postgres:// DSN. Skip silently.
	if e.SourceFlavor() == console.FlavorPostgres {
		return nil
	}
	if m.registry == nil {
		return nil
	}
	var entries []console.ServerEntry
	for _, p := range m.registry.List() {
		if p.ID != e.ID && p.SourceDSN != "" && p.DSN != "" {
			entries = append(entries, p)
		}
	}
	if len(entries) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, replicaOverlapTimeout)
	defer cancel()

	srcDB, err := connectSource(e.SourceDSN, e.SourceSSL())
	if err != nil {
		return &console.DoctorCheck{Name: replicaCheckName, Status: "skip",
			Detail: "could not connect to the source to compare GTID lineage: " + err.Error()}
	}
	defer srcDB.Close()

	// The server says what it is; the saved Source type may be stale. A
	// MariaDB has no server_uuid and no gtid_mode: it has its own check. A
	// failed ask falls through to the MySQL reads, which report their own
	// failure as a skip.
	if flavor, ferr := detectFlavorCtx(ctx, srcDB); ferr == nil && flavor == console.FlavorMariaDB {
		return mariadbReplicaOverlap(ctx, e, srcDB, entries)
	}

	gtidMode, candUUID, candExecuted, err := loadCandidateIdentity(ctx, srcDB)
	if err != nil {
		return &console.DoctorCheck{Name: replicaCheckName, Status: "skip",
			Detail: "could not read gtid_mode / server_uuid / gtid_executed: " + err.Error()}
	}
	if !strings.EqualFold(gtidMode, "ON") {
		return &console.DoctorCheck{Name: replicaCheckName, Status: "skip",
			Detail: fmt.Sprintf("gtid_mode is %s on this source: replica detection needs GTID; a position-mode duplicate cannot be detected", gtidMode)}
	}

	peers := make([]peerIdentity, 0, len(entries))
	for _, p := range entries {
		uuid, executed, err := loadPeerIdentity(ctx, p.DSN)
		// Best-effort: a peer whose index DB is unreadable (never started,
		// dropped, server down) is unverified, not a failure.
		peers = append(peers, peerIdentity{
			name: p.Name, uuid: uuid, executed: executed, unreadable: err != nil,
		})
	}

	return evaluateReplicaOverlap(candUUID, candExecuted, peers)
}

// loadCandidateIdentity reads the GTID-lineage identity of the source being
// added. Separate from replicaOverlapCheck so integration tests can exercise
// the exact production queries.
func loadCandidateIdentity(ctx context.Context, db *sql.DB) (gtidMode, serverUUID, gtidExecuted string, err error) {
	if err := db.QueryRowContext(ctx, "SELECT @@gtid_mode").Scan(&gtidMode); err != nil {
		return "", "", "", err
	}
	if err := db.QueryRowContext(ctx,
		"SELECT @@server_uuid, @@global.gtid_executed").Scan(&serverUUID, &gtidExecuted); err != nil {
		return "", "", "", err
	}
	return gtidMode, serverUUID, gtidExecuted, nil
}

// loadPeerIdentity reads a monitored entry's recorded server_uuid and
// accumulated executed GTID set from its per-source index database.
func loadPeerIdentity(ctx context.Context, indexDSN string) (peerUUID, peerExecuted string, err error) {
	db, err := config.Connect(indexDSN)
	if err != nil {
		return "", "", err
	}
	defer db.Close()

	if err := db.QueryRowContext(ctx, `
		SELECT server_uuid FROM bintrail_servers
		WHERE decommissioned_at IS NULL
		ORDER BY updated_at DESC LIMIT 1`).Scan(&peerUUID); err != nil {
		return "", "", err
	}
	// stream_state may not exist yet (monitoring never started) or hold no
	// GTID set (position mode) — both leave peerExecuted empty, which only
	// disables the primary-of-monitored-replica direction.
	var gtidSet sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT gtid_set FROM stream_state WHERE id = 1`).Scan(&gtidSet); err == nil && gtidSet.Valid {
		peerExecuted = gtidSet.String
	}
	return peerUUID, peerExecuted, nil
}

// evaluateReplicaOverlap builds the doctor card from pre-resolved identities.
// Pure — unit-testable without MySQL.
func evaluateReplicaOverlap(candUUID, candExecuted string, peers []peerIdentity) *console.DoctorCheck {
	// A malformed candidate set would silently disable the main (replica-of)
	// direction inside gtidSetContainsUUID — surface it as skip instead.
	if candExecuted != "" && !gtidSetParseable(candExecuted) {
		return &console.DoctorCheck{Name: replicaCheckName, Status: "skip",
			Detail: "could not parse the source's gtid_executed set; replica detection unavailable"}
	}

	var findings []string
	unverified := 0
	for _, p := range peers {
		if p.unreadable {
			unverified++
			continue
		}
		if rel := classifyReplicaOverlap(candUUID, candExecuted, p.uuid, p.executed); rel != "" {
			findings = append(findings, fmt.Sprintf("%s %q", rel, p.name))
			continue
		}
		// A peer set that does not parse silently disables the primary-of-
		// monitored-replica direction — count it as unverified so the pass
		// card stays honest.
		if p.executed != "" && !gtidSetParseable(p.executed) {
			unverified++
		}
	}

	if len(findings) > 0 {
		return &console.DoctorCheck{
			Name:        replicaCheckName,
			Status:      "warn",
			Detail:      "this server " + strings.Join(findings, "; "),
			Remediation: replicaOverlapRemediation,
		}
	}
	detail := fmt.Sprintf("no replica relationship detected among %d monitored source(s)", len(peers))
	if unverified > 0 {
		detail += fmt.Sprintf(" (%d could not be verified)", unverified)
	}
	return &console.DoctorCheck{Name: replicaCheckName, Status: "pass", Detail: detail}
}

// classifyReplicaOverlap describes the GTID-lineage relationship between a
// candidate server and one monitored peer, or "" when none is detected.
// Pure — unit-testable without MySQL.
func classifyReplicaOverlap(candUUID, candExecuted, peerUUID, peerExecuted string) string {
	if strings.EqualFold(candUUID, peerUUID) {
		return "is the same server as already-monitored"
	}
	// The peer's UUID appears in the candidate's executed set: the candidate
	// has applied transactions originated on the peer — it is (or was) a
	// replica of it.
	if gtidSetContainsUUID(candExecuted, peerUUID) {
		return "appears to be a replica of already-monitored"
	}
	// The candidate's UUID appears in the monitored peer's executed set: the
	// peer replicates from the candidate — the candidate is its primary.
	if gtidSetContainsUUID(peerExecuted, candUUID) {
		return "appears to be the primary of already-monitored replica"
	}
	return ""
}

// gtidSetParseable reports whether a stored GTID set parses — callers use it
// to tell "no overlap" apart from "could not even look".
func gtidSetParseable(gtidSet string) bool {
	_, err := gomysql.ParseMysqlGTIDSet(streamrun.NormalizeGTIDSet(gtidSet))
	return err == nil
}

// gtidSetContainsUUID reports whether the GTID set contains any transactions
// originated at the given server UUID. Malformed input is never a match —
// the caller treats this as best-effort detection. Comparison is
// case-insensitive (go-mysql lowercases UUIDs).
func gtidSetContainsUUID(gtidSet, serverUUID string) bool {
	if gtidSet == "" || serverUUID == "" {
		return false
	}
	parsed, err := gomysql.ParseMysqlGTIDSet(streamrun.NormalizeGTIDSet(gtidSet))
	if err != nil {
		return false
	}
	set, ok := parsed.(*gomysql.MysqlGTIDSet)
	if !ok {
		return false
	}
	// go-mysql v1.15.0+ keys MysqlGTIDSet by a typed uuid.UUID; String()
	// renders it canonical-lowercase, so EqualFold absorbs any uppercase
	// in the stored serverUUID without importing the uuid package here.
	for sid := range *set {
		if strings.EqualFold(sid.String(), serverUUID) {
			return true
		}
	}
	return false
}
