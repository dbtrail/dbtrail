package streamrun

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
)

// A GTID-mode resume trusts the saved GTID set to name what capture has
// already read. That only holds while the source's GTID history is the one the
// set was taken from. RESET BINARY LOGS AND GTIDS / RESET MASTER (or a restore
// from an older backup, or a rebuilt server) starts the numbering over, and
// then (#2171):
//
//   - MySQL refuses the saved set ("Replica has more GTIDs than the source")
//     until the new numbering passes the old one, and from then on silently
//     skips every new transaction whose number falls inside the saved set;
//   - the resume cleanup, before that refusal, deletes the rows captured after
//     the last checkpoint, which the source will never send again;
//   - nothing records the break.
//
// detectGTIDRenumbering tells "the source went BACKWARDS" from "the source is
// merely BEHIND" (a lagging replica that will catch up), which must never be
// reported as a loss. The rule is the one MySQL applies itself: only the
// source's OWN transactions (its server_uuid; on MariaDB its server_id) can
// prove it went backwards, because nothing but the source itself creates them.
// A replica that lags is only ever missing transactions of other servers.

// gtidRenumbering is a verdict that the source's GTID history no longer
// continues the saved checkpoint.
type gtidRenumbering struct {
	// Detail says what happened, for the log, stdout and gap_lost_detail.
	Detail string
	// ResumeSet (MySQL) is the GTID set to restart from: the saved set without
	// the source's own UUID and without UUIDs the source no longer knows, plus
	// its purged set, so the source sends its whole surviving binary log.
	ResumeSet string
	// EarliestFile is the source's oldest binary log ("" when unknown), where
	// that restart begins.
	EarliestFile string
}

// binlogFileEntry is one row of SHOW BINARY LOGS.
type binlogFileEntry struct {
	name string
	size int64
}

// listBinaryLogs reads SHOW BINARY LOGS, oldest first.
func listBinaryLogs(ctx context.Context, db *sql.DB) ([]binlogFileEntry, error) {
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("SHOW BINARY LOGS: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("SHOW BINARY LOGS columns: %w", err)
	}
	if len(cols) < 2 {
		return nil, fmt.Errorf("SHOW BINARY LOGS returned %d columns, expected at least 2", len(cols))
	}
	var logs []binlogFileEntry
	for rows.Next() {
		var e binlogFileEntry
		vals := make([]any, len(cols))
		vals[0], vals[1] = &e.name, &e.size
		for i := 2; i < len(cols); i++ {
			vals[i] = new(sql.RawBytes)
		}
		if err := rows.Scan(vals...); err != nil {
			return nil, fmt.Errorf("scan SHOW BINARY LOGS: %w", err)
		}
		logs = append(logs, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SHOW BINARY LOGS: %w", err)
	}
	return logs, nil
}

// checkpointPastBinlogEnd reports whether the checkpoint (file, pos) lies past
// the end of the source's binary log: a file numbered after the newest one, or
// the newest file at an offset beyond its size. On one server binlog numbers
// only grow, so this is a numbering that started over (or, in the newest file,
// a lost tail). It compares as continuesNumbering does (#2170), and only names
// with the newest file's base and an all-digit number: another base name (a
// log_bin rename) is no verdict, not a loss. It cannot see a numbering that
// started over and has rotated back up to the checkpoint's file number.
func checkpointPastBinlogEnd(file string, pos uint64, logs []binlogFileEntry) bool {
	if file == "" || len(logs) == 0 {
		return false
	}
	newest := logs[len(logs)-1]
	base, num := splitBinlogName(file)
	nbase, nnum := splitBinlogName(newest.name)
	if base != nbase || !allDigits(num) || !allDigits(nnum) {
		return false
	}
	return !continuesNumbering(file, pos, newest.name, uint64(newest.size))
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func mysqlSetHasUUID(s *gomysql.MysqlGTIDSet, sid uuid.UUID) bool {
	for _, ivs := range (*s)[sid] {
		if len(ivs) > 0 {
			return true
		}
	}
	return false
}

func mysqlSubset(s *gomysql.MysqlGTIDSet, keep func(uuid.UUID) bool) *gomysql.MysqlGTIDSet {
	out := gomysql.NewMysqlGTIDSet()
	for sid, tags := range *s {
		if !keep(sid) {
			continue
		}
		for tag, ivs := range tags {
			if len(ivs) == 0 {
				continue
			}
			if out[sid] == nil {
				out[sid] = make(map[gomysql.Tag]gomysql.IntervalSlice)
			}
			out[sid][tag] = append(gomysql.IntervalSlice(nil), ivs...)
		}
	}
	return &out
}

func parseMysqlSet(s string) (*gomysql.MysqlGTIDSet, error) {
	set, err := gomysql.ParseMysqlGTIDSet(NormalizeGTIDSet(strings.TrimSpace(s)))
	if err != nil {
		return nil, err
	}
	return set.(*gomysql.MysqlGTIDSet), nil
}

func setOrNone(s string) string {
	if s == "" {
		return "nothing"
	}
	return s
}

// earliestOrUnknown names where a restart from the beginning lands.
func earliestOrUnknown(logs []binlogFileEntry) string {
	if len(logs) == 0 {
		return ""
	}
	return logs[0].name
}

// classifyMySQLRenumbering is the MySQL verdict over the source's variables
// and binary log list (logs may be nil when SHOW BINARY LOGS failed; only
// the past-the-end check needs it). nil means the saved set still continues
// the source's history: equal, behind, or ahead only on other servers' GTIDs.
func classifyMySQLRenumbering(saved, executed, purged, serverUUID, file string, pos uint64, sameServer bool, logs []binlogFileEntry) (*gtidRenumbering, error) {
	s, err := parseMysqlSet(saved)
	if err != nil {
		return nil, fmt.Errorf("parse checkpoint GTID set: %w", err)
	}
	if s.IsEmpty() {
		return nil, nil // nothing to compare
	}
	e, err := parseMysqlSet(executed)
	if err != nil {
		return nil, fmt.Errorf("parse @@gtid_executed: %w", err)
	}
	p, err := parseMysqlSet(purged)
	if err != nil {
		return nil, fmt.Errorf("parse @@gtid_purged: %w", err)
	}
	own, err := uuid.Parse(strings.TrimSpace(serverUUID))
	if err != nil {
		return nil, fmt.Errorf("parse @@server_uuid %q: %w", serverUUID, err)
	}

	ownPart := mysqlSubset(s, func(sid uuid.UUID) bool { return sid == own })
	shared, foreignPresent := false, false
	for sid := range *s {
		if mysqlSetHasUUID(s, sid) && mysqlSetHasUUID(e, sid) {
			shared = true
			if sid != own {
				foreignPresent = true
			}
		}
	}
	earliest := earliestOrUnknown(logs)
	var why string
	switch {
	case !ownPart.IsEmpty() && !e.Contain(ownPart):
		ownNow := mysqlSubset(e, func(sid uuid.UUID) bool { return sid == own })
		// Nothing reachable from SQL proves which: a reset whose new
		// numbering rotated back past the checkpoint's file name looks like
		// a crash that lost the end of the binary log (sync_binlog != 1). It
		// is read as a reset either way: that costs duplicates after a lost
		// tail, while the other reading would skip new changes for good.
		why = fmt.Sprintf("the source's GTID numbering went backwards: the saved checkpoint holds %s of the source's own "+
			"server_uuid, but the source's gtid_executed now holds %s of it (RESET BINARY LOGS AND GTIDS / RESET MASTER, "+
			"a restore from an older backup, or a crash that lost the end of its binary log; after a crash, changes still "+
			"in the source's binary log that capture had read are indexed again)", ownPart.String(), setOrNone(ownNow.String()))
	case !shared:
		why = fmt.Sprintf("the source shares no GTID history with the saved checkpoint: no server UUID of the saved set "+
			"(%s) appears in the source's gtid_executed (%s), so the source was rebuilt or replaced (its server_uuid is %s)",
			s.String(), setOrNone(e.String()), own)
	case sameServer && !ownPart.IsEmpty() && !foreignPresent && checkpointPastBinlogEnd(file, pos, logs):
		newest := logs[len(logs)-1]
		why = fmt.Sprintf("the source's binary log and GTID numbering started over: the saved checkpoint is at %s:%d, past "+
			"the end of the source's newest binary log %s (%d bytes), so the source's transactions numbered inside the saved set "+
			"since then are new ones (RESET BINARY LOGS AND GTIDS / RESET MASTER)", file, pos, newest.name, newest.size)
	default:
		return nil, nil
	}

	// Restart from the source's whole surviving binary log: drop the source's
	// own UUID (its old numbers now name other transactions) and every UUID
	// it no longer knows (keeping one would make the next restart find "no
	// shared history" again and stamp a second loss for the same break). The
	// purged set joins, because the source cannot send it.
	resume := mysqlSubset(s, func(sid uuid.UUID) bool { return sid != own && mysqlSetHasUUID(e, sid) })
	lostPurged := !resume.Contain(p)
	if err := resume.Update(p.String()); err != nil {
		return nil, fmt.Errorf("merge @@gtid_purged into the resume set: %w", err)
	}
	where := "the start of the source's binary log"
	if earliest != "" {
		where += " (" + earliest + ")"
	}
	detail := why + "; capture restarts from " + where + " and keeps every event already indexed; changes the source " +
		"made between the last capture and that point that were not captured are permanently lost"
	if lostPurged {
		detail += fmt.Sprintf("; the source has also purged %s, which capture can no longer read", p.String())
	}
	return &gtidRenumbering{Detail: detail, ResumeSet: NormalizeGTIDSet(resume.String()), EarliestFile: earliest}, nil
}

// parseMariadbBinlogState parses @@gtid_binlog_state ("0-1-5,0-2-9,1-2-3":
// the last GTID of every domain and server pair) into domain → server → seq.
func parseMariadbBinlogState(s string) (map[uint32]map[uint32]uint64, error) {
	out := make(map[uint32]map[uint32]uint64)
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		g, err := gomysql.ParseMariadbGTID(part)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", part, err)
		}
		if out[g.DomainID] == nil {
			out[g.DomainID] = make(map[uint32]uint64)
		}
		if g.SequenceNumber > out[g.DomainID][g.ServerID] {
			out[g.DomainID][g.ServerID] = g.SequenceNumber
		}
	}
	return out, nil
}

// classifyMariaDBRenumbering is the MariaDB verdict. The source's own
// transactions are those carrying its server_id; @@gtid_binlog_state keeps
// the last one per domain and server, so it can be looked up even when
// another server wrote last in that domain.
func classifyMariaDBRenumbering(saved, binlogState string, serverID uint32, file string, pos uint64, sameServer bool, logs []binlogFileEntry) (*gtidRenumbering, error) {
	s, err := parseMariadbSet(strings.TrimSpace(saved))
	if err != nil {
		return nil, fmt.Errorf("parse checkpoint GTID set: %w", err)
	}
	if len(s.Sets) == 0 {
		return nil, nil
	}
	state, err := parseMariadbBinlogState(binlogState)
	if err != nil {
		return nil, fmt.Errorf("parse @@gtid_binlog_state: %w", err)
	}
	domains := make([]uint32, 0, len(s.Sets))
	for d := range s.Sets {
		domains = append(domains, d)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i] < domains[j] })

	shared, ownInSaved, foreignPresent := false, false, false
	var backwards []string
	for _, d := range domains {
		g := s.Sets[d]
		_, present := state[d]
		if present {
			shared = true
		}
		if g.ServerID != serverID {
			if present {
				foreignPresent = true
			}
			continue
		}
		ownInSaved = true
		if seq, ok := state[d][serverID]; !ok || seq < g.SequenceNumber {
			now := "none"
			if ok {
				now = fmt.Sprintf("%d-%d-%d", d, serverID, seq)
			}
			backwards = append(backwards, fmt.Sprintf("%s (the source's last now: %s)", g.String(), now))
		}
	}
	var why string
	switch {
	case len(backwards) > 0:
		why = fmt.Sprintf("the source's GTID numbering went backwards: the saved checkpoint holds %s written by the "+
			"source itself (server_id %d), which its binary log no longer reaches (RESET MASTER, or a restore from an "+
			"older backup)", strings.Join(backwards, ", "), serverID)
	case !shared:
		why = fmt.Sprintf("the source shares no GTID history with the saved checkpoint: no domain of the saved position "+
			"(%s) appears in the source's @@gtid_binlog_state (%s), so its binary log was reset or the server was replaced",
			saved, setOrNone(strings.TrimSpace(binlogState)))
	case sameServer && ownInSaved && !foreignPresent && checkpointPastBinlogEnd(file, pos, logs):
		newest := logs[len(logs)-1]
		why = fmt.Sprintf("the source's binary log and GTID numbering started over: the saved checkpoint is at %s:%d, past "+
			"the end of the source's newest binary log %s (%d bytes) (RESET MASTER)", file, pos, newest.name, newest.size)
	default:
		return nil, nil
	}
	return &gtidRenumbering{Detail: why, EarliestFile: earliestOrUnknown(logs)}, nil
}

// SourceRenumberedError is the MariaDB refusal to resume a GTID checkpoint
// from a source whose GTID numbering went backwards. It carries the steps
// that resume capture.
type SourceRenumberedError struct{ msg string }

func (e *SourceRenumberedError) Error() string { return e.msg }

// TelemetryClass implements telemetry.Classed: the same bucket as the
// server's own 1236 for this condition.
func (e *SourceRenumberedError) TelemetryClass() string { return "binlog_not_found" }

// mariadbRenumberedError builds the refusal, with the resume steps in it.
func mariadbRenumberedError(r *gtidRenumbering) error {
	file := r.EarliestFile
	if file == "" {
		file = "<the first file SHOW BINARY LOGS lists on the source>"
	}
	return &SourceRenumberedError{msg: fmt.Sprintf(
		"cannot resume capture: %s. Nothing was deleted from the index. To resume, restart capture once with "+
			"--reset --start-file %s --start-pos 4: it reads the source's new numbering from its first transaction "+
			"(in position mode) and records the jump as a capture loss. To return to GTID mode later, stop capture, "+
			"read its checkpoint (binlog file and position, shown by bintrail status), and restart once with --start-gtid "+
			"set to what SELECT BINLOG_GTID_POS('<file>', <position>) returns on the source for that checkpoint. "+
			"--reset alone also resumes, from the source's current position, and skips everything the source wrote "+
			"since its reset",
		r.Detail, file)}
}

// detectGTIDRenumbering reads what the verdict needs from the source.
// savedIdentity is the source identity the checkpoint was written against,
// identity the one capture reads now (sourceIdentity): binlog file numbers
// are only comparable on one server, so the past-the-end check runs only when
// the two match. When it runs, failing to list the binary logs is fatal: a
// verdict that silently changes with a failed query is the loss this check
// exists to stop. Otherwise the list only names where a restart begins, and a
// failure there is a warning.
func detectGTIDRenumbering(sourceDB *sql.DB, flavor, savedSet, file string, pos uint64, savedIdentity, identity string, timeout time.Duration) (*gtidRenumbering, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	sameServer := savedIdentity != "" && savedIdentity == identity && file != ""
	binaryLogs := func() ([]binlogFileEntry, error) {
		logs, err := listBinaryLogs(ctx, sourceDB)
		if err == nil {
			return logs, nil
		}
		if sameServer {
			return nil, fmt.Errorf("%w; the GTID history check needs the source's binary log list, retry once the source answers SHOW BINARY LOGS", err)
		}
		slog.Warn("could not list the source's binary logs; a restart after a GTID renumbering will not name its first file",
			"error", err)
		return nil, nil
	}
	if flavor == gomysql.MariaDBFlavor {
		var serverID uint32
		var state string
		if err := sourceDB.QueryRowContext(ctx, "SELECT @@server_id, @@GLOBAL.gtid_binlog_state").Scan(&serverID, &state); err != nil {
			return nil, fmt.Errorf("query @@server_id, @@gtid_binlog_state: %w", err)
		}
		logs, err := binaryLogs()
		if err != nil {
			return nil, err
		}
		return classifyMariaDBRenumbering(savedSet, state, serverID, file, pos, sameServer, logs)
	}
	var mode, own, executed, purged string
	if err := sourceDB.QueryRowContext(ctx,
		"SELECT @@GLOBAL.gtid_mode, @@server_uuid, @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged").Scan(&mode, &own, &executed, &purged); err != nil {
		return nil, fmt.Errorf("query @@gtid_mode, @@server_uuid, @@gtid_executed, @@gtid_purged: %w", err)
	}
	// Only gtid_mode=ON serves a GTID resume: MySQL refuses the dump on any
	// other mode, ON_PERMISSIVE included (observed on 8.4.9: "cannot start
	// in AUTO_POSITION mode: this server has GTID_MODE = ON_PERMISSIVE
	// instead of ON"), so there is no history to judge.
	if !strings.EqualFold(strings.TrimSpace(mode), "ON") {
		slog.Warn("source gtid_mode is not ON, so a GTID-mode resume cannot work and the GTID history check is skipped",
			"gtid_mode", mode)
		return nil, nil
	}
	logs, err := binaryLogs()
	if err != nil {
		return nil, err
	}
	return classifyMySQLRenumbering(savedSet, executed, purged, own, file, pos, sameServer, logs)
}

// sourceIdentity names the server capture reads, for stream_state's
// source_identity: @@server_uuid on MySQL, "server_id:<n>" on MariaDB (which
// has no server_uuid; servers that replicate from each other must have
// distinct server_ids). Two checkpoints with the same identity were read from
// one server, whose binlog file numbers can be compared.
func sourceIdentity(ctx context.Context, sourceDB *sql.DB, flavor string) (string, error) {
	if flavor == gomysql.MariaDBFlavor {
		var id uint32
		if err := sourceDB.QueryRowContext(ctx, "SELECT @@server_id").Scan(&id); err != nil {
			return "", fmt.Errorf("query @@server_id: %w", err)
		}
		return fmt.Sprintf("server_id:%d", id), nil
	}
	var u string
	if err := sourceDB.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&u); err != nil {
		return "", fmt.Errorf("query @@server_uuid: %w", err)
	}
	return strings.ToLower(strings.TrimSpace(u)), nil
}
