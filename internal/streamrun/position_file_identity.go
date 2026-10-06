package streamrun

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/parser"
)

// A position-mode checkpoint is a binlog file NAME and an offset, and a name
// does not name one file (#2172): RESET BINARY LOGS AND GTIDS / RESET MASTER
// starts the numbering over and the new numbering reuses every name. Once it
// has grown back to the checkpoint's name and past its offset, nothing in the
// names or sizes tells the new file from the one the checkpoint was read from,
// and the resume used to delete the rows captured after the checkpoint (the
// source will never send them again) and read the new file from the middle of
// an event.
//
// Every checkpoint therefore also records the identity of its file
// (stream_state.binlog_file_identity, parser.BinlogFileIdentity: the creation
// time and server_id in the file's FORMAT_DESCRIPTION event). On a resume the
// source is asked for the identity of the file it now serves under that name,
// and a different one is read as a renumbering: no indexed row is deleted, the
// restart begins at the source's oldest file, and the jump is stamped as a
// capture loss, exactly as #2170 does when the checkpoint's file is gone.

// fileIdentities is the identity of every binlog file this run's stream has
// opened, by name. The parse goroutine writes it as each file is entered; the
// checkpoint reads the identity of the file it names. Nothing is evicted: a
// burst of rotations can leave the checkpoint on a file several names back,
// and an evicted entry would silently store NULL. One short entry per file.
type fileIdentities struct {
	mu sync.Mutex
	m  map[string]string
}

func newFileIdentities() *fileIdentities { return &fileIdentities{m: make(map[string]string)} }

func (f *fileIdentities) set(file, identity string) {
	if f == nil || file == "" || identity == "" {
		return
	}
	f.mu.Lock()
	f.m[file] = identity
	f.mu.Unlock()
}

func (f *fileIdentities) get(file string) string {
	if f == nil {
		return ""
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.m[file]
}

// positionRenumberKind is a reason found before the gap auto-advance why the
// restart position is not in the checkpoint's own numbering.
type positionRenumberKind int

const (
	// positionContinues: no such reason found here (the existing name and
	// position comparisons still apply).
	positionContinues positionRenumberKind = iota
	// positionFileReplaced: the file under the checkpoint's name is another
	// file (its identity differs).
	positionFileReplaced
	// positionOtherServer: the checkpoint was written against another server
	// (stream_state.source_identity differs), and its file's identity could
	// not settle it.
	positionOtherServer
)

// positionFileCheck is checkPositionCheckpointFile's verdict.
type positionFileCheck struct {
	// gap is the gap to act on: the detector's, or an unfillable one that
	// restarts at the oldest file when kind says the checkpoint's file is not
	// the one it was read from.
	gap *gapResult
	// verified: the source still serves the checkpoint's own file under its
	// name, so the resume may read it from the checkpoint's offset.
	verified bool
	// kind, when not positionContinues, forces the renumbered handling of the
	// advance: no resume cleanup, a fresh dedup floor.
	kind positionRenumberKind
}

// checkPositionCheckpointFile decides, before anything is deleted, whether a
// position-mode checkpoint's coordinates still belong to the source's binary
// log. gap is detectPositionGap's result for the checkpoint; probe returns the
// identity of the file the source now serves under a name, and is called only
// when that identity decides: the file is listed and long enough, and the
// checkpoint recorded its file's identity.
//
// The file's identity wins over the server's: a server whose server_id (or,
// after a clone, server_uuid) changed still serves byte-identical files, and
// reading them as another server's would re-read its whole binary log. The
// server's identity decides only when the file's cannot: a checkpoint written
// before file identities were recorded, or a checkpoint whose file is gone.
// A purge advance onto another server keeps every row (kind set, gap kept);
// one that stays on the same server is left to the existing comparisons.
// A probe that fails is an error, never a guess.
func checkPositionCheckpointFile(gap *gapResult, saved *streamState, srcIdentity string, probe func(file string) (string, error)) (positionFileCheck, error) {
	out := positionFileCheck{gap: gap}
	if gap == nil || saved == nil || saved.binlogFile == "" {
		return out, nil
	}
	otherServer := saved.sourceIdentity != "" && srcIdentity != "" && saved.sourceIdentity != srcIdentity
	if !gap.RebuildUndetectable {
		// The checkpoint's file is gone or shorter than the checkpoint: the
		// advance below already restarts at the oldest file.
		if otherServer && gap.HasGap && !gap.Fillable {
			g := *gap
			g.Message += fmt.Sprintf("; the checkpoint was written against another server (source identity %s; this source is %s), "+
				"so its positions say nothing about this server's binary log and no indexed event is removed before the restart",
				saved.sourceIdentity, srcIdentity)
			out.gap, out.kind = &g, positionOtherServer
		}
		return out, nil
	}
	if saved.fileIdentity != "" {
		now, err := probe(saved.binlogFile)
		if err != nil {
			return out, fmt.Errorf("read the identity of binlog file %s on the source: %w", saved.binlogFile, err)
		}
		if now == "" {
			return out, fmt.Errorf("the source sent no identity for binlog file %s", saved.binlogFile)
		}
		if now == saved.fileIdentity {
			out.verified = true
			return out, nil
		}
		g, err := restartAtOldestFile(gap, fmt.Sprintf("binlog file %s on the source is not the file the checkpoint %s:%d was read from "+
			"(the file's identity is now %s, the checkpoint recorded %s): the source's binary log numbering started over "+
			"(RESET BINARY LOGS AND GTIDS / RESET MASTER) and grew back past the checkpoint",
			saved.binlogFile, saved.binlogFile, saved.binlogPos, now, saved.fileIdentity))
		if err != nil {
			return out, err
		}
		out.gap, out.kind = g, positionFileReplaced
		return out, nil
	}
	if otherServer {
		g, err := restartAtOldestFile(gap, fmt.Sprintf("the checkpoint %s:%d was written against another server (source identity %s; "+
			"this source is %s), so its binlog positions say nothing about this server's binary log, and changes this "+
			"server's binary log shares with the old one are indexed again",
			saved.binlogFile, saved.binlogPos, saved.sourceIdentity, srcIdentity))
		if err != nil {
			return out, err
		}
		out.gap, out.kind = g, positionOtherServer
	}
	return out, nil
}

// restartAtOldestFile is the unfillable gap that restarts capture at the
// source's oldest binary log, with why at the head of its message.
func restartAtOldestFile(gap *gapResult, why string) (*gapResult, error) {
	if gap.EarliestFile == "" {
		return nil, fmt.Errorf("%s; the source's oldest binary log is unknown, so the restart cannot be placed", why)
	}
	return &gapResult{
		HasGap:       true,
		EarliestFile: gap.EarliestFile,
		EarliestPos:  4,
		Message: why + "; capture restarts from the start of the source's binary log (" + gap.EarliestFile +
			") and keeps every event already indexed; changes the source made between the last capture and that point " +
			"that were not captured are permanently lost",
	}, nil
}

// positionCheckSkipNotice is the resume cleanup's skip notice for a verdict
// of checkPositionCheckpointFile (positionCleanupSkipNotice covers the #2170
// shapes): the slog warning and the stdout line.
func positionCheckSkipNotice(kind positionRenumberKind, savedFile string, savedPos uint64, startFile string, startPos uint32) (warn, line string) {
	if kind == positionOtherServer {
		return "dedup-on-resume: skipped; the checkpoint was written against another server, so no indexed row can be " +
				"placed against this server's binary log and every one is kept; changes both servers' binary logs hold " +
				"that were already indexed will be indexed again (duplicates); the capture loss stamped for the jump " +
				"reports the changes in between",
			fmt.Sprintf("Cleanup: skipped, the checkpoint %s:%d was written against another server (now starting at %s:%d); indexed events are kept",
				savedFile, savedPos, startFile, startPos)
	}
	return "dedup-on-resume: skipped; the source's binlog numbering started over and grew back past the checkpoint, so " +
			"the file under the checkpoint's name is another file, no indexed row can be placed against the new start, " +
			"and every one is kept; the capture loss stamped for the jump reports the changes in between",
		fmt.Sprintf("Cleanup: skipped, %s on the source is not the file the checkpoint %s:%d was read from (the binlog "+
			"numbering started over; now starting at %s:%d); indexed events are kept",
			savedFile, savedFile, savedPos, startFile, startPos)
}

// startBinlogSync creates a syncer from cfg and starts it with start. Under
// --ssl-mode preferred it retries once WITHOUT TLS, but only when the source
// does not support TLS (#947): any other failure (auth denied, unreachable
// host, bad position) must not resend the credentials in the clear. The
// syncer is returned even with an error, for the caller to close.
func startBinlogSync(cfg replication.BinlogSyncerConfig, sslMode string,
	start func(*replication.BinlogSyncer) (*replication.BinlogStreamer, error),
) (*replication.BinlogSyncer, *replication.BinlogStreamer, error) {
	syncer := replication.NewBinlogSyncer(cfg)
	streamer, err := start(syncer)
	if err != nil && sslMode == "preferred" && isTLSUnsupportedError(err) {
		// This path DOES transmit credentials unencrypted, so warn loudly.
		slog.Warn("source does not support TLS; retrying WITHOUT encryption "+
			"(--ssl-mode preferred) — credentials and data will be sent in cleartext",
			"error", err)
		syncer.Close()
		cfg.TLSConfig = nil
		syncer = replication.NewBinlogSyncer(cfg)
		streamer, err = start(syncer)
	}
	return syncer, streamer, err
}

// probeBinlogFileIdentity asks the source for the identity of binlog file
// file: a binlog dump from its start, read up to its FORMAT_DESCRIPTION event,
// through the same connection setup as the stream (cfg, TLS fallback). It
// needs only what capture already needs (REPLICATION SLAVE); SHOW BINLOG
// EVENTS does not show the event's timestamp. Bounded by ctx; a broken
// connection is retried once, not forever.
func probeBinlogFileIdentity(ctx context.Context, cfg replication.BinlogSyncerConfig, sslMode, file string) (string, error) {
	cfg.MaxReconnectAttempts = 1
	syncer, streamer, err := startBinlogSync(cfg, sslMode, func(s *replication.BinlogSyncer) (*replication.BinlogStreamer, error) {
		return s.StartSync(gomysql.Position{Name: file, Pos: 4})
	})
	defer syncer.Close()
	if err != nil {
		return "", fmt.Errorf("StartSync(%s, 4): %w", file, parser.WrapReplicationError(err))
	}
	current := ""
	// A dump opens with an artificial rotate naming the file, then the file's
	// FORMAT_DESCRIPTION event; a few more events of slack.
	for range 8 {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return "", fmt.Errorf("read the start of binlog file %s: %w", file, parser.WrapReplicationError(err))
		}
		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			current = string(e.NextLogName)
		case *replication.FormatDescriptionEvent:
			if current != file {
				return "", fmt.Errorf("asked for binlog file %s, the source sent the start of %q", file, current)
			}
			return parser.BinlogFileIdentity(ev.Header), nil
		}
	}
	return "", fmt.Errorf("the source sent no format description event at the start of binlog file %s", file)
}

// The limits below are what no identity reachable at restart closes (#2172).
// Each is named at the moment it applies instead of passing in silence.

// floorlessCleanupNotice is the warning for a resume cleanup that runs with
// no dedup floor ("" when there is one). Only a checkpoint written before the
// floor existed (0.83.0, #1690) and never rewritten carries none.
func floorlessCleanupNotice(floor int64) (warn, line string) {
	if floor > 0 {
		return "", ""
	}
	return "dedup-on-resume: the checkpoint carries no dedup floor (it was written by a build older than 0.83.0), so " +
			"this cleanup selects indexed rows by binlog position alone: if the source's binary log numbering was ever " +
			"reset (RESET MASTER) since capture began, rows of the older numbering in files that sort at or after the " +
			"checkpoint are deleted too, and the source will never send them again. Once this run indexes a change, " +
			"its checkpoints carry a floor and later restarts are bounded",
		"Cleanup: the checkpoint has no dedup floor (written before 0.83.0); this cleanup compares binlog positions only, " +
			"which also removes rows of an older binlog numbering if the source's binary log was ever reset"
}

func warnFloorlessCleanup(floor int64) {
	if warn, line := floorlessCleanupNotice(floor); warn != "" {
		slog.Warn(warn)
		fmt.Println(line)
	}
}

// purgeAdvanceCleanupNotice is the warning for a resume cleanup after an
// advance past purged binlog files that stays in the checkpoint's numbering by
// every name: the checkpoint's file is gone, so its identity cannot be read,
// and a numbering that started over, grew past the checkpoint's name and was
// purged below it looks exactly like an ordinary purge.
func purgeAdvanceCleanupNotice(savedFile string, savedPos uint64, startFile string, startPos uint32) (warn, line string) {
	return "dedup-on-resume: the checkpoint's binlog file was purged and capture restarts at the oldest surviving file; " +
			"the cleanup deletes indexed rows at or after that point because the source sends them again. If the " +
			"source's binary log numbering was reset (RESET MASTER) before that purge, those rows belong to the older " +
			"numbering and the source will never send them again; nothing the source still has tells the two apart",
		fmt.Sprintf("Cleanup: the checkpoint %s:%d was purged; removing rows at or after %s:%d, which is right unless the "+
			"source's binary log was also reset before the purge", savedFile, savedPos, startFile, startPos)
}

func warnPurgeAdvanceCleanup(savedFile string, savedPos uint64, startFile string, startPos uint32) {
	warn, line := purgeAdvanceCleanupNotice(savedFile, savedPos, startFile, startPos)
	slog.Warn(warn, "checkpoint_file", savedFile, "checkpoint_pos", savedPos, "start_file", startFile, "start_pos", startPos)
	fmt.Println(line)
}

// lateIndexWriteNotice names the race of a fresh dedup floor taken while a
// batch of an earlier run may still be committing: the driver cancels a timed
// out INSERT by closing the socket (#1482), so the statement can commit after
// this run read MAX(event_id). Those rows sit above the fresh floor with the
// old numbering's positions.
const lateIndexWriteNotice = "dedup-on-resume: this start took a fresh dedup floor because no indexed row belongs " +
	"to the binary log the source now serves; if the previous run stopped on an index write deadline, a batch it was writing can still commit " +
	"after that floor was read. Such rows carry the old numbering's positions above the floor, and if this run stops " +
	"before its first checkpoint, the next start's cleanup can delete them"

func warnLateIndexWrite() { slog.Warn(lateIndexWriteNotice) }
