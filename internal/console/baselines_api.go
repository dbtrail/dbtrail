package console

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// baselinesMaxSnapshots caps the snapshots GET /api/baselines returns — the
// Storage panel is a recency view ("do I have a usable baseline, how stale is
// it"), not an inventory dump.
const baselinesMaxSnapshots = 50

// listBaselinesForPage is reconstruct.ListBaselinesNewestReport, indirected
// so a test can pin the bound the page asks for (#1679): the cap above used
// to cap the RESPONSE while every location was still listed whole.
var listBaselinesForPage = reconstruct.ListBaselinesNewestReport

// baselineSnapshotDTO is one snapshot (one timestamped dump) in the listing:
// the grouped view of its per-table Parquet files.
type baselineSnapshotDTO struct {
	Time     string  `json:"time"`
	AgeHours float64 `json:"age_hours"`
	// Tables lists schema.table, sorted (the lister's stable order).
	Tables []string `json:"tables"`
	// Binlog coordinates from Parquet metadata — where this snapshot's deltas
	// start. Local sources only: reading per-snapshot Parquet footers over S3
	// is not worth the listing latency.
	BinlogFile string `json:"binlog_file,omitempty"`
	BinlogPos  int64  `json:"binlog_pos,omitempty"`
	GTIDSet    string `json:"gtid_set,omitempty"`
	// Staleness grades this snapshot's anchor against the oldest available
	// delta coverage of the selected server's index (#1193):
	// ok | aging | broken | unknown. "unknown" when the coverage floor could
	// not be read — never reported as ok.
	Staleness string `json:"staleness,omitempty"`
	// Kinds names every location this snapshot was found in, sorted: "dir",
	// "s3", or both (#1542). The union over the snapshot's files, which is
	// exactly what it claims and no more — a snapshot listing both was seen in
	// both places, not necessarily with every table in each.
	Kinds []string `json:"kinds,omitempty"`
}

type baselinesResponse struct {
	// Configured reports whether the selected server has a baseline source at
	// all (dir or s3). Reconstruct additionally requires archives enabled and
	// no RBAC profile (the bundle's gate) — a server can list baselines that
	// Time-travel still refuses to use.
	Configured bool   `json:"configured"`
	Source     string `json:"source,omitempty"`
	Kind       string `json:"kind,omitempty"` // "dir" | "s3"
	// Sources reports EVERY location that was read, in consult order, with a
	// per-location count and error (#1542). Source/Kind above still name the
	// primary; they were the whole answer when the listing only ever read one
	// place, and a server with a local directory and an S3 destination has two.
	Sources []baselineSourceDTO `json:"sources,omitempty"`
	// Incomplete says at least one configured location could not be read, so
	// the snapshots below are a SUBSET. The listing is still served, because a
	// briefly unreachable bucket must not blank a page whose local half works,
	// but a shorter list rendered as if it were the whole set is the exact bug
	// this endpoint had.
	Incomplete  bool                  `json:"incomplete,omitempty"`
	Reconstruct bool                  `json:"reconstruct"`
	Snapshots   []baselineSnapshotDTO `json:"snapshots"`
	Truncated   bool                  `json:"truncated,omitempty"`
	// Refresh is the daemon's last PERIODIC baseline refresh for this server
	// (#1171), omitted when the daemon does not run one. It belongs in the
	// listing rather than in a status endpoint of its own because the question
	// it answers — "is this snapshot list going to keep moving on its own?" — is
	// only meaningful next to the list.
	Refresh *BaselineStatus `json:"refresh,omitempty"`
	// Schedule is the selected server's backup schedule (#1442) and what it
	// last did, omitted when the server has none. Here for the same reason
	// Refresh is: "will this list keep moving on its own" belongs next to the
	// list. Present even on a daemon that cannot run it, with Runnable false
	// and the reason, because a saved schedule that nothing executes is the
	// silent failure this feature exists to prevent.
	Schedule *backupScheduleDTO `json:"schedule,omitempty"`
	// Staleness is the panel headline: the worst verdict across each table's
	// NEWEST snapshot (#1193). An old superseded snapshot being past coverage
	// is routine — grading every row red on a healthy retention cadence would
	// cry wolf, so the per-row verdicts inform and this field decides.
	Staleness string `json:"staleness,omitempty"`
}

// selectedServerID is the id the request EFFECTIVELY selected: the header
// when present, else the same default connManager.Resolve("") lands on. The
// old fallback was the literal "default", which under HideBoot names an entry
// the selection never resolves to — so the refresh chip and the run-history
// join silently missed on every fresh tab of a single-server watch.
func (s *Server) selectedServerID(r *http.Request) string {
	if id := r.Header.Get(serverHeader); id != "" {
		return id
	}
	if id := s.cm.defaultID(); id != "" {
		return id
	}
	// HideBoot with an empty registry: defaultID has nothing to name, but
	// Resolve("") serves the hidden boot bundle — and the refresh loop
	// registers that server's runs under the boot id.
	return bootServerID
}

// handleBaselines serves GET /api/baselines: a read-only listing of the
// selected server's baseline snapshots (the inputs Time-travel reconstructs
// from), grouped per snapshot timestamp, newest first. The listing is
// path-derived; only local sources additionally read one Parquet footer per
// snapshot for its binlog coordinates (best-effort — a missing/corrupt footer
// just omits them).
func (s *Server) handleBaselines(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	// Baseline snapshot reads bypass RBAC redaction, so a session carrying a data
	// profile is refused the listing (#1075) — the same invariant that gates
	// reconstruct. A startup profile already forced baselineConfigured false.
	if sessionRestricted(r) {
		recordProfileGateDeny(r, "baselines")
		writeJSONError(w, http.StatusForbidden,
			"backup listings are unavailable while an access-control profile is active: baseline reads aren't redacted")
		return
	}
	resp := baselinesResponse{Reconstruct: b.baselineConfigured, Snapshots: []baselineSnapshotDTO{}}
	if s.baselineRefresh != nil {
		if st := s.baselineRefresh.RefreshStatus(s.selectedServerID(r)); st.State != "idle" {
			resp.Refresh = &st
		}
	}
	if e, ok := s.cm.reg.Get(s.selectedServerID(r)); ok && e.BackupSchedule != nil {
		// Bounded like the listing below: this probe lists the newest
		// snapshot of the fold source, which on an S3-backed server is the
		// bucket (#1679); an unbounded leg beside a bounded one is the
		// handler pinned by the half nobody timed.
		sctx, cancel := context.WithTimeout(r.Context(), baselineListTimeout)
		resp.Schedule = s.backupScheduleDTO(sctx, e, s.scheduleClock())
		cancel()
	}
	if b.baselineSrc == "" {
		writeJSON(w, http.StatusOK, resp)
		return
	}
	resp.Configured = true
	resp.Source = b.baselineSrc
	resp.Kind = "dir"
	if strings.HasPrefix(b.baselineSrc, "s3://") {
		resp.Kind = "s3"
	}

	// Every configured location, not just the primary (#1542). A server with a
	// local directory AND an S3 destination keeps the bucket as the bundle's
	// per-table fallback, which findBaseline consults and this listing did not:
	// once retention pruned the local copies, the page showed nothing while the
	// bucket held dozens, and Time-travel resolved tables from a bucket the page
	// behind it did not list.
	// One more than the cap, so the loop below still learns there were
	// more (Truncated) without reading them (#1679); and the listing's own
	// word for it, because a window whose newest snapshots include an
	// incomplete one comes back SHORTER than the cap with older ones
	// unread, and the loop alone would call that everything.
	anyMore := false
	merged := listBaselinesMerged(r.Context(), baselineSourcesOf(b), func(ctx context.Context, src string) ([]reconstruct.BaselineFile, int, error) {
		files, skipped, more, err := listBaselinesForPage(ctx, src, baselinesMaxSnapshots+1)
		if err == nil && more {
			anyMore = true
		}
		return files, skipped, err
	})
	resp.Truncated = anyMore
	resp.Sources = merged.Sources
	if merged.Listed == 0 {
		// Nothing could be read anywhere. Still a hard failure, and the message
		// names every location so the operator is not left guessing which one.
		var parts []string
		for _, src := range merged.Sources {
			parts = append(parts, src.Source+": "+src.Error)
		}
		writeJSONError(w, http.StatusBadGateway, "list baselines: "+strings.Join(parts, "; "))
		return
	}
	// Incomplete when a location did not answer OR answered in part: a listing
	// that dropped an unreadable snapshot reads exactly like a complete one
	// otherwise (#1601).
	resp.Incomplete = merged.Listed < len(merged.Sources) || merged.Skipped > 0
	files := merged.Files

	now := time.Now().UTC()
	// Staleness floor (#1193): best-effort but never silent and never a
	// fabricated verdict — an unopened bundle connection or an unreadable
	// index yields the explicit "unknown".
	var floor status.DeltaFloor
	serverID := s.selectedServerID(r)
	if b.db == nil {
		slog.Warn("console: baseline staleness not evaluated — the server's index connection is not open", "server", serverID)
	} else if f, err := status.OldestDeltaFromDB(r.Context(), b.db, b.dbName); err != nil {
		slog.Warn("console: could not load delta-coverage floor for baseline staleness", "server", serverID, "error", err)
	} else {
		floor = f
	}
	var cur *baselineSnapshotDTO
	var curTime time.Time
	for _, f := range files {
		if cur == nil || !f.SnapshotTime.Equal(curTime) {
			if len(resp.Snapshots) >= baselinesMaxSnapshots {
				resp.Truncated = true
				break
			}
			curTime = f.SnapshotTime
			dto := baselineSnapshotDTO{
				Time:     f.SnapshotTime.Format(consoleTSFormat),
				AgeHours: now.Sub(f.SnapshotTime).Hours(),
			}
			dto.Staleness = string(floor.Grade(f.SnapshotTime, now))
			dto.Kinds = merged.Kinds[f.SnapshotTime.UnixNano()]
			// Keyed on the file's OWN path, not on the primary source's kind:
			// with two locations merged, the primary being S3 says nothing about
			// whether THIS snapshot came off local disk, and the reverse would
			// try to open an s3:// URL as a file.
			if baselineKindOf(f.Path) == "dir" {
				if meta, err := baseline.ReadParquetMetadata(f.Path); err == nil {
					dto.BinlogFile = meta.BinlogFile
					dto.BinlogPos = meta.BinlogPos
					dto.GTIDSet = meta.GTIDSet
				} else {
					// Tolerated (the listing must not die on one bad footer), but
					// never silent: a corrupt snapshot rendered as merely
					// "coordinate-less" would look identical to a healthy
					// pre-metadata baseline.
					slog.Warn("console: baseline Parquet metadata unreadable", "path", f.Path, "error", err)
				}
			}
			resp.Snapshots = append(resp.Snapshots, dto)
			cur = &resp.Snapshots[len(resp.Snapshots)-1]
		}
		cur.Tables = append(cur.Tables, f.Schema+"."+f.Table)
	}
	// Headline over the files of the snapshots the page reads (the cap plus
	// one, #1679), delegated to the status package's newest-per-table
	// rollup. The CLI's status walks every snapshot, so the two can differ
	// on one shape: a table whose newest baseline is older than the page
	// window (dropped from the backup set long ago) grades the CLI's
	// headline and not this one. Reading the whole inventory for that
	// grade is the cost this bound removes.
	infos := make([]status.BaselineInfo, len(files))
	for i, f := range files {
		infos[i] = status.BaselineInfo{Database: f.Schema, Table: f.Table, SnapshotTime: f.SnapshotTime}
	}
	status.AnnotateBaselineStaleness(infos, floor, now)
	resp.Staleness = string(status.OverallBaselineStaleness(infos))
	writeJSON(w, http.StatusOK, resp)
}
