package console

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/storage"
	"github.com/dbtrail/dbtrail/internal/views"
)

// dashboardsDTO is what the Overview's "Dashboards for the team" card shows
// (#2014): whether this server's snapshots can be read from somewhere other
// than this machine, and if so the views file that reads them there.
//
// State is one of:
//   - "s3": the snapshots are on S3 and one has finished uploading. ViewsSQL
//     is the file, generated from exactly the facts beside it.
//   - "s3_empty": S3 is set, and no snapshot has finished uploading there.
//   - "s3_pattern_chars": the S3 location's name holds a character DuckDB
//     reads as a wildcard, so no file is offered.
//   - "s3_unreadable": S3 is set, and this process could not list it. Error
//     says why. Never reported as s3_empty: the fix is access, not waiting.
//   - "local_only": the snapshots are kept only in a folder on this machine,
//     which a teammate's DuckDB cannot reach. No file is offered.
//   - "none": no snapshot location at all.
//   - "s3_timeout": S3 did not answer within dashboardsTimeout.
//   - "no_archive": reading the copy is turned off by this server's setting.
//   - "no_archive_profile": the console runs under a data profile, which
//     turns reading the copy off for every server.
type dashboardsDTO struct {
	State string `json:"state"`
	// S3 is the snapshot location the file reads, as configured.
	S3 string `json:"s3,omitempty"`
	// Snapshot is the newest snapshot the file reads now (on S3), or for
	// local_only the newest one in the local folder; consoleTSFormat, UTC.
	Snapshot string `json:"snapshot,omitempty"`
	Tables   int    `json:"tables,omitempty"`
	// Follows is whether each new session of the file reads the newest
	// snapshot rather than the one named above.
	Follows bool `json:"follows,omitempty"`
	// Region is the bucket's region when it was detected, and then it is
	// also pinned in the file's secret. Empty: the reader's own AWS setup
	// has to name it.
	Region string `json:"region,omitempty"`
	// Endpoint is the S3-compatible store the bucket lives in, when it is
	// not AWS.
	Endpoint string `json:"endpoint,omitempty"`
	// NewerLocal is a local snapshot newer than the newest on S3: published
	// here, not uploaded yet, so the file does not see it.
	NewerLocal string `json:"newer_local,omitempty"`
	// LeftOutTables are the tables the full read behind this snapshot left
	// out (#2006), so they have no view.
	LeftOutTables        []RefusedTable `json:"left_out_tables,omitempty"`
	LeftOutTablesOmitted int            `json:"left_out_tables_omitted,omitempty"`
	// TimeoutSeconds is the deadline S3 did not answer within, for
	// s3_timeout.
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
	Error          string `json:"error,omitempty"`
	ViewsSQL       string `json:"views_sql,omitempty"`
}

// dashboardsTimeout bounds the whole answer. The request context has no
// deadline, and an S3 endpoint that accepts a connection and never answers
// would leave the card loading forever. A variable so a test can shorten it.
var dashboardsTimeout = baselineListTimeout

// handleDashboards serves GET /api/dashboards. The views file travels inside
// the answer rather than from a second request, so the card can never describe
// one snapshot and hand out a file generated against another.
//
// Same gates as GET /api/views.sql, for the same reasons: settings:read, a
// refusal under a data profile (the file is a map straight past the profile's
// filter, and its view names are table names the profile may hide), and not
// audited (view definitions, no row read). The file carries no credential:
// the secret uses the reader's own credential chain.
func (s *Server) handleDashboards(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	if sessionRestricted(r) {
		recordProfileGateDeny(r, "dashboards")
		writeJSONError(w, http.StatusForbidden,
			"the dashboards file is unavailable while an access-control profile is active: "+
				"it maps directly onto the unredacted Parquet files")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	ctx, cancel := context.WithTimeout(r.Context(), dashboardsTimeout)
	defer cancel()
	doc := s.dashboardsDoc(r.WithContext(ctx), b)
	writeJSON(w, http.StatusOK, doc)
}

func (s *Server) dashboardsDoc(r *http.Request, b *bundle) dashboardsDTO {
	if b.noArchive {
		if b.noArchiveProfile {
			return dashboardsDTO{State: "no_archive_profile"}
		}
		return dashboardsDTO{State: "no_archive"}
	}
	s3 := bundleBaselineS3(b)
	if s3 == "" {
		dir := bundleBaselineDir(b)
		if dir == "" {
			return dashboardsDTO{State: "none"}
		}
		doc := dashboardsDTO{State: "local_only"}
		files, _, _, err := reconstruct.ListBaselinesNewestReport(r.Context(), dir, 1)
		switch {
		case err != nil && !errors.Is(err, fs.ErrNotExist):
			// The card's advice (add an S3 location) holds either way; only
			// the snapshot time is lost, and the Snapshots page names the
			// folder's fault in full.
			slog.Warn("dashboards card: could not list the local snapshot folder", "dir", dir, "error", err)
		case len(files) > 0:
			doc.Snapshot = files[0].SnapshotTime.UTC().Format(consoleTSFormat)
		}
		return doc
	}
	doc := dashboardsDTO{S3: s3}
	// DuckDB reads [ * ? { in an s3:// path as a pattern, and the generated
	// file passes its paths to glob() and read_parquet as they are. Measured
	// on DuckDB 1.5.5 against an S3-compatible store: under "team [x]/" the
	// follow finds no snapshot, and even a pinned read_parquet of the exact
	// key answers "No files found". A file that cannot load is not offered.
	if strings.ContainsAny(s3, "[*?{") {
		doc.State = "s3_pattern_chars"
		return doc
	}
	in, err := s.buildViewsInput(r.Context(), b, viewsRequest{OmitEvents: true, StateOnly: true, S3Baseline: true})
	switch {
	case errors.Is(err, errNoViewSources):
		doc.State = "s3_empty"
		return doc
	case err != nil && errors.Is(r.Context().Err(), context.DeadlineExceeded):
		doc.State = "s3_timeout"
		doc.TimeoutSeconds = int(math.Ceil(dashboardsTimeout.Seconds()))
		return doc
	case err != nil:
		// Not only a listing that failed: an S3 endpoint setting that does
		// not parse lands here too, so the card words it as "could not
		// prepare" and shows the error, rather than blaming bucket access.
		doc.State, doc.Error = "s3_unreadable", err.Error()
		return doc
	case len(in.Baselines) == 0:
		doc.State = "s3_empty"
		return doc
	case !in.RendersAnyView():
		// Snapshots were found and no table in them became a view: that is
		// not "nothing uploaded", and saying so would send the reader to wait.
		doc.State, doc.Error = "s3_unreadable", "the newest snapshot on S3 has no table this file can describe"
		return doc
	}
	doc.State = "s3"
	doc.Snapshot = in.BaselineSnapshot.UTC().Format(consoleTSFormat)
	doc.Tables = len(in.Baselines)
	doc.Follows = in.Follow == views.FollowNewest
	doc.Region = in.ArchiveRegion
	if in.S3Endpoint.Set() {
		doc.Endpoint = in.S3Endpoint.URL
	}
	// A bucket with a store of its own (#1575) is read through its scoped
	// secret, which names that store's endpoint and region. The ambient pair
	// above describes every OTHER bucket, and archiveRegion skips a routed one
	// on purpose, so without this the card names the wrong place.
	if bucket, _, perr := storage.ParseS3URL(s3); perr == nil {
		if st, routed := storage.BucketStoreFor(bucket); routed {
			// SigningRegion, not Region: a store with an endpoint and no
			// region signs with us-east-1, and that is what the file pins.
			doc.Region, doc.Endpoint = st.SigningRegion(), ""
			if st.Endpoint.Set() {
				doc.Endpoint = st.Endpoint.URL
			}
		}
	}
	if !in.NewerElsewhere.IsZero() {
		doc.NewerLocal = in.NewerElsewhere.UTC().Format(consoleTSFormat)
		in.NewerElsewhereSource = "the server's local snapshot folder"
		in.NewerElsewhereHowTo = "It reaches this file once it is uploaded to S3."
	}
	// The other location is this host's folder, and this file is for a
	// teammate's machine: say what it is, not where it is on this host.
	if in.NewerElsewhereUnchecked != "" {
		in.NewerElsewhereUnchecked = "the server's local snapshot folder"
	}
	doc.LeftOutTables, doc.LeftOutTablesOmitted = s.baselineHistory.LeftOutAsOf(s.selectedServerID(r), in.BaselineSnapshot)
	doc.ViewsSQL = views.Generate(in)
	return doc
}

// LeftOutAsOf is the tables left out of the snapshot published at `at`: those
// the newest full read at or before it left out (#2006). An update folds from
// the full read below it and never adds a table that read could not store, so
// its snapshot lacks the same ones while its own record names none. Nil-safe:
// a serve-only process keeps no history.
func (h *BaselineRunHistory) LeftOutAsOf(serverID string, at time.Time) ([]RefusedTable, int) {
	if h == nil {
		return nil, 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	for i := len(recs) - 1; i >= 0; i-- {
		r := recs[i]
		if r.Kind != BaselineRunDump || r.SkipReason != "" || r.SnapshotTime == "" {
			continue
		}
		snap, err := time.Parse(time.RFC3339, r.SnapshotTime)
		if err != nil || snap.After(at) {
			continue
		}
		return append([]RefusedTable(nil), r.LeftOutTables...), r.LeftOutTablesOmitted
	}
	return nil, 0
}
