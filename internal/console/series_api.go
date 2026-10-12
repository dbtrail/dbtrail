package console

import (
	"net/http"
	"strings"
	"time"
)

// handleSeries serves GET /api/series?name=<metric>&labels=<k=v,...>&window=
// 15m|1h|6h|24h: one of this service's own metrics over time, one point per
// minute (#2330). Only numbers leave here, never label values the caller did
// not already name, so there is nothing in it for a data profile to withhold.
func (s *Server) handleSeries(w http.ResponseWriter, r *http.Request) {
	if s.series == nil {
		// A read-only web interface runs apart from capture: the numbers
		// are in another process, and an empty chart would read as "quiet".
		writeJSONError(w, http.StatusForbidden,
			"this web interface runs apart from the DBTrail service that captures, so it keeps no history of its numbers")
		return
	}
	q := r.URL.Query()
	name := q.Get("name")
	if !strings.HasPrefix(name, seriesPrefix) || len(name) > 200 {
		writeJSONError(w, http.StatusBadRequest, "name must be one of DBTrail's own metrics (it starts with "+seriesPrefix+")")
		return
	}
	labels, err := ParseSeriesLabels(q.Get("labels"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	window := q.Get("window")
	if window == "" {
		window = "1h"
	}
	ans, err := s.series.Query(name, labels, window, time.Now())
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ans)
}
