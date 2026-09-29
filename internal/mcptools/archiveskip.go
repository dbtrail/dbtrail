package mcptools

import (
	"context"
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/query"
)

// planArchiveSkip builds the plan the live-first check hands to the archive
// short-circuits. A variable so unit tests can supply a plan (and count the
// round trips) without mocking every planner read.
var planArchiveSkip = query.PlanArchiveSkip

// liveAnswers reports whether the rows the live index returned already answer
// the request, so the tool's archive loop can be skipped (#1410). It consults
// the same proofs FetchMerged takes (query.ArchivesNotNeeded) and nothing
// else: the tool keeps its own loop, and with it recover's rule that a failed
// archive source is a refusal. A skip only removes reads that provably could
// not change the result; any source that IS read still fails the old way.
//
// Declines, and the loop runs as before, when:
//
//   - no archive source was resolved (nothing to skip);
//   - the sources came from the BINTRAIL_ARCHIVE_S3 + BINTRAIL_ID pair. Such
//     an archive may have no archive_state rows at all, and the proofs judge
//     "every archive sits below the live floor" from those rows: with none,
//     the premise holds vacuously and a skip could drop data;
//   - the target has no database name (the planner cannot run), or no proof
//     holds for this request.
//
// A planning error is an ordinary decline: the plan only ever enables a skip.
func (t *Target) liveAnswers(ctx context.Context, opts query.Options, rows []query.ResultRow, archSources []string) ([]query.ResultRow, bool) {
	if len(archSources) == 0 {
		return rows, false
	}
	if t.EnvArchiveDiscovery {
		if _, ok := envArchiveBase(); ok {
			return rows, false
		}
	}
	var plan *query.QueryPlan
	if t.DBName != "" && query.ArchiveSkipNeedsPlan(opts, rows) {
		p, err := planArchiveSkip(ctx, t.DB, t.DBName, opts, archSources)
		if err != nil {
			slog.Debug("planning the archive skip failed; reading the archives", "error", err)
		} else {
			plan = p
		}
	}
	return query.ArchivesNotNeeded(opts, rows, plan)
}

// queryArchivesSkippedNote and recoverArchivesSkippedNote are the record a
// skip leaves in the response. A skip must never be silent: without it, an
// answer from the live index alone reads the same as one from an index that
// has no archives. They state the fact, not which proof fired, and carry no
// flag names (an MCP client reads them). A note, not a warning: the archives
// could not add rows. They deliberately do NOT say "nothing is missing": the
// newest-first proof fires on a full page, which sits next to a truncation
// warning that says older rows were cut, and both must stay true together.
func queryArchivesSkippedNote() string {
	return "archives_skipped: this result came from the live index alone; the registered archives were not read because they could not add rows to this answer"
}

func recoverArchivesSkippedNote() string {
	return "archives_skipped: this reversal was built from the live index alone; the registered archives were not read because they could not add rows to this answer"
}
