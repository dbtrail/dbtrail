package reconstruct

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// Where a search for the snapshot's cut may start (#2269).
//
// firstEventPast walks binlog_events upward by event_id and stops at the
// first event stamped past the refresh's time. A refresh targets "now", so
// nearly every event is before it and the walk reads them all: bounded to
// the partition of the current hour (#1692), it read every event the hour
// had indexed so far, and a refresh took longer with every minute of the
// hour (measured: 0.1 s at the hour's first refresh, 12 s at its last, at
// 1.5 M events an hour).
//
// What one search establishes holds for every later one. A search for the
// time T that ends on the event F (or, finding none, having seen everything
// up to the newest event N) has shown that no event below F (up to N) is
// stamped past T. An event not past T is not past any later time. So a
// search for T2 >= T may start at F (at N+1): it reads the events indexed
// since the search before it, one refresh interval's worth, whatever the
// minute of the hour.
//
// This is a statement about rows, not about clocks: it assumes nothing of
// how timestamps relate to commit order, which ResolveSnapshotCut's rule
// does not either. What it does assume is that an event_id keeps naming the
// event it named, and that is checked: the floor is remembered beside the
// newest event read with it, and is used only while that row still holds
// that file and position. A row gone (the stream's cleanup after a restart,
// rotation, an index rebuilt) or naming another event means no floor, and
// the search runs as it did before. So does a search for a time before the
// remembered one, and the first search a process makes.
//
// Kept in memory, per index: the watch daemon resolves a cut every refresh
// and is the caller this is for. A command-line run resolves one and gains
// nothing, as before.
type cutFloorStore struct {
	mu sync.Mutex
	m  map[string]cutFloor
}

// cutFloor is what one search established: no event of this index with an
// id below floor is stamped past at. witness is the newest event read with
// that search.
type cutFloor struct {
	at      time.Time
	floor   uint64
	witness newestEvent
}

var cutFloors = &cutFloorStore{m: map[string]cutFloor{}}

// remember records what a search for at established on index, unless a
// search for a later time already did: the later one's floor is the higher,
// and a search for an earlier time (a fixed --at in the past) must not
// replace it.
func (s *cutFloorStore) remember(index string, at time.Time, floor uint64, witness *newestEvent) {
	if index == "" || witness == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if have, ok := s.m[index]; ok && have.at.After(at) {
		return
	}
	s.m[index] = cutFloor{at: at, floor: floor, witness: *witness}
}

// floorFor returns the event_id a search for at on index may start at, or 0
// for "from the start". Not zero only when an earlier search was for a time
// at or before at AND the event read with it is still the row it was.
func (s *cutFloorStore) floorFor(ctx context.Context, db *sql.DB, index string, at time.Time) uint64 {
	if index == "" {
		return 0
	}
	s.mu.Lock()
	have, ok := s.m[index]
	s.mu.Unlock()
	if !ok || have.at.After(at) {
		return 0
	}
	var file sql.NullString
	var end sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT binlog_file, end_pos FROM binlog_events WHERE event_id = ?`, have.witness.id).Scan(&file, &end)
	if err != nil || !file.Valid || !end.Valid || file.String != have.witness.file || uint64(end.Int64) != have.witness.end {
		// Gone, or another event under that id: nothing remembered about
		// this index holds any more.
		s.forget(index)
		return 0
	}
	return have.floor
}

func (s *cutFloorStore) forget(index string) {
	s.mu.Lock()
	delete(s.m, index)
	s.mu.Unlock()
}
