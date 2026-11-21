package console

import (
	"fmt"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/rotation"
)

// deltaRetainFloor is the retention table deltas need, for the copy that asks
// for the most (#2959): the one updated least often among those a retention
// can make room for.
type deltaRetainFloor struct {
	// Must is what the retention has to be longer than, and Comfortable the
	// retention from which table deltas do their job on every update
	// (BaselineRefreshDefaults.MinRetainForTableDeltas for Every). Just over
	// Must most updates still write the tables in full: rotation drops whole
	// hours, so the oldest hour the index keeps can be up to an hour newer
	// than the retention says.
	Must        time.Duration
	Comfortable time.Duration
	// Every is how often that copy is updated, and EveryRaw the operator's
	// spelling of it. Server is its name, empty for the daemon-wide
	// --baseline-refresh-interval, which updates every server's.
	Every    time.Duration
	EveryRaw string
	Server   string
}

// allows reports whether a chain of table deltas can ever survive an update
// under retain. It is the line the form refuses on: nothing that works
// today, however little, is under it.
func (f deltaRetainFloor) allows(retain time.Duration) bool { return retain > f.Must }

// wholeHours writes the shortest retention of at least d in the form's own
// grammar, which is whole hours (cliutil.ParseRetain).
func wholeHours(d time.Duration) string {
	h := int64(d / time.Hour)
	if d%time.Hour != 0 {
		h++
	}
	return fmt.Sprintf("%dh", h)
}

// shortest is the shortest retention the form saves: the first whole hour
// over Must.
func (f deltaRetainFloor) shortest() string { return wholeHours(f.Must + time.Nanosecond) }

// recommended is the retention the texts recommend: the first whole hour
// from Comfortable on.
func (f deltaRetainFloor) recommended() string { return wholeHours(f.Comfortable) }

// wellFrom is the sentence that recommends, empty when the shortest retention
// that saves is already the recommended one (an interval of whole half hours).
func (f deltaRetainFloor) wellFrom() string {
	if f.shortest() == f.recommended() {
		return ""
	}
	return " Table deltas work well from " + f.recommended() + "."
}

// who names the copy the floor comes from, as the start of a sentence.
func (f deltaRetainFloor) who() string {
	if f.Server == "" {
		return "The copies are updated every " + f.EveryRaw + " (--baseline-refresh-interval)"
	}
	return fmt.Sprintf("Server %q updates its copy every %s", f.Server, f.EveryRaw)
}

// tableDeltasOffWords is the way out every refusal and warning offers. Table
// deltas are a startup setting, so no control on the page turns them off, and
// the option file comes first: it is the one way that needs no edit of the
// service's command, which is what a Docker Compose install would need for
// the flag.
const tableDeltasOffWords = "turn table deltas off (baseline-table-deltas = false in dbtrail.cnf, or --baseline-table-deltas=false; then restart DBTrail)"

// refusal is why PUT /api/rotation does not save retainRaw.
func (f deltaRetainFloor) refusal(retainRaw string) string {
	return fmt.Sprintf("Retention %s is too short while table deltas are on. %s, so with less than %s every update writes every table again in full.%s "+
		"Set a longer retention, or first %s.",
		retainRaw, f.who(), f.shortest(), f.wellFrom(), tableDeltasOffWords)
}

// hint is the line the rotation form carries while the retention in use
// allows a chain: the limit before a save meets it, and that the limit is not
// where table deltas start to pay.
func (f deltaRetainFloor) hint() string {
	if f.wellFrom() == "" {
		return fmt.Sprintf("Table deltas are on. %s: the shortest retention that can be saved is %s.", f.who(), f.shortest())
	}
	return fmt.Sprintf("Table deltas are on. %s: the shortest retention that can be saved is %s, and that close to the minimum "+
		"tables are still written in full often.%s", f.who(), f.shortest(), f.wellFrom())
}

// warning is the line the rotation form carries when the retention in use
// does not allow a chain. It got there by another door than this form: the
// daemon's own --rotate-retain, a policy saved by an earlier version, or a
// schedule made longer since.
func (f deltaRetainFloor) warning(retainRaw string) string {
	return fmt.Sprintf("The retention in use, %s, is too short for table deltas: %s, and with less than %s every update writes every table again in full.%s "+
		"Set a longer retention here, or %s.",
		retainRaw, lowerFirst(f.who()), f.shortest(), f.wellFrom(), tableDeltasOffWords)
}

// scheduleWarning is the same finding on the server's own schedule card.
func (f deltaRetainFloor) scheduleWarning(retainRaw string) string {
	return fmt.Sprintf("Every update writes every table again in full. The index keeps %s of changes, and with an update every %s "+
		"table deltas need a retention of %s or more.%s Set a longer retention (Settings, Retention), or %s.",
		retainRaw, f.EveryRaw, f.shortest(), f.wellFrom(), tableDeltasOffWords)
}

// scheduleDeltasNoteWords is the schedule card's line for updates too far
// apart for table deltas on any retention. It asks for nothing.
const scheduleDeltasNoteWords = "With updates more than a day apart, every update writes every table in full: " +
	"a table's changes are kept beside its file for a day at most."

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

// deltaFloorOf is the floor one update cadence sets. False when table deltas
// are off in this daemon, when it updates no copy, and when the updates are
// so far apart that no retention makes room for a chain (a day or more: the
// refresh ends every chain by age), so none is asked for.
func (s *Server) deltaFloorOf(every time.Duration, everyRaw, server string) (deltaRetainFloor, bool) {
	need := s.baselineRefreshDefaults.MinRetainForTableDeltas
	if !s.baselineRefreshDefaults.TableDeltas || need == nil || every <= 0 {
		return deltaRetainFloor{}, false
	}
	must, comfortable, ok := need(every)
	if !ok {
		return deltaRetainFloor{}, false
	}
	return deltaRetainFloor{Must: must, Comfortable: comfortable, Every: every, EveryRaw: everyRaw, Server: server}, true
}

// updatesOnSchedule is e's schedule as the daemon parses it, when its updates
// write chains of table deltas at all. False for:
//
//   - a server with no schedule, or one that does not parse;
//   - a schedule this daemon will not run (CheckBackupSchedule: no loop, a
//     location another server writes too), or whose runs can only be full
//     reads (no index or no local folder to update a copy in);
//   - a schedule whose every run is a full read the operator asked for;
//   - a PostgreSQL source. Its update carries an unchanged table forward on
//     the same line (the file's own start against the index's floor), but a
//     changed table is written in full on any retention, and this rule is
//     about what a retention buys a table that keeps changing.
func (s *Server) updatesOnSchedule(e ServerEntry) (ParsedBackupSchedule, bool) {
	if e.BackupSchedule == nil || e.IsPostgres() {
		return ParsedBackupSchedule{}, false
	}
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		return ParsedBackupSchedule{}, false
	}
	if p.FullEvery > 0 && p.FullEvery <= p.Every {
		return ParsedBackupSchedule{}, false
	}
	gates := s.scheduleGates()
	if CheckBackupSchedule(e, *e.BackupSchedule, gates) != nil || rebuildPossible(e, gates) != nil {
		return ParsedBackupSchedule{}, false
	}
	return p, true
}

// serverDeltaFloor is the floor e's own schedule sets.
func (s *Server) serverDeltaFloor(e ServerEntry) (deltaRetainFloor, bool) {
	p, ok := s.updatesOnSchedule(e)
	if !ok {
		return deltaRetainFloor{}, false
	}
	return s.deltaFloorOf(p.Every, strings.TrimSpace(e.BackupSchedule.Every), e.Name)
}

// deltaRetainFloor is the highest floor any update cadence of this daemon
// sets: every server's schedule, and the daemon-wide refresh interval. The
// retention is one for every index, so the copy updated least often decides,
// among those a retention can do something for. False when no cadence sets
// one.
func (s *Server) deltaRetainFloor() (deltaRetainFloor, bool) {
	var top deltaRetainFloor
	found := false
	consider := func(f deltaRetainFloor, ok bool) {
		if ok && (!found || f.Must > top.Must) {
			top, found = f, true
		}
	}
	d := s.baselineRefreshDefaults
	servers := s.cm.reg.List()
	// The daemon-wide interval updates every server's copy, and the boot
	// index's when no server is saved. With servers saved and every one a
	// PostgreSQL source, no table that changes keeps a chain.
	mysql := len(servers) == 0
	for _, e := range servers {
		consider(s.serverDeltaFloor(e))
		mysql = mysql || !e.IsPostgres()
	}
	if mysql {
		consider(s.deltaFloorOf(d.RefreshEvery, d.RefreshEveryRaw, ""))
	}
	return top, found
}

// retainInUse is the retention the refresh measures chains against, as a
// duration and as written: the saved policy, else what the daemon was started
// with (effectiveRotationValues). Zero when that turns rotation off: nothing
// is then dropped on a clock this daemon knows, and no floor applies.
func (s *Server) retainInUse() (time.Duration, string) {
	cur := s.effectiveRotationValues()
	st, err := rotation.ParseSettings(cur.Retain, cur.Interval, cur.AddFuture, true)
	if err != nil || !st.Enabled {
		return 0, ""
	}
	return st.Retain, cur.Retain
}

// rotationDeltasDTO is what table deltas ask of the retention (#2959), on
// GET /api/rotation. Absent when they ask nothing.
type rotationDeltasDTO struct {
	// MinRetain is the shortest retention a PUT accepts, and
	// RecommendedRetain the one from which table deltas do their job on
	// every update, both in the form's grammar.
	MinRetain         string `json:"min_retain"`
	RecommendedRetain string `json:"recommended_retain"`
	// TooShort: the retention in use is below MinRetain. Note is then the
	// warning, and otherwise the hint that states the limit.
	TooShort bool   `json:"too_short,omitempty"`
	Note     string `json:"note"`
}

func (s *Server) rotationDeltas() *rotationDeltasDTO {
	f, ok := s.deltaRetainFloor()
	if !ok {
		return nil
	}
	dto := &rotationDeltasDTO{MinRetain: f.shortest(), RecommendedRetain: f.recommended(), Note: f.hint()}
	if retain, raw := s.retainInUse(); retain > 0 && !f.allows(retain) {
		dto.TooShort, dto.Note = true, f.warning(raw)
	}
	return dto
}

// scheduleDeltasWarning is the line e's schedule card carries when the
// retention in use ends every chain of table deltas an update of e begins.
func (s *Server) scheduleDeltasWarning(e ServerEntry) string {
	f, ok := s.serverDeltaFloor(e)
	if !ok {
		return ""
	}
	if retain, raw := s.retainInUse(); retain > 0 && !f.allows(retain) {
		return f.scheduleWarning(raw)
	}
	return ""
}

// scheduleDeltasNote is the line e's schedule card carries when its updates
// are so far apart that the refresh ends every chain by age, on any
// retention: it says what each update costs and asks for nothing.
func (s *Server) scheduleDeltasNote(e ServerEntry) string {
	d := s.baselineRefreshDefaults
	if !d.TableDeltas || d.MinRetainForTableDeltas == nil || d.TableDeltaMaxAge <= 0 {
		return ""
	}
	// More than, not at: updates exactly a day apart end a chain or keep it
	// by the second each one starts on, so nothing certain can be said.
	if p, ok := s.updatesOnSchedule(e); ok && p.Every > d.TableDeltaMaxAge {
		return scheduleDeltasNoteWords
	}
	return ""
}
