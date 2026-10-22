package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

// Run is one seed's run: Steps steps of OpsPerStep ops each on the source,
// and at the end of every step a cut, a Step applied to the copy, the port's
// read of the copy and the three checks over every table.
type Run struct {
	Seed       uint64
	Steps      int
	OpsPerStep int // 0 = DefaultOpsPerStep

	Schema string
	Tables []Table
	Source *sql.DB
	Index  *sql.DB
	// Capture is where the capture goroutine's result lands (buffered). A
	// result there while a barrier waits fails the run with it at once.
	Capture chan error
	// Root is the baselines root the copy lives under.
	Root string

	// Between are the steps drawn at each cut, by weight; the first cut
	// always takes Between[0] (there is no copy to refresh yet).
	Between []Step
	// Fallback is applied at the same cut, one second later, when a step
	// refuses it (Refuser) for a reason the generator caused. nil = fail.
	Fallback Step
	// Read builds the port's read of the copy: the views SQL and the
	// snapshot directory it reads, with the DATETIME columns as wall clocks
	// or as the files hold them. nil = PortViews over Root.
	Read func(ctx context.Context, now time.Time, wallClock bool) (sqlText, snapshotDir string, err error)
	// OpenDuckDB opens the session the views run in. nil = an in-memory
	// DuckDB.
	OpenDuckDB func() (*sql.DB, error)
	// Barrier bounds the wait for capture at each cut. 0 = 60s.
	Barrier time.Duration
	Logf    func(format string, args ...any)
}

// DefaultOpsPerStep is how many ops a step runs when Run.OpsPerStep is 0.
const DefaultOpsPerStep = 10

// Result is what a run did, for the log.
type Result struct {
	Steps     int
	Ops       int
	Applied   map[string]int // step name → times applied
	Fallbacks int
	Wall      time.Duration
}

// Do runs the seed. The error of a failed check is a *Failure.
func (r *Run) Do(ctx context.Context) (Result, error) {
	start := time.Now()
	res := Result{Applied: map[string]int{}}
	if r.Steps <= 0 || len(r.Between) == 0 || len(r.Tables) == 0 {
		return res, fmt.Errorf("oracle run: Steps, Between and Tables are required")
	}
	ops := r.OpsPerStep
	if ops <= 0 {
		ops = DefaultOpsPerStep
	}
	barrier := r.Barrier
	if barrier <= 0 {
		barrier = 60 * time.Second
	}
	read := r.Read
	if read == nil {
		read = func(ctx context.Context, now time.Time, wallClock bool) (string, string, error) {
			return PortViews(ctx, r.Root, now, wallClock)
		}
	}
	open := r.OpenDuckDB
	if open == nil {
		open = func() (*sql.DB, error) { return sql.Open("duckdb", "") }
	}
	logf := r.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}

	g := NewGenerator(r.Seed, r.Tables)
	// The step draw has its own stream so the DML of a seed is the same
	// whatever the list of steps: slice 2 adds steps without changing what
	// the source sees.
	draw := rand.New(rand.NewPCG(r.Seed, 1))
	var lastAt time.Time
	for step := 1; step <= r.Steps; step++ {
		cut := &Cut{Step: step, Schema: r.Schema, Tables: r.Tables, Rows: map[string]TableRows{}}
		for i := range ops {
			op := g.Next()
			if err := execOp(ctx, r.Source, op); err != nil {
				return res, fmt.Errorf("seed %d step %d op %d (%s on %s): %w\n%s", r.Seed, step, i+1, op.Kind, op.Table, err, strings.Join(op.SQL, "\n"))
			}
			cut.Ops = append(cut.Ops, op)
			res.Ops++
		}
		// The quiet point: the position first, then the clock, then the
		// barrier, then the rows. Nothing of this run is in flight, so the
		// rows are the state at the position.
		pos, err := ReadPosition(r.Source)
		if err != nil {
			return res, fmt.Errorf("seed %d step %d: %w", r.Seed, step, err)
		}
		cut.Position = pos
		if cut.At, err = r.nextCutTime(ctx, lastAt); err != nil {
			return res, fmt.Errorf("seed %d step %d: %w", r.Seed, step, err)
		}
		if _, err := WaitCaptured(ctx, r.Index, r.Capture, pos, barrier); err != nil {
			return res, fmt.Errorf("seed %d step %d: %w", r.Seed, step, err)
		}
		for _, t := range r.Tables {
			rows, err := ReadSource(ctx, r.Source, r.Schema, t)
			if err != nil {
				return res, fmt.Errorf("seed %d step %d: %w", r.Seed, step, err)
			}
			cut.Rows[t.Name] = rows
		}

		st := r.Between[0]
		if step > 1 {
			st = r.drawStep(draw)
		}
		name := st.Name()
		if err := st.Apply(ctx, cut); err != nil {
			ref, ok := st.(Refuser)
			if !ok || r.Fallback == nil || !ref.Refused(err, cut) {
				return res, fmt.Errorf("seed %d step %d: %s: %w", r.Seed, step, name, err)
			}
			logf("seed %d step %d: %s refused the cut, taking a %s instead: %v", r.Seed, step, name, r.Fallback.Name(), err)
			cut.At = cut.At.Add(time.Second)
			if err := r.Fallback.Apply(ctx, cut); err != nil {
				return res, fmt.Errorf("seed %d step %d: %s after a refused %s: %w", r.Seed, step, r.Fallback.Name(), name, err)
			}
			name = name + "→" + r.Fallback.Name()
			res.Fallbacks++
		}
		res.Applied[name]++
		lastAt = cut.At

		// The copy is read twice, the two ways the port reads it (the
		// console's sqlWallClockDatetimes): under a session zone other than
		// UTC, with the DATETIME columns cast to wall clocks in the view; and
		// under UTC, with them left as the files hold them, where the read
		// formats the wall clock itself. Both must equal the source; a zone
		// bug in either path shows against the other.
		var dir string
		for _, s := range sessions {
			sqlText, d, err := read(ctx, cut.At, s.wallClock)
			if err != nil {
				return res, fmt.Errorf("seed %d step %d: read the copy after %s: %w", r.Seed, step, name, err)
			}
			dir = d
			cx := Context{Seed: r.Seed, Step: step, Steps: r.Steps, StepName: name, Session: s.zone, SnapshotDir: dir}
			if err := r.check(ctx, open, s, sqlText, cx, cut); err != nil {
				return res, err
			}
		}
		res.Steps = step
		logf("seed %d step %d: %d ops, %s at %s, copy %s equals the source", r.Seed, step, ops, name, pos, dir)
	}
	res.Wall = time.Since(start)
	return res, nil
}

// session is one way of reading the copy: the DuckDB session zone, whether
// the view casts DATETIME columns to wall clocks, and whether the read must
// format them itself.
type session struct {
	zone      string
	wallClock bool
}

var sessions = []session{{"America/Bogota", true}, {"UTC", false}}

func (r *Run) check(ctx context.Context, open func() (*sql.DB, error), s session, sqlText string, cx Context, cut *Cut) error {
	ddb, err := open()
	if err != nil {
		return fmt.Errorf("seed %d step %d: open DuckDB: %w", r.Seed, cx.Step, err)
	}
	defer ddb.Close()
	if _, err := ddb.ExecContext(ctx, "SET TimeZone = '"+s.zone+"'"); err != nil {
		return fmt.Errorf("seed %d step %d: %w", r.Seed, cx.Step, err)
	}
	if _, err := ddb.ExecContext(ctx, sqlText); err != nil {
		return fmt.Errorf("seed %d step %d: DuckDB rejected the views SQL: %w\n--- generated ---\n%s", r.Seed, cx.Step, err, sqlText)
	}
	for _, t := range r.Tables {
		source := cut.Rows[t.Name]
		var datetimes []string
		if !s.wallClock {
			datetimes = t.Datetimes
		}
		view, err := ReadView(ctx, ddb, r.Schema, t.Name, source.Columns, source.PK, datetimes)
		if err != nil {
			return fmt.Errorf("seed %d step %d (%s, session %s): %w", r.Seed, cx.Step, cx.StepName, s.zone, err)
		}
		if err := Check(cx, source, view); err != nil {
			return err
		}
	}
	return nil
}

// nextCutTime reads the server clock and makes it strictly later than the
// previous cut's: a snapshot directory is named by its second, and two cuts
// in one second would be one directory.
func (r *Run) nextCutTime(ctx context.Context, lastAt time.Time) (time.Time, error) {
	now, err := ServerNow(ctx, r.Source)
	if err != nil {
		return time.Time{}, err
	}
	if !now.After(lastAt) {
		time.Sleep(lastAt.Sub(now) + time.Second)
		if now, err = ServerNow(ctx, r.Source); err != nil {
			return time.Time{}, err
		}
		if !now.After(lastAt) {
			return time.Time{}, fmt.Errorf("the server clock did not pass %s", lastAt.Format(time.RFC3339))
		}
	}
	return now, nil
}

func (r *Run) drawStep(draw *rand.Rand) Step {
	total := 0
	for _, s := range r.Between {
		total += s.Weight()
	}
	if total <= 0 {
		return r.Between[0]
	}
	n := draw.IntN(total)
	for _, s := range r.Between {
		if n < s.Weight() {
			return s
		}
		n -= s.Weight()
	}
	return r.Between[0]
}

// execOp runs an op's statements on the source, several inside one
// transaction.
func execOp(ctx context.Context, db *sql.DB, op Op) error {
	if len(op.SQL) == 1 {
		_, err := db.ExecContext(ctx, op.SQL[0])
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, s := range op.SQL {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}
