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

	// Script, when set, is the ops of each step, written by hand: step i runs
	// Script[i-1] on the source instead of OpsPerStep draws of the generator,
	// and Steps must be len(Script). For a window whose shape is the point (a
	// PK change followed by a change of the old or the new key, #2501) rather
	// than one a seed happens to draw. The step draw stays by seed.
	Script [][]Op
	// Between are the steps drawn at each cut, by weight; the first cut
	// always takes Between[0] (there is no copy to refresh yet).
	Between []Step
	// Read builds the port's read of the copy at cut: the views SQL and the
	// snapshot directory it reads, with the DATETIME columns as wall clocks
	// or as the files hold them, and which tables it answers as of the
	// published cut. nil = PortViews over Root, every table compared with
	// the cut's rows, which is right only when every step publishes.
	Read func(ctx context.Context, cut *Cut, wallClock bool) (Read, error)
	// OpenDuckDB opens the session the views run in. nil = an in-memory
	// DuckDB.
	OpenDuckDB func() (*sql.DB, error)
	// Barrier bounds the wait for capture at each cut. 0 = 60s.
	Barrier time.Duration
	Logf    func(format string, args ...any)
}

// DefaultOpsPerStep is how many ops a step runs when Run.OpsPerStep is 0.
const DefaultOpsPerStep = 10

// Read is one read of the copy: the views SQL, the snapshot directory it
// reads, and the tables the port answers as of the published cut rather
// than the cut being checked (no tail over them). Those are compared with
// the rows of the published cut; the others with the cut's.
type Read struct {
	SQL         string
	SnapshotDir string
	AsPublished map[string]bool
}

// published is a table's rows at the last cut a step published.
type published struct {
	step int
	rows TableRows
}

// Result is what a run did, for the log.
type Result struct {
	Steps   int
	Ops     int
	Applied map[string]int // step name → times applied
	Wall    time.Duration
}

// Do runs the seed. The error of a failed check is a *Failure.
func (r *Run) Do(ctx context.Context) (Result, error) {
	start := time.Now()
	res := Result{Applied: map[string]int{}}
	if r.Steps <= 0 || len(r.Between) == 0 || len(r.Tables) == 0 {
		return res, fmt.Errorf("oracle run: Steps, Between and Tables are required")
	}
	if r.Script != nil && len(r.Script) != r.Steps {
		return res, fmt.Errorf("oracle run: Script has %d steps, Steps is %d", len(r.Script), r.Steps)
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
		read = func(ctx context.Context, cut *Cut, wallClock bool) (Read, error) {
			sqlText, dir, err := PortViews(ctx, r.Root, cut.At, wallClock)
			return Read{SQL: sqlText, SnapshotDir: dir}, err
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
	// The published cut: its step, the rows read there, and the ops since.
	pub := map[string]published{}
	var sincePublish []Op
	publishedStep := 0
	for step := 1; step <= r.Steps; step++ {
		cut := &Cut{Step: step, Schema: r.Schema, Tables: r.Tables, Rows: map[string]TableRows{}, PublishedStep: publishedStep}
		var stepOps []Op
		if r.Script != nil {
			stepOps = r.Script[step-1]
		} else {
			for range ops {
				stepOps = append(stepOps, g.Next())
			}
		}
		for i, op := range stepOps {
			if err := execOp(ctx, r.Source, op); err != nil {
				return res, fmt.Errorf("seed %d step %d op %d (%s on %s): %w\n%s", r.Seed, step, i+1, op.Kind, op.Table, err, strings.Join(op.SQL, "\n"))
			}
			cut.Ops = append(cut.Ops, op)
			sincePublish = append(sincePublish, op)
			res.Ops++
		}
		cut.SincePublish = sincePublish
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
		publishes := st.Publishes()
		// A step that fails fails the run. There is no fallback: the one
		// refusal a step made by design (the fold's, of a window with a
		// PK-changing UPDATE) went with #2501, and a refusal is a finding.
		if err := st.Apply(ctx, cut); err != nil {
			return res, fmt.Errorf("seed %d step %d: %s: %w", r.Seed, step, name, err)
		}
		res.Applied[name]++
		lastAt = cut.At
		if publishes {
			// The read after this step sees the cut as published: nothing
			// is since it yet.
			for _, t := range r.Tables {
				pub[t.Name] = published{step: step, rows: cut.Rows[t.Name]}
			}
			sincePublish, cut.SincePublish = nil, nil
			publishedStep, cut.PublishedStep = step, step
		}

		// The copy is read twice, the two ways the port reads it (the
		// console's sqlWallClockDatetimes): under a session zone other than
		// UTC, with the DATETIME columns cast to wall clocks in the view; and
		// under UTC, with them left as the files hold them, where the read
		// formats the wall clock itself. Both must equal the source; a zone
		// bug in either path shows against the other.
		var dir string
		for _, s := range sessions {
			rd, err := read(ctx, cut, s.wallClock)
			if err != nil {
				return res, fmt.Errorf("seed %d step %d: read the copy after %s: %w", r.Seed, step, name, err)
			}
			dir = rd.SnapshotDir
			cx := Context{Seed: r.Seed, Step: step, Steps: r.Steps, StepName: name, Session: s.zone, SnapshotDir: dir}
			if err := r.check(ctx, open, s, rd, cx, cut, pub); err != nil {
				return res, err
			}
		}
		res.Steps = step
		logf("seed %d step %d: %d ops, %s at %s, copy %s equals the source", r.Seed, step, len(stepOps), name, pos, dir)
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

func (r *Run) check(ctx context.Context, open func() (*sql.DB, error), s session, rd Read, cx Context, cut *Cut, pub map[string]published) error {
	sqlText := rd.SQL
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
		source, tcx, err := sourceFor(cx, t.Name, cut, rd, pub)
		if err != nil {
			return fmt.Errorf("seed %d step %d (%s, session %s): %w", r.Seed, cx.Step, cx.StepName, s.zone, err)
		}
		var datetimes []string
		if !s.wallClock {
			datetimes = t.Datetimes
		}
		view, err := ReadView(ctx, ddb, r.Schema, t.Name, source.Columns, source.PK, datetimes)
		if err != nil {
			return fmt.Errorf("seed %d step %d (%s, session %s): %w", r.Seed, cx.Step, cx.StepName, s.zone, err)
		}
		if err := Check(tcx, source, view); err != nil {
			return err
		}
	}
	return nil
}

// sourceFor is what the copy's read of table must equal: the cut's rows,
// or, for a table the read answers as of the published cut, the rows read
// at that cut, with the check's context saying so. A table answered as of
// a published cut when none was published is a read that invented one.
func sourceFor(cx Context, table string, cut *Cut, rd Read, pub map[string]published) (TableRows, Context, error) {
	if !rd.AsPublished[table] {
		return cut.Rows[table], cx, nil
	}
	p, ok := pub[table]
	if !ok {
		return TableRows{}, cx, fmt.Errorf("table %s: the read answers as of the published cut, and no step published one", table)
	}
	cx.AsOf = fmt.Sprintf("the published cut (step %d)", p.step)
	return p.rows, cx, nil
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
