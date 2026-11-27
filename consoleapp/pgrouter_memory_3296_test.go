package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

const pgTestGiB = uint64(1) << 30

// machine is a host with no ceiling of its own and tails at a fifth of it,
// as the router plans by default.
func pgTestMachine(mem uint64) console.PGMemoryInputs {
	return console.PGMemoryInputs{MachineBytes: mem, BudgetBytes: mem, TailsBytes: int64(mem / 5)}
}

// The two hosts #3296 measured are the calibration: the 8 GB one was killed
// for memory, the 16 GB one ran with about 4 GB free. Same setup on both:
// 2 workers at 2 GB, tails at a fifth, a 3 GB buffer pool on compose's
// index-mysql.
func TestPGMemoryVerdict_theTwoHostsOfTheIssue(t *testing.T) {
	plan := func(mem uint64) pgMemoryPlan {
		return pgMemoryPlan{Workers: 2, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(mem),
			Pools: []pgIndexPool{{Bytes: 3 * pgTestGiB, Place: pgPoolBundled}}}
	}
	d := pgMemoryVerdict(plan(8 * pgTestGiB))
	if d.Verdict != "over" || d.ComparedWith != "machine" {
		t.Fatalf("8 GB host: %+v, want over, compared with the machine", d)
	}
	// 3 statements x 2.5 GiB + 1.6 GiB tails + 0.25 GiB results + 3 GiB pool.
	if want := 3*(2*pgTestGiB+pgTestGiB/2) + 8*pgTestGiB/5 + 256<<20 + 3*pgTestGiB; d.NeedBytes != want {
		t.Errorf("8 GB host needs %d, want %d", d.NeedBytes, want)
	}
	if d := pgMemoryVerdict(plan(16 * pgTestGiB)); d.Verdict != "fits" {
		t.Errorf("16 GB host: %+v, want fits (it ran with 4 GB free)", d)
	}
}

// The factory settings (4 workers at 2 GB) are worked out here so the
// number is written down: on 16 GB they do not fit in the worst case, on
// 32 GB they do.
func TestPGMemoryVerdict_factorySettings(t *testing.T) {
	plan := func(mem uint64) pgMemoryPlan {
		return pgMemoryPlan{Workers: defaultPGRouterWorkers, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(mem),
			Pools: []pgIndexPool{{Bytes: mem / 4, Place: pgPoolBundled}}}
	}
	if d := pgMemoryVerdict(plan(16 * pgTestGiB)); d.Verdict != "over" {
		t.Errorf("factory settings on 16 GB: %+v, want over", d)
	}
	if d := pgMemoryVerdict(plan(32 * pgTestGiB)); d.Verdict != "fits" {
		t.Errorf("factory settings on 32 GB: %+v, want fits", d)
	}
}

func TestPGMemoryVerdict_edges(t *testing.T) {
	base := func() pgMemoryPlan {
		return pgMemoryPlan{Workers: 2, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(8 * pgTestGiB),
			Pools: []pgIndexPool{{Bytes: 3 * pgTestGiB, Place: pgPoolBundled}}}
	}
	t.Run("memory unknown", func(t *testing.T) {
		p := base()
		p.In = console.PGMemoryInputs{}
		if d := pgMemoryVerdict(p); d.Verdict != "unknown" || d.Advice != "" {
			t.Errorf("%+v, want unknown and no advice", d)
		}
	})
	t.Run("SQL memory that does not parse", func(t *testing.T) {
		for _, m := range []string{"", "lots", "0", "-1GB"} {
			p := base()
			p.Memory = m
			if d := pgMemoryVerdict(p); d.Verdict != "unknown" || !strings.Contains(d.Why, "SQL memory") {
				t.Errorf("memory %q: %+v, want unknown naming the SQL memory", m, d)
			}
		}
	})
	t.Run("index elsewhere is not counted", func(t *testing.T) {
		p := base()
		p.Pools = []pgIndexPool{{Bytes: 3 * pgTestGiB, Place: pgPoolElsewhere}}
		d := pgMemoryVerdict(p)
		if d.IndexPoolBytes != 0 || d.IndexPool != "elsewhere" {
			t.Errorf("%+v, want no pool counted, elsewhere", d)
		}
	})
	t.Run("index unread is not counted and is said", func(t *testing.T) {
		p := base()
		p.Pools = []pgIndexPool{{Place: pgPoolUnread}}
		d := pgMemoryVerdict(p)
		if d.IndexPoolBytes != 0 || !strings.Contains(pgMemoryLines(d), "could not be read") {
			t.Errorf("%+v / %q", d, pgMemoryLines(d))
		}
	})
	t.Run("two indexes on this machine add up, one elsewhere does not", func(t *testing.T) {
		p := base()
		p.Pools = []pgIndexPool{{Bytes: pgTestGiB, Place: pgPoolBundled}, {Bytes: 2 * pgTestGiB, Place: pgPoolThisMachine}, {Bytes: 9 * pgTestGiB, Place: pgPoolElsewhere}}
		d := pgMemoryVerdict(p)
		if d.IndexPoolBytes != 3*pgTestGiB || d.IndexPool != "bundled,this_machine,elsewhere" {
			t.Errorf("%+v", d)
		}
	})
	t.Run("no PostgreSQL server yet", func(t *testing.T) {
		p := base()
		p.Pools = nil
		if d := pgMemoryVerdict(p); d.IndexPool != "none" {
			t.Errorf("%+v", d)
		}
	})
	t.Run("tails unbounded are said, not added as zero", func(t *testing.T) {
		p := base()
		p.In.TailsBytes, p.In.TailsUnbounded = 0, true
		d := pgMemoryVerdict(p)
		if !d.TailsUnbounded || !strings.Contains(pgMemoryLines(d), "no bound") {
			t.Errorf("%+v / %q", d, pgMemoryLines(d))
		}
	})
	t.Run("tails off take nothing", func(t *testing.T) {
		p := base()
		p.In.TailsBytes, p.In.TailsOff = 0, true
		if d := pgMemoryVerdict(p); d.TailsBytes != 0 || d.TailsUnbounded {
			t.Errorf("%+v", d)
		}
	})
	t.Run("own ceiling: compared with it, the pool left out", func(t *testing.T) {
		p := base()
		p.In = console.PGMemoryInputs{MachineBytes: 8 * pgTestGiB, BudgetBytes: 4 * pgTestGiB, OwnCeiling: true, TailsBytes: int64(4 * pgTestGiB / 5)}
		d := pgMemoryVerdict(p)
		if d.ComparedWith != "own_ceiling" || d.AvailableBytes != 4*pgTestGiB || d.IndexPoolBytes != 0 || d.Verdict != "over" {
			t.Fatalf("%+v", d)
		}
		lines := pgMemoryLines(d)
		if !strings.Contains(lines, "its own statements are stopped") || strings.Contains(lines, "index MySQL, which capture") {
			t.Errorf("own ceiling says the wrong consequence: %q", lines)
		}
		if !strings.Contains(lines, "raise the ceiling by") {
			t.Errorf("own ceiling advice should raise the ceiling: %q", lines)
		}
	})
}

func TestPGMemoryAdvice(t *testing.T) {
	t.Run("fewer workers fit", func(t *testing.T) {
		// 32 GB, 8 workers at 4 GB (5 GiB each with headroom): 9 x 5 = 45.
		p := pgMemoryPlan{Workers: 8, Memory: "4GiB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(32 * pgTestGiB),
			Pools: []pgIndexPool{{Bytes: 8 * pgTestGiB, Place: pgPoolBundled}}}
		d := pgMemoryVerdict(p)
		// Room: 32 - 6.4 - 0.25 - 8 = 17.35 GiB, 3 slots of 5: 2 workers.
		if !strings.Contains(d.Advice, "2 workers fit (PG_ROUTER_WORKERS=2") {
			t.Errorf("advice %q", d.Advice)
		}
	})
	t.Run("not one worker fits: less memory each", func(t *testing.T) {
		p := pgMemoryPlan{Workers: 2, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(8 * pgTestGiB),
			Pools: []pgIndexPool{{Bytes: 3 * pgTestGiB, Place: pgPoolBundled}}}
		d := pgMemoryVerdict(p)
		// Room: 8 - 1.6 - 0.25 - 3 = 3.15 GiB, two slots of 1.575 GiB:
		// 1.25 GiB of SQL memory (1.25 + 0.3125 headroom = 1.5625).
		if !strings.Contains(d.Advice, "PG_ROUTER_WORKERS=1") || !strings.Contains(d.Advice, "SQL_MEMORY=1280MiB") {
			t.Errorf("advice %q", d.Advice)
		}
	})
	t.Run("the rest alone is over", func(t *testing.T) {
		p := pgMemoryPlan{Workers: 1, Memory: "1GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(4 * pgTestGiB),
			Pools: []pgIndexPool{{Bytes: 4 * pgTestGiB, Place: pgPoolThisMachine}}}
		d := pgMemoryVerdict(p)
		if d.Verdict != "over" || !strings.Contains(d.Advice, "lower the index MySQL's buffer pool") {
			t.Errorf("%+v", d)
		}
	})
	t.Run("advice never asks for more workers than set", func(t *testing.T) {
		// 1 worker, too much memory each: lowering workers is no help.
		p := pgMemoryPlan{Workers: 1, Memory: "4GiB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(8 * pgTestGiB),
			Pools: []pgIndexPool{{Bytes: 2 * pgTestGiB, Place: pgPoolBundled}}}
		d := pgMemoryVerdict(p)
		if strings.Contains(d.Advice, "PG_ROUTER_WORKERS=2") || strings.Contains(d.Advice, "PG_ROUTER_WORKERS=0") {
			t.Errorf("advice %q", d.Advice)
		}
	})
}

func TestPGMemoryLimitFor(t *testing.T) {
	for _, c := range []struct {
		per, want int64
	}{
		// 2 GiB + 512 MiB.
		{int64(5 * pgTestGiB / 2), 2 << 30},
		// 768 + 256 = 1024.
		{int64(pgTestGiB), 768 << 20},
		// 256 + 256.
		{512 << 20, 256 << 20},
		// Nothing of 256 MiB fits.
		{400 << 20, 0},
		{0, 0},
	} {
		if got := pgMemoryLimitFor(c.per); got != c.want {
			t.Errorf("pgMemoryLimitFor(%d MiB) = %d MiB, want %d MiB", c.per>>20, got>>20, c.want>>20)
		}
		if got := pgMemoryLimitFor(c.per); got > 0 {
			if per, _ := pgMemoryPerSlot(pgMiBSetting(got)); per > c.per {
				t.Errorf("limit %s takes %d MiB, more than %d", pgMiBSetting(got), per>>20, c.per>>20)
			}
		}
	}
}

func TestPGPoolPlaceOf(t *testing.T) {
	for _, c := range []struct {
		dsn, server, local string
		want               pgPoolPlace
	}{
		{"u:p@tcp(index-mysql:3306)/bintrail", "", "", pgPoolBundled},
		{"u:p@tcp(INDEX-MYSQL:3306)/bintrail", "", "", pgPoolBundled},
		{"u:p@tcp(index-mysql)/bintrail", "", "", pgPoolBundled},
		{"u:p@tcp(127.0.0.1:3306)/b", "box1", "box1", pgPoolThisMachine},
		{"u:p@tcp(localhost:3306)/b", "box1.example.com", "BOX1", pgPoolThisMachine},
		{"u:p@unix(/run/mysqld.sock)/b", "box1", "box1", pgPoolThisMachine},
		{"u:p@tcp(127.0.0.1:3306)/b", "other", "box1", pgPoolElsewhere}, // a tunnel
		{"u:p@tcp(127.0.0.1:3306)/b", "", "box1", pgPoolElsewhere},
		{"u:p@tcp(db.example.com:3306)/b", "box1", "box1", pgPoolElsewhere},
		{"not a dsn", "", "", pgPoolElsewhere},
	} {
		if got := pgPoolPlaceOf(c.dsn, c.server, c.local); got != c.want {
			t.Errorf("pgPoolPlaceOf(%q, %q, %q) = %s, want %s", c.dsn, c.server, c.local, got, c.want)
		}
	}
}

// The lines are generated from real numbers and printed, so the wording is
// read before it ships.
func TestPGMemoryLines_text(t *testing.T) {
	over := pgMemoryVerdict(pgMemoryPlan{Workers: 2, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(8 * pgTestGiB),
		Pools: []pgIndexPool{{Bytes: 3 * pgTestGiB, Place: pgPoolBundled}}})
	fits := pgMemoryVerdict(pgMemoryPlan{Workers: 2, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(16 * pgTestGiB),
		Pools: []pgIndexPool{{Bytes: 3 * pgTestGiB, Place: pgPoolBundled}}})
	for name, d := range map[string]pgMemoryDoc{"over": over, "fits": fits} {
		lines := pgMemoryLines(d)
		t.Logf("%s:\n%s", name, lines)
		if !strings.HasSuffix(lines, "\n") || strings.Contains(lines, "%!") {
			t.Errorf("%s: malformed %q", name, lines)
		}
	}
	if !strings.Contains(pgMemoryLines(over), "WARNING:") || strings.Contains(pgMemoryLines(fits), "WARNING") {
		t.Error("only the over verdict warns")
	}
}

func TestPGMemoryWatch_readsAgainAfterAMinute(t *testing.T) {
	calls := 0
	now := time.Unix(1000, 0)
	w := newPGMemoryWatch(func(context.Context) pgMemoryPlan {
		calls++
		return pgMemoryPlan{Workers: 1, Memory: "1GB", In: pgTestMachine(8 * pgTestGiB)}
	})
	w.now = func() time.Time { return now }
	first := w.doc()
	if w.doc() != first || calls != 1 {
		t.Fatalf("read %d times within the minute, want 1", calls)
	}
	now = now.Add(time.Minute)
	if w.doc(); calls != 2 {
		t.Errorf("read %d times after a minute, want 2", calls)
	}
	var none *pgMemoryWatch
	if none.doc() != nil {
		t.Error("no watch, no document")
	}
}
