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
		return pgMemoryPlan{Servers: 1, Workers: 2, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(mem),
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
	// The two PostgreSQL boards, healthy, read from their /proc/meminfo.
	for _, kb := range []uint64{15988900, 16041000} {
		if d := pgMemoryVerdict(plan(kb << 10)); d.Verdict != "fits" {
			t.Errorf("board with %d kB: %+v, want fits", kb, d)
		}
	}
}

// The factory settings (4 workers at 2 GB) are worked out here so the
// number is written down: on 16 GB they do not fit in the worst case, on
// 32 GB they do.
func TestPGMemoryVerdict_factorySettings(t *testing.T) {
	plan := func(mem uint64) pgMemoryPlan {
		return pgMemoryPlan{Servers: 1, Workers: defaultPGRouterWorkers, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(mem),
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
		return pgMemoryPlan{Servers: 1, Workers: 2, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(8 * pgTestGiB),
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
		p.Pools = []pgIndexPool{{Place: pgPoolUnread, Unread: true}}
		d := pgMemoryVerdict(p)
		if d.IndexPoolBytes != 0 || !d.IndexPoolUnread || !strings.Contains(pgMemoryLines(d), "could not be read") {
			t.Errorf("%+v / %q", d, pgMemoryLines(d))
		}
		// Over even without it: still over.
		if d.Verdict != "over" {
			t.Errorf("8 GB without the pool is still over: %+v", d)
		}
		// Fits without it: not said to fit.
		p.In = pgTestMachine(16 * pgTestGiB)
		p.Pools = []pgIndexPool{{Place: pgPoolBundled, Unread: true}}
		d = pgMemoryVerdict(p)
		if d.Verdict != "unknown" || d.IndexPool != "bundled" || !strings.Contains(pgMemoryLines(d), "not said") {
			t.Errorf("unread bundled index on 16 GB: %+v / %q", d, pgMemoryLines(d))
		}
		// An unread index elsewhere does not matter.
		p.Pools = []pgIndexPool{{Place: pgPoolElsewhere, Unread: true}}
		if d := pgMemoryVerdict(p); d.Verdict != "fits" {
			t.Errorf("unread index elsewhere: %+v", d)
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
		p.Pools, p.Servers = nil, 0
		// The MySQL board: the factory settings on 15.3 GiB are over, but
		// with no PostgreSQL server nothing runs on the copy.
		p.Workers = defaultPGRouterWorkers
		p.In = pgTestMachine(16041004 << 10)
		d := pgMemoryVerdict(p)
		if d.IndexPool != "none" || d.Verdict != "idle" || d.Advice != "" || strings.Contains(pgMemoryLines(d), "WARNING") {
			t.Errorf("%+v / %q", d, pgMemoryLines(d))
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
	t.Run("each PostgreSQL server has its own tails", func(t *testing.T) {
		// 16 GB, 2 workers, a 4 GB pool: one server fits, two do not.
		p := pgMemoryPlan{Servers: 1, Workers: 2, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(16 * pgTestGiB),
			Pools: []pgIndexPool{{Bytes: 4 * pgTestGiB, Place: pgPoolBundled}}}
		if d := pgMemoryVerdict(p); d.Verdict != "fits" {
			t.Errorf("one server: %+v", d)
		}
		p.Servers = 2
		d := pgMemoryVerdict(p)
		if d.Verdict != "over" || d.TailsBytes != 2*uint64(p.In.TailsBytes) || !strings.Contains(pgMemoryLines(d), "2 PostgreSQL servers") {
			t.Errorf("two servers: %+v / %q", d, pgMemoryLines(d))
		}
	})
	t.Run("a share is not a ceiling: the machine and the pool still count", func(t *testing.T) {
		// What PGMemoryInputsNow gives with PG_ROUTER_MEMORY_SHARE=0.95 and
		// no mem_limit: a smaller budget, no ceiling of its own.
		p := base()
		p.In = console.PGMemoryInputs{MachineBytes: 16 * pgTestGiB, BudgetBytes: 16 * pgTestGiB * 95 / 100, TailsBytes: int64(16 * pgTestGiB * 95 / 100 / 5)}
		p.Workers = 3
		p.Pools = []pgIndexPool{{Bytes: 4 * pgTestGiB, Place: pgPoolBundled}}
		d := pgMemoryVerdict(p)
		if d.ComparedWith != "machine" || d.IndexPoolBytes != 4*pgTestGiB || d.Verdict != "over" {
			t.Errorf("%+v", d)
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
		p.In = console.PGMemoryInputs{MachineBytes: 8 * pgTestGiB, BudgetBytes: 4 * pgTestGiB, OwnCeiling: true, CeilingBytes: 4 * pgTestGiB, TailsBytes: int64(4 * pgTestGiB / 5)}
		d := pgMemoryVerdict(p)
		if d.ComparedWith != "own_ceiling" || d.AvailableBytes != 4*pgTestGiB || d.NeedBytes != 3*(5*pgTestGiB/2)+uint64(p.In.TailsBytes)+256<<20 || d.Verdict != "over" {
			t.Fatalf("%+v", d)
		}
		lines := pgMemoryLines(d)
		if !strings.Contains(lines, "its own statements are stopped") || strings.Contains(lines, "index MySQL, which capture") {
			t.Errorf("own ceiling says the wrong consequence: %q", lines)
		}
		// 8.6 GiB needed, and the machine holds 5 beside the 3 GiB pool:
		// raising the ceiling is no advice.
		if strings.Contains(lines, "raise PG_ROUTER_MEMORY") || !strings.Contains(lines, "give the machine more memory") || strings.Contains(lines, "buffer pool") {
			t.Errorf("own ceiling advice: %q", lines)
		}
	})
	t.Run("raising the ceiling is advised only as far as the machine holds it", func(t *testing.T) {
		// 16 GiB, a 4 GiB pool, a 10 GiB ceiling; 3 workers at 2 GB (4 x
		// 2.5 = 10), 1.25 GiB of tails, 256 MiB of results: 11.5 GiB.
		p := base()
		p.Workers = 3
		p.In = console.PGMemoryInputs{MachineBytes: 16 * pgTestGiB, BudgetBytes: 10 * pgTestGiB, OwnCeiling: true, CeilingBytes: 10 * pgTestGiB, TailsBytes: int64(5 * pgTestGiB / 4)}
		p.Pools = []pgIndexPool{{Bytes: 4 * pgTestGiB, Place: pgPoolBundled}}
		d := pgMemoryVerdict(p)
		if d.ComparedWith != "own_ceiling" || !strings.Contains(d.Advice, "raise PG_ROUTER_MEMORY to at least 11.5 GiB (at most 12.0 GiB") {
			t.Errorf("%+v", d)
		}
	})
	t.Run("the ceiling is the container's limit, not times the share", func(t *testing.T) {
		// PG_ROUTER_MEMORY=8g with PG_ROUTER_MEMORY_SHARE=0.5: the kernel
		// enforces 8, the router plans with 4.
		p := base()
		p.Workers, p.Memory = 1, "1GB"
		p.In = console.PGMemoryInputs{MachineBytes: 32 * pgTestGiB, BudgetBytes: 4 * pgTestGiB, OwnCeiling: true, CeilingBytes: 8 * pgTestGiB, TailsBytes: int64(4 * pgTestGiB / 5)}
		d := pgMemoryVerdict(p)
		// 2 x 1.25 + 0.8 + 0.25 = 3.55, under either; with 2 workers 4.8,
		// over 4 but under the real 8.
		p.Workers = 2
		d = pgMemoryVerdict(p)
		if d.Verdict != "fits" || d.OwnCeilingBytes != 8*pgTestGiB {
			t.Errorf("%+v", d)
		}
	})
	t.Run("a ceiling as big as the machine does not hide the pool", func(t *testing.T) {
		// PG_ROUTER_MEMORY=8g on 8 GB with a 3 GB pool, 1 worker at 1 GB.
		p := base()
		p.Workers, p.Memory = 1, "1GB"
		p.In = console.PGMemoryInputs{MachineBytes: 8 * pgTestGiB, BudgetBytes: 8 * pgTestGiB, OwnCeiling: true, CeilingBytes: 8 * pgTestGiB, TailsBytes: int64(8 * pgTestGiB / 5)}
		d := pgMemoryVerdict(p)
		// Own: 2 x 1.25 + 1.6 + 0.25 = 4.35 of 8 fits the ceiling; with the
		// pool, 7.35 of 8 fits the machine.
		if d.Verdict != "fits" || d.ComparedWith != "machine" {
			t.Fatalf("%+v", d)
		}
		p.Workers = 2
		d = pgMemoryVerdict(p)
		// 3 x 1.25 + 1.6 + 0.25 + 3 = 8.6 of 8.
		if d.Verdict != "over" || d.ComparedWith != "machine" || !strings.Contains(d.Advice, "lower PG_ROUTER_MEMORY to at most 5.0 GiB") {
			t.Errorf("%+v", d)
		}
		if !strings.Contains(pgMemoryLines(d), "fits its own 8.0 GiB ceiling") {
			t.Errorf("%q", pgMemoryLines(d))
		}
	})
}

func TestPGMemoryAdvice(t *testing.T) {
	t.Run("fewer workers fit", func(t *testing.T) {
		// 32 GB, 8 workers at 4 GB (5 GiB each with headroom): 9 x 5 = 45.
		p := pgMemoryPlan{Servers: 1, Workers: 8, Memory: "4GiB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(32 * pgTestGiB),
			Pools: []pgIndexPool{{Bytes: 8 * pgTestGiB, Place: pgPoolBundled}}}
		d := pgMemoryVerdict(p)
		// Room: 32 - 6.4 - 0.25 - 8 = 17.35 GiB, 3 slots of 5: 2 workers.
		if !strings.Contains(d.Advice, "2 workers fit (PG_ROUTER_WORKERS=2") {
			t.Errorf("advice %q", d.Advice)
		}
	})
	t.Run("not one worker fits: less memory each", func(t *testing.T) {
		p := pgMemoryPlan{Servers: 1, Workers: 2, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(8 * pgTestGiB),
			Pools: []pgIndexPool{{Bytes: 3 * pgTestGiB, Place: pgPoolBundled}}}
		d := pgMemoryVerdict(p)
		// Room: 8 - 1.6 - 0.25 - 3 = 3.15 GiB, two slots of 1.575 GiB:
		// 1.25 GiB of SQL memory (1.25 + 0.3125 headroom = 1.5625).
		if !strings.Contains(d.Advice, "PG_ROUTER_WORKERS=1") || !strings.Contains(d.Advice, "SQL_MEMORY=1280MiB") {
			t.Errorf("advice %q", d.Advice)
		}
	})
	t.Run("the rest alone is over", func(t *testing.T) {
		p := pgMemoryPlan{Servers: 1, Workers: 1, Memory: "1GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(4 * pgTestGiB),
			Pools: []pgIndexPool{{Bytes: 4 * pgTestGiB, Place: pgPoolThisMachine}}}
		d := pgMemoryVerdict(p)
		if d.Verdict != "over" || !strings.Contains(d.Advice, "lower the index MySQL's buffer pool") {
			t.Errorf("%+v", d)
		}
	})
	t.Run("advice never asks for more workers than set", func(t *testing.T) {
		// 1 worker, too much memory each: lowering workers is no help.
		p := pgMemoryPlan{Servers: 1, Workers: 1, Memory: "4GiB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(8 * pgTestGiB),
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
		{512 << 20, 0},
		// Under the 512 MiB floor the router refuses at start: none.
		{1000 << 20, 640 << 20},
		{900 << 20, 640 << 20},
		{800 << 20, 512 << 20},
		{700 << 20, 0},
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
	over := pgMemoryVerdict(pgMemoryPlan{Servers: 1, Workers: 2, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(8 * pgTestGiB),
		Pools: []pgIndexPool{{Bytes: 3 * pgTestGiB, Place: pgPoolBundled}}})
	fits := pgMemoryVerdict(pgMemoryPlan{Servers: 1, Workers: 2, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, In: pgTestMachine(16 * pgTestGiB),
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

func TestPGMemoryWatch(t *testing.T) {
	calls := 0
	w := newPGMemoryWatch(func(context.Context) pgMemoryPlan {
		calls++
		return pgMemoryPlan{Servers: 1, Workers: 1, Memory: "1GB", In: pgTestMachine(8 * pgTestGiB)}
	})
	if w.doc() != nil {
		t.Fatal("a verdict before the first read")
	}
	first := w.read(context.Background())
	// /status takes the last one and never reads.
	if w.doc() != first || w.doc() != first || calls != 1 {
		t.Fatalf("doc() read the plan: %d reads", calls)
	}
	if w.read(context.Background()); calls != 2 || w.doc() == first {
		t.Errorf("read() did not read again: %d reads", calls)
	}
	var none *pgMemoryWatch
	if none.doc() != nil {
		t.Error("no watch, no document")
	}
}

func TestPGDSNKind(t *testing.T) {
	for dsn, want := range map[string]string{
		"u:p@tcp(index-mysql:3306)/b":    "bundled",
		"u:p@tcp(127.0.0.1:3306)/b":      "loopback",
		"u:p@unix(/run/m.sock)/b":        "loopback",
		"u:p@tcp(db.example.com:3306)/b": "remote",
		"not a dsn":                      "",
	} {
		if got := pgDSNKind(dsn); got != want {
			t.Errorf("pgDSNKind(%q) = %q, want %q", dsn, got, want)
		}
	}
}

// An index on another machine is never asked: its memory is not this one's.
func TestPGReadIndexPool_remoteIsNotAsked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	got := pgReadIndexPool(ctx, "u:p@tcp(192.0.2.1:3306)/b")
	if got.Place != pgPoolElsewhere || got.Unread || time.Since(start) > 500*time.Millisecond {
		t.Errorf("%+v after %s", got, time.Since(start))
	}
}
