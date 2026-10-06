package doctor

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// BufferPoolCheckName is the index buffer pool check (#2141).
const BufferPoolCheckName = "Index buffer pool"

// DefaultBufferPool is InnoDB's innodb_buffer_pool_size when nobody sets it:
// 128 MiB on every machine. On a busy source it caps capture far below what
// the disk can do (measured: about 1,400 row changes per second with lag
// growing without end, against 16,000 with a sized pool).
const DefaultBufferPool = uint64(128) << 20

const (
	// bufferPoolCap bounds the recommendation on very large machines: the
	// index is append-mostly, so past a few tens of GB more pool buys little,
	// while the rest of the machine (the source database, when it shares the
	// host) keeps needing it.
	bufferPoolCap = uint64(32) << 30
	// bufferPoolFloor is the smallest pool worth recommending. Below it the
	// machine is too small to spare memory, and the default stays.
	bufferPoolFloor = uint64(256) << 20
	// bufferPoolWarnFrom: the warning fires only when the recommendation is
	// at least this much (a machine of about 4 GB or more), so small
	// machines, where 128 MB is a reasonable share, stay quiet.
	bufferPoolWarnFrom = uint64(1) << 30
)

// RecommendedBufferPool is the index buffer pool for a machine (or Docker
// VM) with memTotal bytes: a quarter of it, rounded DOWN to whole GiB from
// 1 GiB up (MySQL rounds the pool UP to a multiple of chunk size times
// instances, which divides a whole GiB, so the value it runs with is the one
// computed here), in 128 MiB steps below that, at most 32 GiB. A quarter,
// not InnoDB's usual "most of the RAM", because the index rarely has the
// machine to itself: the web interface's SQL engine takes up to 4 GB, and the
// source database may run on the same host. ok is false when a quarter is
// under 256 MiB: keep the default. install.sh carries a shell copy of this
// rule; test/installer holds the two together.
func RecommendedBufferPool(memTotal uint64) (uint64, bool) {
	q := memTotal / 4
	if q < bufferPoolFloor {
		return 0, false
	}
	if q >= 1<<30 {
		return min(q>>30<<30, bufferPoolCap), true
	}
	return q >> 27 << 27, true
}

// BufferPoolSetting renders a pool size the way it is written in .env and on
// the mysqld command line: whole GiB as "4G", anything else as "384M".
func BufferPoolSetting(b uint64) string {
	if b >= 1<<30 && b%(1<<30) == 0 {
		return fmt.Sprintf("%dG", b>>30)
	}
	return fmt.Sprintf("%dM", b>>20)
}

// bufferPoolPlace is where the index MySQL runs, as far as this process can
// tell: that decides whose memory the pool is compared with.
type bufferPoolPlace int

const (
	// placeElsewhere: another machine, or one this process cannot confirm
	// is this one. Its memory is unknown.
	placeElsewhere bufferPoolPlace = iota
	// placeBundled: the compose stack's index-mysql container, which shares
	// this container's host (or Docker Desktop VM) and its memory.
	placeBundled
	// placeThisMachine: a MySQL of the operator's own on this same machine.
	placeThisMachine
)

// checkIndexBufferPool compares the index server's buffer pool with the
// memory of the machine it runs on, when that machine is this one. It only
// reads: DBTrail never changes a setting on the index MySQL. It never FAILS
// either: watch refuses to boot on a failed check, and a tuning finding must
// not stop capture.
func checkIndexBufferPool(ctx context.Context, dsn string) CheckResult {
	db, err := connectWithoutDB(dsn)
	if err != nil {
		// checkIndexConnection already reported the dead server.
		return CheckResult{Name: BufferPoolCheckName, Status: StatusSkip, Detail: "cannot connect to the index server: " + err.Error()}
	}
	defer db.Close()
	return probeBufferPool(ctx, db, dsn)
}

func probeBufferPool(ctx context.Context, db *sql.DB, dsn string) CheckResult {
	var pool uint64
	if err := db.QueryRowContext(ctx, "SELECT @@innodb_buffer_pool_size").Scan(&pool); err != nil {
		return CheckResult{Name: BufferPoolCheckName, Status: StatusSkip, Detail: "could not read innodb_buffer_pool_size: " + err.Error()}
	}
	place := bufferPoolPlaceOf(ctx, db, dsn)
	var mem uint64
	known := false
	if place != placeElsewhere {
		mem, known = memoryFrom("/")
	}
	return gradeBufferPool(pool, mem, known, place)
}

// bufferPoolPlaceOf decides where the index runs from the DSN, and for a
// loopback DSN from the server's own @@hostname: a port forward or tunnel on
// 127.0.0.1 reaches another machine, whose memory this one says nothing
// about. Any doubt is placeElsewhere, which never warns.
func bufferPoolPlaceOf(ctx context.Context, db *sql.DB, dsn string) bufferPoolPlace {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return placeElsewhere
	}
	if dsnTargetsBundledIndex(cfg) {
		return placeBundled
	}
	if !dsnTargetsLocalhost(cfg) {
		return placeElsewhere
	}
	var serverHost string
	if err := db.QueryRowContext(ctx, "SELECT @@hostname").Scan(&serverHost); err != nil {
		return placeElsewhere
	}
	local, err := os.Hostname()
	if err != nil || !sameHostname(serverHost, local) {
		return placeElsewhere
	}
	return placeThisMachine
}

// gradeBufferPool turns the measurement into the check. It warns only for
// the case that throttles capture: the pool still at the 128 MB default on a
// machine with far more memory. A pool the operator chose, even a small one,
// is their decision and passes.
func gradeBufferPool(pool, mem uint64, memKnown bool, place bufferPoolPlace) CheckResult {
	c := CheckResult{Name: BufferPoolCheckName}
	poolText := humanBytes(float64(pool))
	atDefault := pool <= DefaultBufferPool
	if !memKnown {
		if !atDefault {
			c.Status, c.Detail = StatusPass, poolText
			return c
		}
		c.Status = StatusSkip
		c.Detail = poolText + ", the InnoDB default; the memory of the machine it runs on cannot be seen from here"
		if place == placeBundled {
			c.Detail += ". If that machine has 4 GB or more, set INDEX_BUFFER_POOL in the .env next to docker-compose.yml to about a quarter of it"
		} else {
			c.Detail += ". If that machine has 4 GB or more, set innodb_buffer_pool_size on it to about a quarter of its memory, or more if it runs nothing else"
		}
		return c
	}
	rec, ok := RecommendedBufferPool(mem)
	if !atDefault || !ok || rec < bufferPoolWarnFrom {
		c.Status = StatusPass
		c.Detail = fmt.Sprintf("%s, on a machine with %s", poolText, humanBytes(float64(mem)))
		return c
	}
	c.Status = StatusWarn
	c.Detail = fmt.Sprintf("the index MySQL runs with %s, the InnoDB default, on a machine with %s. "+
		"On a busy server capture then falls behind and the lag keeps growing", poolText, humanBytes(float64(mem)))
	setting := BufferPoolSetting(rec)
	if place == placeBundled {
		c.Remediation = "Give the bundled index MySQL about a quarter of the memory. In the .env next to docker-compose.yml add:\n\n" +
			"  INDEX_BUFFER_POOL=" + setting + "\n\n" +
			"then run `docker compose up -d`, which restarts it; capture resumes from its checkpoint. " +
			"A docker-compose.yml from before this setting ignores it: take the current file first (docs/docker.md, Upgrading the stack)."
	} else {
		c.Remediation = "Raise innodb_buffer_pool_size on that MySQL to about a quarter of the machine's memory, more if the index is all it runs. " +
			"It resizes online:\n\n" +
			"  SET PERSIST innodb_buffer_pool_size = " + strconv.FormatUint(rec, 10) + "; -- " + setting + "\n\n" +
			"DBTrail only reads this setting; it never changes it."
	}
	return c
}

// memoryFrom reads the memory this machine (or Docker VM) has, under root
// ("/" in production, a temp dir in tests): MemTotal from /proc/meminfo,
// lowered to the cgroup limit when one is set. Outside Linux there is no
// /proc/meminfo, and the answer is unknown.
func memoryFrom(root string) (uint64, bool) {
	f, err := os.Open(filepath.Join(root, "proc", "meminfo"))
	if err != nil {
		return 0, false
	}
	defer f.Close()
	var total uint64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kb, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, false
			}
			total = kb * 1024
			break
		}
	}
	if total == 0 {
		return 0, false
	}
	for _, p := range []string{
		filepath.Join(root, "sys", "fs", "cgroup", "memory.max"),                      // cgroup v2
		filepath.Join(root, "sys", "fs", "cgroup", "memory", "memory.limit_in_bytes"), // cgroup v1
	} {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		// "max" (v2) does not parse and a v1 "unlimited" is a huge number:
		// both leave the total alone.
		if limit, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); err == nil && limit > 0 && limit < total {
			total = limit
		}
	}
	return total, true
}
