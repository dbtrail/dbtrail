package consoleapp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

func writeSQLSettings(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path+".tmp", []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}

// The memory a statement may use on the router: what it was started with,
// else what the web interface saved, else the default. A saved setting that
// cannot be read is an error with the default standing in, never a value
// made up from it.
func TestRouterMemory_whereItComesFrom(t *testing.T) {
	path := filepath.Join(t.TempDir(), console.SQLSettingsFileName)
	cases := []struct {
		name, started, file string
		want, from          string
		wantErr             string
	}{
		{name: "nothing set, nothing saved", want: "", from: "the default"},
		{name: "saved in the web interface", file: `{"version":1,"memory_mib":4096}`, want: "4096MiB", from: "saved in the web interface"},
		{name: "started with a value, another saved", started: "1024MiB", file: `{"version":1,"memory_mib":4096}`, want: "1024MiB", from: "set where the router is started"},
		{name: "started with a value, a broken file beside it", started: "1024MiB", file: `{"memory_mib":`, want: "1024MiB", from: "set where the router is started"},
		{name: "a file with no setting", file: `{"version":1}`, want: "", from: "the default"},
		{name: "a file that does not parse", file: `{"memory_mib":`, want: "", from: "the default", wantErr: "parse"},
		{name: "a file from a newer version", file: `{"version":9,"memory_mib":4096}`, want: "", from: "the default", wantErr: "newer"},
		{name: "a value under the floor", file: `{"version":1,"memory_mib":100}`, want: "", from: "the default", wantErr: "floor"},
		{name: "a key this version does not know", file: `{"version":1,"memory_mb":4096}`, want: "", from: "the default", wantErr: "parse"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			os.Remove(path)
			if tc.file != "" {
				writeSQLSettings(t, path, tc.file)
			}
			st := routerSettings{SQLMemory: tc.started}
			from, err := routerMemory(&st, path)
			if st.SQLMemory != tc.want || !strings.HasPrefix(from, tc.from) {
				t.Errorf("memory %q from %q, want %q from %q", st.SQLMemory, from, tc.want, tc.from)
			}
			if (err == nil) != (tc.wantErr == "") || (err != nil && !strings.Contains(err.Error(), tc.wantErr)) {
				t.Errorf("error %v, want one saying %q", err, tc.wantErr)
			}
		})
	}
}

// The startup lines say where the memory came from and that a change saved
// later needs a restart, and say loudly when the saved one could not be read.
func TestRouterStartupLines_theMemory(t *testing.T) {
	base := routerStartup{Listen: "127.0.0.1:3310", Pool: console.SQLPool{Workers: 2, Threads: 2, Why: "4 cores allow 2"}, MemoryLimit: "4096MiB", Cfg: routerFlashbackConfig(routerSettings{}), ServersFile: "/x/console-servers.yaml", ServersExists: true}
	saved := base
	saved.MemoryFrom = "saved in the web interface, read from /x/console-sql-settings.json"
	if got := routerStartupLines(saved); !strings.Contains(got, "up to 4096MiB of memory each") || !strings.Contains(got, "that memory is saved in the web interface, read from /x/console-sql-settings.json") || !strings.Contains(got, "applied when the router is restarted") {
		t.Errorf("a saved memory:\n%s", got)
	}
	broken := base
	broken.MemoryFrom, broken.MemoryErr = "the default", fmt.Errorf("parse /x/console-sql-settings.json: unexpected end")
	if got := routerStartupLines(broken); !strings.Contains(got, "could NOT be read, so the default is used: parse /x/console-sql-settings.json") {
		t.Errorf("a saved memory that cannot be read:\n%s", got)
	}
}

// A memory saved after the router started is not applied, and the router
// says so once per value: nothing else would tell why a change made in the
// web interface did nothing here. A save that leaves it as applied, or puts
// it back, says nothing more.
func TestFollowRouterMemory_saysAChangeThatIsNotApplied(t *testing.T) {
	path := filepath.Join(t.TempDir(), console.SQLSettingsFileName)
	writeSQLSettings(t, path, `{"version":1,"memory_mib":2048}`)
	var mu sync.Mutex
	var said []string
	warn := func(msg string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		said = append(said, fmt.Sprint(msg, " ", args))
	}
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(said) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go followRouterMemory(ctx, path, "2048MiB", 10*time.Millisecond, warn)

	time.Sleep(80 * time.Millisecond)
	if n := count(); n != 0 {
		t.Fatalf("said %d time(s) while the saved memory was the one applied: %v", n, said)
	}
	writeSQLSettings(t, path, `{"version":1,"memory_mib":8192}`)
	waitUntil(t, "the change to be said", func() bool { return count() == 1 })
	time.Sleep(80 * time.Millisecond)
	if n := count(); n != 1 {
		t.Fatalf("the same value was said %d times", n)
	}
	mu.Lock()
	first := said[0]
	mu.Unlock()
	if !strings.Contains(first, "NOT applied") || !strings.Contains(first, "restart the router") || !strings.Contains(first, "8192MiB") || !strings.Contains(first, "2048MiB") {
		t.Errorf("what was said: %s", first)
	}
	// Put back: nothing to say. Removed: the default is not what runs.
	writeSQLSettings(t, path, `{"version":1,"memory_mib":2048}`)
	time.Sleep(80 * time.Millisecond)
	if n := count(); n != 1 {
		t.Fatalf("putting the applied value back was said: %v", said)
	}
	os.Remove(path)
	waitUntil(t, "the removal to be said", func() bool { return count() == 2 })
	// A file that cannot be read is not a new value.
	writeSQLSettings(t, path, `{"memory_mib":`)
	time.Sleep(80 * time.Millisecond)
	if n := count(); n != 2 {
		t.Fatalf("an unreadable file was said as a change: %v", said)
	}
}
