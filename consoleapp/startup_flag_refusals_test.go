package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io/fs"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestWatchFlagRefusalsGetThePage drives the real runWatch with each setting
// it refuses before it reaches the index. A restart cannot fix any of them, so
// each must reach telemetry as config_invalid and get the startup page with
// its message, instead of exiting into a restart loop as "unknown" (a plain
// fmt.Errorf), which is what an install showed in telemetry for weeks.
func TestWatchFlagRefusalsGetThePage(t *testing.T) {
	origDSN, origFormat, origFlavor := upIndexDSN, upFormat, upSourceFlavor
	origRetain, origInterval, origFuture := upRotateRetain, upRotateInterval, upRotateAddFuture
	origInFlight, origMemory, origTLS := upSQLMaxInFlight, upSQLMemory, upConsoleTLSCert
	t.Cleanup(func() {
		upIndexDSN, upFormat, upSourceFlavor = origDSN, origFormat, origFlavor
		upRotateRetain, upRotateInterval, upRotateAddFuture = origRetain, origInterval, origFuture
		upSQLMaxInFlight, upSQLMemory, upConsoleTLSCert = origInFlight, origMemory, origTLS
		watchReached.Store(false)
		watchListening.Store(false)
	})
	for _, name := range []string{"sql-memory", "sql-max-in-flight", "console-listen", "console-tls-cert", "console-allow-setup", "console-allowed-hosts", "rotate-retain"} {
		f := watchCmd.Flags().Lookup(name)
		was := f.Changed
		t.Cleanup(func() { f.Changed = was })
	}
	// A refusal this test expects but the code no longer makes would fall
	// through to the index wait (90 seconds): the deadline turns that into a
	// quick failure instead.
	origCtx := watchCmd.Context()
	t.Cleanup(func() { watchCmd.SetContext(origCtx) })

	reset := func() {
		// Valid, and never reached: every case is refused before the index.
		upIndexDSN = "u:p@tcp(127.0.0.1:1)/binlog_index"
		upFormat, upSourceFlavor = "text", ""
		upRotateRetain, upRotateInterval, upRotateAddFuture = "7d", "1h", 2
		upSQLMaxInFlight, upSQLMemory, upConsoleTLSCert = 2, "", ""
		for _, name := range []string{"sql-memory", "sql-max-in-flight", "console-listen", "console-tls-cert", "console-allow-setup", "console-allowed-hosts", "rotate-retain"} {
			watchCmd.Flags().Lookup(name).Changed = false
		}
		watchReached.Store(false)
		watchListening.Store(false)
	}

	cases := []struct {
		name   string
		set    func(t *testing.T)
		words  string // what the message must still say
		hidden string // what the page must not show to anyone; "" when the message is shown
	}{
		{"a DSN pasted into the flavor", func(*testing.T) { upSourceFlavor = "u:Secr3t@tcp(db.internal:3306)/app" }, "--source-flavor", "Secr3t"},
		{"a DSN pasted into the memory variable", func(t *testing.T) {
			t.Setenv("BINTRAIL_CONSOLE_SQL_MEMORY", "u:Secr3t@tcp(db.internal:3306)/app")
		}, "BINTRAIL_CONSOLE_SQL_MEMORY", "Secr3t"},
		{"output format", func(*testing.T) { upFormat = "yaml" }, `invalid --format "yaml"`, ""},
		{"source flavor", func(*testing.T) { upSourceFlavor = "postgres" }, `invalid source flavor "postgres"`, ""},
		{"rotation retain", func(*testing.T) { upRotateRetain = "seven days" }, "--rotate-retain:", ""},
		{"rotation interval", func(*testing.T) { upRotateInterval = "hourly" }, "--rotate-interval:", ""},
		{"rotation interval not positive", func(*testing.T) { upRotateInterval = "-1h" }, "--rotate-interval must be positive", ""},
		{"rotation future partitions", func(*testing.T) { upRotateAddFuture = -1 }, "--rotate-add-future cannot be negative", ""},
		{"sql memory variable", func(t *testing.T) { t.Setenv("BINTRAIL_CONSOLE_SQL_MEMORY", "1KB") }, `BINTRAIL_CONSOLE_SQL_MEMORY="1KB"`, ""},
		{"sql memory flag", func(*testing.T) {
			upSQLMemory = "100MB"
			watchCmd.Flags().Lookup("sql-memory").Changed = true
		}, `--sql-memory="100MB"`, ""},
		{"sql statements at once variable", func(t *testing.T) { t.Setenv("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT", "zero") }, `BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT="zero"`, ""},
		{"sql statements at once flag", func(*testing.T) {
			upSQLMaxInFlight = 0
			watchCmd.Flags().Lookup("sql-max-in-flight").Changed = true
		}, "--sql-max-in-flight 0", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reset()
			t.Setenv("BINTRAIL_CONSOLE_SQL_MEMORY", "")
			t.Setenv("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT", "")
			t.Setenv("BINTRAIL_CONSOLE_LISTEN", "0.0.0.0:18091")
			t.Setenv(startupPageEnv, "")
			t.Setenv("BINTRAIL_CONSOLE_TLS_CERT", "")
			t.Setenv("BINTRAIL_CONSOLE_ALLOW_SETUP", "")
			t.Setenv("BINTRAIL_CONSOLE_ALLOWED_HOSTS", "")
			tc.set(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			watchCmd.SetContext(ctx)

			start := time.Now()
			err := runWatch(watchCmd, nil)
			if err == nil {
				t.Fatal("runWatch accepted the setting")
			}
			if time.Since(start) > 2*time.Second {
				t.Fatalf("refused only after %s, so it waited for the index first: %v", time.Since(start), err)
			}
			if got := telemetry.ClassifyError(err); got != telemetry.ClassConfigInvalid {
				t.Errorf("class = %q, want %q (err: %v)", got, telemetry.ClassConfigInvalid, err)
			}
			if !strings.Contains(err.Error(), tc.words) {
				t.Errorf("message lost its words %q: %v", tc.words, err)
			}
			f := watchStartupFailure(err)
			if f == nil {
				t.Fatal("no startup page: the process would exit into a restart loop")
			}
			if f.message != err.Error() {
				t.Errorf("page message = %q, want the error itself", f.message)
			}
			// What a stranger reaching the public address reads.
			req := httptest.NewRequest("GET", "/", nil)
			req.Host = "203.0.113.5:18091"
			rec := httptest.NewRecorder()
			f.ServeHTTP(rec, req)
			body := rec.Body.String()
			if tc.hidden == "" {
				if !f.public || !strings.Contains(body, html.EscapeString(tc.words)) {
					t.Errorf("page does not show the message to anyone (public=%v): %s", f.public, body)
				}
			} else if f.public || strings.Contains(body, tc.hidden) {
				t.Errorf("page shows %q to anyone (public=%v)", tc.hidden, f.public)
			}
		})
	}
}

// TestConfigRefusal: the wrapper keeps the words, passes nil through, leaves
// an error that already has its class (or comes from the network or the
// filesystem) as it is, and shows the message to anyone only when every value
// it repeats is plainly a setting.
func TestConfigRefusal(t *testing.T) {
	if configRefusal(nil, "x") != nil {
		t.Error("nil became an error")
	}
	already := configError{"--index-dsn must include a database name"}
	if got := configRefusal(already); got != error(already) {
		t.Errorf("an existing refusal was re-wrapped: %#v", got)
	}
	netErr := &net.OpError{Op: "dial", Err: errors.New("refused")}
	if got := configRefusal(fmt.Errorf("x: %w", netErr)); telemetry.ClassifyError(got) != telemetry.ClassDBConnection {
		t.Errorf("a network error was relabelled: %v -> %s", got, telemetry.ClassifyError(got))
	}
	pathErr := &fs.PathError{Op: "open", Path: "/etc/x", Err: fs.ErrPermission}
	if got := configRefusal(pathErr); got != error(pathErr) {
		t.Errorf("a file error was relabelled: %#v", got)
	}
	for _, tc := range []struct {
		typed  []string
		public bool
	}{
		{nil, true},
		{[]string{"4GB"}, true},
		{[]string{"", "-1h"}, true},
		{[]string{"seven days"}, true},
		{[]string{"u:pw@tcp(h:3306)/db"}, false},
		{[]string{"/var/lib/bintrail"}, false},
		{[]string{"7d", "a-very-long-value-indeed"}, false},
		{[]string{"line\nbreak"}, false},
	} {
		got := configRefusal(errorString("--rotate-interval: bad"), tc.typed...)
		if got.Error() != "--rotate-interval: bad" {
			t.Errorf("%q: words changed: %q", tc.typed, got.Error())
		}
		if c := telemetry.ClassifyError(got); c != telemetry.ClassConfigInvalid {
			t.Errorf("%q: class = %q", tc.typed, c)
		}
		pub, ok := got.(shownToAnyone)
		if !ok || pub.ShownToAnyone() != tc.public {
			t.Errorf("%q: shown to anyone = %v, want %v", tc.typed, ok && pub.ShownToAnyone(), tc.public)
		}
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }
