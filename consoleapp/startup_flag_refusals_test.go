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
		name  string
		set   func(t *testing.T)
		words string // what the message says around the value, on every page
		value string // what the operator gave: in the log, never on a stranger's page; "" for a number
	}{
		{"a DSN pasted into the flavor", func(*testing.T) { upSourceFlavor = "u:Secr3t@tcp(db.internal:3306)/app" }, "--source-flavor: invalid source flavor", "u:Secr3t@tcp(db.internal:3306)/app"},
		{"a DSN pasted into the memory variable", func(t *testing.T) {
			t.Setenv("BINTRAIL_CONSOLE_SQL_MEMORY", "u:Secr3t@tcp(db.internal:3306)/app")
		}, "BINTRAIL_CONSOLE_SQL_MEMORY=", "u:Secr3t@tcp(db.internal:3306)/app"},
		{"a short password pasted into the format", func(*testing.T) { upFormat = "hunter2" }, "invalid --format", "hunter2"},
		{"output format", func(*testing.T) { upFormat = "yaml" }, "invalid --format", "yaml"},
		{"source flavor", func(*testing.T) { upSourceFlavor = "postgres" }, "invalid source flavor", "postgres"},
		{"rotation retain", func(*testing.T) { upRotateRetain = "seven days" }, "--rotate-retain:", "seven days"},
		{"rotation retain unit", func(*testing.T) { upRotateRetain = "7x" }, "--rotate-retain: invalid unit", "7x"},
		{"rotation interval", func(*testing.T) { upRotateInterval = "hourly" }, "--rotate-interval:", "hourly"},
		{"rotation interval not positive", func(*testing.T) { upRotateInterval = "-1h" }, "--rotate-interval must be positive", "-1h"},
		{"rotation future partitions", func(*testing.T) { upRotateAddFuture = -1 }, "--rotate-add-future cannot be negative, got -1", ""},
		{"sql memory variable", func(t *testing.T) { t.Setenv("BINTRAIL_CONSOLE_SQL_MEMORY", "1KB") }, "BINTRAIL_CONSOLE_SQL_MEMORY=", "1KB"},
		{"sql memory flag", func(*testing.T) {
			upSQLMemory = "100MB"
			watchCmd.Flags().Lookup("sql-memory").Changed = true
		}, "--sql-memory=", "100MB"},
		{"sql statements at once variable", func(t *testing.T) { t.Setenv("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT", "zero") }, "BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT=", "zero"},
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
			if !strings.Contains(err.Error(), tc.words) || !strings.Contains(err.Error(), tc.value) {
				t.Errorf("the log lost words %q or the value %q: %v", tc.words, tc.value, err)
			}
			f := watchStartupFailure(err)
			if f == nil {
				t.Fatal("no startup page: the process would exit into a restart loop")
			}
			page := func(host string) string {
				req := httptest.NewRequest("GET", "/", nil)
				req.Host = host
				rec := httptest.NewRecorder()
				f.ServeHTTP(rec, req)
				return html.UnescapeString(rec.Body.String())
			}
			// A stranger reaching the public address: the words, never the value.
			stranger := page("203.0.113.5:18091")
			if !strings.Contains(stranger, tc.words) {
				t.Errorf("a stranger's page does not say %q:\n%s", tc.words, stranger)
			}
			if tc.value != "" && strings.Contains(stranger, tc.value) {
				t.Errorf("a stranger's page shows the value %q", tc.value)
			}
			if tc.value != "" && !strings.Contains(stranger, valueGiven) {
				t.Errorf("a stranger's page does not say a value was taken out")
			}
			// The operator on the machine itself (trusted bind, known host) reads it whole.
			f.trusted = true
			if own := page("localhost:18091"); !strings.Contains(own, err.Error()) {
				t.Errorf("the operator's own page lost the message %q", err.Error())
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
	got := configRefusal(errorString(`--source-flavor: invalid source flavor "mysqll": must be "mysql" or "mariadb"`), "mysqll")
	if got.Error() != `--source-flavor: invalid source flavor "mysqll": must be "mysql" or "mariadb"` {
		t.Errorf("words changed: %q", got.Error())
	}
	if c := telemetry.ClassifyError(got); c != telemetry.ClassConfigInvalid {
		t.Errorf("class = %q", c)
	}
	pm, ok := got.(publicMessenger)
	if !ok {
		t.Fatal("no public message")
	}
	// The value goes, and so does a fixed word it contains ("mysql" is in
	// "mysqll"): an odd sentence beats a leak. "mariadb" is not in it, so it stays.
	if want := `--source-flavor: invalid source flavor (the value given): must be (the value given) or "mariadb"`; pm.PublicMessage() != want {
		t.Errorf("public message = %q, want %q", pm.PublicMessage(), want)
	}
	if p := configRefusal(errorString(`bad "x"`)).(publicMessenger).PublicMessage(); p != `bad "x"` {
		t.Errorf("with nothing typed, nothing is taken out: %q", p)
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }
