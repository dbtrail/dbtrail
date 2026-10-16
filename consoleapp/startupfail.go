package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// A watch that dies on its configuration before the web interface listens
// used to exit, and its supervisor (compose's restart policy, systemd) started
// it again, hundreds of times a day, while the browser showed nothing and the
// reason scrolled past in the log. Instead, it now keeps the console's port
// with one page that says why, for startupPageFor.
//
// Only for failures classed as configuration, permissions or a missing
// database. Some of those still heal with time (an app's migrations create the
// tables a minute later, grants are applied after the pod starts, the source
// fails over mid-preflight), so the page is shown for startupPageFor and then
// the process exits, letting the supervisor try again: a transient failure
// heals as it did, at one restart per startupPageFor instead of a tight loop.
// Failures classed as transient (the index not reachable yet) exit at once.

// watchReached and watchListening are set by runWatch: the page is for a
// watch run that failed before its console accepted its listener. Failures
// after that keep exiting as before.
var (
	watchReached   atomic.Bool
	watchListening atomic.Bool
)

// startupPageEnv turns the page off ("0", "false", "off"): the process then
// exits on a startup failure as it did before, for an operator whose alerting
// watches for a process that is down.
const startupPageEnv = "BINTRAIL_CONSOLE_STARTUP_PAGE"

// startupPageFor is how long the page stays before the process exits so its
// supervisor starts it again. A var for tests.
var startupPageFor = 5 * time.Minute

// startupState is what newStartupFailure reads, gathered by the caller so the
// decision is testable without the process's globals.
type startupState struct {
	reached, listening bool
	getenv             func(string) string
	changed            func(flag string) bool
	listen             string // --console-listen as parsed
	tlsCert            string // --console-tls-cert as parsed
	allowSetup         bool   // --console-allow-setup as parsed
	allowedHosts       []string
}

// startupFailure is the page for one failed start.
type startupFailure struct {
	listen       string
	message      string
	public       bool     // the message is one we wrote: shown to anyone
	checks       []string // the failed preflight checks' names, if that is what failed
	trusted      bool     // loopback bind, or access control asserted by allow-setup
	allowedHosts []string
}

// shownToAnyone is implemented by the startup errors whose text we wrote and
// know carries nothing internal (no hostnames, users or paths).
type shownToAnyone interface{ ShownToAnyone() bool }

func (configError) ShownToAnyone() bool { return true }

// permanentClasses are the failures a restart cannot fix.
var permanentClasses = map[string]bool{
	telemetry.ClassConfigInvalid: true,
	telemetry.ClassDBPermission:  true,
	telemetry.ClassNotFound:      true,
}

func newStartupFailure(err error, st startupState) *startupFailure {
	if err == nil || !st.reached || st.listening {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(st.getenv(startupPageEnv))) {
	case "0", "false", "off", "no":
		return nil
	}
	if !permanentClasses[telemetry.ClassifyError(err)] {
		return nil
	}
	// The flag wins when given; otherwise the variable the run may have died
	// before reading. Same order as resolveUpConsoleEnv.
	pick := func(flag, val, env string) string {
		if st.changed(flag) {
			return val
		}
		if v := st.getenv(env); v != "" {
			return v
		}
		return val
	}
	// A browser expecting HTTPS would get a protocol error from a plain page.
	if pick("console-tls-cert", st.tlsCert, "BINTRAIL_CONSOLE_TLS_CERT") != "" {
		return nil
	}
	listen := pick("console-listen", st.listen, "BINTRAIL_CONSOLE_LISTEN")
	allowSetup := st.allowSetup
	if !st.changed("console-allow-setup") {
		if v := st.getenv("BINTRAIL_CONSOLE_ALLOW_SETUP"); v == "1" || v == "true" {
			allowSetup = true
		}
	}
	hosts := st.allowedHosts
	if !st.changed("console-allowed-hosts") {
		if v := st.getenv("BINTRAIL_CONSOLE_ALLOWED_HOSTS"); v != "" {
			hosts = strings.Split(v, ",")
		}
	}
	// The error itself, not one wrapped inside it: a wrapper may add text we
	// did not write, and the whole of err.Error() is what the page shows.
	pub, _ := err.(shownToAnyone)
	var pf *doctor.PreflightError
	var checks []string
	if errors.As(err, &pf) {
		checks = pf.Checks // check names are constants: nothing internal
	}
	return &startupFailure{
		listen:       listen,
		message:      err.Error(),
		public:       pub != nil && pub.ShownToAnyone(),
		checks:       checks,
		trusted:      console.IsLoopbackAddr(listen) || allowSetup,
		allowedHosts: hosts,
	}
}

// watchStartupFailure is newStartupFailure over this process's own state.
func watchStartupFailure(err error) *startupFailure {
	return newStartupFailure(err, startupState{
		reached:      watchReached.Load(),
		listening:    watchListening.Load(),
		getenv:       os.Getenv,
		changed:      func(f string) bool { return watchCmd.Flags().Changed(f) },
		listen:       upConsoleListen,
		tlsCert:      upConsoleTLSCert,
		allowSetup:   upConsoleAllowSetup,
		allowedHosts: upConsoleAllowedHost,
	})
}

// knownHost mirrors the console's Host check: IP literals and localhost
// always, plus the configured names. A page reached under any other name may
// be a DNS-rebinding page in someone's browser, so it gets no internal detail.
func (f *startupFailure) knownHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if net.ParseIP(host) != nil || strings.EqualFold(host, "localhost") {
		return true
	}
	for _, a := range f.allowedHosts {
		if strings.EqualFold(strings.TrimSpace(a), host) {
			return true
		}
	}
	return false
}

var startupPageTmpl = template.Must(template.New("p").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>dbtrail could not start</title>
<style>
body{font:16px/1.5 system-ui,sans-serif;margin:0;padding:48px 16px;background:#fafaf9;color:#1c1917}
main{max-width:44rem;margin:0 auto}
h1{font-size:1.5rem;margin:0 0 8px}
pre{white-space:pre-wrap;word-break:break-word;background:#fff;border:1px solid #e7e5e4;border-left:4px solid #b91c1c;border-radius:6px;padding:12px 16px;font-size:.9rem}
code{background:#f5f5f4;padding:1px 4px;border-radius:4px}
p{margin:12px 0}
@media (prefers-color-scheme:dark){body{background:#1c1917;color:#f5f5f4}pre{background:#292524;border-color:#44403c;border-left-color:#ef4444}code{background:#292524}}
</style></head><body><main>
<h1>dbtrail could not start</h1>
{{if .Show}}<p>It stopped on this, before the web interface opened:</p>
<pre>{{.Message}}</pre>
{{else}}<p>It stopped before the web interface opened. The reason is in dbtrail's log (with the Docker Compose stack: <code>docker compose logs bintrail</code>; with the package: <code>journalctl -u bintrail-console</code>).</p>
{{if .Trusted}}<p>This page does not show it under this hostname. Open it by IP or as <code>localhost</code>, or add this hostname to <code>BINTRAIL_CONSOLE_ALLOWED_HOSTS</code>.</p>
{{else}}<p>This page does not show it: the web interface listens on a public address with no password set, so anyone reaching it would read it.</p>{{end}}{{end}}
{{if .Checks}}<p>Checks that failed: {{range $i, $c := .Checks}}{{if $i}}, {{end}}<code>{{$c}}</code>{{end}}. The log shows how to fix each one.</p>{{end}}
<p>After {{.Every}} dbtrail exits; Docker Compose and the package's service start it again by themselves. After fixing it, to try now: <code>docker compose restart bintrail</code> (or restart the service the way you run it).</p>
</main></body></html>`))

func (f *startupFailure) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	show := f.public || (f.trusted && f.knownHost(r.Host))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	// 503 on every path, /api/healthz included, so a health check reports
	// the failed start instead of reading the page as a healthy console.
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = startupPageTmpl.Execute(w, struct {
		Show    bool
		Trusted bool
		Message string
		Checks  []string
		Every   string
	}{show, f.trusted, f.message, f.checks, humanMinutes(startupPageFor)})
}

func humanMinutes(d time.Duration) string {
	if m := int(d.Minutes()); m >= 1 {
		if m == 1 {
			return "minute"
		}
		return fmt.Sprintf("%d minutes", m)
	}
	return d.String()
}

// serve binds the console's address and shows the page until SIGINT/SIGTERM
// or startupPageFor, whichever comes first. It returns the process exit code:
// 1, so a supervisor sees a failed run and starts it again. A bind failure
// (the port taken) prints why and returns 1 at once.
func (f *startupFailure) serve(stderr io.Writer) int {
	ln, err := net.Listen("tcp", f.listen)
	if err != nil {
		fmt.Fprintf(stderr, "could not open %s to show why dbtrail did not start: %v\n", f.listen, err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return f.run(ctx, ln, stderr)
}

func (f *startupFailure) run(ctx context.Context, ln net.Listener, stderr io.Writer) int {
	fmt.Fprintf(stderr, "dbtrail did not start; the web interface at %s shows why for %s, then dbtrail exits to be started again (%s=0 exits at once)\n", ln.Addr(), startupPageFor, startupPageEnv)
	srv := &http.Server{Handler: f, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	timer := time.NewTimer(startupPageFor)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	return 1
}
