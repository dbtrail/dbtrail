//go:build integration

package doctor

import (
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The two kinds only a live MySQL can produce: access denied from a real
// handshake, and the loopback proof, where the retry address reaches a server
// the typed loopback address did not.

func liveAddr(t *testing.T) string {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	return cfg.Addr
}

func TestIntegrationClassifyConnectError_accessDenied(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	err := connectErr(t, "root:definitely-not-the-password@tcp("+liveAddr(t)+")/?timeout=3s")
	if got := ClassifyConnectError(err); got != KindAccessDenied {
		t.Errorf("ClassifyConnectError(%v) = %q, want %q", err, got, KindAccessDenied)
	}
}

func TestIntegrationBuild_loopbackProvenByTheRetry(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	live := liveAddr(t)
	var asked [2]string
	retry := func(host, port string) string { asked = [2]string{host, port}; return live }

	got := buildConnect(t, "root:testroot@tcp(localhost:"+closedPort(t)+")/?timeout=2s", WithLoopbackRetry(retry))
	if got.Kind != KindLoopbackInContainer {
		t.Fatalf("kind = %q, want %q: the retry reached a database the typed address did not", got.Kind, KindLoopbackInContainer)
	}
	if asked[0] != "localhost" {
		t.Errorf("retry asked about host %q, want the typed one", asked[0])
	}
	liveHost, _, _ := strings.Cut(live, ":")
	if !strings.Contains(got.Remediation, "Use that as the host:\n\n  "+liveHost) {
		t.Errorf("remediation does not name the address to use instead:\n%s", got.Remediation)
	}

	// A wrong password at the retry address still proves a database is there.
	got = buildConnect(t, "root:wrong@tcp(127.0.0.1:"+closedPort(t)+")/?timeout=2s", WithLoopbackRetry(retry))
	if got.Kind != KindLoopbackInContainer {
		t.Errorf("kind = %q when the retry answered access denied, want %q", got.Kind, KindLoopbackInContainer)
	}
}

// A loopback address that ANSWERED — here with access denied — reached a
// database: the address was right, the credentials were not. Retrying it
// somewhere else would send the typed password to a host nobody typed, and
// could relabel a password problem as a container problem.
func TestIntegrationBuild_noRetryWhenTheLoopbackAddressAnswered(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	live := liveAddr(t)
	host, port, _ := strings.Cut(live, ":")
	if !isLoopbackHost(host) {
		t.Skipf("the test MySQL is at %s, not a loopback address; this case needs one", live)
	}
	calls := 0
	retry := func(string, string) string { calls++; return live }
	got := buildConnect(t, "root:definitely-not-the-password@tcp("+host+":"+port+")/?timeout=3s", WithLoopbackRetry(retry))
	if got.Kind != KindAccessDenied {
		t.Errorf("kind = %q, want %q", got.Kind, KindAccessDenied)
	}
	if calls != 0 {
		t.Errorf("an answered loopback address was retried elsewhere %d time(s)", calls)
	}
}
