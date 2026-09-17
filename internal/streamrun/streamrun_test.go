package streamrun

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/parser"
)

// TestDrainParser_cancelsBeforeReceive verifies drainParser's contract in
// isolation: it cancels the stream context BEFORE draining parseErrCh, so a
// parser goroutine that returns only on cancellation (the real sp.Run blocking
// behavior in GetEvent or on a full events buffer) is unblocked rather than left
// to wedge One's drain (#652). It exercises the helper, not One()'s call site.
// The 5s watchdog makes the regression observable: a drain that forgot to cancel
// blocks here and fails (proven by reverting the cancel()).
func TestDrainParser_cancelsBeforeReceive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	parseErrCh := make(chan error, 1)
	// Stand-in for sp.Run: it only returns once the context is cancelled.
	go func() {
		<-ctx.Done()
		parseErrCh <- ctx.Err()
	}()

	done := make(chan error, 1)
	go func() { done <- drainParser(cancel, parseErrCh) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("parser error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drainParser hung: it must cancel the context before receiving so a ctx-blocked parser unblocks (#652)")
	}
}

// selfSignedCAPEM generates a minimal self-signed CA certificate as PEM bytes.
func selfSignedCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-ca"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// ─── resolveStart ────────────────────────────────────────────────────────────

func TestResolveStart_noStateNoFlags(t *testing.T) {
	_, _, _, _, _, err := resolveStart("", "", 4, nil)
	if err == nil {
		t.Error("expected error when no flags and no saved state")
	}
}

func TestResolveStart_positionFlagsNoState(t *testing.T) {
	mode, file, gtidStr, pos, accGTID, err := resolveStart("binlog.000001", "", 4, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position, got %q", mode)
	}
	if file != "binlog.000001" {
		t.Errorf("expected file=binlog.000001, got %q", file)
	}
	if pos != 4 {
		t.Errorf("expected pos=4, got %d", pos)
	}
	if gtidStr != "" {
		t.Errorf("expected empty gtidStr, got %q", gtidStr)
	}
	if accGTID != nil {
		t.Error("expected nil accGTID in position mode")
	}
}

func TestResolveStart_savedStateWinsOverFlags(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000010",
		binlogPos:  9999,
	}
	mode, file, _, pos, _, err := resolveStart("binlog.000020", "", 100, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position, got %q", mode)
	}
	if file != "binlog.000010" {
		t.Errorf("expected saved file=binlog.000010, got %q", file)
	}
	if pos != 9999 {
		t.Errorf("expected saved pos=9999, got %d", pos)
	}
}

func TestResolveStart_resumePosition(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000005",
		binlogPos:  1234,
	}
	mode, file, _, pos, accGTID, err := resolveStart("", "", 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position, got %q", mode)
	}
	if file != "binlog.000005" {
		t.Errorf("expected file=binlog.000005, got %q", file)
	}
	if pos != 1234 {
		t.Errorf("expected pos=1234, got %d", pos)
	}
	if accGTID != nil {
		t.Error("expected nil accGTID in position mode")
	}
}

// TestResolveStart_resumePositionOver4GiBFailsLoud pins #845: a saved position
// past uint32 (a >4GiB binlog from one oversized transaction) must error with a
// pointer to GTID mode, never silently wrap via uint32() to an earlier offset.
func TestResolveStart_resumePositionOver4GiBFailsLoud(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000005",
		binlogPos:  math.MaxUint32 + 1,
	}
	_, _, _, _, _, err := resolveStart("", "", 4, saved)
	if err == nil {
		t.Fatal("expected error for saved position > MaxUint32, got nil")
	}
	if !strings.Contains(err.Error(), "GTID") {
		t.Errorf("error should direct the operator to GTID mode, got: %v", err)
	}
	if !strings.Contains(err.Error(), "binlog.000005") {
		t.Errorf("error should name the binlog file, got: %v", err)
	}
}

func TestResolveStart_resumePositionExactlyMaxUint32(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000005",
		binlogPos:  math.MaxUint32,
	}
	_, _, _, pos, _, err := resolveStart("", "", 4, saved)
	if err != nil {
		t.Fatalf("unexpected error at exactly MaxUint32: %v", err)
	}
	if pos != math.MaxUint32 {
		t.Errorf("expected pos=MaxUint32, got %d", pos)
	}
}

// TestResolveStart_positionOver4GiB_gtidFlagStillSwitches proves the remediation
// the #845 error suggests actually works: --start-gtid takes the mode-switch
// branch before the overflow guard, so an oversized saved position is escapable.
func TestResolveStart_positionOver4GiB_gtidFlagStillSwitches(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000005",
		binlogPos:  math.MaxUint32 + 1,
	}
	gtidSet := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	mode, _, returnedGTID, _, accGTID, err := resolveStart("", gtidSet, 0, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid, got %q", mode)
	}
	if returnedGTID != gtidSet {
		t.Errorf("expected GTID=%q, got %q", gtidSet, returnedGTID)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID in gtid mode")
	}
}

func TestResolveStart_resumeGTID(t *testing.T) {
	gtidSet := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100"
	saved := &streamState{
		mode:    "gtid",
		gtidSet: gtidSet,
	}
	mode, _, returnedGTID, _, accGTID, err := resolveStart("", "", 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid, got %q", mode)
	}
	if returnedGTID != gtidSet {
		t.Errorf("expected GTID=%q, got %q", gtidSet, returnedGTID)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID in gtid mode")
	}
}

func TestResolveStart_mutuallyExclusive(t *testing.T) {
	_, _, _, _, _, err := resolveStart("binlog.000001", "uuid:1", 4, nil)
	if err == nil {
		t.Error("expected error for mutually exclusive --start-file and --start-gtid")
	}
}

func TestResolveStart_gtidFlagsNoState(t *testing.T) {
	gtidSet := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
	mode, _, returnedGTID, _, accGTID, err := resolveStart("", gtidSet, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid, got %q", mode)
	}
	if returnedGTID != gtidSet {
		t.Errorf("expected GTID=%q, got %q", gtidSet, returnedGTID)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID")
	}
}

// TestResolveStart_invalidSavedGTID verifies that a corrupt gtid_set in
// stream_state results in a clear error (not a panic).
func TestResolveStart_invalidSavedGTID(t *testing.T) {
	saved := &streamState{mode: "gtid", gtidSet: "not-a-valid-gtid"}
	_, _, _, _, _, err := resolveStart("", "", 4, saved)
	if err == nil {
		t.Error("expected error for invalid saved GTID set")
	}
}

// TestResolveStart_invalidStartGTIDFlag verifies that --start-gtid with an
// invalid GTID string is rejected with a clear error.
func TestResolveStart_invalidStartGTIDFlag(t *testing.T) {
	_, _, _, _, _, err := resolveStart("", "garbage-gtid", 0, nil)
	if err == nil {
		t.Error("expected error for invalid --start-gtid value")
	}
}

// ─── MariaDB flavor: GTID parsing + resolveStart (alpha) ──────────────────────

// TestParseGTIDSetForFlavor verifies the flavor dispatch: MariaDB strings parse
// to *MariadbGTIDSet (domain-server-seq, not zero-padded), MySQL strings to
// *MysqlGTIDSet, and an unparseable string errors for both flavors.
func TestParseGTIDSetForFlavor(t *testing.T) {
	gs, err := parseGTIDSetForFlavor(gomysql.MySQLFlavor, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	if err != nil {
		t.Fatalf("mysql parse: %v", err)
	}
	if _, ok := gs.(*gomysql.MysqlGTIDSet); !ok {
		t.Errorf("mysql flavor: expected *MysqlGTIDSet, got %T", gs)
	}

	mgs, err := parseGTIDSetForFlavor(gomysql.MariaDBFlavor, "0-1-100")
	if err != nil {
		t.Fatalf("mariadb parse: %v", err)
	}
	if _, ok := mgs.(*gomysql.MariadbGTIDSet); !ok {
		t.Errorf("mariadb flavor: expected *MariadbGTIDSet, got %T", mgs)
	}
	if mgs.String() != "0-1-100" {
		t.Errorf("mariadb GTID string = %q, want %q (must not be zero-padded)", mgs.String(), "0-1-100")
	}

	if _, err := parseGTIDSetForFlavor(gomysql.MySQLFlavor, "garbage"); err == nil {
		t.Error("expected error for invalid mysql GTID")
	}
	if _, err := parseGTIDSetForFlavor(gomysql.MariaDBFlavor, "garbage"); err == nil {
		t.Error("expected error for invalid mariadb GTID")
	}
}

// TestNormalizeGTIDSet_mariadbPassthrough pins that the MySQL UUID zero-padder
// leaves MariaDB domain-server-seq GTIDs untouched (3 segments is not a 5-part
// UUID, so it passes through). Defense in depth in case a MariaDB string reaches
// NormalizeGTIDSet.
func TestNormalizeGTIDSet_mariadbPassthrough(t *testing.T) {
	for _, s := range []string{"0-1-100", "0-1-100,1-2-50"} {
		if got := NormalizeGTIDSet(s); got != s {
			t.Errorf("NormalizeGTIDSet(%q) = %q, want unchanged", s, got)
		}
	}
}

// TestResolveStartForFlavor_mariadbGTID verifies a fresh MariaDB GTID start: the
// GTID is parsed with the MariaDB flavor and returned without zero-padding, and
// accGTID's dynamic type is *MariadbGTIDSet.
func TestResolveStartForFlavor_mariadbGTID(t *testing.T) {
	mode, _, gtidStr, _, accGTID, err := resolveStartForFlavor("", "0-1-100", 4, nil, gomysql.MariaDBFlavor)
	if err != nil {
		t.Fatalf("resolveStartForFlavor: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("mode = %q, want gtid", mode)
	}
	if gtidStr != "0-1-100" {
		t.Errorf("gtidStr = %q, want 0-1-100", gtidStr)
	}
	if accGTID == nil {
		t.Fatal("expected non-nil accGTID for mariadb GTID mode")
	}
	if _, ok := accGTID.(*gomysql.MariadbGTIDSet); !ok {
		t.Errorf("accGTID dynamic type = %T, want *MariadbGTIDSet", accGTID)
	}
}

// TestResolveStartForFlavor_resumeMariaDBGTID verifies the resume path: a saved
// MariaDB checkpoint (flavor=mariadb) re-parses its gtid_set with the MariaDB
// parser. The negative control proves a MariaDB set parsed as MySQL fails — the
// exact break that the persisted flavor column prevents.
func TestResolveStartForFlavor_resumeMariaDBGTID(t *testing.T) {
	saved := &streamState{mode: "gtid", gtidSet: "0-1-100", flavor: gomysql.MariaDBFlavor}
	mode, _, gtidStr, _, accGTID, err := resolveStartForFlavor("", "", 0, saved, gomysql.MariaDBFlavor)
	if err != nil {
		t.Fatalf("resume mariadb: %v", err)
	}
	if mode != "gtid" || gtidStr != "0-1-100" {
		t.Errorf("resume: mode=%q gtidStr=%q, want gtid/0-1-100", mode, gtidStr)
	}
	if _, ok := accGTID.(*gomysql.MariadbGTIDSet); !ok {
		t.Errorf("resume accGTID type = %T, want *MariadbGTIDSet", accGTID)
	}

	// Negative control: a MariaDB set parsed under the MySQL flavor must fail.
	if _, err := parseGTIDSetForFlavor(gomysql.MySQLFlavor, "0-1-100"); err == nil {
		t.Error("expected MariaDB GTID parsed as MySQL to error (this is why flavor must be persisted)")
	}
}

// TestResolveStartForFlavor_flavorMismatchErrors verifies that resuming a saved
// checkpoint under a different source flavor is rejected (latent-corruption
// guard): the saved set would be parsed as one flavor while the syncer handshake
// and the next checkpoint use another. A legacy checkpoint (empty flavor) adopts
// the requested flavor instead of erroring.
func TestResolveStartForFlavor_flavorMismatchErrors(t *testing.T) {
	// Saved as mariadb, requested mysql → error (covers GTID mode).
	mdb := &streamState{mode: "gtid", gtidSet: "0-1-100", flavor: gomysql.MariaDBFlavor}
	if _, _, _, _, _, err := resolveStartForFlavor("", "", 0, mdb, gomysql.MySQLFlavor); err == nil {
		t.Error("expected error resuming a mariadb checkpoint under mysql flavor")
	}

	// Saved as mysql, requested mariadb → error (covers position mode).
	my := &streamState{mode: "position", binlogFile: "binlog.000001", binlogPos: 4, flavor: gomysql.MySQLFlavor}
	if _, _, _, _, _, err := resolveStartForFlavor("", "", 0, my, gomysql.MariaDBFlavor); err == nil {
		t.Error("expected error resuming a mysql checkpoint under mariadb flavor")
	}

	// Legacy checkpoint (empty flavor) adopts the requested flavor — no error.
	legacy := &streamState{mode: "position", binlogFile: "binlog.000001", binlogPos: 4}
	if _, _, _, _, _, err := resolveStartForFlavor("", "", 0, legacy, gomysql.MariaDBFlavor); err != nil {
		t.Errorf("legacy checkpoint (empty flavor) should adopt requested flavor, got error: %v", err)
	}
}

// TestResolveStart_positionModeReturnsTrueNil pins that position mode returns a
// genuine nil interface — not a typed-nil *MysqlGTIDSet wrapped in the interface
// — so advanceGTID's `state.accGTID == nil` guard keeps working after the
// interface widening (the typed-nil-interface trap).
func TestResolveStart_positionModeReturnsTrueNil(t *testing.T) {
	_, _, _, _, accGTID, err := resolveStart("binlog.000001", "", 4, nil)
	if err != nil {
		t.Fatalf("resolveStart: %v", err)
	}
	if accGTID != nil {
		t.Errorf("position mode must return a true-nil accGTID interface, got %T", accGTID)
	}
}

// ─── GTID accumulation ────────────────────────────────────────────────────────

// TestStreamState_gtidAccumulation verifies that accGTID.Update correctly
// accumulates multiple GTIDs from a single server UUID into a range.
func TestStreamState_gtidAccumulation(t *testing.T) {
	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562" // go-mysql lowercases UUIDs
	gs, err := gomysql.ParseMysqlGTIDSet(uuid + ":1")
	if err != nil {
		t.Fatalf("ParseMysqlGTIDSet: %v", err)
	}
	acc := gs.(*gomysql.MysqlGTIDSet)

	for _, gtid := range []string{uuid + ":2", uuid + ":3", uuid + ":4"} {
		if err := acc.Update(gtid); err != nil {
			t.Fatalf("Update(%q): %v", gtid, err)
		}
	}

	got := acc.String()
	// Should contain the UUID and a range covering 1-4.
	if !strings.Contains(got, uuid) {
		t.Errorf("expected UUID in GTID set string, got %q", got)
	}
	if !strings.Contains(got, "1-4") {
		t.Errorf("expected range 1-4 in GTID set string, got %q", got)
	}
}

// TestStreamState_gtidAccumulationMultiServer verifies accumulation across
// two different server UUIDs — each gets its own range entry.
func TestStreamState_gtidAccumulationMultiServer(t *testing.T) {
	uuid1 := "3e11fa47-71ca-11e1-9e33-c80aa9429562" // go-mysql lowercases UUIDs
	uuid2 := "7d93a8e1-0b3c-11e2-ab3d-0022114ef123"

	gs, _ := gomysql.ParseMysqlGTIDSet(uuid1 + ":1")
	acc := gs.(*gomysql.MysqlGTIDSet)

	if err := acc.Update(uuid2 + ":1"); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := acc.Update(uuid1 + ":2"); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got := acc.String()
	if !strings.Contains(got, uuid1) {
		t.Errorf("expected %s in result, got %q", uuid1, got)
	}
	if !strings.Contains(got, uuid2) {
		t.Errorf("expected %s in result, got %q", uuid2, got)
	}
}

// ─── buildTLSConfig ───────────────────────────────────────────────────────────

func TestBuildTLSConfig_disabled(t *testing.T) {
	cfg, err := buildTLSConfig("disabled", "", "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil tls.Config for disabled mode")
	}
}

func TestBuildTLSConfig_preferred(t *testing.T) {
	cfg, err := buildTLSConfig("preferred", "", "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil tls.Config for preferred mode")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=true for preferred mode")
	}
}

func TestBuildTLSConfig_required(t *testing.T) {
	cfg, err := buildTLSConfig("required", "", "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil tls.Config for required mode")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=true for required mode")
	}
}

func TestBuildTLSConfig_invalidMode(t *testing.T) {
	_, err := buildTLSConfig("bogus", "", "", "", "")
	if err == nil {
		t.Error("expected error for unknown ssl-mode")
	}
	if !strings.Contains(err.Error(), "bogus") {
		t.Errorf("expected mode name in error, got: %v", err)
	}
}

func TestBuildTLSConfig_certWithoutKey(t *testing.T) {
	_, err := buildTLSConfig("required", "", "cert.pem", "", "")
	if err == nil {
		t.Error("expected error when cert provided without key")
	}
}

func TestBuildTLSConfig_keyWithoutCert(t *testing.T) {
	_, err := buildTLSConfig("required", "", "", "key.pem", "")
	if err == nil {
		t.Error("expected error when key provided without cert")
	}
}

func TestBuildTLSConfig_nonexistentCA(t *testing.T) {
	_, err := buildTLSConfig("verify-ca", "/nonexistent/ca.pem", "", "", "")
	if err == nil {
		t.Error("expected error for non-existent CA file")
	}
}

func TestBuildTLSConfig_verifyIdentitySetsServerName(t *testing.T) {
	cfg, err := buildTLSConfig("verify-identity", "", "", "", "db.example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil tls.Config")
	}
	if cfg.ServerName != "db.example.com" {
		t.Errorf("expected ServerName=db.example.com, got %q", cfg.ServerName)
	}
	if cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=false for verify-identity")
	}
}

func TestBuildTLSConfig_verifyCAHasVerifyConnection(t *testing.T) {
	cfg, err := buildTLSConfig("verify-ca", "", "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil tls.Config")
	}
	if cfg.VerifyConnection == nil {
		t.Error("expected VerifyConnection to be set for verify-ca mode")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=true for verify-ca (hostname skipped via VerifyConnection)")
	}
}

func TestBuildTLSConfig_validCAFile(t *testing.T) {
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, selfSignedCAPEM(t), 0600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}

	cfg, err := buildTLSConfig("verify-ca", caFile, "", "", "")
	if err != nil {
		t.Fatalf("unexpected error with valid CA file: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Error("expected RootCAs to be set when --ssl-ca is provided")
	}
}

// ─── resolveStart additional paths ───────────────────────────────────────────

// TestResolveStart_customStartPos verifies that a non-default startPos is
// preserved through the position-mode path (not hardcoded to 4).
func TestResolveStart_customStartPos(t *testing.T) {
	_, _, _, pos, _, err := resolveStart("binlog.000001", "", 1234, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos != 1234 {
		t.Errorf("expected pos=1234, got %d", pos)
	}
}

// TestResolveStart_savedGTID_fileFlagSwitchesMode verifies that a saved GTID-mode
// checkpoint is overridden when --start-file requests a mode switch to position.
func TestResolveStart_savedGTID_fileFlagSwitchesMode(t *testing.T) {
	saved := &streamState{
		mode:    "gtid",
		gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
	}
	mode, file, gtidStr, pos, accGTID, err := resolveStart("binlog.000001", "", 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position after switch, got %q", mode)
	}
	if file != "binlog.000001" {
		t.Errorf("expected file from flag, got %q", file)
	}
	if pos != 4 {
		t.Errorf("expected pos=4, got %d", pos)
	}
	if gtidStr != "" {
		t.Errorf("expected empty gtidStr, got %q", gtidStr)
	}
	if accGTID != nil {
		t.Error("expected nil accGTID in position mode")
	}
}

// TestResolveStart_savedGTIDWinsOverGTIDFlag verifies that a saved GTID-mode
// checkpoint is used even when --start-gtid provides a different GTID set.
func TestResolveStart_savedGTIDWinsOverGTIDFlag(t *testing.T) {
	saved := &streamState{
		mode:    "gtid",
		gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
	}
	newGTID := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-200"
	mode, _, returnedGTID, _, accGTID, err := resolveStart("", newGTID, 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid, got %q", mode)
	}
	if returnedGTID != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100" {
		t.Errorf("expected saved GTID, got %q", returnedGTID)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID")
	}
}

// TestResolveStart_savedPosition_gtidFlagSwitchesMode verifies that when a
// position-mode checkpoint exists and --start-gtid is provided, the mode
// switches to GTID (explicit user intent to change tracking mode).
func TestResolveStart_savedPosition_gtidFlagSwitchesMode(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000010",
		binlogPos:  9999,
	}
	gtidSet := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"
	mode, file, returnedGTID, pos, accGTID, err := resolveStart("", gtidSet, 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid (switched), got %q", mode)
	}
	if file != "" {
		t.Errorf("expected empty file after switch, got %q", file)
	}
	if pos != 0 {
		t.Errorf("expected pos=0 after switch, got %d", pos)
	}
	if returnedGTID != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5" {
		t.Errorf("expected flag GTID, got %q", returnedGTID)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID after GTID switch")
	}
}

// ─── Mode switching (issue #68) ──────────────────────────────────────────────

// TestResolveStart_modeSwitch_positionToGTID verifies that a saved position-mode
// checkpoint is overridden when the user passes --start-gtid (without --start-file).
func TestResolveStart_modeSwitch_positionToGTID(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000010",
		binlogPos:  9999,
	}
	newGTID := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50"
	mode, file, returnedGTID, pos, accGTID, err := resolveStart("", newGTID, 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid after switch, got %q", mode)
	}
	if file != "" {
		t.Errorf("expected empty file in gtid mode, got %q", file)
	}
	if returnedGTID != newGTID {
		t.Errorf("expected flag GTID %q, got %q", newGTID, returnedGTID)
	}
	if pos != 0 {
		t.Errorf("expected pos=0 in gtid mode, got %d", pos)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID after switch to gtid mode")
	}
}

// TestResolveStart_modeSwitch_gtidToPosition verifies that a saved GTID-mode
// checkpoint is overridden when the user passes --start-file (without --start-gtid).
func TestResolveStart_modeSwitch_gtidToPosition(t *testing.T) {
	saved := &streamState{
		mode:    "gtid",
		gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
	}
	mode, file, gtidStr, pos, accGTID, err := resolveStart("binlog.000020", "", 100, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position after switch, got %q", mode)
	}
	if file != "binlog.000020" {
		t.Errorf("expected file=binlog.000020, got %q", file)
	}
	if pos != 100 {
		t.Errorf("expected pos=100, got %d", pos)
	}
	if gtidStr != "" {
		t.Errorf("expected empty gtidStr in position mode, got %q", gtidStr)
	}
	if accGTID != nil {
		t.Error("expected nil accGTID in position mode")
	}
}

// TestResolveStart_modeSwitch_bothFlagsWithSaved verifies that passing both
// --start-file and --start-gtid is still rejected even with a saved checkpoint.
func TestResolveStart_modeSwitch_bothFlagsWithSaved(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000010",
		binlogPos:  9999,
	}
	_, _, _, _, _, err := resolveStart("binlog.000001", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", 4, saved)
	if err == nil {
		t.Error("expected error for mutually exclusive flags with saved state")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("expected 'mutually exclusive' in error, got: %v", err)
	}
}

// TestResolveStart_modeSwitch_invalidGTID verifies that an invalid --start-gtid
// during a mode switch produces a clear error.
func TestResolveStart_modeSwitch_invalidGTID(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000010",
		binlogPos:  9999,
	}
	_, _, _, _, _, err := resolveStart("", "not-a-valid-gtid", 0, saved)
	if err == nil {
		t.Error("expected error for invalid GTID during mode switch")
	}
	if !strings.Contains(err.Error(), "invalid --start-gtid") {
		t.Errorf("expected 'invalid --start-gtid' in error, got: %v", err)
	}
}

// ─── NormalizeGTIDSet ────────────────────────────────────────────────────────

func TestNormalizeGTIDSet_standard(t *testing.T) {
	// Already standard 36-char UUID — no change expected.
	input := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"
	got := NormalizeGTIDSet(input)
	if got != input {
		t.Errorf("expected no change, got %q", got)
	}
}

func TestNormalizeGTIDSet_rdsShortened(t *testing.T) {
	// RDS-style shortened UUID (first segment 7 chars instead of 8).
	input := "5512139-1432-11f1-8d8d-0693b428a89b:1-7594394"
	want := "05512139-1432-11f1-8d8d-0693b428a89b:1-7594394"
	got := NormalizeGTIDSet(input)
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeGTIDSet_multipleEntries(t *testing.T) {
	input := "5512139-1432-11f1-8d8d-0693b428a89b:1-100,ab-cdef-1234-5678-abcdefabcdef:1-5"
	want := "05512139-1432-11f1-8d8d-0693b428a89b:1-100,000000ab-cdef-1234-5678-abcdefabcdef:1-5"
	got := NormalizeGTIDSet(input)
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeGTIDSet_parsesAfterNormalization(t *testing.T) {
	// The RDS GTID should parse successfully after normalization.
	input := "5512139-1432-11f1-8d8d-0693b428a89b:1-7594394"
	normalized := NormalizeGTIDSet(input)
	_, err := gomysql.ParseMysqlGTIDSet(normalized)
	if err != nil {
		t.Fatalf("ParseMysqlGTIDSet failed after normalization: %v", err)
	}
}

func TestNormalizeGTIDSet_empty(t *testing.T) {
	got := NormalizeGTIDSet("")
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestResolveStart_rdsShortGTID(t *testing.T) {
	// Verify that resolveStart accepts an RDS-style shortened GTID.
	rdsGTID := "5512139-1432-11f1-8d8d-0693b428a89b:1-7594394"
	wantGTID := "05512139-1432-11f1-8d8d-0693b428a89b:1-7594394"

	mode, _, gtidStr, _, accGTID, err := resolveStart("", rdsGTID, 0, nil)
	if err != nil {
		t.Fatalf("resolveStart with RDS GTID: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid, got %q", mode)
	}
	if gtidStr != wantGTID {
		t.Errorf("expected normalized GTID %q, got %q", wantGTID, gtidStr)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID")
	}
}

// ─── streamLoop GTID tracking ─────────────────────────────────────────────────

// TestStreamLoop_gtidOnlyEventsAccumulated verifies that EventGTID events
// (transactions with no row changes on tracked tables) are accumulated into
// the GTID set without gaps. This is the fix for issue #124: without these
// events, the checkpoint GTID set had gaps, causing ERROR 1236 on resume.
func TestStreamLoop_gtidOnlyEventsAccumulated(t *testing.T) {
	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"

	gs, err := gomysql.ParseMysqlGTIDSet(uuid + ":1-5")
	if err != nil {
		t.Fatalf("ParseMysqlGTIDSet: %v", err)
	}

	state := &streamState{
		mode:    "gtid",
		gtidSet: uuid + ":1-5",
		accGTID: gs.(*gomysql.MysqlGTIDSet),
	}

	// Simulate: GTID 6 is a row event, GTID 7 is a GTID-only event (no rows),
	// GTID 8 is another row event. Without the fix, GTID 7 would be missing.
	events := make(chan parser.Event, 10)
	events <- parser.Event{
		GTID:      uuid + ":6",
		EventType: parser.EventInsert,
		Schema:    "test",
		Table:     "t1",
		EndPos:    100,
	}
	events <- parser.Event{
		GTID:      uuid + ":7",
		EventType: parser.EventGTID,
		EndPos:    200,
	}
	events <- parser.Event{
		GTID:      uuid + ":8",
		EventType: parser.EventInsert,
		Schema:    "test",
		Table:     "t1",
		EndPos:    300,
	}
	close(events)

	// Simulate what streamLoop does: accumulate GTIDs from every event.
	// We can't call streamLoop directly (needs real DB/indexer), but the
	// GTID accumulation logic is the core of this fix.
	for ev := range events {
		if ev.GTID != "" && state.accGTID != nil {
			if err := state.accGTID.Update(ev.GTID); err != nil {
				t.Fatalf("Update(%q): %v", ev.GTID, err)
			}
			state.gtidSet = state.accGTID.String()
		}
	}

	// The GTID set should be contiguous: 1-8 with no gaps.
	got := state.accGTID.String()
	if !strings.Contains(got, "1-8") {
		t.Errorf("expected contiguous range 1-8, got %q", got)
	}
	// Verify no gaps (should NOT contain colons separating ranges within the UUID).
	// A gapped set would look like "uuid:1-6:8" — the ":" after "1-6" splits ranges.
	parts := strings.SplitN(got, ":", 2) // split off UUID
	if len(parts) == 2 && strings.Contains(parts[1], ":") {
		t.Errorf("GTID set has gaps: %q", got)
	}
}

// ─── Gap detection ────────────────────────────────────────────────────────────

// TestDetectPositionGap_noGap verifies that no gap is reported when the
// checkpoint file is the current (latest) binlog file.
func TestDetectPositionGap_noGap(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size"}).
			AddRow("mysql-bin.000001", 1048576).
			AddRow("mysql-bin.000002", 524288).
			AddRow("mysql-bin.000003", 100))

	gap, err := detectPositionGap(db, "mysql-bin.000003", 50, 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gap.HasGap {
		t.Error("expected no gap when checkpoint is on latest file")
	}
	// Resuming in place: a same-named regrown binlog after a source rebuild is
	// undetectable here, so the guard flag must be set (#780).
	if !gap.RebuildUndetectable {
		t.Error("expected RebuildUndetectable=true on an in-place position-mode resume")
	}
}

// TestDetectPositionGap_fillable verifies that a fillable gap is reported when
// the checkpoint file exists but is not the latest.
func TestDetectPositionGap_fillable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size"}).
			AddRow("mysql-bin.000001", 1048576).
			AddRow("mysql-bin.000002", 524288).
			AddRow("mysql-bin.000003", 100))

	gap, err := detectPositionGap(db, "mysql-bin.000001", 9999, 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap when checkpoint is behind latest file")
	}
	if !gap.Fillable {
		t.Error("expected fillable gap when checkpoint file still exists")
	}
	if !strings.Contains(gap.Message, "mysql-bin.000001") {
		t.Errorf("expected checkpoint file in message, got: %s", gap.Message)
	}
	// Fillable gap still resumes reading the existing checkpoint file — rebuild
	// undetectable (#780).
	if !gap.RebuildUndetectable {
		t.Error("expected RebuildUndetectable=true on a fillable position-mode gap")
	}
}

// TestDetectPositionGap_unfillable verifies that an unfillable gap is reported
// when the checkpoint file has been purged.
func TestDetectPositionGap_unfillable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size"}).
			AddRow("mysql-bin.000050", 1048576).
			AddRow("mysql-bin.000051", 524288))

	gap, err := detectPositionGap(db, "mysql-bin.000038", 7890, 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap when checkpoint file is purged")
	}
	if gap.Fillable {
		t.Error("expected unfillable gap when file is purged")
	}
	if gap.EarliestFile != "mysql-bin.000050" {
		t.Errorf("expected earliest file mysql-bin.000050, got %q", gap.EarliestFile)
	}
	if gap.EarliestPos != 4 {
		t.Errorf("expected earliest pos 4, got %d", gap.EarliestPos)
	}
	if !strings.Contains(gap.Message, "purged") {
		t.Errorf("expected 'purged' in message, got: %s", gap.Message)
	}
	if !strings.Contains(gap.Message, "mysql-bin.000038") {
		t.Errorf("expected checkpoint file in message, got: %s", gap.Message)
	}
	// A purged (unfillable) gap is already surfaced loudly and auto-advanced, so
	// the rebuild-undetectable guard must NOT fire here (#780).
	if gap.RebuildUndetectable {
		t.Error("expected RebuildUndetectable=false on an unfillable/purged gap")
	}
}

// TestDetectPositionGap_extraColumns verifies that SHOW BINARY LOGS with extra
// columns (MySQL 8.0.14+ adds Encrypted) is handled correctly.
func TestDetectPositionGap_extraColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size", "Encrypted"}).
			AddRow("mysql-bin.000010", 1048576, "No").
			AddRow("mysql-bin.000011", 524288, "No"))

	gap, err := detectPositionGap(db, "mysql-bin.000010", 100, 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap (checkpoint is not latest)")
	}
	if !gap.Fillable {
		t.Error("expected fillable gap")
	}
	if !gap.RebuildUndetectable {
		t.Error("expected RebuildUndetectable=true on a fillable position-mode gap")
	}
}

// TestDetectPositionGap_rebuildUndetectable pins the #780 guard: the source was
// rebuilt (RESET MASTER + restore) and the same-named checkpoint binlog regrew
// PAST the checkpoint offset. The file exists and checkpointPos < size, so the
// check passes and the stream would resume reading a divergent history. Position
// mode cannot tell — the guard flag is the only signal, and it must be set.
func TestDetectPositionGap_rebuildUndetectable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Checkpoint was mysql-bin.000001:5000. After a rebuild the source only has a
	// regenerated mysql-bin.000001 that has already grown to 20000 bytes.
	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size"}).
			AddRow("mysql-bin.000001", 20000))

	gap, err := detectPositionGap(db, "mysql-bin.000001", 5000, 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gap.HasGap {
		t.Error("expected no reported gap (checkpoint file is the latest and pos <= size)")
	}
	if !gap.RebuildUndetectable {
		t.Error("expected RebuildUndetectable=true: a regrown same-named binlog is undetectable in position mode")
	}
}

// TestDetectPositionGap_posExceedsSize verifies the pos>size branch (a truncated
// or freshly-regenerated shorter file) is treated as an unfillable gap and does
// NOT set the rebuild-undetectable guard — that path is already loud (#780).
func TestDetectPositionGap_posExceedsSize(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size"}).
			AddRow("mysql-bin.000001", 500))

	gap, err := detectPositionGap(db, "mysql-bin.000001", 9000, 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap || gap.Fillable {
		t.Fatalf("expected an unfillable gap when checkpoint pos exceeds file size, got %+v", gap)
	}
	if gap.RebuildUndetectable {
		t.Error("expected RebuildUndetectable=false on the loud pos>size branch")
	}
}

// TestDetectGTIDGap_noGap verifies no gap when checkpoint matches executed.
func TestDetectGTIDGap_noGap(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	gtid := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_purged"}).AddRow(""))
	mock.ExpectQuery("SELECT @@gtid_executed").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_executed"}).AddRow(gtid))

	gap, err := detectGTIDGap(db, gtid, 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gap.HasGap {
		t.Error("expected no gap when checkpoint matches executed")
	}
}

// TestDetectGTIDGap_fillable verifies a fillable gap when checkpoint is behind
// executed but nothing is purged.
func TestDetectGTIDGap_fillable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_purged"}).AddRow(""))
	mock.ExpectQuery("SELECT @@gtid_executed").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_executed"}).AddRow("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-200"))

	gap, err := detectGTIDGap(db, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap when checkpoint is behind executed")
	}
	if !gap.Fillable {
		t.Error("expected fillable gap when nothing is purged")
	}
}

// TestDetectGTIDGap_unfillable verifies an unfillable gap when the checkpoint
// includes GTIDs that have been purged.
func TestDetectGTIDGap_unfillable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_purged"}).AddRow(uuid + ":1-500"))
	mock.ExpectQuery("SELECT @@gtid_executed").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_executed"}).AddRow(uuid + ":1-1000"))

	gap, err := detectGTIDGap(db, uuid+":1-100", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap")
	}
	if gap.Fillable {
		t.Error("expected unfillable gap when checkpoint is within purged range")
	}
	if gap.PurgedGTIDSet == "" {
		t.Error("expected purged GTID set in result")
	}
	if !strings.Contains(gap.Message, "purged") {
		t.Errorf("expected 'purged' in message, got: %s", gap.Message)
	}
}

// TestDetectGTIDGap_fillableWithPurged verifies a fillable gap when checkpoint
// is ahead of the purged set (common case: purged set exists but checkpoint
// has progressed beyond it).
func TestDetectGTIDGap_fillableWithPurged(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_purged"}).AddRow(uuid + ":1-50"))
	mock.ExpectQuery("SELECT @@gtid_executed").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_executed"}).AddRow(uuid + ":1-1000"))

	// Checkpoint at :1-200 — well past the purged range of :1-50.
	gap, err := detectGTIDGap(db, uuid+":1-200", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap when checkpoint is behind executed")
	}
	if !gap.Fillable {
		t.Error("expected fillable gap when checkpoint is past purged range")
	}
}

// TestDetectGTIDGap_unfillableMissingUUID verifies an unfillable gap when the
// purged set contains a UUID that the checkpoint has never seen.
func TestDetectGTIDGap_unfillableMissingUUID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	uuidA := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	uuidB := "7d93a8e1-0b3c-11e2-ab3d-0022114ef123"
	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_purged"}).AddRow(uuidA + ":1-50," + uuidB + ":1-200"))
	mock.ExpectQuery("SELECT @@gtid_executed").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_executed"}).AddRow(uuidA + ":1-500," + uuidB + ":1-300"))

	// Checkpoint only knows about uuidA (past its purged range), not uuidB at all.
	gap, err := detectGTIDGap(db, uuidA+":1-100", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap")
	}
	if gap.Fillable {
		t.Error("expected unfillable gap: purged set has UUID not in checkpoint")
	}
}

// TestDetectGTIDGap_noGapStructuralComparison verifies that GTID sets are
// compared structurally, not by string equality (different formatting of the
// same set should still report no gap).
func TestDetectGTIDGap_noGapStructuralComparison(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Checkpoint uses lowercase UUID; MySQL returns uppercase.
	checkpoint := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	executed := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100"

	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_purged"}).AddRow(""))
	mock.ExpectQuery("SELECT @@gtid_executed").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_executed"}).AddRow(executed))

	gap, err := detectGTIDGap(db, checkpoint, 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gap.HasGap {
		t.Error("expected no gap: same GTID set in different case should be equal")
	}
}

// TestGtidSetsEqual verifies structural comparison of GTID sets.
func TestGtidSetsEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{"identical", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100", true},
		{"case difference", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100", true},
		{"different range", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-200", false},
		{"empty both", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := gtidSetsEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("gtidSetsEqual(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// ─── resolveStartWithAutoDiscoverForFlavor ────────────────────────────────────────────

// TestResolveStartWithAutoDiscover_firesOnFirstRun verifies that the
// auto-discover callback is invoked when saved is nil and no flags are set,
// and that its result becomes the start position.
func TestResolveStartWithAutoDiscover_firesOnFirstRun(t *testing.T) {
	called := false
	mode, file, _, pos, _, err := resolveStartWithAutoDiscoverForFlavor("", "", 4, nil, gomysql.MySQLFlavor,
		func() (string, uint32, error) {
			called = true
			return "mysql-bin.000042", 1234, nil
		}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("expected autoDiscover to be called")
	}
	if mode != "position" || file != "mysql-bin.000042" || pos != 1234 {
		t.Errorf("got mode=%q file=%q pos=%d, want position/mysql-bin.000042/1234",
			mode, file, pos)
	}
}

// TestResolveStartWithAutoDiscover_skippedWhenFlagSet verifies that an
// explicit --start-file bypasses auto-discover (preserves operator override).
func TestResolveStartWithAutoDiscover_skippedWhenFlagSet(t *testing.T) {
	called := false
	mode, file, _, pos, _, err := resolveStartWithAutoDiscoverForFlavor("binlog.000001", "", 100, nil, gomysql.MySQLFlavor,
		func() (string, uint32, error) {
			called = true
			return "should-not-be-used", 999, nil
		}, gtidDiscoverMustNotBeCalled(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Error("autoDiscover should not be called when --start-file is set")
	}
	if mode != "position" || file != "binlog.000001" || pos != 100 {
		t.Errorf("got mode=%q file=%q pos=%d, want position/binlog.000001/100",
			mode, file, pos)
	}
}

// TestResolveStartWithAutoDiscover_skippedWhenGTIDFlagSet verifies the
// symmetric guard: an explicit --start-gtid bypasses auto-discover. The
// wrapper checks `startFile != "" || startGTID != ""` — a future
// refactor that asymmetrically drops the startGTID check would silently
// invoke discovery on GTID-mode first runs and overwrite the operator's
// declared base GTID. This test catches that.
func TestResolveStartWithAutoDiscover_skippedWhenGTIDFlagSet(t *testing.T) {
	gtidSet := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"
	called := false
	mode, _, returnedGTID, _, accGTID, err := resolveStartWithAutoDiscoverForFlavor("", gtidSet, 4, nil, gomysql.MySQLFlavor,
		func() (string, uint32, error) {
			called = true
			return "should-not-be-used", 999, nil
		}, gtidDiscoverMustNotBeCalled(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Error("autoDiscover should not be called when --start-gtid is set")
	}
	if mode != "gtid" {
		t.Errorf("mode = %q, want gtid", mode)
	}
	if returnedGTID != gtidSet {
		t.Errorf("returnedGTID = %q, want %q", returnedGTID, gtidSet)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID in gtid mode")
	}
}

// TestResolveStartWithAutoDiscover_skippedWhenSavedExists verifies that
// a saved checkpoint bypasses auto-discover (preserves resume behavior).
func TestResolveStartWithAutoDiscover_skippedWhenSavedExists(t *testing.T) {
	saved := &streamState{
		mode: "position", binlogFile: "saved.000007", binlogPos: 500,
	}
	called := false
	mode, file, _, pos, _, err := resolveStartWithAutoDiscoverForFlavor("", "", 4, saved, gomysql.MySQLFlavor,
		func() (string, uint32, error) {
			called = true
			return "should-not-be-used", 999, nil
		}, gtidDiscoverMustNotBeCalled(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Error("autoDiscover should not be called when a saved checkpoint exists")
	}
	if mode != "position" || file != "saved.000007" || pos != 500 {
		t.Errorf("got mode=%q file=%q pos=%d, want position/saved.000007/500",
			mode, file, pos)
	}
}

// TestResolveStartWithAutoDiscover_nilCallbackPreservesOriginalError verifies
// that when no callback is wired and no flags/saved exist, the original
// "no start position" error still surfaces (back-compat).
func TestResolveStartWithAutoDiscover_nilCallbackPreservesOriginalError(t *testing.T) {
	_, _, _, _, _, err := resolveStartWithAutoDiscoverForFlavor("", "", 4, nil, gomysql.MySQLFlavor, nil, nil)
	if err == nil {
		t.Fatal("expected error when nil callback and no flags/saved, got nil")
	}
	if !strings.Contains(err.Error(), "no start position specified") {
		t.Errorf("expected original 'no start position' error, got: %v", err)
	}
}

// TestResolveStartWithAutoDiscover_discoveryErrorWrapped verifies that an
// auto-discover failure is surfaced (wrapped) rather than silently masked.
func TestResolveStartWithAutoDiscover_discoveryErrorWrapped(t *testing.T) {
	stubErr := errors.New("SHOW BINARY LOG STATUS returned no rows")
	_, _, _, _, _, err := resolveStartWithAutoDiscoverForFlavor("", "", 4, nil, gomysql.MySQLFlavor,
		func() (string, uint32, error) {
			return "", 0, stubErr
		}, nil)
	if err == nil {
		t.Fatal("expected discovery error to surface, got nil")
	}
	if !errors.Is(err, stubErr) {
		t.Errorf("expected wrapped stubErr via errors.Is, got: %v", err)
	}
	if !strings.Contains(err.Error(), "auto-discover binlog position") {
		t.Errorf("expected error wrap prefix, got: %v", err)
	}
}

// TestResolveStartWithAutoDiscover_mutuallyExclusiveFlagsErrorPropagates
// verifies the wrapper does NOT swallow a real resolveStart error and try
// auto-discover instead.
func TestResolveStartWithAutoDiscover_mutuallyExclusiveFlagsErrorPropagates(t *testing.T) {
	called := false
	_, _, _, _, _, err := resolveStartWithAutoDiscoverForFlavor("binlog.000001", "uuid:1", 4, nil, gomysql.MySQLFlavor,
		func() (string, uint32, error) {
			called = true
			return "", 0, nil
		}, gtidDiscoverMustNotBeCalled(t))
	if err == nil {
		t.Fatal("expected mutually-exclusive error, got nil")
	}
	if called {
		t.Error("autoDiscover must not be called when flags are mutually exclusive")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("expected mutually-exclusive error, got: %v", err)
	}
}

// gtidDiscoverMustNotBeCalled returns a GTID auto-discover stub that fails the
// test if invoked. Used on paths (saved checkpoint, explicit flags, MariaDB
// flavor, non-discovery errors) where GTID discovery must never fire.
func gtidDiscoverMustNotBeCalled(t *testing.T) func() (string, error) {
	t.Helper()
	return func() (string, error) {
		t.Error("gtid auto-discover must not be called on this path")
		return "", nil
	}
}

// TestResolveStartWithAutoDiscover_gtidDiscoveredSelectsGTIDMode verifies the
// #1131 fix: on a first run with no flags, a non-empty executed GTID set from
// the GTID callback selects GTID mode, and the position callback is never
// consulted.
func TestResolveStartWithAutoDiscover_gtidDiscoveredSelectsGTIDMode(t *testing.T) {
	const set = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77"
	posCalled := false
	mode, file, gtidStr, _, accGTID, err := resolveStartWithAutoDiscoverForFlavor("", "", 4, nil, gomysql.MySQLFlavor,
		func() (string, uint32, error) {
			posCalled = true
			return "should-not-be-used", 999, nil
		},
		func() (string, error) { return set, nil })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if posCalled {
		t.Error("position auto-discover must not be called when a GTID set was discovered")
	}
	if mode != "gtid" {
		t.Errorf("mode = %q, want gtid", mode)
	}
	if gtidStr != set {
		t.Errorf("gtidStr = %q, want %q", gtidStr, set)
	}
	if file != "" {
		t.Errorf("file = %q, want empty in gtid mode", file)
	}
	if accGTID == nil {
		t.Error("expected non-nil pre-parsed accGTID in gtid mode")
	}
}

// TestResolveStartWithAutoDiscover_gtidEmptyFallsBackToPosition verifies that
// an empty executed set (gtid_mode not ON, or a fresh server with zero
// transactions) falls back to position auto-discovery — starting GTID
// replication from an empty set would replay the binlog from the beginning
// instead of "starting from now".
func TestResolveStartWithAutoDiscover_gtidEmptyFallsBackToPosition(t *testing.T) {
	gtidCalled := false
	mode, file, _, pos, _, err := resolveStartWithAutoDiscoverForFlavor("", "", 4, nil, gomysql.MySQLFlavor,
		func() (string, uint32, error) { return "mysql-bin.000042", 1234, nil },
		func() (string, error) {
			gtidCalled = true
			return "", nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gtidCalled {
		t.Error("expected the GTID callback to be tried first")
	}
	if mode != "position" || file != "mysql-bin.000042" || pos != 1234 {
		t.Errorf("got mode=%q file=%q pos=%d, want position/mysql-bin.000042/1234",
			mode, file, pos)
	}
}

// TestResolveStartWithAutoDiscover_gtidErrorIsFatal verifies that a GTID
// discovery FAILURE is a hard error, not a silent fallback to position mode —
// an operator on a GTID source must not silently end up with an index that
// live-source verify calls inconclusive (the exact dead end of #1131).
func TestResolveStartWithAutoDiscover_gtidErrorIsFatal(t *testing.T) {
	stubErr := errors.New("SELECT @@GLOBAL.gtid_mode: connection reset")
	posCalled := false
	_, _, _, _, _, err := resolveStartWithAutoDiscoverForFlavor("", "", 4, nil, gomysql.MySQLFlavor,
		func() (string, uint32, error) {
			posCalled = true
			return "mysql-bin.000042", 1234, nil
		},
		func() (string, error) { return "", stubErr })
	if err == nil {
		t.Fatal("expected GTID discovery error to surface, got nil")
	}
	if !errors.Is(err, stubErr) {
		t.Errorf("expected wrapped stubErr via errors.Is, got: %v", err)
	}
	if !strings.Contains(err.Error(), "auto-discover executed GTID set") {
		t.Errorf("expected GTID-discovery wrap prefix, got: %v", err)
	}
	if posCalled {
		t.Error("position auto-discover must not run after a GTID discovery failure")
	}
}

// TestResolveStartWithAutoDiscover_gtidUnparseableIsFatal verifies that a
// discovered set the flavor parser rejects fails loud instead of degrading to
// position mode.
func TestResolveStartWithAutoDiscover_gtidUnparseableIsFatal(t *testing.T) {
	_, _, _, _, _, err := resolveStartWithAutoDiscoverForFlavor("", "", 4, nil, gomysql.MySQLFlavor,
		func() (string, uint32, error) { return "mysql-bin.000042", 1234, nil },
		func() (string, error) { return "not-a-gtid-set", nil })
	if err == nil {
		t.Fatal("expected parse error for unparseable discovered set, got nil")
	}
	if !strings.Contains(err.Error(), "unparseable") {
		t.Errorf("expected unparseable-set error, got: %v", err)
	}
}

// TestResolveStartWithAutoDiscoverForFlavor_mariadbSkipsGTIDDiscovery verifies
// MariaDB keeps today's position-only auto-discovery: the GTID callback is
// never invoked for the mariadb flavor (its GTID coordinates have different
// discovery semantics, and live-source verify only consumes MySQL GTID sets).
func TestResolveStartWithAutoDiscoverForFlavor_mariadbSkipsGTIDDiscovery(t *testing.T) {
	mode, file, _, pos, _, err := resolveStartWithAutoDiscoverForFlavor("", "", 4, nil,
		gomysql.MariaDBFlavor,
		func() (string, uint32, error) { return "mariadb-bin.000002", 1483, nil },
		gtidDiscoverMustNotBeCalled(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" || file != "mariadb-bin.000002" || pos != 1483 {
		t.Errorf("got mode=%q file=%q pos=%d, want position/mariadb-bin.000002/1483",
			mode, file, pos)
	}
}

// TestResolveStartWithAutoDiscover_gtidSkippedWhenSavedExists verifies a saved
// checkpoint bypasses BOTH discovery callbacks — resuming must never
// re-discover.
func TestResolveStartWithAutoDiscover_gtidSkippedWhenSavedExists(t *testing.T) {
	saved := &streamState{mode: "position", binlogFile: "saved.000007", binlogPos: 500}
	mode, file, _, pos, _, err := resolveStartWithAutoDiscoverForFlavor("", "", 4, saved, gomysql.MySQLFlavor,
		func() (string, uint32, error) {
			t.Error("position auto-discover must not be called with a saved checkpoint")
			return "", 0, nil
		},
		gtidDiscoverMustNotBeCalled(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" || file != "saved.000007" || pos != 500 {
		t.Errorf("got mode=%q file=%q pos=%d, want position/saved.000007/500",
			mode, file, pos)
	}
}

// ─── classifyResetDiscard ────────────────────────────────────────────────────

// TestClassifyResetDiscard_positionToGTIDAtCurrentIsNoop verifies the
// spurious-loss fix: --reset on a gtid_mode=ON source whose old checkpoint was
// position mode, landing (via auto-discovery) exactly at the source's current
// coordinates, is a NO-OP — no gap_lost stamp, no "permanently lost" banner.
func TestClassifyResetDiscard_positionToGTIDAtCurrentIsNoop(t *testing.T) {
	old := &streamState{mode: "position", binlogFile: "mysql-bin.000003", binlogPos: 2410667}
	noop, detail := classifyResetDiscard(old, gomysql.MySQLFlavor,
		"gtid", "", 0, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77",
		true,
		func() (string, uint32, error) { return "mysql-bin.000003", 2410667, nil })
	if !noop {
		t.Errorf("expected noop=true when the old position checkpoint equals the source's current coordinates; detail=%q", detail)
	}
	if detail != "" {
		t.Errorf("expected empty detail for a no-op, got %q", detail)
	}
}

// TestClassifyResetDiscard_positionToGTIDGenuineJumpStamps verifies a real
// jump (old checkpoint behind the source's current position) keeps the
// conservative gap record.
func TestClassifyResetDiscard_positionToGTIDGenuineJumpStamps(t *testing.T) {
	old := &streamState{mode: "position", binlogFile: "mysql-bin.000002", binlogPos: 1000}
	noop, detail := classifyResetDiscard(old, gomysql.MySQLFlavor,
		"gtid", "", 0, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77",
		true,
		func() (string, uint32, error) { return "mysql-bin.000003", 2410667, nil })
	if noop {
		t.Error("expected noop=false for a genuine position→GTID jump")
	}
	if !strings.Contains(detail, "permanently lost") {
		t.Errorf("expected the permanent-loss detail, got %q", detail)
	}
}

// TestClassifyResetDiscard_discoveryErrorKeepsConservativeStamp verifies that
// when the source's current position cannot be read, the classification stays
// conservative (stamp) rather than assuming a no-op.
func TestClassifyResetDiscard_discoveryErrorKeepsConservativeStamp(t *testing.T) {
	old := &streamState{mode: "position", binlogFile: "mysql-bin.000003", binlogPos: 2410667}
	noop, _ := classifyResetDiscard(old, gomysql.MySQLFlavor,
		"gtid", "", 0, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77",
		true,
		func() (string, uint32, error) { return "", 0, errors.New("source unreachable") })
	if noop {
		t.Error("expected noop=false when the current position cannot be confirmed")
	}
}

// TestClassifyResetDiscard_explicitStartGTIDNotRefined verifies an
// operator-specified --start-gtid (autoDiscovered=false) never consults the
// source: the operator chose the start point, so the cross-mode jump keeps
// resetJumpDetail's conservative classification untouched.
func TestClassifyResetDiscard_explicitStartGTIDNotRefined(t *testing.T) {
	old := &streamState{mode: "position", binlogFile: "mysql-bin.000003", binlogPos: 2410667}
	noop, _ := classifyResetDiscard(old, gomysql.MySQLFlavor,
		"gtid", "", 0, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77",
		false,
		func() (string, uint32, error) {
			t.Error("currentPosition must not be consulted for an explicit --start-gtid")
			return "", 0, nil
		})
	if noop {
		t.Error("expected noop=false for an explicit cross-mode reset")
	}
}

// TestClassifyResetDiscard_sameModeNoopUnchanged verifies the refinement does
// not disturb resetJumpDetail's existing same-mode no-op arm (and never
// consults the source there).
func TestClassifyResetDiscard_sameModeNoopUnchanged(t *testing.T) {
	old := &streamState{mode: "position", binlogFile: "mysql-bin.000003", binlogPos: 2410667}
	noop, detail := classifyResetDiscard(old, gomysql.MySQLFlavor,
		"position", "mysql-bin.000003", 2410667, "",
		true,
		func() (string, uint32, error) {
			t.Error("currentPosition must not be consulted when resetJumpDetail already classified a no-op")
			return "", 0, nil
		})
	if !noop || detail != "" {
		t.Errorf("expected same-position no-op to pass through, got noop=%v detail=%q", noop, detail)
	}
}

// TestDeleteEventsSinceCheckpoint_noPriorCheckpointIsNoop verifies the
// dedup-on-resume helper (#759) skips the DELETE entirely (and never touches
// db) when there is no prior checkpoint file — the first-run case where
// dedup does not apply.
func TestDeleteEventsSinceCheckpoint_noPriorCheckpointIsNoop(t *testing.T) {
	n, err := deleteEventsSinceCheckpoint(nil, "", 0, noDedupFloor)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 rows affected, got %d", n)
	}
}

// TestDeleteEventsSinceCheckpoint_deletesAtOrBeyond verifies the position-mode
// dedup delete issues a single DELETE keyed on the given (file, pos) and
// returns the affected row count.
func TestDeleteEventsSinceCheckpoint_deletesAtOrBeyond(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("DELETE FROM binlog_events").
		WithArgs("mysql-bin.000005", "mysql-bin.000005", "mysql-bin.000005", "mysql-bin.000005", uint64(1234)).
		WillReturnResult(sqlmock.NewResult(0, 3))

	n, err := deleteEventsSinceCheckpoint(db, "mysql-bin.000005", 1234, noDedupFloor)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 3 {
		t.Errorf("expected 3 rows affected, got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestDeleteEventsSinceCheckpointGTID_noStragglers verifies that when every
// pre-checkpoint gtid found in the checkpoint's binlog file is already
// contained in the saved GTID set, no straggler DELETE is issued.
func TestDeleteEventsSinceCheckpointGTID_noStragglers(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	savedSet, err := gomysql.ParseMysqlGTIDSet(uuid + ":1-100")
	if err != nil {
		t.Fatalf("parse saved set: %v", err)
	}

	mock.ExpectExec("DELETE FROM binlog_events").
		WithArgs("mysql-bin.000005", "mysql-bin.000005", "mysql-bin.000005", "mysql-bin.000005", uint64(1234)).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectQuery("SELECT DISTINCT gtid FROM binlog_events").
		WithArgs("mysql-bin.000005", uint64(1234)).
		WillReturnRows(sqlmock.NewRows([]string{"gtid"}).AddRow(uuid + ":50"))

	n, err := deleteEventsSinceCheckpointGTID(db, "mysql-bin.000005", 1234, savedSet, gomysql.MySQLFlavor, noDedupFloor)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 rows affected (no stragglers), got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestDeleteEventsSinceCheckpointGTID_deletesStragglers verifies that a
// pre-checkpoint gtid NOT contained in the saved GTID set (a transaction that
// was still open — flushed but not yet committed — when the checkpoint was
// written) is deleted via a second, straggler-targeted DELETE.
func TestDeleteEventsSinceCheckpointGTID_deletesStragglers(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	savedSet, err := gomysql.ParseMysqlGTIDSet(uuid + ":1-100")
	if err != nil {
		t.Fatalf("parse saved set: %v", err)
	}
	stragglerGTID := uuid + ":101"

	mock.ExpectExec("DELETE FROM binlog_events").
		WithArgs("mysql-bin.000005", "mysql-bin.000005", "mysql-bin.000005", "mysql-bin.000005", uint64(1234)).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectQuery("SELECT DISTINCT gtid FROM binlog_events").
		WithArgs("mysql-bin.000005", uint64(1234)).
		WillReturnRows(sqlmock.NewRows([]string{"gtid"}).AddRow(stragglerGTID))
	mock.ExpectExec("DELETE FROM binlog_events WHERE gtid IN").
		WithArgs(stragglerGTID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	n, err := deleteEventsSinceCheckpointGTID(db, "mysql-bin.000005", 1234, savedSet, gomysql.MySQLFlavor, noDedupFloor)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 3 {
		t.Errorf("expected 3 total rows affected (2 + 1 straggler), got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── #775: position-mode safe checkpoint never mid-statement ──────────────────

// TestPositionCheckpoint_neverMidStatement is the unit guard for #775. In
// POSITION mode the persisted checkpoint must be the last statement/commit/DDL
// boundary, never a byte offset between the ROWS_EVENTs of one split statement
// (a statement > binlog_row_event_max_size is split into several ROWS_EVENTs
// under ONE TABLE_MAP — resuming StartSync mid-statement hits a rows event with
// no preceding TABLE_MAP and crash-loops). It drives the same two decisions
// streamLoop makes per event — the per-event binlogPos advance and the
// boundary-gated safePos advance — then asserts checkpointPosition (what
// saveCheckpoint persists) tracks the committed boundary.
func TestPositionCheckpoint_neverMidStatement(t *testing.T) {
	const file = "binlog.000001"
	// Seeded at the resolved start boundary (pos 100), as One() does.
	state := &streamState{mode: "position", binlogFile: file, binlogPos: 100, safeFile: file, safePos: 100}

	// apply mirrors streamLoop's per-event position bookkeeping.
	apply := func(ev parser.Event) {
		if ev.BinlogFile != "" {
			state.binlogFile = ev.BinlogFile
		}
		state.binlogPos = ev.EndPos
		if isPositionCheckpointBoundary(ev) {
			state.safeFile, state.safePos = state.binlogFile, ev.EndPos
		}
	}

	// A bulk UPDATE split across two ROWS_EVENTs under one TABLE_MAP: the first
	// chunk (no STMT_END) then a second chunk, still not the statement end.
	apply(parser.Event{EventType: parser.EventUpdate, BinlogFile: file, EndPos: 300, StmtEnd: false})
	apply(parser.Event{EventType: parser.EventUpdate, BinlogFile: file, EndPos: 500, StmtEnd: false})

	// A ticker checkpoint fires HERE — mid-statement. It must persist the prior
	// committed boundary (100), not the per-event offset (500).
	if f, p := checkpointPosition(state); f != file || p != 100 {
		t.Fatalf("mid-statement checkpoint must stay at the committed boundary %s:100, got %s:%d", file, f, p)
	}
	if state.binlogPos != 500 {
		t.Fatalf("per-event binlogPos should still track the latest event for metrics/display, got %d", state.binlogPos)
	}

	// The statement's last chunk (STMT_END_F) arrives — safePos may now advance.
	apply(parser.Event{EventType: parser.EventUpdate, BinlogFile: file, EndPos: 700, StmtEnd: true})
	if _, p := checkpointPosition(state); p != 700 {
		t.Fatalf("after STMT_END the safe checkpoint must advance to 700, got %d", p)
	}

	// EventGTID (transaction-start marker) is NOT a boundary.
	apply(parser.Event{EventType: parser.EventGTID, BinlogFile: file, EndPos: 720})
	if _, p := checkpointPosition(state); p != 700 {
		t.Fatalf("EventGTID must not advance the safe checkpoint; want 700, got %d", p)
	}

	// EventCommit (XID) IS a boundary.
	apply(parser.Event{EventType: parser.EventCommit, BinlogFile: file, EndPos: 750})
	if _, p := checkpointPosition(state); p != 750 {
		t.Fatalf("after commit the safe checkpoint must advance to 750, got %d", p)
	}

	// EventDDL (auto-commit) IS a boundary, and carries the new file after a rotate.
	apply(parser.Event{EventType: parser.EventDDL, BinlogFile: "binlog.000002", EndPos: 900})
	if f, p := checkpointPosition(state); f != "binlog.000002" || p != 900 {
		t.Fatalf("EventDDL must advance the safe checkpoint to binlog.000002:900, got %s:%d", f, p)
	}
}

// TestCheckpointPosition_gtidModeUsesPerEventPos pins that #775 does NOT change
// GTID-mode checkpointing: there the per-event binlogPos is persisted (its resume
// is transaction-aligned and deleteEventsSinceCheckpointGTID depends on the
// offset), even when a stale safePos is present.
func TestCheckpointPosition_gtidModeUsesPerEventPos(t *testing.T) {
	state := &streamState{mode: "gtid", binlogFile: "binlog.000009", binlogPos: 4096, safeFile: "binlog.000009", safePos: 100}
	if f, p := checkpointPosition(state); f != "binlog.000009" || p != 4096 {
		t.Fatalf("GTID mode must persist the per-event position 4096, got %s:%d", f, p)
	}
}

// TestCheckpointPosition_positionFallback pins the backward-compat fallback: a
// position-mode state that never populated safe* (external/test callers) keeps
// the pre-#775 behavior of persisting binlogFile/binlogPos.
func TestCheckpointPosition_positionFallback(t *testing.T) {
	state := &streamState{mode: "position", binlogFile: "binlog.000001", binlogPos: 4}
	if f, p := checkpointPosition(state); f != "binlog.000001" || p != 4 {
		t.Fatalf("unseeded safe* must fall back to binlog*, got %s:%d", f, p)
	}
}

// noDedupFloor is the floor value that reduces both resume-dedup passes to the
// unbounded statements they were before #1690 — what a checkpoint written by
// an older build carries, and what every test predating the floor asserts
// against. Named rather than a bare 0 so a reader of those call sites sees
// which behaviour is being pinned.
const noDedupFloor = int64(0)
