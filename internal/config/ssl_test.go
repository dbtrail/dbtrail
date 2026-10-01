package config

import (
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// fakeOpen records every connect attempt and answers from a script, one
// error per attempt (nil = connected).
type fakeOpen struct {
	answers []error
	got     []*tls.Config
}

func (f *fakeOpen) open(_ string, c *tls.Config) (*sql.DB, error) {
	f.got = append(f.got, c)
	err := f.answers[len(f.got)-1]
	if err != nil {
		return nil, err
	}
	// A handle that never dialed: sql.Open does not connect.
	return sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/x")
}

const sslTestDSN = "u:p@tcp(db.example.com:3306)/x"

// The cleartext retry is the one place a source connection may go out
// unencrypted, so it must happen for exactly one cause: the server offered no
// TLS, under preferred. Every other answer is returned as it came.
func TestConnectSSL_RetryRule(t *testing.T) {
	refusedPlain := &mysql.MySQLError{Number: 3159, Message: "Connections using insecure transport are prohibited while --require_secure_transport=ON."}
	denied := &mysql.MySQLError{Number: 1045, Message: "Access denied"}

	tests := []struct {
		name        string
		mode        string
		answers     []error
		wantOK      bool
		wantTLS     []bool // per attempt: was a TLS config passed
		wantWarned  bool
		wantErrIs   error
		wantErrText string
	}{
		{"preferred, server speaks TLS", "preferred", []error{nil}, true, []bool{true}, false, nil, ""},
		{"preferred, server has no TLS: warn once, retry in cleartext", "preferred",
			[]error{fmt.Errorf("failed to ping MySQL: %w", mysql.ErrNoTLS), nil}, true, []bool{true, false}, true, nil, ""},
		{"preferred, cleartext retry fails too", "preferred",
			[]error{mysql.ErrNoTLS, denied}, false, []bool{true, false}, true, denied, "cleartext retry"},
		{"preferred, server demands TLS (3159) is not a reason to drop TLS", "preferred",
			[]error{refusedPlain}, false, []bool{true}, false, refusedPlain, ""},
		{"preferred, access denied never retries", "preferred", []error{denied}, false, []bool{true}, false, denied, ""},
		{"required, server has no TLS: fail closed", "required", []error{mysql.ErrNoTLS}, false, []bool{true}, false, mysql.ErrNoTLS, ""},
		{"disabled sends no TLS config", "disabled", []error{nil}, true, []bool{false}, false, nil, ""},
		{"disabled against a TLS-only server keeps the server's error", "disabled",
			[]error{refusedPlain}, false, []bool{false}, false, refusedPlain, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeOpen{answers: tt.answers}
			warned := 0
			db, err := connectSSL(sslTestDSN, SSL{Mode: tt.mode}, func(error) { warned++ }, f.open)
			if db != nil {
				db.Close()
			}
			if (err == nil) != tt.wantOK {
				t.Fatalf("err = %v, want ok=%v", err, tt.wantOK)
			}
			if len(f.got) != len(tt.wantTLS) {
				t.Fatalf("%d connect attempts, want %d", len(f.got), len(tt.wantTLS))
			}
			for i, want := range tt.wantTLS {
				if (f.got[i] != nil) != want {
					t.Errorf("attempt %d: TLS config passed = %v, want %v", i+1, f.got[i] != nil, want)
				}
			}
			if (warned == 1) != tt.wantWarned || warned > 1 {
				t.Errorf("onCleartext called %d times, want warned=%v", warned, tt.wantWarned)
			}
			if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
				t.Errorf("err = %v, want it to wrap %v", err, tt.wantErrIs)
			}
			if tt.wantErrText != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErrText)) {
				t.Errorf("err = %v, want it to mention %q", err, tt.wantErrText)
			}
		})
	}
}

// A nil onCleartext is allowed: the retry still happens.
func TestConnectSSL_NilOnCleartext(t *testing.T) {
	f := &fakeOpen{answers: []error{mysql.ErrNoTLS, nil}}
	db, err := connectSSL(sslTestDSN, SSL{Mode: "preferred"}, nil, f.open)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}

// The caller picks the default, so an empty or misspelled mode is an error
// (as it is for capture), never a quietly weaker connection, and nothing dials.
func TestConnectSSL_RejectsUnknownMode(t *testing.T) {
	for _, mode := range []string{"", "PREFERRED", "require", " preferred"} {
		f := &fakeOpen{}
		_, err := connectSSL(sslTestDSN, SSL{Mode: mode}, nil, f.open)
		if err == nil || !strings.Contains(err.Error(), "invalid") {
			t.Errorf("mode %q: err = %v, want an invalid-mode error", mode, err)
		}
		if len(f.got) != 0 {
			t.Errorf("mode %q: dialed %d times before rejecting the mode", mode, len(f.got))
		}
	}
}

// verify-ca and verify-identity read the CA file and name the DSN's host, the
// same as capture: a probe that ignored them would pass a server capture
// refuses.
func TestConnectSSL_VerifyModesHonorFiles(t *testing.T) {
	t.Run("an unreadable CA fails before dialing", func(t *testing.T) {
		f := &fakeOpen{}
		_, err := connectSSL(sslTestDSN, SSL{Mode: "verify-ca", CA: "/nonexistent/ca.pem"}, nil, f.open)
		if err == nil || !strings.Contains(err.Error(), "/nonexistent/ca.pem") {
			t.Fatalf("err = %v, want it to name the CA file", err)
		}
		if len(f.got) != 0 {
			t.Fatal("dialed with a CA that could not be read")
		}
	})
	t.Run("verify-identity checks the DSN's host name", func(t *testing.T) {
		f := &fakeOpen{answers: []error{nil}}
		db, err := connectSSL(sslTestDSN, SSL{Mode: "verify-identity"}, nil, f.open)
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
		if got := f.got[0]; got == nil || got.ServerName != "db.example.com" || got.InsecureSkipVerify {
			t.Fatalf("tls config = %+v, want ServerName db.example.com with verification on", got)
		}
	})
	t.Run("preferred does not verify the certificate", func(t *testing.T) {
		f := &fakeOpen{answers: []error{nil}}
		db, err := connectSSL(sslTestDSN, SSL{Mode: "preferred"}, nil, f.open)
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
		if !f.got[0].InsecureSkipVerify {
			t.Fatal("preferred must not verify: a self-signed server certificate (MariaDB, RDS without the CA bundle) would refuse")
		}
	})
}
