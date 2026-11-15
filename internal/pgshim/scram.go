package pgshim

import (
	"bytes"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/text/secure/precis"
)

// The port's login is SCRAM-SHA-256 (RFC 5802, RFC 7677), on a connection
// with TLS and on one without (#2766): the client proves it knows the
// password, and the password itself never crosses the wire, encrypted or
// not. On TLS the port also offers SCRAM-SHA-256-PLUS, which binds the proof
// to the certificate the client saw (tls-server-end-point, RFC 5929): a
// client that asks for it (channel_binding=require) cannot be answered by
// something in the middle that holds another certificate.
//
// The port knows each password it takes as text, so nothing is stored for
// SCRAM: the salt is made up for each connection, and each password is
// derived with it when the client's proof arrives.

const (
	scramMechanism     = "SCRAM-SHA-256"
	scramPlusMechanism = "SCRAM-SHA-256-PLUS"
	// scramIterations is PostgreSQL's default (scram_iterations).
	scramIterations = 4096
	scramSaltLen    = 16
	scramNonceLen   = 18
	// scramMinDerivations is how many passwords a proof is always checked
	// against, made-up ones filling the rest. A login checks a username's
	// own password beside the port's (an application login's), so the
	// number of real ones says something about the username; checking the
	// same number every time keeps a wrong password and a username that is
	// no login taking the same time. Three: the token, the port's password
	// and one login's.
	scramMinDerivations = 3
)

// Password is what a client proved in its SCRAM exchange. Matches checks it
// against the passwords that open the port.
type Password struct {
	salt        []byte
	authMessage []byte
	proof       []byte
	// salted is the derived password that matched, kept to sign the
	// server's last message; nil until one matched.
	salted []byte
}

// Matches says whether the client's proof was made with one of candidates.
// An empty candidate opens nothing. Every candidate is checked, and at least
// scramMinDerivations of them are derived, so the time it takes does not say
// which one matched, or how many there were.
func (p *Password) Matches(candidates []string) bool {
	if p == nil {
		return false
	}
	var hit []byte
	for i := range max(len(candidates), scramMinDerivations) {
		real := i < len(candidates) && candidates[i] != ""
		pw := "\x00 not a password"
		if real {
			pw = candidates[i]
		}
		salted := scramDerive(saslPrep(pw), p.salt, scramIterations)
		clientKey := hmacSHA256(salted, []byte("Client Key"))
		stored := sha256.Sum256(clientKey)
		signature := hmacSHA256(stored[:], p.authMessage)
		// The proof is the client key XOR the signature: undone, it is the
		// client key again when the password was this one.
		recovered := make([]byte, len(signature))
		subtle.XORBytes(recovered, p.proof, signature)
		got := sha256.Sum256(recovered)
		if subtle.ConstantTimeCompare(got[:], stored[:]) == 1 && real && hit == nil {
			hit = salted
		}
	}
	p.salted = hit
	return hit != nil
}

// matched says whether Matches found the password.
func (p *Password) matched() bool { return p != nil && p.salted != nil }

// serverFinal is the server's last SCRAM message: its signature, which shows
// the client the port knew the password too.
func (p *Password) serverFinal() []byte {
	serverKey := hmacSHA256(p.salted, []byte("Server Key"))
	return []byte("v=" + base64.StdEncoding.EncodeToString(hmacSHA256(serverKey, p.authMessage)))
}

// scramDerive is SaltedPassword (RFC 5802): PBKDF2 with HMAC-SHA-256. A
// variable so a test can count the derivations.
var scramDerive = func(password string, salt []byte, iterations int) []byte {
	k, err := pbkdf2.Key(sha256.New, password, salt, iterations, sha256.Size)
	if err != nil {
		// Only for a key length or a FIPS rule this never asks for.
		panic(fmt.Sprintf("pgshim: PBKDF2: %v", err))
	}
	return k
}

// saslPrep is the password as a client hashes it: SASLprep, by the PRECIS
// profile pgx uses for it, or the password as it is when that refuses it, as
// PostgreSQL and pgx both fall back to.
func saslPrep(pw string) string {
	if out, err := precis.OpaqueString.String(pw); err == nil {
		return out
	}
	return pw
}

func hmacSHA256(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)
}

// scramError is a SCRAM exchange that cannot go on: the code and the message
// the client is told.
type scramError struct{ code, msg string }

func (e *scramError) Error() string { return e.msg }

func malformedSCRAM(what string) *scramError {
	return &scramError{"08P01", "malformed SCRAM message: " + what}
}

// scramExchange runs a SCRAM-SHA-256 exchange up to the client's proof and
// returns it, to be checked against the passwords (Password.Matches). On an
// exchange that cannot go on the client is sent the reason as a FATAL error,
// and the error says the connection is over. endPoint is the TLS channel
// binding data, used only when encrypted.
func scramExchange(be *pgproto3.Backend, encrypted bool, endPoint []byte) (*Password, error) {
	pw, err := scramSteps(be, encrypted, endPoint)
	var se *scramError
	if errors.As(err, &se) {
		sendFatal(be, se.code, se.msg)
	}
	return pw, err
}

func scramSteps(be *pgproto3.Backend, encrypted bool, endPoint []byte) (*Password, error) {
	// Before anyone is authenticated a message is part of the login and
	// nothing else. The protocol library sizes its buffer from the length a
	// message announces and has no ceiling of its own, so without one a
	// client that holds no credential could announce two gigabytes and have
	// them set aside in this process.
	be.SetMaxBodyLen(maxPasswordMessage)
	plus := encrypted && endPoint != nil
	mechanisms := []string{scramMechanism}
	if plus {
		// PostgreSQL's order: the one with channel binding first.
		mechanisms = []string{scramPlusMechanism, scramMechanism}
	}
	be.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: mechanisms})
	if err := be.Flush(); err != nil {
		return nil, err
	}
	if err := be.SetAuthType(pgproto3.AuthTypeSASL); err != nil {
		return nil, err
	}
	msg, err := be.Receive()
	if err != nil {
		return nil, err
	}
	initial, ok := msg.(*pgproto3.SASLInitialResponse)
	if !ok {
		return nil, &scramError{"08P01", fmt.Sprintf("expected a SASL initial response, got %T: this port logs in with SCRAM-SHA-256, which needs libpq 10, JDBC 42.2 or later", msg)}
	}
	if initial.AuthMechanism != scramMechanism && !(plus && initial.AuthMechanism == scramPlusMechanism) {
		return nil, &scramError{"08P01", fmt.Sprintf("the client chose the SASL mechanism %q, which this port did not offer (it offered %s)", initial.AuthMechanism, strings.Join(mechanisms, ", "))}
	}
	first, err := parseClientFirst(string(initial.Data))
	if err != nil {
		return nil, err
	}
	switch {
	case initial.AuthMechanism == scramPlusMechanism && first.binding != "tls-server-end-point":
		return nil, malformedSCRAM("SCRAM-SHA-256-PLUS without the channel binding tls-server-end-point")
	case initial.AuthMechanism == scramMechanism && first.binding != "":
		return nil, malformedSCRAM("SCRAM-SHA-256 with channel binding data; channel binding is SCRAM-SHA-256-PLUS")
	case first.flag == 'y' && plus:
		// The client can bind to the channel and believes the port cannot,
		// when the port offered it: something in between took
		// SCRAM-SHA-256-PLUS out of the offer.
		return nil, &scramError{"28000", "SCRAM channel binding negotiation error: the client did not see this port's offer of SCRAM-SHA-256-PLUS"}
	}

	salt := make([]byte, scramSaltLen)
	nonce := make([]byte, scramNonceLen)
	_, _ = rand.Read(salt)
	_, _ = rand.Read(nonce)
	combined := first.nonce + base64.RawStdEncoding.EncodeToString(nonce)
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", combined, base64.StdEncoding.EncodeToString(salt), scramIterations)
	be.Send(&pgproto3.AuthenticationSASLContinue{Data: []byte(serverFirst)})
	if err := be.Flush(); err != nil {
		return nil, err
	}
	if err := be.SetAuthType(pgproto3.AuthTypeSASLContinue); err != nil {
		return nil, err
	}
	if msg, err = be.Receive(); err != nil {
		return nil, err
	}
	resp, ok := msg.(*pgproto3.SASLResponse)
	if !ok {
		return nil, &scramError{"08P01", fmt.Sprintf("expected a SASL response, got %T", msg)}
	}
	wantBinding := []byte(first.gs2Header)
	if first.binding != "" {
		wantBinding = append(wantBinding, endPoint...)
	}
	withoutProof, proof, err := parseClientFinal(string(resp.Data), combined, wantBinding)
	if err != nil {
		return nil, err
	}
	return &Password{
		salt:        salt,
		authMessage: []byte(first.bare + "," + serverFirst + "," + withoutProof),
		proof:       proof,
	}, nil
}

// clientFirst is the client's first SCRAM message, read.
type clientFirst struct {
	// flag is 'n' (no channel binding), 'y' (the client could bind, and
	// believes the server cannot) or 'p' (it binds, by binding).
	flag      byte
	binding   string
	gs2Header string
	bare      string
	nonce     string
}

// parseClientFirst reads gs2-header client-first-message-bare (RFC 5802
// section 7). The username in it is ignored, as PostgreSQL ignores it (libpq
// sends none): the start-up message names the user.
func parseClientFirst(m string) (clientFirst, error) {
	var cf clientFirst
	flag, rest, ok := strings.Cut(m, ",")
	if !ok {
		return cf, malformedSCRAM("no GS2 header")
	}
	switch {
	case flag == "n", flag == "y":
		cf.flag = flag[0]
	case strings.HasPrefix(flag, "p="):
		cf.flag, cf.binding = 'p', flag[2:]
		if cf.binding == "" {
			return cf, malformedSCRAM("an empty channel binding name")
		}
	default:
		return cf, malformedSCRAM(fmt.Sprintf("the channel binding flag %q", flag))
	}
	authzid, bare, ok := strings.Cut(rest, ",")
	if !ok {
		return cf, malformedSCRAM("no GS2 header")
	}
	if authzid != "" {
		return cf, &scramError{"0A000", "the client named an authorization identity, which this port does not take"}
	}
	cf.gs2Header = flag + "," + authzid + ","
	cf.bare = bare
	attrs := strings.Split(bare, ",")
	if strings.HasPrefix(attrs[0], "m=") {
		return cf, &scramError{"0A000", "the client sent a mandatory SCRAM extension, which this port does not take"}
	}
	if !strings.HasPrefix(attrs[0], "n=") {
		return cf, malformedSCRAM("no username attribute")
	}
	if len(attrs) < 2 || !strings.HasPrefix(attrs[1], "r=") {
		return cf, malformedSCRAM("no nonce")
	}
	cf.nonce = attrs[1][2:]
	if !printableNonce(cf.nonce) {
		return cf, malformedSCRAM("the client nonce is empty or holds a character a nonce cannot")
	}
	return cf, nil
}

// printableNonce says whether n is a nonce RFC 5802 allows: printable ASCII
// but the comma, at least one character.
func printableNonce(n string) bool {
	if n == "" {
		return false
	}
	for i := range len(n) {
		if c := n[i]; c < 0x21 || c > 0x7e || c == ',' {
			return false
		}
	}
	return true
}

// parseClientFinal reads client-final-message (RFC 5802 section 7): the
// channel binding it repeats must be wantBinding and the nonce must be the
// one the server sent. It returns the message without its proof (part of
// what the proof signs) and the proof.
func parseClientFinal(m, nonce string, wantBinding []byte) (withoutProof string, proof []byte, err error) {
	idx := strings.LastIndex(m, ",")
	if idx < 0 || !strings.HasPrefix(m[idx+1:], "p=") {
		return "", nil, malformedSCRAM("no proof")
	}
	withoutProof = m[:idx]
	attrs := strings.Split(withoutProof, ",")
	if !strings.HasPrefix(attrs[0], "c=") {
		return "", nil, malformedSCRAM("no channel binding attribute")
	}
	binding, err := base64.StdEncoding.DecodeString(attrs[0][2:])
	if err != nil {
		return "", nil, malformedSCRAM("the channel binding is not base64")
	}
	if !bytes.Equal(binding, wantBinding) {
		// What the client says it bound to is not what the port serves, or
		// it is not what it said in its first message.
		return "", nil, &scramError{"28000", "SCRAM channel binding check failed"}
	}
	if len(attrs) < 2 || !strings.HasPrefix(attrs[1], "r=") {
		return "", nil, malformedSCRAM("no nonce")
	}
	if attrs[1][2:] != nonce {
		return "", nil, malformedSCRAM("the nonce is not the one this port sent")
	}
	proof, err = base64.StdEncoding.DecodeString(m[idx+3:])
	if err != nil {
		return "", nil, malformedSCRAM("the proof is not base64")
	}
	if len(proof) != sha256.Size {
		return "", nil, malformedSCRAM(fmt.Sprintf("the proof is %d bytes, not %d", len(proof), sha256.Size))
	}
	return withoutProof, proof, nil
}
