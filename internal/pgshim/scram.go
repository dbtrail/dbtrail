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
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/text/secure/precis"
	"golang.org/x/text/unicode/norm"
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
// SCRAM on disk. One salt is made up when the process starts and sent to
// every client, and every password that opens the port is derived with it
// ahead of the check, all of them at once (WarmPasswords): a client that
// holds no password then costs the port a few HMACs per attempt, not a PBKDF2
// per password, which is what PostgreSQL's stored verifier gives it too.
// Nothing about one username decides what is derived or checked: the salt is
// the same for all, the set derived is the whole set, and every attempt
// checks the same number of passwords in the same number of forms.

const (
	scramMechanism     = "SCRAM-SHA-256"
	scramPlusMechanism = "SCRAM-SHA-256-PLUS"
	// scramIterations is PostgreSQL's default (scram_iterations).
	scramIterations = 4096
	scramSaltLen    = 16
	scramNonceLen   = 18
	// scramMinCandidates is how many passwords a proof is always checked
	// against, made-up ones filling the rest. A login checks a username's
	// own password beside the port's (an application login's), so the
	// number of real ones says something about the username; checking the
	// same number every time keeps a wrong password and a username that is
	// no login taking the same time. Three: the token, the port's password
	// and one login's.
	scramMinCandidates = 3
	// scramForms is how many forms of each password are checked
	// (saslPrepForms), the made-up password's filling the rest, so that what
	// is checked does not say which characters a password holds.
	scramForms = 3
	// scramDummy is the made-up password the checks are filled with.
	scramDummy = "\x00 not a password"
)

// scramSalt is the salt every exchange of this process sends.
var scramSalt = func() []byte {
	b := make([]byte, scramSaltLen)
	_, _ = rand.Read(b)
	return b
}()

// scramCache holds every password that opens the port derived with
// scramSalt, by the form a client hashes it in (saslPrepForms): the whole set
// WarmPasswords was last given, nothing more and nothing less.
var scramCache struct {
	mu     sync.Mutex
	salted map[string][]byte
}

// WarmPasswords derives every form of every password in passwords, and the
// made-up one, and makes that the set a proof is checked with. A host calls
// it with ALL the passwords that open the port, for any username, before it
// checks a proof (Password.Matches): what is derived then depends on the
// port's passwords alone, never on the username a client names, and a
// change of passwords is paid once, by whichever attempt comes first. A set
// that did not change derives nothing.
func WarmPasswords(passwords []string) {
	forms := map[string]bool{}
	for _, pw := range append([]string{scramDummy}, passwords...) {
		if pw == "" {
			continue
		}
		for _, f := range saslPrepForms(pw) {
			forms[f] = true
		}
	}
	scramCache.mu.Lock()
	defer scramCache.mu.Unlock()
	same := len(forms) == len(scramCache.salted)
	for f := range forms {
		if _, ok := scramCache.salted[f]; !ok {
			same = false
			break
		}
	}
	if same {
		return
	}
	next := make(map[string][]byte, len(forms))
	for f := range forms {
		if k, ok := scramCache.salted[f]; ok {
			next[f] = k
		} else {
			next[f] = scramDerive(f, scramSalt, scramIterations)
		}
	}
	scramCache.salted = next
}

// scramSalted is form derived with scramSalt, from the set WarmPasswords
// derived. A form outside it (a host that checks a password it did not warm)
// is derived here, and kept until the next set.
func scramSalted(form string) []byte {
	scramCache.mu.Lock()
	defer scramCache.mu.Unlock()
	if k, ok := scramCache.salted[form]; ok {
		return k
	}
	if scramCache.salted == nil {
		scramCache.salted = map[string][]byte{}
	}
	k := scramDerive(form, scramSalt, scramIterations)
	scramCache.salted[form] = k
	return k
}

// scramChecks counts the password forms proofs were checked against, for a
// test.
var scramChecks atomic.Int64

// Password is what a client proved in its SCRAM exchange. Matches checks it
// against the passwords that open the port.
type Password struct {
	authMessage []byte
	proof       []byte
	// salted is the derived password that matched, kept to sign the
	// server's last message; nil until one matched.
	salted []byte
}

// Matches says whether the client's proof was made with one of candidates.
// An empty candidate opens nothing. Every candidate is checked, and at least
// scramMinCandidates of them, so the time it takes does not say which one
// matched, or how many there were.
func (p *Password) Matches(candidates []string) bool {
	if p == nil {
		return false
	}
	var hit []byte
	for i := range max(len(candidates), scramMinCandidates) {
		real := i < len(candidates) && candidates[i] != ""
		pw := scramDummy
		if real {
			pw = candidates[i]
		}
		forms := saslPrepForms(pw)
		for j := range scramForms {
			scramChecks.Add(1)
			form := scramDummy
			if j < len(forms) {
				form = forms[j]
			}
			salted := scramSalted(form)
			if p.proves(salted) && real && j < len(forms) && hit == nil {
				hit = salted
			}
		}
	}
	p.salted = hit
	return hit != nil
}

// proves says whether the client's proof was made with salted.
func (p *Password) proves(salted []byte) bool {
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	stored := sha256.Sum256(clientKey)
	signature := hmacSHA256(stored[:], p.authMessage)
	// The proof is the client key XOR the signature: undone, it is the
	// client key again when the password was this one.
	recovered := make([]byte, len(signature))
	subtle.XORBytes(recovered, p.proof, signature)
	got := sha256.Sum256(recovered)
	return subtle.ConstantTimeCompare(got[:], stored[:]) == 1
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

// saslPrepForms is the password in each form a client may hash it: drivers
// prepare a password before hashing it, and not all the same way. pgx uses
// the PRECIS profile OpaqueString (NFC); libpq, and so psql and psycopg, and
// JDBC use SASLprep (RFC 4013: spaces mapped, invisible characters dropped,
// NFKC). Each falls back to the password as it is when its preparation
// refuses it. An ASCII password reads the same in every form, and is one.
func saslPrepForms(pw string) []string {
	ascii := true
	for i := range len(pw) {
		if pw[i] >= 0x80 {
			ascii = false
			break
		}
	}
	if ascii {
		return []string{pw}
	}
	forms := []string{pw}
	if out, err := precis.OpaqueString.String(pw); err == nil && out != pw {
		forms = append(forms, out)
	}
	if out := saslPrepRFC4013(pw); out != forms[0] && (len(forms) == 1 || out != forms[1]) {
		forms = append(forms, out)
	}
	return forms
}

// saslPrepRFC4013 maps pw as SASLprep does before its checks: a non-ASCII
// space becomes a space (RFC 3454 C.1.2), a character "commonly mapped to
// nothing" goes (B.1), and the result is NFKC. The checks for prohibited
// characters are not made: libpq hashes the password as it is when they
// fail, and that form is already among saslPrepForms.
func saslPrepRFC4013(pw string) string {
	var b strings.Builder
	for _, r := range pw {
		switch {
		case r == 0x00A0, r == 0x1680, r >= 0x2000 && r <= 0x200B, r == 0x202F, r == 0x205F, r == 0x3000:
			b.WriteByte(' ')
		case r == 0x00AD, r == 0x034F, r == 0x1806, r >= 0x180B && r <= 0x180D, r == 0x200C, r == 0x200D,
			r == 0x2060, r >= 0xFE00 && r <= 0xFE0F, r == 0xFEFF:
		default:
			b.WriteRune(r)
		}
	}
	return norm.NFKC.String(b.String())
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
		return nil, unreadSASL(err)
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

	salt := scramSalt
	nonce := make([]byte, scramNonceLen)
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
		return nil, unreadSASL(err)
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
		authMessage: []byte(first.bare + "," + serverFirst + "," + withoutProof),
		proof:       proof,
	}, nil
}

// unreadSASL is a SASL message that could not be read: the client is told
// why (08P01) unless the connection itself ended, and then there is nobody
// to tell.
func unreadSASL(err error) error {
	var ne net.Error
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) || errors.As(err, &ne) {
		return err
	}
	return malformedSCRAM(fmt.Sprintf("the SASL message could not be read: %v", err))
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
