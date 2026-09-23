package doctor

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// The loopback proof (#1803) must never send the typed user or password to
// host.docker.internal: on a host where nothing maps that name, a DNS answer
// for it (a search domain will do) points at some other machine, and a login
// there hands it the password — the Go driver even fetches that server's key
// and encrypts the password TO it. A MySQL server speaks first, before any
// login, so its greeting proves a database is there with no credentials.

// mysqlGreeting is a real-shaped initial handshake packet (protocol 10).
func mysqlGreeting() []byte {
	p := []byte{0x0a}
	p = append(p, "8.4.9"...)
	p = append(p, 0)
	p = append(p, 1, 0, 0, 0)                             // connection id
	p = append(p, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h') // salt part 1
	p = append(p, 0)                                      // filler
	p = append(p, 0xff, 0xf7, 0xff, 0x02, 0, 0xff, 0xc1, 21)
	p = append(p, make([]byte, 10)...)
	p = append(p, "ijklmnopqrst"...)
	p = append(p, 0)
	return packet(0, p)
}

func packet(seq byte, payload []byte) []byte {
	n := len(payload)
	return append([]byte{byte(n), byte(n >> 8), byte(n >> 16), seq}, payload...)
}

// recordingListener sends hello to every connection, then records every byte
// the client sends until the client closes.
type recordingListener struct {
	net.Listener
	mu       sync.Mutex
	received []byte
	done     chan struct{}
}

func listenSaying(t *testing.T, hello []byte) *recordingListener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &recordingListener{Listener: l, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		if hello != nil {
			_, _ = c.Write(hello)
		}
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		b, _ := io.ReadAll(c)
		r.mu.Lock()
		r.received = append(r.received, b...)
		r.mu.Unlock()
	}()
	t.Cleanup(func() { l.Close() })
	return r
}

func (r *recordingListener) receivedAfterClose(t *testing.T) []byte {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(6 * time.Second):
		t.Fatal("the listener never saw the connection close")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.received
}

func TestProveLoopback_sendsNoCredentials(t *testing.T) {
	l := listenSaying(t, mysqlGreeting())
	got := proveLoopback("dbtrail:S3cret-Pass@tcp(localhost:"+closedPort(t)+")/", KindPortClosed,
		func(string, string) string { return l.Addr().String() })
	if got != l.Addr().String() {
		t.Errorf("a real MySQL greeting was not taken as proof: %q", got)
	}
	if b := l.receivedAfterClose(t); len(b) != 0 {
		t.Errorf("the probe sent %d byte(s) to the other address; it must send nothing: %q", len(b), b)
	}
}

// A MySQL server that refuses this client's host still greets with an error
// packet, and that still proves a database is there.
func TestGreetingAt_anErrorPacketIsStillMySQL(t *testing.T) {
	msg := append([]byte{0xff, 0x6a, 0x04}, "Host '10.0.0.5' is not allowed to connect to this MySQL server"...)
	l := listenSaying(t, packet(0, msg))
	if !greetingAt(t.Context(), l.Addr().String()) {
		t.Error("a MySQL error greeting (1130) was not recognised")
	}
}

func TestGreetingAt_somethingElseIsNotMySQL(t *testing.T) {
	for name, hello := range map[string][]byte{
		"http":              []byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n"),
		"ssh":               []byte("SSH-2.0-OpenSSH_9.6\r\n"),
		"wrong sequence":    append(mysqlGreeting()[:3:3], append([]byte{1}, mysqlGreeting()[4:]...)...),
		"protocol 9":        packet(0, append([]byte{0x09}, mysqlGreeting()[5:]...)),
		"no version end":    packet(0, append([]byte{0x0a}, "8.4.9 and nothing else"...)),
		"version not text":  packet(0, append(append([]byte{0x0a, 0x01, 0x02}, 0), make([]byte, 20)...)),
		"truncated":         mysqlGreeting()[:10],
		"too short error":   packet(0, []byte{0xff, 0x6a}),
		"error code zero":   packet(0, append([]byte{0xff, 0, 0}, "nope"...)),
		"cut after version": packet(0, append([]byte{0x0a}, "8.4.9\x00abc"...)),
		"postgres refusal":  append([]byte("E"), []byte{0, 0, 0, 8, 'S', 'E', 0, 0}...),
	} {
		t.Run(name, func(t *testing.T) {
			l := listenSaying(t, hello)
			if greetingAt(t.Context(), l.Addr().String()) {
				t.Errorf("%s was taken for a MySQL greeting", name)
			}
		})
	}
}

func TestGreetingAt_nothingListening(t *testing.T) {
	if greetingAt(t.Context(), "127.0.0.1:"+closedPort(t)) {
		t.Error("a closed port was taken for a MySQL greeting")
	}
}

// A peer that claims a huge first packet is not a MySQL greeting, and must
// not make the probe wait on (or allocate for) what a stranger asks for.
func TestGreetingAt_aHugeClaimIsRefusedAtOnce(t *testing.T) {
	l := listenSaying(t, []byte{0xff, 0xff, 0xff, 0}) // claims 16 MiB, sends none of it
	start := time.Now()
	if greetingAt(t.Context(), l.Addr().String()) {
		t.Error("a 16 MiB claim was taken for a greeting")
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("the probe waited %v on a claim it should have refused on the header", d)
	}
}
