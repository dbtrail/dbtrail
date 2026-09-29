//go:build integration

package metadata

import (
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestFormatMariaDBFixed_matchesServer_MariaDB holds the Go rendering of
// UUID/INET4/INET6 to the server's own: thousands of values, most of them
// shaped to hit the INET6 rules (runs of zero words of every length and
// position, ties, the IPv4-mapped and IPv4-compatible forms), stored through
// X'…' literals and read back as text. Any drift in a MariaDB version would
// hand a reader a value that differs from the source.
func TestFormatMariaDBFixed_matchesServer_MariaDB(t *testing.T) {
	db, _ := testutil.CreateTestMariaDB(t)
	var v string
	if err := db.QueryRow("SELECT VERSION()").Scan(&v); err != nil {
		t.Fatalf("VERSION(): %v", err)
	}
	if strings.HasPrefix(v, "10.") && !strings.HasPrefix(v, "10.11") {
		t.Skipf("MariaDB %s: INET4 needs 10.10+, the supported minimum is 10.11", v)
	}
	testutil.MustExec(t, db, "CREATE TABLE f (id INT PRIMARY KEY, u UUID, i4 INET4, i6 INET6)")

	rng := rand.New(rand.NewPCG(1944, 5))
	word := func() uint16 {
		switch rng.IntN(6) {
		case 0, 1, 2:
			return 0
		case 3:
			return uint16(rng.IntN(0x100))
		case 4:
			return 0xffff
		default:
			return uint16(rng.IntN(0x10000))
		}
	}
	const n = 3000
	type row struct{ u, i4, i6 []byte }
	rows := make([]row, n)
	for i := range rows {
		u := make([]byte, 16)
		for j := range u {
			if rng.IntN(3) > 0 {
				u[j] = byte(rng.IntN(256))
			}
		}
		i4 := make([]byte, 4)
		for j := range i4 {
			if rng.IntN(3) > 0 {
				i4[j] = byte(rng.IntN(256))
			}
		}
		i6 := make([]byte, 16)
		for w := 0; w < 8; w++ {
			x := word()
			i6[2*w], i6[2*w+1] = byte(x>>8), byte(x)
		}
		switch rng.IntN(5) { // force the two embedded-IPv4 shapes often
		case 0:
			copy(i6[:12], make([]byte, 12))
		case 1:
			copy(i6[:10], make([]byte, 10))
			i6[10], i6[11] = 0xff, 0xff
		}
		rows[i] = row{u, i4, i6}
	}
	for start := 0; start < n; start += 500 {
		var vals []string
		for i := start; i < start+500 && i < n; i++ {
			r := rows[i]
			vals = append(vals, fmt.Sprintf("(%d, X'%x', X'%x', X'%x')", i, r.u, r.i4, r.i6))
		}
		testutil.MustExec(t, db, "INSERT INTO f VALUES "+strings.Join(vals, ","))
	}

	res, err := db.Query("SELECT id, HEX(u), HEX(i4), HEX(i6), CAST(u AS CHAR), CAST(i4 AS CHAR), CAST(i6 AS CHAR) FROM f ORDER BY id")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer res.Close()
	seen := 0
	for res.Next() {
		var id int
		var hu, h4, h6, tu, t4, t6 string
		if err := res.Scan(&id, &hu, &h4, &h6, &tu, &t4, &t6); err != nil {
			t.Fatalf("scan: %v", err)
		}
		seen++
		for _, c := range []struct{ dt, hexv, text string }{{"uuid", hu, tu}, {"inet4", h4, t4}, {"inet6", h6, t6}} {
			b, _ := hex.DecodeString(c.hexv)
			got, ok := FormatMariaDBFixed(c.dt, b)
			if !ok || got != c.text {
				t.Errorf("%s %s: Go renders %q, MariaDB %q", c.dt, c.hexv, got, c.text)
			}
			back, err := ParseMariaDBFixed(c.dt, c.text)
			if err != nil || !strings.EqualFold(hex.EncodeToString(back), c.hexv) {
				t.Errorf("%s %q: parsed to %x (%v), MariaDB stores %s", c.dt, c.text, back, err, c.hexv)
			}
		}
	}
	if err := res.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if seen != n {
		t.Fatalf("read back %d rows, want %d", seen, n)
	}
}
