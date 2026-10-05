package serverid

import "testing"

const testSource = "user:pass@tcp(source.example.com:3306)/mydb"

// TestDeriveServerID_SourceOnlyValueIsUnchanged pins the id an installation
// used before the index was part of it. It is still what capture falls back
// to, and the number an operator may have seen in a source's replica list.
func TestDeriveServerID_SourceOnlyValueIsUnchanged(t *testing.T) {
	got, err := DeriveServerID(testSource)
	if err != nil || got != 3762492331 {
		t.Fatalf("DeriveServerID = %d, %v; want 3762492331", got, err)
	}
}

// TestDeriveServerIDSalted: an empty salt is the source-only id; a salt moves
// it, and two salts move it to two places.
func TestDeriveServerIDSalted(t *testing.T) {
	plain, _ := DeriveServerID(testSource)
	if got, err := DeriveServerIDSalted(testSource, ""); err != nil || got != plain {
		t.Fatalf("empty salt = %d, %v; want %d", got, err, plain)
	}
	a, _ := DeriveServerIDSalted(testSource, "a")
	b, _ := DeriveServerIDSalted(testSource, "b")
	if a == plain || b == plain || a == b || a < 100000000 || b < 100000000 {
		t.Fatalf("salted ids %d and %d against %d", a, b, plain)
	}
	if _, err := DeriveServerIDSalted("not-a-dsn", "a"); err == nil {
		t.Fatal("an unparseable DSN derived an id")
	}
}
