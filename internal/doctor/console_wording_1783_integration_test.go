//go:build integration

package doctor

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIndexNotCreatedYetWording (#1783): an index database that does not exist
// yet is a PASS whose detail says who creates it. The path needs a live
// server answering 1049, which sqlmock cannot give through a DSN.
func TestIndexNotCreatedYetWording(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	const name = "bintrail_doctor_1783_absent"
	c := checkIndexConnection(t.Context(), testutil.BaseDSN()+"/"+name, name)
	if c.Status != StatusPass || !strings.Contains(c.Detail, "does not exist yet") {
		t.Fatalf("index connection = %s: %s", c.Status, c.Detail)
	}
	if !strings.Contains(c.Detail, "the console creates it when capture starts") {
		t.Errorf("detail does not say the console creates it: %s", c.Detail)
	}
	assertConsoleWording(t, "index not created yet", c)
}
