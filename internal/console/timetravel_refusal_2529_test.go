package console

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/hiderules"
)

func TestTimeTravelRefusal(t *testing.T) {
	f, reg := hiddenSQLFixture(t, nil)
	if why := f.s.timeTravelRefusal(f.s.cm.boot); why != "" {
		t.Fatalf("no rules: %q", why)
	}
	set, err := hiderules.Parse(hiderules.Rules{Columns: []string{"shop.orders.status"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetHidden(f.s.cm.boot.serverID, set); err != nil {
		t.Fatal(err)
	}
	if why := f.s.timeTravelRefusal(f.s.cm.boot); !strings.Contains(why, "_flashback") {
		t.Fatalf("rules: %q", why)
	}
	f2, _ := hiddenSQLFixture(t, &hiderules.Rules{Tables: []string{"orders"}})
	if why := f2.s.timeTravelRefusal(f2.s.cm.boot); !strings.Contains(why, "refused") {
		t.Fatalf("unreadable rules: %q", why)
	}
}
