package console

import (
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// The Access profiles page is advertised only where the build turns it on:
// off in the stock binary, on after ext.SetAccessProfilesVisible(true).
func TestCapabilitiesAccessProfilesFollowsExt(t *testing.T) {
	t.Cleanup(func() { ext.SetAccessProfilesVisible(false) })
	srv := newSettingsServer(t, nil, nil)
	if v, ok := capsFor(t, srv, "static-tok")["access_profiles"].(bool); !ok || v {
		t.Errorf("stock build: access_profiles = %v (present %v), want false", v, ok)
	}
	ext.SetAccessProfilesVisible(true)
	if v, _ := capsFor(t, srv, "static-tok")["access_profiles"].(bool); !v {
		t.Error("after SetAccessProfilesVisible(true): access_profiles = false, want true")
	}
}
