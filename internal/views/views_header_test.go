package views

import (
	"strings"
	"testing"
)

const routingSentence = "listed by its S3 location"

// The S3-over-local sentence describes what registry discovery DID. Roots the
// operator named with --archive-dir/--archive-s3 are listed as named, both of
// them if both were passed, so the sentence would be false there (#1456).
func TestGenerate_routingSentenceOnlyForRegistrySources(t *testing.T) {
	in := goldenInput()
	in.PortableRouting = true
	if out := Generate(in); !strings.Contains(out, routingSentence) {
		t.Errorf("registry-discovered sources: header lacks the routing sentence:\n%s", out)
	}
	in.PortableRouting = false
	if out := Generate(in); strings.Contains(out, routingSentence) {
		t.Errorf("explicitly named sources: header states a routing that did not happen:\n%s", out)
	}
}

// A registry that could not be read is named as such; "none registered" would
// assert a cause the caller does not know.
func TestGenerate_discoveryErrorInHeader(t *testing.T) {
	in := goldenInput()
	in.ArchiveSources = nil
	in.ArchiveDiscoveryFailed = true
	out := Generate(in)
	// Flattened: the needle is a whole clause, and a clause long enough to
	// mean something straddles the wrap. Pinning the wrap is the failure the
	// time-zone guard was rewritten to avoid, and this one had it too.
	if !strings.Contains(prose(out), "(could not be read from archive_state; the log of the run that wrote this file has the error)") {
		t.Errorf("header does not name the read failure:\n%s", out)
	}
	// Header and body must agree: neither may claim an empty registry.
	for _, claim := range []string{"none registered in archive_state", "no archive sources are registered"} {
		if strings.Contains(out, claim) {
			t.Errorf("file claims an empty registry after a failed read (%q):\n%s", claim, out)
		}
	}
	if !strings.Contains(out, "-- (skipped: archive_state could not be read; see the header)") {
		t.Errorf("events body does not point at the header:\n%s", out)
	}
	// Nothing was listed, so there is no routing to describe.
	if strings.Contains(out, routingSentence) {
		t.Errorf("routing sentence emitted over an empty, failed listing:\n%s", out)
	}
}
