package console

import (
	"strings"
	"testing"
)

// A PostgreSQL server's tails take a higher count of keys by default, and
// only when their memory is bounded; what is set is what is used, and the
// other families keep their own default.
func TestPGTailBudgetFor(t *testing.T) {
	const host = 8 << 30
	for name, c := range map[string]struct {
		keys, bytes string
		host        uint64
		wantKeys    int
		wantBytes   int64
		warns       string
	}{
		"nothing set":                        {"", "", host, pgTailDefaultMaxKeys, host / tailBytesShare, ""},
		"only spaces":                        {"  ", "", host, pgTailDefaultMaxKeys, host / tailBytesShare, ""},
		"the keys set":                       {"1000", "", host, 1000, host / tailBytesShare, ""},
		"the keys set to the common default": {"500000", "", host, 500000, host / tailBytesShare, ""},
		"the keys set above PostgreSQL's":    {"3000000", "", host, 3000000, host / tailBytesShare, ""},
		"keys that are not a number":         {"abc", "", host, pgTailDefaultMaxKeys, host / tailBytesShare, "the default of 2000000 keys per table stands"},
		"keys of zero":                       {"0", "", host, pgTailDefaultMaxKeys, host / tailBytesShare, "the default of 2000000 keys per table stands"},
		"negative keys":                      {"-5", "", host, pgTailDefaultMaxKeys, host / tailBytesShare, "the default of 2000000 keys per table stands"},
		"the memory set":                     {"", "512MiB", host, pgTailDefaultMaxKeys, 512 << 20, ""},
		"no bound on the memory":             {"", "off", host, tailDefaultMaxKeys, 0, ""},
		"no bound on the memory, as zero":    {"", "0", host, tailDefaultMaxKeys, 0, ""},
		"memory not known":                   {"", "", 0, tailDefaultMaxKeys, 0, ""},
		"memory not known, keys not a number": {"abc", "", 0, tailDefaultMaxKeys, 0, "the default of 500000 keys per table stands"},
		"memory that is not a size":          {"", "lots", host, pgTailDefaultMaxKeys, host / tailBytesShare, "is not a size"},
	} {
		b, warnings := pgTailBudgetFor(c.keys, c.bytes, c.host)
		if b.Keys != c.wantKeys || b.Bytes != c.wantBytes {
			t.Errorf("%s: %d keys and %d bytes, want %d and %d", name, b.Keys, b.Bytes, c.wantKeys, c.wantBytes)
		}
		all := strings.Join(warnings, " | ")
		if (c.warns == "") != (len(warnings) == 0) || !strings.Contains(all, c.warns) {
			t.Errorf("%s: warnings %q, want %q", name, all, c.warns)
		}
		// The other families: as they were.
		if common, _ := tailBudgetFor(c.keys, c.bytes, c.host); common.Bytes != b.Bytes || (common.Keys != b.Keys && common.Keys != tailDefaultMaxKeys) {
			t.Errorf("%s: the common budget is %d keys and %d bytes", name, common.Keys, common.Bytes)
		}
	}
	if common, _ := tailBudgetFor("", "", host); common.Keys != tailDefaultMaxKeys {
		t.Fatalf("the common default is %d keys, want %d", common.Keys, tailDefaultMaxKeys)
	}
}
