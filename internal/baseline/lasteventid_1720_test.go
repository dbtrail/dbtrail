package baseline

import "testing"

// #1720: the footer key a fold stamps with the highest event_id it applied.
// Unreadable values read as 0 (no floor; the fetch keeps its time floor), never
// as an error — a footer a newer build wrote must not make the file unusable.
func TestParseLastEventID(t *testing.T) {
	cases := map[string]uint64{
		"0":                    0,
		"42":                   42,
		"18446744073709551615": 18446744073709551615,
		"18446744073709551616": 0, // overflows uint64
		"-1":                   0,
		"4.5":                  0,
		"":                     0,
		"abc":                  0,
		" 42":                  0,
	}
	for raw, want := range cases {
		if got := parseLastEventID("x.parquet", raw); got != want {
			t.Errorf("parseLastEventID(%q) = %d, want %d", raw, got, want)
		}
	}
}

// TestApplyS3FooterKV_readsTheDeltaAndFloorKeys: the S3 reader sees the
// footer as rows through DuckDB, so its key set is a separate switch from the
// local reader's lookups. Pinned here without S3: a key the local reader
// knows and this switch does not would make an S3-hosted chain lose its
// anchor, its sequence or its floor with nothing in the log.
func TestApplyS3FooterKV_readsTheDeltaAndFloorKeys(t *testing.T) {
	m := DumpMetadata{DeltaSeq: -1}
	for k, v := range map[string]string{
		MetaKeyLastEventID:     "4500",
		MetaKeyDeltaSeq:        "3",
		MetaKeyDeltaBaseSize:   "1024",
		MetaKeyDeltaBaseAnchor: "binlog.000009:3000",
		MetaKeyDeltaChainStart: "2026-05-01T09:00:00Z",
		MetaKeyBinlogFile:      "binlog.000009",
		MetaKeyBinlogPos:       "3000",
		MetaKeyRowCount:        "7",
		MetaKeyContentDigest:   "abc",
	} {
		if applyS3FooterKV(&m, "s3://b/k.parquet", k, v) {
			t.Fatalf("%s=%q reported a corrupt row count", k, v)
		}
	}
	if m.LastEventID != 4500 || m.DeltaSeq != 3 || m.DeltaBaseSize != 1024 || m.DeltaBaseAnchor != "binlog.000009:3000" ||
		m.DeltaChainStart.IsZero() || m.BinlogFile != "binlog.000009" || m.BinlogPos != 3000 || m.RowCount != 7 || m.ContentDigest != "abc" {
		t.Fatalf("applied footer = %+v", m)
	}
	if !applyS3FooterKV(&m, "s3://b/k.parquet", MetaKeyRowCount, "seven") {
		t.Fatal("a corrupt row count was not reported")
	}
	if applyS3FooterKV(&m, "s3://b/k.parquet", MetaKeyLastEventID, "x"); m.LastEventID != 0 {
		t.Fatalf("an unreadable stamp left %d, want 0 (no floor)", m.LastEventID)
	}
}
