package baseline

import "testing"

// A range name under an S3 prefix joins with "/" like any pair, and the S3
// footer reader fills the low end (#1723).
func TestMarkTableDeltaFiles_s3RangeNames(t *testing.T) {
	names := []string{"orders.000000.posdel", "orders.000000.upserts", "orders.000001-000006.posdel", "orders.000001-000006.upserts",
		"orders.000007.posdel", "orders.000007.upserts"}
	chains, err := MarkTableDeltaFiles("s3://b/snap/shop/", names)
	if err != nil {
		t.Fatal(err)
	}
	c := chains["s3://b/snap/shop/orders.parquet"]
	if c == nil || len(c.Files) != 3 || c.Files[1].SeqLo != 1 || c.Files[1].Seq != 6 ||
		c.Files[1].Posdel != "s3://b/snap/shop/orders.000001-000006.posdel" || c.Files[1].Upserts != "s3://b/snap/shop/orders.000001-000006.upserts" {
		t.Fatalf("chains = %+v", chains)
	}
}

func TestApplyS3FooterKV_deltaSeqLo(t *testing.T) {
	m := DumpMetadata{DeltaSeqLo: -1}
	if applyS3FooterKV(&m, "s3://b/k.upserts", MetaKeyDeltaSeqLo, "3"); m.DeltaSeqLo != 3 {
		t.Fatalf("DeltaSeqLo = %d, want 3", m.DeltaSeqLo)
	}
	if applyS3FooterKV(&m, "s3://b/k.upserts", MetaKeyDeltaSeqLo, "x"); m.DeltaSeqLo != -1 {
		t.Fatalf("DeltaSeqLo = %d after an unreadable value, want -1 (set aside)", m.DeltaSeqLo)
	}
}
