package views

import (
	"strings"
	"testing"
	"time"
)

// TestApplyFollow_aRootNamedLikeAGlobIsNotFollowed (#2246): a file that
// follows the newest snapshot finds it with a glob over the root, and the
// root's own text is a pattern there. Under a root named "da?a" the glob
// also matches the snapshots of a sibling "data", the newest of the two
// wins, and every view reads that one's files. So such a root is not
// followed: the file is pinned, where each path is named through a class.
func TestApplyFollow_aRootNamedLikeAGlobIsNotFollowed(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	for root, follows := range map[string]bool{
		"s3://bucket/baselines/":    true,
		"s3://bucket/da?a/":         false,
		"s3://bucket/st*r/":         false,
		"s3://bucket/[prod]/":       false,
		"s3://bucket/{a,b}/":        false,
		"s3://bucket/ok/da?a/deep/": false,
	} {
		in := Input{
			GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
			BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
			Baselines: []BaselineTable{{Schema: "shop", Table: "orders", Path: root + stamp + "/shop/orders.parquet", SchemaKnown: true}},
		}
		ApplyFollow(&in, root, false)
		if got := in.Follow == FollowNewest; got != follows {
			t.Errorf("root %s: follows the newest snapshot = %v, want %v", root, got, follows)
			continue
		}
		if follows {
			continue
		}
		// Pinned: no listing of the root, and the table's file named so
		// that the root's characters match themselves.
		sqlText := Generate(in)
		if strings.Contains(sqlText, "FROM glob(") {
			t.Errorf("root %s: the pinned file still lists the root", root)
		}
		if want := sqlString(globLiteral(root + stamp + "/shop/orders.parquet")); !strings.Contains(sqlText, want) {
			t.Errorf("root %s: the file does not read %s", root, want)
		}
	}
}
