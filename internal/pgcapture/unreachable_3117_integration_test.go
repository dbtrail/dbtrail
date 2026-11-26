//go:build integration

package pgcapture

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Refusals from a real PostgreSQL, which arrive wrapped in a ConnectError like
// a network failure does, must not be read as the source being unreachable:
// they do not heal by waiting.
func TestIntegrationRefusalsAreNotUnreachable_3117(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	base, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	build := func(user, database string) string {
		u := url.URL{Scheme: "postgres", User: url.UserPassword(user, base.Password),
			Host: net.JoinHostPort(base.Host, fmt.Sprint(base.Port)), Path: "/" + database, RawQuery: "sslmode=disable"}
		return u.String()
	}
	for name, d := range map[string]string{
		"role that does not exist":     build("no_such_role_3117", base.Database),
		"database that does not exist": build(base.User, "no_such_database_3117"),
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			c := New(Config{ReplDSN: d + "&replication=database", QueryDSN: d, SlotName: "s3117", Publication: "p3117"})
			err := c.Run(ctx, make(chan event.Event, 1))
			if err == nil {
				t.Fatal("Run connected")
			}
			if IsSourceUnreachable(err) {
				t.Fatalf("classified as unreachable: %v", err)
			}
		})
	}
	t.Run("publication that does not exist", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		c := New(Config{ReplDSN: dsn + sep + "replication=database", QueryDSN: dsn, SlotName: "s3117", Publication: "no_such_pub_3117"})
		err := c.Run(ctx, make(chan event.Event, 1))
		if err == nil || IsSourceUnreachable(err) {
			t.Fatalf("got %v; want a refusal that is not an outage", err)
		}
	})
}
