package console

import (
	"strings"
	"testing"
)

// TestBuildPGRouteDSN is the forwarding account of a PostgreSQL server
// (#2771): set over the source's URL or as a URL of its own, never the
// capture account, and following the source when it moves.
func TestBuildPGRouteDSN(t *testing.T) {
	const src = "postgres://repl:cap@db.example.com:5432/shop?sslmode=require"
	cases := []struct {
		name                   string
		req                    serverRequest
		stored, oldSrc, newSrc string
		want, wantErr          string
	}{
		{name: "user and password over the source", req: serverRequest{RouteUser: ptr("app_ro"), RoutePassword: ptr("pw")},
			newSrc: src, want: "postgres://app_ro:pw@db.example.com:5432/shop?sslmode=require"},
		{name: "the capture user is refused", req: serverRequest{RouteUser: ptr("repl"), RoutePassword: ptr("x")},
			newSrc: src, wantErr: "captures with"},
		{name: "a new user needs a password", req: serverRequest{RouteUser: ptr("app_ro")},
			newSrc: src, wantErr: "route_password is required"},
		{name: "a password alone needs a user", req: serverRequest{RoutePassword: ptr("pw")},
			newSrc: src, wantErr: "route_user is required"},
		{name: "a password alone replaces the stored one", req: serverRequest{RoutePassword: ptr("new")},
			stored: "postgres://app_ro:old@db.example.com:5432/shop?sslmode=require", oldSrc: src, newSrc: src,
			want: "postgres://app_ro:new@db.example.com:5432/shop?sslmode=require"},
		{name: "the stored user sent again is a keep", req: serverRequest{RouteUser: ptr("app_ro")},
			stored: "postgres://app_ro:old@db.example.com:5432/shop", oldSrc: src, newSrc: src,
			want: "postgres://app_ro:old@db.example.com:5432/shop"},
		{name: "nothing sent keeps", stored: "postgres://app_ro:old@db.example.com:5432/shop", oldSrc: src, newSrc: src,
			want: "postgres://app_ro:old@db.example.com:5432/shop"},
		{name: "the account follows the source to a new host",
			stored: "postgres://app_ro:old@db.example.com:5432/shop?sslmode=require", oldSrc: src,
			newSrc: "postgres://repl:cap@db2.example.com:5433/shop?sslmode=verify-full",
			want:   "postgres://app_ro:old@db2.example.com:5433/shop?sslmode=verify-full"},
		{name: "an account on another host stays there",
			stored: "postgres://app_ro:old@replica:5432/shop", oldSrc: src,
			newSrc: "postgres://repl:cap@db2.example.com:5432/shop", want: "postgres://app_ro:old@replica:5432/shop"},
		{name: "the port left out is 5432, the host's case and trailing dot do not count",
			req: serverRequest{RouteDSN: ptr("postgres://repl:x@db.example.com:5432/other")}, newSrc: "postgres://repl:cap@DB.Example.com./shop",
			wantErr: "captures with"},
		{name: "localhost and 127.0.0.1 are one host",
			req: serverRequest{RouteDSN: ptr("postgres://repl:x@localhost:5432/shop")}, newSrc: "postgres://repl:cap@127.0.0.1/shop",
			wantErr: "captures with"},
		{name: "a user with characters a URL escapes", req: serverRequest{RouteUser: ptr("a@b:c"), RoutePassword: ptr("p@ss/w:rd")},
			newSrc: src, want: "postgres://a%40b%3Ac:p%40ss%2Fw%3Ard@db.example.com:5432/shop?sslmode=require"},
		{name: "a raw URL of its own", req: serverRequest{RouteDSN: ptr("postgres://app_ro:pw@replica:5432/shop")},
			newSrc: src, want: "postgres://app_ro:pw@replica:5432/shop"},
		{name: "a raw URL that is the capture account", req: serverRequest{RouteDSN: ptr("postgres://repl:other@db.example.com/shop")},
			newSrc: src, wantErr: "captures with"},
		{name: "a raw URL without a database", req: serverRequest{RouteDSN: ptr("postgres://app_ro:pw@replica:5432")},
			newSrc: src, wantErr: "must name the database"},
		{name: "a raw URL without a user", req: serverRequest{RouteDSN: ptr("postgres://replica:5432/shop")},
			newSrc: src, wantErr: "names no user"},
		{name: "a raw libpq string is not a URL", req: serverRequest{RouteDSN: ptr("host=replica user=a dbname=shop")},
			newSrc: src, wantErr: "postgres:// URL"},
		{name: "a raw URL to a socket directory", req: serverRequest{RouteDSN: ptr("postgres://app_ro@/shop?host=/var/run/postgresql")},
			newSrc: src, wantErr: "over the network"},
		{name: "a raw URL for replication", req: serverRequest{RouteDSN: ptr("postgres://app_ro:pw@replica/shop?replication=database")},
			newSrc: src, wantErr: "replication"},
		{name: "a raw URL that names its user as a parameter", req: serverRequest{RouteDSN: ptr("postgres://fwd:x@db.example.com/shop?user=repl&password=cap")},
			newSrc: src, wantErr: "must not set user"},
		{name: "a raw URL that moves its port as a parameter", req: serverRequest{RouteDSN: ptr("postgres://fwd:x@replica/shop?port=6432")},
			newSrc: src, wantErr: "must not set port"},
		{name: "a raw URL with options", req: serverRequest{RouteDSN: ptr("postgres://fwd:x@replica/shop?options=-c%20default_transaction_read_only%3Doff")},
			newSrc: src, wantErr: "must not set options"},
		{name: "a source whose URL names its user as a parameter", req: serverRequest{RouteUser: ptr("app_ro"), RoutePassword: ptr("pw")},
			newSrc: "postgres://x@db.example.com/shop?user=repl", wantErr: "sets user as a parameter"},
		{name: "a raw URL with the structured fields", req: serverRequest{RouteDSN: ptr("postgres://a:b@h/d"), RouteUser: ptr("x")},
			newSrc: src, wantErr: "not both"},
		{name: "no source, asked", req: serverRequest{RouteUser: ptr("app_ro"), RoutePassword: ptr("pw")}, wantErr: "needs a source"},
		{name: "no source, not asked", want: ""},
		{name: "an empty user removes", req: serverRequest{RouteUser: ptr("")}, stored: "postgres://app_ro:pw@db.example.com/shop",
			oldSrc: src, newSrc: src, want: ""},
		{name: "a stored value that does not read is kept", stored: "garbage", oldSrc: src, newSrc: src, want: "garbage"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := buildRouteDSN(c.req, c.stored, c.oldSrc, c.newSrc, FlavorPostgres)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want one with %q (got %q)", err, c.wantErr, got)
				}
				if strings.Contains(err.Error(), "pw") || strings.Contains(err.Error(), "cap@") {
					t.Errorf("the error carries a password: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

// TestPGRouteAccountView: what the API shows of a PostgreSQL server's
// forwarding account, and what the PostgreSQL port is handed.
func TestPGRouteAccountView(t *testing.T) {
	const src = "postgres://repl:cap@db:5432/shop"
	for _, c := range []struct {
		name, route string
		want        serverDTO
	}{
		{"none", "", serverDTO{}},
		{"its own", "postgres://app_ro:pw@db/shop", serverDTO{HasRoute: true, RouteUser: "app_ro", HasRoutePassword: true}},
		{"elsewhere", "postgres://app_ro@replica:6432/shop", serverDTO{HasRoute: true, RouteUser: "app_ro", RouteHost: "replica", RoutePort: "6432"}},
		{"the capture account", "postgres://repl:x@db/shop", serverDTO{RouteIsCapture: true, RouteUser: "repl"}},
		{"unreadable", "u:p@tcp(db:3306)/", serverDTO{RouteUnreadable: true}},
		{"its user in a parameter", "postgres://app_ro:pw@db/shop?user=repl", serverDTO{RouteUnreadable: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			var dto serverDTO
			fillPGRouteDSNParts(&dto, c.route, src)
			if dto.HasRoute != c.want.HasRoute || dto.RouteUser != c.want.RouteUser || dto.HasRoutePassword != c.want.HasRoutePassword ||
				dto.RouteHost != c.want.RouteHost || dto.RoutePort != c.want.RoutePort || dto.RouteIsCapture != c.want.RouteIsCapture ||
				dto.RouteUnreadable != c.want.RouteUnreadable {
				t.Errorf("got %+v, want %+v", dto, c.want)
			}
		})
	}
}

// TestPostgresForwardAccount: the PostgreSQL port gets a server's own
// forwarding account and nothing else, and the MySQL port's view of a
// PostgreSQL server is unchanged: it still refuses it (#2317, #2771).
func TestPostgresForwardAccount(t *testing.T) {
	s, reg := newFlashbackServer(t, "tok")
	if s.routed == nil {
		s.routed = newRoutedConns()
	}
	pg, err := reg.Add(ServerEntry{Name: "pg", DSN: "u:p@tcp(h:3306)/idx", Flavor: FlavorPostgres,
		SourceDSN: "postgres://repl:cap@db/shop", SourceSlot: "s", SourcePublication: "p", RouteDSN: "postgres://app_ro:pw@db/shop"})
	if err != nil {
		t.Fatal(err)
	}
	dsn, _, refused := s.PostgresForwardAccount(pg.ID)
	if dsn != "postgres://app_ro:pw@db/shop" || refused != "" {
		t.Errorf("PostgresForwardAccount = %q, %q", dsn, refused)
	}
	if mysqlDSN, why := s.flashbackForwardDSN(pg.ID); mysqlDSN != "" || why != RouteRefusedNotMySQL {
		t.Errorf("the MySQL port's flashbackForwardDSN = %q, %q; want it to refuse a PostgreSQL source as before", mysqlDSN, why)
	}
	capture, err := reg.Add(ServerEntry{Name: "pg2", DSN: "u:p@tcp(h:3306)/idx2", Flavor: FlavorPostgres,
		SourceDSN: "postgres://repl:cap@db/shop", SourceSlot: "s", SourcePublication: "p", RouteDSN: "postgres://repl:cap@db/shop"})
	if err != nil {
		t.Fatal(err)
	}
	if dsn, _, refused := s.PostgresForwardAccount(capture.ID); dsn != "" || refused != PGRouteRefusedIsCapture {
		t.Errorf("the capture account: %q, %q", dsn, refused)
	}
	none, err := reg.Add(ServerEntry{Name: "pg3", DSN: "u:p@tcp(h:3306)/idx3", Flavor: FlavorPostgres,
		SourceDSN: "postgres://repl:cap@db/shop", SourceSlot: "s", SourcePublication: "p"})
	if err != nil {
		t.Fatal(err)
	}
	if dsn, _, refused := s.PostgresForwardAccount(none.ID); dsn != "" || refused != PGRouteRefusedNoAccount {
		t.Errorf("no account: %q, %q", dsn, refused)
	}
	my := addByHand(t, reg, ServerEntry{Name: "my", DSN: "u:p@tcp(h:3306)/idx4", SourceDSN: "repl:cap@tcp(db:3306)/", RouteDSN: "app:pw@tcp(db:3306)/"})
	if dsn, _, refused := s.PostgresForwardAccount(my.ID); dsn != "" || refused != "" {
		t.Errorf("a MySQL server: %q, %q; the PostgreSQL port has nothing of it", dsn, refused)
	}
}

// TestMySQLRouteDSNUnchanged pins the MySQL branch: the same request builds
// the same DSN it built before the PostgreSQL one existed.
func TestMySQLRouteDSNUnchanged(t *testing.T) {
	got, err := buildRouteDSN(serverRequest{RouteUser: ptr("app_ro"), RoutePassword: ptr("pw")}, "", "", "repl:cap@tcp(db:3306)/?tls=true", FlavorMySQL)
	if err != nil || got != "app_ro:pw@tcp(db:3306)/?tls=true" {
		t.Errorf("MySQL forwarding DSN = %q, %v", got, err)
	}
	if _, err := buildRouteDSN(serverRequest{RouteUser: ptr("x"), RoutePassword: ptr("y")}, "", "", "postgres://repl:cap@db/shop", "oracle"); err == nil {
		t.Error("a flavor that is neither MySQL nor PostgreSQL took a forwarding account")
	}
}
