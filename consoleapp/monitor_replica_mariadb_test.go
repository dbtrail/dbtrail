package consoleapp

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
)

// Replica / duplicate detection for a MariaDB source. MariaDB has no
// @@server_uuid, and its GTIDs name a server by @@server_id, which is 1 on
// every server nobody configured. So a GTID match never counts here. What
// counts is what the servers say about each other: a replication channel whose
// master is the other server, by address AND server id, or the two
// connections landing on one running server.

// mdb builds a MariaDB server as the check reads it.
func mdb(name, dsnHost string, dsnPort int, hostname string, port int, serverID uint32, startedAt int64, node string, channels ...replicationChannel) mariadbServer {
	return mariadbServer{
		name: name, dsnHost: dsnHost, dsnPort: dsnPort, flavor: console.FlavorMariaDB,
		hostname: hostname, port: port, serverID: serverID, startedAt: startedAt, uuidNode: node,
		channels: channels, channelsRead: true,
	}
}

func TestEvaluateMariaDBReplicaOverlap(t *testing.T) {
	const t0 = int64(1_790_000_000)
	cand := func(channels ...replicationChannel) mariadbServer {
		return mdb("", "db-replica.internal", 3306, "db-replica", 3306, 1, t0, "0242ac110002", channels...)
	}
	primary := mdb("primary", "db-primary.internal", 3306, "db-primary", 3306, 7, t0-500, "0242ac110003")

	cases := []struct {
		name       string
		cand       mariadbServer
		peers      []mariadbServer
		wantStatus string
		wantDetail []string
		notDetail  []string
	}{
		{
			// The case a GTID-based rule gets wrong: two servers nobody
			// configured, both server_id 1, both writing domain 0.
			name:       "two unrelated servers with the default server id",
			cand:       mdb("", "10.0.0.5", 3306, "a1b2c3", 3306, 1, t0, "0242ac110002"),
			peers:      []mariadbServer{mdb("other", "10.0.0.6", 3306, "d4e5f6", 3306, 1, t0, "0242ac110009")},
			wantStatus: "pass",
			wantDetail: []string{"no replica relationship detected among 1 monitored source"},
		},
		{
			name:       "replicates from a monitored server, by its address",
			cand:       cand(replicationChannel{host: "db-primary.internal", port: 3306, serverID: 7}),
			peers:      []mariadbServer{primary},
			wantStatus: "warn",
			wantDetail: []string{`appears to be a replica of already-monitored "primary"`},
		},
		{
			name:       "replicates from a monitored server, by its hostname, in another case",
			cand:       cand(replicationChannel{host: "DB-PRIMARY", port: 3306, serverID: 7}),
			peers:      []mariadbServer{primary},
			wantStatus: "warn",
			wantDetail: []string{`appears to be a replica of already-monitored "primary"`},
		},
		{
			name:       "one channel of a multi-source replica is a monitored server",
			cand:       cand(replicationChannel{host: "elsewhere", port: 3306, serverID: 9}, replicationChannel{host: "db-primary.internal", port: 3306, serverID: 7}),
			peers:      []mariadbServer{primary},
			wantStatus: "warn",
			wantDetail: []string{`replica of already-monitored "primary"`},
		},
		{
			name:       "same address, another server id: another server",
			cand:       cand(replicationChannel{host: "db-primary.internal", port: 3306, serverID: 8}),
			peers:      []mariadbServer{primary},
			wantStatus: "pass",
		},
		{
			name:       "same server id, another address",
			cand:       cand(replicationChannel{host: "db-other.internal", port: 3306, serverID: 7}),
			peers:      []mariadbServer{primary},
			wantStatus: "pass",
		},
		{
			name:       "same host, another port",
			cand:       cand(replicationChannel{host: "db-primary.internal", port: 3307, serverID: 7}),
			peers:      []mariadbServer{primary},
			wantStatus: "pass",
		},
		{
			// The hostname is compared with the server's own port, the DSN
			// host with the DSN port: a port published by Docker differs.
			name:       "hostname with the DSN's port is not a match",
			cand:       cand(replicationChannel{host: "db-primary", port: 13307, serverID: 7}),
			peers:      []mariadbServer{mdb("primary", "127.0.0.1", 13307, "db-primary", 3306, 7, t0, "0242ac110003")},
			wantStatus: "pass",
		},
		{
			// A loopback master is the replica's own machine, which is not
			// where this process's 127.0.0.1 points.
			name:       "loopback master never matches",
			cand:       cand(replicationChannel{host: "127.0.0.1", port: 3306, serverID: 7}),
			peers:      []mariadbServer{mdb("primary", "127.0.0.1", 3306, "localhost", 3306, 7, t0, "0242ac110003")},
			wantStatus: "pass",
		},
		{
			name:       "localhost master never matches",
			cand:       cand(replicationChannel{host: "LocalHost", port: 3306, serverID: 7}),
			peers:      []mariadbServer{mdb("primary", "localhost", 3306, "localhost", 3306, 7, t0, "0242ac110003")},
			wantStatus: "pass",
		},
		{
			name:       "::1 master never matches",
			cand:       cand(replicationChannel{host: "::1", port: 3306, serverID: 7}),
			peers:      []mariadbServer{mdb("primary", "::1", 3306, "h", 3306, 7, t0, "0242ac110003")},
			wantStatus: "pass",
		},
		{
			// Master_Server_Id is 0 until the channel has connected once.
			name:       "a channel that never connected",
			cand:       cand(replicationChannel{host: "db-primary.internal", port: 3306, serverID: 0}),
			peers:      []mariadbServer{mdb("primary", "db-primary.internal", 3306, "db-primary", 3306, 0, t0, "0242ac110003")},
			wantStatus: "pass",
		},
		{
			name: "a monitored server replicates from this one",
			cand: cand(),
			peers: []mariadbServer{mdb("replica", "db-r2.internal", 3306, "db-r2", 3306, 3, t0, "0242ac110004",
				replicationChannel{host: "db-replica.internal", port: 3306, serverID: 1})},
			wantStatus: "warn",
			wantDetail: []string{`appears to be the primary of already-monitored replica "replica"`},
		},
		{
			name:       "the same address twice",
			cand:       mdb("", "DB-Replica.Internal", 3306, "x", 3306, 1, t0, "aa"),
			peers:      []mariadbServer{mdb("again", "db-replica.internal", 3306, "y", 3306, 2, t0+900, "bb")},
			wantStatus: "warn",
			wantDetail: []string{`is the same server as already-monitored "again"`},
		},
		{
			name:       "one server reached at two addresses",
			cand:       mdb("", "10.0.0.5", 3306, "db-replica", 3306, 1, t0, "0242ac110002"),
			peers:      []mariadbServer{mdb("again", "db-replica.internal", 3306, "DB-REPLICA", 3306, 1, t0+2, "0242ac110002")},
			wantStatus: "warn",
			wantDetail: []string{`is the same server as already-monitored "again"`},
		},
		{
			name:       "same hostname, port and server id, started at another time",
			cand:       mdb("", "10.0.0.5", 3306, "mariadb-0", 3306, 1, t0, "0242ac110002"),
			peers:      []mariadbServer{mdb("twin", "10.1.0.5", 3306, "mariadb-0", 3306, 1, t0+100, "0242ac110002")},
			wantStatus: "pass",
		},
		{
			// Two pods named alike in two namespaces, started together.
			name:       "same hostname, port, server id and start, another machine",
			cand:       mdb("", "10.0.0.5", 3306, "mariadb-0", 3306, 1, t0, "0242ac110002"),
			peers:      []mariadbServer{mdb("twin", "10.1.0.5", 3306, "mariadb-0", 3306, 1, t0, "0242ac110077")},
			wantStatus: "pass",
		},
		{
			name:       "no start time read is never the same server",
			cand:       mdb("", "10.0.0.5", 3306, "h", 3306, 1, 0, "0242ac110002"),
			peers:      []mariadbServer{mdb("twin", "10.1.0.5", 3306, "h", 3306, 1, 0, "0242ac110002")},
			wantStatus: "pass",
		},
		{
			name:       "no uuid node read is never the same server",
			cand:       mdb("", "10.0.0.5", 3306, "h", 3306, 1, t0, ""),
			peers:      []mariadbServer{mdb("twin", "10.1.0.5", 3306, "h", 3306, 1, t0, "")},
			wantStatus: "pass",
		},
		{
			name: "this server's replication status could not be read",
			cand: func() mariadbServer {
				c := cand()
				c.channelsRead, c.channelsErr = false, "Error 1227 (42000): Access denied; you need (at least one of) the SUPER, SLAVE MONITOR privilege(s) for this operation"
				return c
			}(),
			peers:      []mariadbServer{primary},
			wantStatus: "skip",
			wantDetail: []string{"SLAVE MONITOR", "whether this server replicates from a monitored server is unknown"},
		},
		{
			name: "unreadable status still reports what was found",
			cand: func() mariadbServer {
				c := mdb("", "db-primary.internal", 3306, "x", 3306, 1, t0, "aa")
				c.channelsRead, c.channelsErr = false, "denied"
				return c
			}(),
			peers:      []mariadbServer{primary},
			wantStatus: "warn",
			wantDetail: []string{`is the same server as already-monitored "primary"`},
		},
		{
			name:       "a monitored server that did not answer",
			cand:       cand(),
			peers:      []mariadbServer{primary, {name: "down", unreachable: true}},
			wantStatus: "pass",
			wantDetail: []string{"among 2 monitored source(s)", "1 could not be verified"},
		},
		{
			name: "a monitored MariaDB whose replication status could not be read",
			cand: cand(),
			peers: []mariadbServer{func() mariadbServer {
				p := primary
				p.channelsRead, p.channelsErr = false, "denied"
				return p
			}()},
			wantStatus: "pass",
			wantDetail: []string{"1 could not be verified"},
		},
		{
			// A MySQL peer: its replication status is not read (MariaDB's
			// SHOW ALL SLAVES STATUS), and it is not counted as unverified
			// for that. This server replicating from it is still found.
			name: "a monitored MySQL server this one replicates from",
			cand: cand(replicationChannel{host: "mysql-primary", port: 3306, serverID: 11}),
			peers: []mariadbServer{{name: "mysql", dsnHost: "mysql-primary", dsnPort: 3306, flavor: console.FlavorMySQL,
				hostname: "mysql-primary", port: 3306, serverID: 11, startedAt: t0, uuidNode: "0242ac110002"}},
			wantStatus: "warn",
			wantDetail: []string{`replica of already-monitored "mysql"`},
			notDetail:  []string{"same server"},
		},
		{
			name: "a monitored MySQL server is never the same server as a MariaDB one by its live identity",
			cand: mdb("", "10.0.0.5", 3306, "h", 3306, 1, t0, "0242ac110002"),
			peers: []mariadbServer{{name: "mysql", dsnHost: "10.0.0.6", dsnPort: 3306, flavor: console.FlavorMySQL,
				hostname: "h", port: 3306, serverID: 1, startedAt: t0, uuidNode: "0242ac110002", channelsRead: true}},
			wantStatus: "pass",
			notDetail:  []string{"could not be verified"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := evaluateMariaDBReplicaOverlap(tc.cand, tc.peers)
			if got == nil {
				t.Fatal("no card")
			}
			if got.Name != replicaCheckName {
				t.Errorf("name = %q", got.Name)
			}
			if got.Status != tc.wantStatus {
				t.Errorf("status = %q (%s), want %q", got.Status, got.Detail, tc.wantStatus)
			}
			for _, w := range tc.wantDetail {
				if !strings.Contains(got.Detail, w) {
					t.Errorf("detail %q does not contain %q", got.Detail, w)
				}
			}
			for _, w := range tc.notDetail {
				if strings.Contains(got.Detail, w) {
					t.Errorf("detail %q contains %q", got.Detail, w)
				}
			}
			if strings.ContainsRune(got.Detail+got.Remediation, '—') {
				t.Errorf("em dash in operator text: %s", got.Detail)
			}
			t.Logf("%s: %s", got.Status, got.Detail)
		})
	}
}

func TestSourceAddress(t *testing.T) {
	cases := []struct {
		dsn      string
		wantHost string
		wantPort int
	}{
		{"u:p@tcp(db.internal:3307)/", "db.internal", 3307},
		{"u:p@tcp(db.internal)/", "db.internal", 3306},
		{"u:p@tcp([::1]:3306)/", "::1", 3306},
		{"u:p@unix(/tmp/mysql.sock)/", "", 0},
		{"not a dsn", "", 0},
		{"", "", 0},
	}
	for _, tc := range cases {
		h, p := sourceAddress(tc.dsn)
		if h != tc.wantHost || p != tc.wantPort {
			t.Errorf("%q: got %q %d, want %q %d", tc.dsn, h, p, tc.wantHost, tc.wantPort)
		}
	}
}

func TestUUIDNode(t *testing.T) {
	cases := map[string]string{
		"6ccd780c-baba-1026-9564-5b8c656024db": "5b8c656024db",
		"6CCD780C-BABA-1026-9564-5B8C656024DB": "5b8c656024db",
		"":                                     "",
		"not-a-uuid":                           "",
		"6ccd780c-baba-4026-9564-5b8c656024db": "", // version 4: random, names no machine
	}
	for in, want := range cases {
		if got := uuidNode(in); got != want {
			t.Errorf("uuidNode(%q) = %q, want %q", in, got, want)
		}
	}
}

// The reads, over sqlmock: identity, start time, and the channels read by
// column name.
func TestLoadMariaDBServer(t *testing.T) {
	identity := func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT @@hostname, @@port, @@server_id, UUID(), UNIX_TIMESTAMP()")).
			WillReturnRows(sqlmock.NewRows([]string{"h", "p", "s", "u", "n"}).
				AddRow("db1", 3306, 7, "6ccd780c-baba-1026-9564-5b8c656024db", 1_790_000_000))
		mock.ExpectQuery(regexp.QuoteMeta("SHOW GLOBAL STATUS LIKE 'Uptime'")).
			WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("Uptime", "100"))
	}
	t.Run("channels read by name", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		identity(mock)
		mock.ExpectQuery(regexp.QuoteMeta("SHOW ALL SLAVES STATUS")).
			WillReturnRows(sqlmock.NewRows([]string{"Connection_name", "Slave_IO_State", "Master_Host", "Master_User", "Master_Port", "Master_Server_Id"}).
				AddRow("", "", " db-primary ", "rep", "3306", "9").
				AddRow("b", "", "db-b", "rep", "3307", "0"))
		s, err := loadMariaDBServer(context.Background(), db, console.FlavorMariaDB)
		if err != nil {
			t.Fatal(err)
		}
		if s.hostname != "db1" || s.port != 3306 || s.serverID != 7 || s.uuidNode != "5b8c656024db" || s.startedAt != 1_789_999_900 {
			t.Errorf("identity = %+v", s)
		}
		want := []replicationChannel{{host: "db-primary", port: 3306, serverID: 9}, {host: "db-b", port: 3307, serverID: 0}}
		if !s.channelsRead || len(s.channels) != 2 || s.channels[0] != want[0] || s.channels[1] != want[1] {
			t.Errorf("channels = %+v (read %v), want %+v", s.channels, s.channelsRead, want)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("no channels is read, and empty", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		identity(mock)
		mock.ExpectQuery(regexp.QuoteMeta("SHOW ALL SLAVES STATUS")).
			WillReturnRows(sqlmock.NewRows([]string{"Master_Host", "Master_Port", "Master_Server_Id"}))
		s, err := loadMariaDBServer(context.Background(), db, console.FlavorMariaDB)
		if err != nil || !s.channelsRead || len(s.channels) != 0 {
			t.Errorf("s = %+v, err %v", s, err)
		}
	})
	t.Run("denied is recorded, not returned", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		identity(mock)
		mock.ExpectQuery(regexp.QuoteMeta("SHOW ALL SLAVES STATUS")).
			WillReturnError(&mysql.MySQLError{Number: 1227, Message: "Access denied; you need (at least one of) the SUPER, SLAVE MONITOR privilege(s) for this operation"})
		s, err := loadMariaDBServer(context.Background(), db, console.FlavorMariaDB)
		if err != nil || s.channelsRead || !strings.Contains(s.channelsErr, "SLAVE MONITOR") {
			t.Errorf("s = %+v, err %v", s, err)
		}
	})
	t.Run("a missing column is not a clean read", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		identity(mock)
		mock.ExpectQuery(regexp.QuoteMeta("SHOW ALL SLAVES STATUS")).
			WillReturnRows(sqlmock.NewRows([]string{"Master_Host", "Master_Port"}).AddRow("h", "3306"))
		s, _ := loadMariaDBServer(context.Background(), db, console.FlavorMariaDB)
		if s.channelsRead || s.channelsErr == "" {
			t.Errorf("s = %+v, want an unread status", s)
		}
	})
	t.Run("a port that is not a number is not a clean read", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		identity(mock)
		mock.ExpectQuery(regexp.QuoteMeta("SHOW ALL SLAVES STATUS")).
			WillReturnRows(sqlmock.NewRows([]string{"Master_Host", "Master_Port", "Master_Server_Id"}).AddRow("h", "x", "1"))
		s, _ := loadMariaDBServer(context.Background(), db, console.FlavorMariaDB)
		if s.channelsRead || s.channelsErr == "" {
			t.Errorf("s = %+v, want an unread status", s)
		}
	})
	t.Run("an unreadable uptime leaves the start unknown", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT @@hostname")).
			WillReturnRows(sqlmock.NewRows([]string{"h", "p", "s", "u", "n"}).AddRow("db1", 3306, 7, "x", 1_790_000_000))
		mock.ExpectQuery(regexp.QuoteMeta("SHOW GLOBAL STATUS")).WillReturnError(fmt.Errorf("boom"))
		s, err := loadMariaDBServer(context.Background(), db, console.FlavorMySQL)
		if err != nil || s.startedAt != 0 || s.uuidNode != "" {
			t.Errorf("s = %+v, err %v", s, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("a MySQL server's channels must not be read: %v", err)
		}
	})
	t.Run("identity read fails", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT @@hostname")).WillReturnError(fmt.Errorf("gone"))
		if _, err := loadMariaDBServer(context.Background(), db, console.FlavorMariaDB); err == nil {
			t.Error("want the error")
		}
	})
}
