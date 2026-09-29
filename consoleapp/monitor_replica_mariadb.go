package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// Replica / duplicate detection for a MariaDB source (#402's card, MariaDB
// half).
//
// The MySQL check reads GTID lineage keyed by @@server_uuid. MariaDB has no
// server_uuid, and a MariaDB GTID (domain-server-sequence) names its server by
// @@server_id, which is 1 on every server nobody configured. Two unrelated
// servers therefore share GTIDs like 0-1-500 all the time, and any rule built
// on GTIDs would call them a replica pair. So no GTID is compared here. A
// relationship is reported only on what the servers say about each other:
//
//   - the same server: the two entries reach the same host:port, or the two
//     connections land on one running server (same @@hostname, @@port,
//     @@server_id, start time within two seconds, and the machine part of a
//     version 1 UUID() it generates);
//   - a replica: one of its replication channels (SHOW ALL SLAVES STATUS)
//     connects to the other server, by host AND port AND Master_Server_Id.
//     The host is compared with the address the other entry is monitored at,
//     or with that server's own @@hostname (with its own @@port). A loopback
//     master is the replica's own machine, not this process's, and never
//     matches.
//
// Anything that could not be read is counted as not verified, never as "no
// relationship". Reading replication status needs SLAVE MONITOR (MariaDB
// 10.5.9 and later), which the documented capture grants do not include: when
// the source being added refuses it, the card is a skip that says so.

// mariadbPeerTimeout bounds the reads of one monitored server, inside
// replicaOverlapTimeout: a dead peer must leave time for the others.
const mariadbPeerTimeout = 5 * time.Second

// replicationChannel is one row of SHOW ALL SLAVES STATUS: the master a
// replication channel connects to.
type replicationChannel struct {
	host     string
	port     int
	serverID uint32 // Master_Server_Id: 0 until the channel has connected once
}

// mariadbServer is one server as this check reads it. The candidate is read
// the same way as the monitored peers.
type mariadbServer struct {
	name string // the entry's name; empty for the candidate
	// dsnHost and dsnPort are the address the entry is monitored at.
	dsnHost string
	dsnPort int
	// flavor is what the server reported (VERSION()), not the saved label.
	flavor string
	// The server's live identity: @@hostname, @@port, @@server_id, when it
	// started (unix seconds, 0 unknown) and the machine part of UUID().
	hostname  string
	port      int
	serverID  uint32
	startedAt int64
	uuidNode  string
	// channels is its replication status; channelsRead false (with
	// channelsErr) when it could not be read. Only read on MariaDB.
	channels     []replicationChannel
	channelsRead bool
	channelsErr  string
	// unreachable: the server could not be read at all.
	unreachable bool
}

// mariadbReplicaOverlap is replicaOverlapCheck for a candidate the source
// reported to be MariaDB. src is the open candidate connection.
func mariadbReplicaOverlap(ctx context.Context, e console.ServerEntry, src *sql.DB, entries []console.ServerEntry) *console.DoctorCheck {
	cand, err := loadMariaDBServer(ctx, src, console.FlavorMariaDB)
	if err != nil {
		return &console.DoctorCheck{Name: replicaCheckName, Status: "skip",
			Detail: "could not read this server's identity: " + config.ScrubDSNError(err, e.SourceDSN)}
	}
	cand.dsnHost, cand.dsnPort = sourceAddress(e.SourceDSN)

	peers := make([]mariadbServer, 0, len(entries))
	for _, p := range entries {
		peers = append(peers, readMariaDBPeer(ctx, p))
	}
	return evaluateMariaDBReplicaOverlap(cand, peers)
}

// readMariaDBPeer reads one monitored entry's source, bounded by
// mariadbPeerTimeout. Any failure is an unreachable peer: counted as not
// verified. The error itself is not shown: it would be about another
// server's credentials.
func readMariaDBPeer(ctx context.Context, p console.ServerEntry) mariadbServer {
	out := mariadbServer{name: p.Name, unreachable: true}
	ctx, cancel := context.WithTimeout(ctx, mariadbPeerTimeout)
	defer cancel()
	db, err := config.Connect(sourceProbeDSN(p.SourceDSN))
	if err != nil {
		return out
	}
	defer db.Close()
	flavor, _, err := metadata.DetectSourceFlavor(db)
	if err != nil {
		return out
	}
	s, err := loadMariaDBServer(ctx, db, flavor)
	if err != nil {
		return out
	}
	s.name = p.Name
	s.dsnHost, s.dsnPort = sourceAddress(p.SourceDSN)
	return s
}

// loadMariaDBServer reads a server's live identity and, on MariaDB, its
// replication channels. A failure to read the channels is recorded on the
// result, not returned: the identity alone still settles "the same server".
func loadMariaDBServer(ctx context.Context, db *sql.DB, flavor string) (mariadbServer, error) {
	s := mariadbServer{flavor: flavor}
	var serverID uint64
	var uuid string
	var now int64
	if err := db.QueryRowContext(ctx, "SELECT @@hostname, @@port, @@server_id, UUID(), UNIX_TIMESTAMP()").
		Scan(&s.hostname, &s.port, &serverID, &uuid, &now); err != nil {
		return s, err
	}
	s.serverID = uint32(serverID)
	s.uuidNode = uuidNode(uuid)
	// SHOW GLOBAL STATUS needs no privilege, on MySQL and MariaDB alike.
	var name, uptime string
	if err := db.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Uptime'").Scan(&name, &uptime); err == nil {
		if up, perr := strconv.ParseInt(uptime, 10, 64); perr == nil && up >= 0 && now > up {
			s.startedAt = now - up
		}
	}
	if flavor != console.FlavorMariaDB {
		return s, nil
	}
	channels, err := readReplicationChannels(ctx, db)
	if err != nil {
		s.channelsErr = err.Error()
		return s, nil
	}
	s.channels, s.channelsRead = channels, true
	return s, nil
}

// readReplicationChannels is SHOW ALL SLAVES STATUS, read by column name: the
// set of columns grows between MariaDB versions.
func readReplicationChannels(ctx context.Context, db *sql.DB) ([]replicationChannel, error) {
	rows, err := db.QueryContext(ctx, "SHOW ALL SLAVES STATUS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	idx := map[string]int{}
	for i, c := range cols {
		idx[strings.ToLower(c)] = i
	}
	hostI, okH := idx["master_host"]
	portI, okP := idx["master_port"]
	idI, okS := idx["master_server_id"]
	if !okH || !okP || !okS {
		return nil, fmt.Errorf("the replication status has no Master_Host, Master_Port or Master_Server_Id column")
	}
	var out []replicationChannel
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		port, perr := strconv.Atoi(string(vals[portI]))
		id, ierr := strconv.ParseUint(string(vals[idI]), 10, 32)
		if perr != nil || ierr != nil {
			return nil, fmt.Errorf("the replication status has a Master_Port or Master_Server_Id that is not a number")
		}
		out = append(out, replicationChannel{host: strings.TrimSpace(string(vals[hostI])), port: port, serverID: uint32(id)})
	}
	return out, rows.Err()
}

// evaluateMariaDBReplicaOverlap builds the card from what was read. Pure.
func evaluateMariaDBReplicaOverlap(cand mariadbServer, peers []mariadbServer) *console.DoctorCheck {
	var findings []string
	unverified := 0
	for _, p := range peers {
		if p.unreachable {
			unverified++
			continue
		}
		rel := ""
		switch {
		case sameAddress(cand, p) || sameRunningServer(cand, p):
			rel = "is the same server as already-monitored"
		case anyChannelPointsAt(cand.channels, p):
			rel = "appears to be a replica of already-monitored"
		case p.flavor == console.FlavorMariaDB && anyChannelPointsAt(p.channels, cand):
			rel = "appears to be the primary of already-monitored replica"
		}
		if rel != "" {
			findings = append(findings, fmt.Sprintf("%s %q", rel, p.name))
			continue
		}
		// A MariaDB peer whose channels were not read may replicate from
		// this server: not verified. A MySQL peer's are never read, and a
		// MySQL server replicating from a MariaDB one is not a setup this
		// check looks for.
		if p.flavor == console.FlavorMariaDB && !p.channelsRead {
			unverified++
		}
	}
	if len(findings) > 0 {
		return &console.DoctorCheck{
			Name:        replicaCheckName,
			Status:      "warn",
			Detail:      "this server " + strings.Join(findings, "; "),
			Remediation: replicaOverlapRemediation,
		}
	}
	if !cand.channelsRead {
		return &console.DoctorCheck{Name: replicaCheckName, Status: "skip",
			Detail: "could not read this server's replication status (" + cand.channelsErr + "), so whether this server replicates from a monitored server is unknown",
			Remediation: "Reading it needs the SLAVE MONITOR privilege (MariaDB 10.5.9 and later).\n" +
				"Grant it to the user DBTrail connects with to have this checked:\n\n" +
				"  GRANT SLAVE MONITOR ON *.* TO 'dbtrail'@'%';",
		}
	}
	detail := fmt.Sprintf("no replica relationship detected among %d monitored source(s)", len(peers))
	if unverified > 0 {
		detail += fmt.Sprintf(" (%d could not be verified)", unverified)
	}
	return &console.DoctorCheck{Name: replicaCheckName, Status: "pass", Detail: detail}
}

// sameAddress: both entries are monitored at the same host:port. From this
// one process that is one server, loopback included.
func sameAddress(a, b mariadbServer) bool {
	return a.dsnHost != "" && a.dsnPort != 0 && strings.EqualFold(a.dsnHost, b.dsnHost) && a.dsnPort == b.dsnPort
}

// sameRunningServer: two connections that land on one running MariaDB. Every
// part must be known and equal; the start time is two reads of the uptime at
// different instants, hence the two seconds.
func sameRunningServer(a, b mariadbServer) bool {
	if a.flavor != console.FlavorMariaDB || b.flavor != console.FlavorMariaDB {
		return false
	}
	if a.hostname == "" || a.port == 0 || a.startedAt == 0 || b.startedAt == 0 || a.uuidNode == "" {
		return false
	}
	d := a.startedAt - b.startedAt
	return strings.EqualFold(a.hostname, b.hostname) && a.port == b.port && a.serverID == b.serverID &&
		d >= -2 && d <= 2 && a.uuidNode == b.uuidNode
}

func anyChannelPointsAt(channels []replicationChannel, t mariadbServer) bool {
	for _, ch := range channels {
		if channelPointsAt(ch, t) {
			return true
		}
	}
	return false
}

// channelPointsAt: the channel's master is t. The server id must match and be
// known, and the host must be t's monitored address (with that port) or t's
// own hostname (with its own port).
func channelPointsAt(ch replicationChannel, t mariadbServer) bool {
	if ch.serverID == 0 || ch.serverID != t.serverID || ch.host == "" || isLoopbackHost(ch.host) {
		return false
	}
	byAddress := t.dsnHost != "" && strings.EqualFold(ch.host, t.dsnHost) && ch.port == t.dsnPort
	byHostname := t.hostname != "" && strings.EqualFold(ch.host, t.hostname) && ch.port == t.port
	return byAddress || byHostname
}

func isLoopbackHost(h string) bool {
	h = strings.Trim(strings.TrimSpace(h), "[]")
	if strings.EqualFold(h, "localhost") {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

// sourceAddress is the host and port of a MySQL-family source DSN; the port
// defaults to 3306. A socket or a DSN that does not parse has no address.
func sourceAddress(dsn string) (string, int) {
	if strings.TrimSpace(dsn) == "" {
		return "", 0 // the driver would read it as 127.0.0.1:3306
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || cfg.Net != "tcp" || cfg.Addr == "" {
		return "", 0
	}
	host, portStr, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return strings.Trim(cfg.Addr, "[]"), 3306
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0
	}
	return host, port
}

// uuidNode is the node (machine) part of a version 1 UUID, lowercase, or ""
// for anything else: a random UUID names no machine.
func uuidNode(u string) string {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(u)), "-")
	if len(parts) != 5 || len(parts[2]) != 4 || parts[2][0] != '1' || len(parts[4]) != 12 {
		return ""
	}
	return parts[4]
}
