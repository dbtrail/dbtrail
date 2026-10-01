package consoleapp

import (
	"context"
	"database/sql"
	"errors"
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
//     connects to the other server, by host AND port AND Master_Server_Id,
//     AND the other server confirms it from its side: its SHOW SLAVE HOSTS
//     lists the replica's server id and port. Without that confirmation a
//     match on names is "could not be verified": the channel's host is
//     resolved in the replica's network, not this process's.
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

// slaveHost is one row of SHOW SLAVE HOSTS: a replica connected to the server.
type slaveHost struct {
	serverID uint32
	port     int
	masterID uint32
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
	// channelsDenied: the read was refused for a missing privilege (1227).
	channelsDenied bool
	// slaveHosts is what SHOW SLAVE HOSTS lists: the replicas connected to
	// this server, as the server itself sees them. slaveHostsRead false
	// when it could not be read. Only read on MariaDB.
	slaveHosts     []slaveHost
	slaveHostsRead bool
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

	if !cand.channelsRead {
		cand.channelsErr = config.ScrubDSNError(errors.New(cand.channelsErr), e.SourceDSN)
	}

	return evaluateMariaDBReplicaOverlap(cand, collectMariaDBPeers(ctx, entries, readMariaDBPeer))
}

// collectMariaDBPeers reads the monitored entries one after another with read.
func collectMariaDBPeers(ctx context.Context, entries []console.ServerEntry, read func(context.Context, console.ServerEntry) mariadbServer) []mariadbServer {
	peers := make([]mariadbServer, 0, len(entries))
	for _, p := range entries {
		// A PostgreSQL source can be neither the same server nor a replica.
		if p.SourceFlavor() == console.FlavorPostgres {
			continue
		}
		// Past the whole check's bound, the rest are not read: not verified.
		if ctx.Err() != nil {
			peers = append(peers, mariadbServer{name: p.Name, unreachable: true})
			continue
		}
		peers = append(peers, read(ctx, p))
	}
	return peers
}

// readMariaDBPeer reads one monitored entry's source, bounded by
// mariadbPeerTimeout. Any failure is an unreachable peer: counted as not
// verified. The error itself is not shown: it would be about another
// server's credentials.
func readMariaDBPeer(ctx context.Context, p console.ServerEntry) mariadbServer {
	out := mariadbServer{name: p.Name, unreachable: true}
	out.dsnHost, out.dsnPort = sourceAddress(p.SourceDSN)
	ctx, cancel := context.WithTimeout(ctx, mariadbPeerTimeout)
	defer cancel()
	// sourceProbeDSN bounds the dial too (probeDSN caps it at
	// windowProbeTimeout): config.Connect's ping does not take a context.
	// The peer's OWN TLS settings, not the candidate's: each saved source is
	// read the way its own capture reads it.
	db, err := connectSourceAsked(sourceProbeDSN(p.SourceDSN), p.SourceSSL())
	if err != nil {
		return out
	}
	defer db.Close()
	flavor, err := detectFlavorCtx(ctx, db)
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

// detectFlavorCtx is metadata.DetectSourceFlavor bounded by ctx.
func detectFlavorCtx(ctx context.Context, db *sql.DB) (string, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return "", err
	}
	if f := metadata.ClassifyVersion(version); f != "" {
		return f, nil
	}
	return "", errors.New("SELECT VERSION() returned an empty string")
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
	if channels, err := readReplicationChannels(ctx, db); err != nil {
		s.channelsErr = err.Error()
		var me *mysql.MySQLError
		s.channelsDenied = errors.As(err, &me) && me.Number == 1227 // ER_SPECIFIC_ACCESS_DENIED_ERROR
	} else {
		s.channels, s.channelsRead = channels, true
	}
	// An unread list only means no replica of this server can be confirmed.
	if hosts, err := readSlaveHosts(ctx, db); err == nil {
		s.slaveHosts, s.slaveHostsRead = hosts, true
	}
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

// readSlaveHosts is SHOW SLAVE HOSTS, read by column name. It needs
// REPLICATION MASTER ADMIN on MariaDB 10.5.2 and later.
func readSlaveHosts(ctx context.Context, db *sql.DB) ([]slaveHost, error) {
	rows, err := db.QueryContext(ctx, "SHOW SLAVE HOSTS")
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
	idI, okS := idx["server_id"]
	portI, okP := idx["port"]
	masterI, okM := idx["master_id"]
	if !okS || !okP || !okM {
		return nil, fmt.Errorf("the replica list has no Server_id, Port or Master_id column")
	}
	var out []slaveHost
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		id, ierr := strconv.ParseUint(string(vals[idI]), 10, 32)
		port, perr := strconv.Atoi(string(vals[portI]))
		master, merr := strconv.ParseUint(string(vals[masterI]), 10, 32)
		if ierr != nil || perr != nil || merr != nil {
			return nil, fmt.Errorf("the replica list has a Server_id, Port or Master_id that is not a number")
		}
		out = append(out, slaveHost{serverID: uint32(id), port: port, masterID: uint32(master)})
	}
	return out, rows.Err()
}

// evaluateMariaDBReplicaOverlap builds the card from what was read. Pure.
func evaluateMariaDBReplicaOverlap(cand mariadbServer, peers []mariadbServer) *console.DoctorCheck {
	var findings []string
	unverified := 0
	for _, p := range peers {
		if p.unreachable {
			// The same address is the same server, answered or not (a
			// monitored entry with another password, say).
			if sameAddress(cand, p) {
				findings = append(findings, fmt.Sprintf("is the same server as already-monitored %q", p.name))
			} else {
				unverified++
			}
			continue
		}
		replicaOf, replicaUnsure := channelsPointAt(cand.channels, cand, p)
		var primaryOf, primaryUnsure bool
		if p.flavor == console.FlavorMariaDB {
			primaryOf, primaryUnsure = channelsPointAt(p.channels, p, cand)
		}
		rel := ""
		switch {
		case (sameAddress(cand, p) && !identitiesDiffer(cand, p)) || sameRunningServer(cand, p):
			rel = "is the same server as already-monitored"
		case replicaOf:
			rel = "appears to be a replica of already-monitored"
		case primaryOf:
			rel = "appears to be the primary of already-monitored replica"
		}
		if rel != "" {
			findings = append(findings, fmt.Sprintf("%s %q", rel, p.name))
			continue
		}
		// Not verified, never "no relationship":
		//   - a channel whose host and port are the other server's, but whose
		//     server id cannot tell (0: never connected; 1: every
		//     unconfigured server's);
		//   - a MariaDB peer whose channels were not read: it may replicate
		//     from this server (a MySQL peer's are never read, and a MySQL
		//     server replicating from a MariaDB one is not a setup this check
		//     looks for);
		//   - an identity read only in part, on either side: the same
		//     server under two addresses could not be recognized.
		if replicaUnsure || primaryUnsure ||
			(p.flavor == console.FlavorMariaDB && (!p.channelsRead || identityIncomplete(p) || identityIncomplete(cand))) {
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
		c := &console.DoctorCheck{Name: replicaCheckName, Status: "skip",
			Detail: "could not read this server's replication status (" + cand.channelsErr + "), so whether this server replicates from a monitored server is unknown"}
		if cand.channelsDenied {
			c.Remediation = "Reading it needs the SLAVE MONITOR privilege (MariaDB 10.5.9 and later).\n" +
				"Grant it to the user DBTrail connects with to have this checked:\n\n" +
				"  GRANT SLAVE MONITOR ON *.* TO 'dbtrail'@'%';"
		}
		return c
	}
	detail := fmt.Sprintf("no replica relationship detected among %d monitored source(s)", len(peers))
	if unverified > 0 {
		detail += fmt.Sprintf(" (%d could not be verified)", unverified)
	}
	return &console.DoctorCheck{Name: replicaCheckName, Status: "pass", Detail: detail}
}

// sameAddress: both entries are monitored at the same host:port. From this
// one process that is one server, loopback included, unless the two
// connections say otherwise (identitiesDiffer: a proxy routing by user).
func sameAddress(a, b mariadbServer) bool {
	return a.dsnHost != "" && a.dsnPort != 0 && strings.EqualFold(a.dsnHost, b.dsnHost) && a.dsnPort == b.dsnPort
}

// identitiesDiffer: the two connections were answered by servers that say
// they are different: another flavor, or, both read in full, another server
// id or another UUID machine part.
func identitiesDiffer(a, b mariadbServer) bool {
	if a.flavor != "" && b.flavor != "" && a.flavor != b.flavor {
		return true
	}
	if identityIncomplete(a) || identityIncomplete(b) {
		return false
	}
	return a.serverID != b.serverID || a.uuidNode != b.uuidNode
}

// identityIncomplete: a MariaDB whose start time or UUID machine part could
// not be read, so sameRunningServer cannot recognize it.
func identityIncomplete(s mariadbServer) bool {
	return s.flavor == console.FlavorMariaDB && (s.startedAt == 0 || s.uuidNode == "")
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

// channelsPointAt reports whether one of replica's channels connects to
// primary (match), and whether one looks like it but cannot be told
// (unsure). A channel names its master by a host as the REPLICA resolves it,
// so a name, a port and a server id are not proof on their own: two cloned
// stacks each have a mariadb-primary with the same server id. A match needs
// the primary's own side to agree: its SHOW SLAVE HOSTS lists the replica
// (primaryListsReplica). Unsure is a channel toward primary's host and port
// with:
//   - server id 0: it never connected;
//   - server id 1: the id of every server nobody configured;
//   - the right server id, but a primary that does not list the replica or
//     whose list could not be read.
func channelsPointAt(channels []replicationChannel, replica, primary mariadbServer) (match, unsure bool) {
	for _, ch := range channels {
		if !channelHostIs(ch, primary) {
			continue
		}
		switch {
		case ch.serverID == 0 || ch.serverID == defaultServerID:
			unsure = true
		case ch.serverID != primary.serverID:
		case primaryListsReplica(primary, replica):
			match = true
		default:
			unsure = true
		}
	}
	return match, unsure
}

// primaryListsReplica: primary's SHOW SLAVE HOSTS has a replica with
// replica's server id and port, connected to primary's server id. A replica
// on the default server id is never confirmed: every clone lists it alike.
func primaryListsReplica(primary, replica mariadbServer) bool {
	if !primary.slaveHostsRead || replica.serverID == 0 || replica.serverID == defaultServerID {
		return false
	}
	for _, h := range primary.slaveHosts {
		if h.serverID == replica.serverID && h.port == replica.port && h.masterID == primary.serverID {
			return true
		}
	}
	return false
}

// defaultServerID is @@server_id on a MariaDB nobody configured.
const defaultServerID = 1

// channelHostIs: the channel's master host and port are t's monitored
// address (with that port) or t's own hostname (with its own port). A
// loopback master is the replica's own machine and never matches.
func channelHostIs(ch replicationChannel, t mariadbServer) bool {
	if ch.host == "" || isLoopbackHost(ch.host) {
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
