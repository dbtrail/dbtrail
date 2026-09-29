package parser

import (
	"errors"
	"fmt"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// ParseMariaDBPosition parses a MariaDB GTID position, the shape of
// @@gtid_binlog_pos and of a MariaDB capture's checkpoint ("0-1-100,1-2-7"),
// into its GTID per replication domain. Whitespace anywhere is dropped: the
// variable can come back with line breaks.
//
// It is stricter than gomysql.ParseMariadbGTIDSet on purpose, for callers
// that compare two positions to decide whether one has everything the other
// has. That parser accepts "" as a set with no domains and an empty entry
// ("0-1-5,,1-2-3") as a nil GTID, and a position with no domains makes a
// per-domain comparison pass without comparing anything. So an empty
// position and an empty entry are errors here. So is a domain named twice
// (a checkpoint an older go-mysql wrote after a failover): which of the two
// GTIDs is the domain's position is a guess, and a comparison built on a
// guess is not an answer.
func ParseMariaDBPosition(s string) (map[uint32]gomysql.MariadbGTID, error) {
	s = strings.Join(strings.Fields(s), "")
	if s == "" {
		return nil, errors.New("empty MariaDB GTID position")
	}
	out := map[uint32]gomysql.MariadbGTID{}
	for part := range strings.SplitSeq(s, ",") {
		if part == "" {
			return nil, fmt.Errorf("empty entry in MariaDB GTID position %q", s)
		}
		g, err := gomysql.ParseMariadbGTID(part)
		if err != nil {
			return nil, err
		}
		if _, dup := out[g.DomainID]; dup {
			return nil, fmt.Errorf("MariaDB GTID position %q names domain %d twice", s, g.DomainID)
		}
		out[g.DomainID] = *g
	}
	return out, nil
}
