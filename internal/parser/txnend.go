package parser

import "strings"

// isTransactionTerminator reports whether a QUERY_EVENT's text ends the binlog
// group it belongs to, the way an XID_EVENT ends an InnoDB transaction (#2391).
//
// The server logs these as plain QUERY_EVENTs instead of an XID:
//   - COMMIT of a transaction that touched a non-transactional table (MyISAM,
//     or a mixed-engine transaction);
//   - ROLLBACK of such a transaction: the non-transactional changes it made
//     stay, so they are binlogged and closed with ROLLBACK;
//   - XA COMMIT / XA ROLLBACK, each of which is its own binlog group;
//   - XA PREPARE, which MariaDB logs as a query (MySQL ends the prepared
//     group with an XA_PREPARE_LOG_EVENT instead, handled by the caller).
//
// ROLLBACK TO SAVEPOINT is deliberately NOT a terminator: it is logged INSIDE
// a transaction that continues, so treating it as one would let a resume point
// land in the middle of that transaction.
func isTransactionTerminator(query string) bool {
	words := strings.Fields(strings.ToUpper(stripLeadingSQLComments(query)))
	switch len(words) {
	case 0:
		return false
	case 1:
		w := strings.TrimSuffix(words[0], ";")
		return w == "COMMIT" || w == "ROLLBACK"
	}
	if words[0] == "XA" {
		switch words[1] {
		case "COMMIT", "ROLLBACK", "PREPARE":
			return true
		}
	}
	return false
}
