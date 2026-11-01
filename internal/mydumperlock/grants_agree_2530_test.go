package mydumperlock

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// The full read (this package's grantSet) and doctor (metadata's parser) read
// the same SHOW GRANTS for PROCESS and RELOAD with two parsers (#2530: doctor
// cannot import this package, which imports doctor). They must agree, or
// doctor passes an account whose full read warns, or the other way round.
func TestGrantParsersAgreeOnGlobalOnlyPrivileges(t *testing.T) {
	fixtures := [][]string{
		{"GRANT RELOAD, SHOW VIEW, REPLICATION SLAVE ON *.* TO `dbtrail`@`%`", "GRANT BACKUP_ADMIN ON *.* TO `dbtrail`@`%`"},
		{"GRANT RELOAD, PROCESS, SHOW VIEW ON *.* TO `dbtrail`@`%`"},
		{"GRANT SELECT ON *.* TO `dbtrail`@`%`", "GRANT FLUSH_TABLES ON *.* TO `dbtrail`@`%`"},
		{"GRANT ALL PRIVILEGES ON *.* TO `root`@`%` WITH GRANT OPTION"},
		{"GRANT RELOAD ON *.* TO `dbtrail`@`%`", "GRANT ALL PRIVILEGES ON `app`.* TO `dbtrail`@`%`"},
		{"GRANT SELECT, LOCK TABLES ON *.* TO 'dbtrail'@'%'"},
		{"GRANT USAGE ON *.* TO `u`@`%`", "GRANT `role_ops`@`%` TO `u`@`%`"},
		{"GRANT SELECT (`a`, `on x`), INSERT ON `db`.`t` TO `u`@`%`", "GRANT PROCESS ON *.* TO `u`@`%`"},
		// MariaDB writes the password hash after the account.
		{"GRANT RELOAD, PROCESS ON *.* TO `dbtrail`@`%` IDENTIFIED BY PASSWORD '*AB12'"},
	}
	for _, lines := range fixtures {
		g := &grantSet{global: map[string]bool{}, scoped: map[string][]string{}, revoked: map[string][]string{}}
		for _, l := range lines {
			g.add(l)
		}
		for _, p := range []string{"PROCESS", "RELOAD", "FLUSH_TABLES"} {
			if a, b := g.grantedGlobally(p), metadata.HasGlobalPrivilege(lines, p); a != b {
				t.Errorf("%s in %q: full read says %v, doctor says %v", p, lines, a, b)
			}
		}
	}
}
