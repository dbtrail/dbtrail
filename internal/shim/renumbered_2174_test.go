package shim

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2174: a binlog-renumbering refusal reaches a MySQL client with the code of
// the other unreadable-history refusals, on both _snapshot paths (the
// single-row one goes through mysqlRenderErr); any other raw error keeps
// passing through as before.
func TestRenumberedRefusal_2174(t *testing.T) {
	refusal := fmt.Errorf("resolve _snapshot: %w: the source's binary log started again", reconstruct.ErrBinlogRenumbered)
	for name, mapErr := range map[string]func(error) error{
		"full table": renumberedRefusal,
		"one row":    mysqlRenderErr,
	} {
		var me *mysql.MyError
		if err := mapErr(refusal); !errors.As(err, &me) || me.Code != mysql.ER_NO_PARTITION_FOR_GIVEN_VALUE || me.Message != refusal.Error() {
			t.Errorf("%s: %#v; want a MySQL error %d carrying %q", name, err, mysql.ER_NO_PARTITION_FOR_GIVEN_VALUE, refusal)
		}
		other := errors.New("a baseline read failed")
		if err := mapErr(other); err != other {
			t.Errorf("%s: another error became %#v; want it unchanged", name, err)
		}
	}
}
