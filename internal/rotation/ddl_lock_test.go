package rotation

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestIsLockWaitTimeout(t *testing.T) {
	lock := &mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded; try restarting transaction"}
	for name, c := range map[string]struct {
		err  error
		want bool
	}{
		"1205":              {lock, true},
		"1205 wrapped":      {fmt.Errorf("drop: %w", lock), true},
		"another number":    {&mysql.MySQLError{Number: 1507, Message: "Error in list of partitions to DROP"}, false},
		"deadlock is not":   {&mysql.MySQLError{Number: 1213}, false},
		"not a MySQL error": {errors.New("Lock wait timeout exceeded"), false},
		"nil":               {nil, false},
	} {
		if got := isLockWaitTimeout(c.err); got != c.want {
			t.Errorf("%s: isLockWaitTimeout = %v, want %v", name, got, c.want)
		}
	}
}
