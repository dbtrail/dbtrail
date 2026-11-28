package installer

import "testing"

func TestZZPrintPG(t *testing.T) {
	r := pgRun(t)
	t.Log("\n" + r.out)
}
