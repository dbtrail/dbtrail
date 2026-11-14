package sqlsandbox

import (
	"context"
	"testing"
)

func TestZZFloatProbe(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	res, err := r.Run(context.Background(), f.job("SELECT 3.14::FLOAT AS a, 0.3333333::FLOAT AS b, 123456792::FLOAT AS c, (3.14::FLOAT)::DOUBLE AS d"))
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range res.Columns {
		t.Logf("PROBE %s %s %T %v", c.Name, c.Type, res.Rows[0][i], res.Rows[0][i])
	}
}
