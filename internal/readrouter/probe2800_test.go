package readrouter

import "testing"

func TestZZProbe2800(t *testing.T) {
	for _, s := range []string{
		"SELECT MIN(side), COUNT(*) FROM ev WHERE dt >= DATE_ADD(NOW(), INTERVAL '-30' DAY) OR dt < NOW() - INTERVAL 1 DAY",
		"SELECT MIN(side), COUNT(*) FROM ev WHERE dt >= DATE_ADD(NOW(), INTERVAL '-30' DAY)",
	} {
		got, r := ForCopyOpts(s, dateAddOpts)
		t.Logf("veto=%q copy=%q refusal=%q", VetoOpts(s, dateAddOpts), got, r)
	}
}
