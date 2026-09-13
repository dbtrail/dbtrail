package mcptools

import (
	"encoding/json"
	"errors"
	"testing"
)

// A summary carries the coverage report and the warnings: a client asks
// "how big, is it complete" precisely to decide whether to pull the script,
// and an answer that drops the caveats would make an incomplete recovery look
// whole.
func TestRecoverCascadeSummaryCarriesCaveatsAndWarnings(t *testing.T) {
	cfg, _ := cascadeMockConfig(t, errors.New("archive_state probe down"))
	res := callRecoverCascade(t, cfg, RecoverCascadeArgs{Schema: "app", Table: "orders", AllowIncomplete: true, SummaryOnly: true})
	if res.IsError {
		t.Fatal(resultText(res))
	}
	var out recoverCascadeResult
	if err := json.Unmarshal([]byte(resultText(res)), &out); err != nil {
		t.Fatal(err)
	}
	if out.SQL != "" {
		t.Errorf("summary_only returned %d bytes of script", len(out.SQL))
	}
	if out.Complete || len(out.Incomplete) == 0 {
		t.Errorf("summary lost the coverage report: %+v", out)
	}
}
