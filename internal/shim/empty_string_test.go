package shim

import "testing"

// A column that held an empty string must reach the client as an empty string, not as
// NULL: the two are different values, and a recovery tool that blurs them
// hands back a row that was never there. Every buffered time-travel result
// is built through resultsetValue.
func TestImagesResult_emptyStringIsNotNull(t *testing.T) {
	want := []byte{0x00, 0x01, 'x', 0xfb}
	res, err := buildImagesResult([]map[string]any{{"a": "", "b": "x", "c": nil}}, []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if got := []byte(res.RowDatas[0]); string(got) != string(want) {
		t.Errorf("buildImagesResult row = % x, want % x (empty string, 'x', NULL)", got, want)
	}
	one, err := imageToResult(map[string]any{"a": "", "b": "x", "c": nil}, []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if got := []byte(one.RowDatas[0]); string(got) != string(want) {
		t.Errorf("imageToResult row = % x, want % x", got, want)
	}
	// A column of empty and non-empty strings across rows still builds: the
	// two Go types carry the same wire type.
	if _, err := buildImagesResult([]map[string]any{{"a": ""}, {"a": "y"}, {"a": nil}}, []string{"a"}); err != nil {
		t.Errorf("mixed empty and non-empty rows: %v", err)
	}
}
