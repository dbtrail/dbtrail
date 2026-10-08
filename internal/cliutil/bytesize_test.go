package cliutil

import "testing"

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"0", 0},
		{"", 0},
		{"1024", 1024},
		{"256MB", 256 << 20},
		{"256mb", 256 << 20},
		{"1GB", 1 << 30},
		{"1gb", 1 << 30},
		{"512KB", 512 << 10},
		// The IEC spellings DuckDB's memory_limit takes, same binary units.
		{"512MiB", 512 << 20},
		{"4GiB", 4 << 30},
		{"4gib", 4 << 30},
		{"64KiB", 64 << 10},
		{" 2GB ", 2 << 30},
	}
	for _, tt := range tests {
		got, err := ParseByteSize(tt.input)
		if err != nil {
			t.Errorf("ParseByteSize(%q): unexpected error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseByteSize(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestParseByteSize_invalid(t *testing.T) {
	for _, input := range []string{"abc", "-1GB", "not_a_number", "1.5GB", "9999999999GB", "2 GB", "GiB", "2XiB"} {
		_, err := ParseByteSize(input)
		if err == nil {
			t.Errorf("ParseByteSize(%q): expected error", input)
		}
	}
}
