package timefmt

import (
	"testing"
	"time"
)

func TestFormatRisingWave(t *testing.T) {
	ts := time.Date(2025, time.September, 18, 15, 18, 52, 123456789, time.FixedZone("UTC-4", -4*3600))
	got := FormatRisingWave(ts)
	want := "2025-09-18 19:18:52.123456+00:00"
	if got != want {
		t.Fatalf("FormatRisingWave() = %q, want %q", got, want)
	}
}

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		expects time.Time
	}{
		{
			name:    "rfc3339nano",
			input:   "2025-09-18T18:24:17.806811415Z",
			expects: time.Date(2025, 9, 18, 18, 24, 17, 806811415, time.UTC),
		},
		{
			name:    "rfc3339",
			input:   "2025-09-18T18:24:17Z",
			expects: time.Date(2025, 9, 18, 18, 24, 17, 0, time.UTC),
		},
		{
			name:    "risingwave canonical",
			input:   "2025-09-18 18:24:17.123456+00:00",
			expects: time.Date(2025, 9, 18, 18, 24, 17, 123456000, time.UTC),
		},
		{
			name:    "risingwave no fraction",
			input:   "2025-09-18 18:24:17+00:00",
			expects: time.Date(2025, 9, 18, 18, 24, 17, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTimestamp(tt.input)
			if err != nil {
				t.Fatalf("ParseTimestamp() unexpected error: %v", err)
			}
			if !got.Equal(tt.expects) {
				t.Fatalf("ParseTimestamp() = %v, want %v", got, tt.expects)
			}
		})
	}

	t.Run("invalid", func(t *testing.T) {
		if _, err := ParseTimestamp("not-a-timestamp"); err == nil {
			t.Fatalf("ParseTimestamp() expected error for invalid input")
		}
	})
}
