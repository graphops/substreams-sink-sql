package timefmt

import (
	"testing"
	"time"
)

func TestFormatRisingWave(t *testing.T) {
	t := time.Date(2025, time.September, 18, 15, 18, 52, 123456789, time.FixedZone("UTC-4", -4*3600))
	got := FormatRisingWave(t)
	want := "2025-09-18 19:18:52.123456+00:00"
	if got != want {
		t.Fatalf("FormatRisingWave() = %q, want %q", got, want)
	}
}
