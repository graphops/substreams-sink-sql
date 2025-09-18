package timefmt

import (
	"fmt"
	"strings"
	"time"
)

// FormatRisingWave renders a UTC timestamp in the layout accepted by RisingWave
// ("YYYY-MM-DD HH:MM:SS[.dddddd]+HH:MM"), always returning a space separator and
// microsecond precision when sub-second data is present.
func FormatRisingWave(t time.Time) string {
	utc := t.UTC()
	base := utc.Format("2006-01-02 15:04:05")
	if ns := utc.Nanosecond(); ns != 0 {
		base = fmt.Sprintf("%s.%06d", base, ns/1000)
	}
	return base + "+00:00"
}

var risingWaveLayouts = []string{
	"2006-01-02 15:04:05.999999-07:00",
	"2006-01-02 15:04:05-07:00",
}

// ParseTimestamp attempts to interpret common timestamp encodings that show up in
// Substreams payloads. It accepts RFC3339/RFC3339Nano as well as the canonical
// RisingWave layout emitted by FormatRisingWave (with or without fractional
// seconds). Returned values are normalized to UTC.
func ParseTimestamp(value string) (time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("parse timestamp: empty input")
	}

	if ts, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return ts.UTC(), nil
	}
	if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return ts.UTC(), nil
	}

	for _, layout := range risingWaveLayouts {
		if ts, err := time.Parse(layout, trimmed); err == nil {
			return ts.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("parse timestamp: unsupported layout %q", value)
}
