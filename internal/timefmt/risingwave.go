package timefmt

import "time"

// RisingWaveTimestampLayout is the canonical layout accepted by RisingWave for timestamptz values.
// RisingWave rejects RFC3339 timestamps with the 'T' separator, so we normalize to the layout
// "YYYY-MM-DD HH:MM:SS[.up to 6 digits]±HH:MM".
const RisingWaveTimestampLayout = "2006-01-02 15:04:05.999999Z07:00"

// FormatRisingWave returns the UTC representation of t formatted according to
// RisingWaveTimestampLayout. RisingWave expects timestamps with a space separator
// between the date and the time component and supports microsecond precision.
func FormatRisingWave(t time.Time) string {
	return t.UTC().Format(RisingWaveTimestampLayout)
}
