package timefmt

import (
	"fmt"
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
