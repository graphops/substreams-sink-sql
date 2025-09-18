package sql

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams-sink-sql/internal/timefmt"
	"github.com/streamingfast/substreams-sink-sql/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NormalizeValue ensures field values are encoded in types the SQL backends
// expect (e.g. converting timestamp semantics to time.Time). Callers may pass
// nil values; they are returned untouched.
func NormalizeValue(fd *desc.FieldDescriptor, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case time.Time:
		return v.UTC(), nil
	case *time.Time:
		if v == nil {
			return nil, nil
		}
		return v.UTC(), nil
	case *timestamppb.Timestamp:
		if v == nil {
			return nil, nil
		}
		return v.AsTime().UTC(), nil
	case json.Number:
		// json.Number implements String() but we want deterministic parsing.
		reparsed, err := parseNumericString(v.String())
		if err != nil {
			return nil, err
		}
		value = reparsed
	}

	semanticType, _, hasSemanticType := proto.SemanticTypeInfo(fd)
	if hasSemanticType {
		switch SemanticType(semanticType) {
		case SemanticUnixTimestamp:
			return normalizeUnixTimestamp(value, false)
		case SemanticUnixTimestampMS:
			return normalizeUnixTimestamp(value, true)
		case SemanticBlockTimestamp:
			return normalizeUnixTimestamp(value, false)
		}
	}

	return value, nil
}

func normalizeUnixTimestamp(value interface{}, isMilliseconds bool) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case time.Time:
		return v.UTC(), nil
	case *time.Time:
		if v == nil {
			return nil, nil
		}
		return v.UTC(), nil
	case *timestamppb.Timestamp:
		if v == nil {
			return nil, nil
		}
		return v.AsTime().UTC(), nil
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return nil, nil
		}
		if hasDateSeparators(trimmed) {
			ts, err := timefmt.ParseTimestamp(trimmed)
			if err != nil {
				return nil, err
			}
			return ts.UTC(), nil
		}
		secs, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("normalize unix timestamp: %w", err)
		}
		return unixToTime(secs, isMilliseconds), nil
	case json.Number:
		parsed, err := parseNumericString(v.String())
		if err != nil {
			return nil, err
		}
		return normalizeUnixTimestamp(parsed, isMilliseconds)
	case int64:
		return unixToTime(v, isMilliseconds), nil
	case int32:
		return unixToTime(int64(v), isMilliseconds), nil
	case uint64:
		if v > uint64(math.MaxInt64) {
			return nil, fmt.Errorf("normalize unix timestamp: value %d overflows int64", v)
		}
		return unixToTime(int64(v), isMilliseconds), nil
	case uint32:
		return unixToTime(int64(v), isMilliseconds), nil
	case float64:
		return unixToTime(int64(v), isMilliseconds), nil
	case float32:
		return unixToTime(int64(v), isMilliseconds), nil
	default:
		return nil, fmt.Errorf("normalize unix timestamp: unsupported type %T", value)
	}
}

func unixToTime(v int64, isMilliseconds bool) time.Time {
	if isMilliseconds {
		seconds := v / 1000
		nanos := (v % 1000) * int64(time.Millisecond)
		return time.Unix(seconds, nanos).UTC()
	}
	return time.Unix(v, 0).UTC()
}

func hasDateSeparators(value string) bool {
	return strings.ContainsAny(value, "-T :")
}

func parseNumericString(value string) (interface{}, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	if strings.ContainsAny(value, ".eE") {
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("parse numeric string: %w", err)
		}
		return f, nil
	}
	i, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		return i, nil
	}
	u, errU := strconv.ParseUint(value, 10, 64)
	if errU == nil {
		return u, nil
	}
	return nil, fmt.Errorf("parse numeric string: %w", err)
}
