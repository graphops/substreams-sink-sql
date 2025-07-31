package risingwave

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DataType string

const (
	// Numeric types - aligning with RisingWave documentation
	TypeSmallInt DataType = "SMALLINT"         // Two-byte integer
	TypeInteger  DataType = "INTEGER"          // Four-byte integer
	TypeBigInt   DataType = "BIGINT"           // Eight-byte integer
	TypeNumeric  DataType = "NUMERIC"          // Exact numeric (28 decimal digits precision)
	TypeReal     DataType = "REAL"             // Single precision floating-point (4 bytes)
	TypeDouble   DataType = "DOUBLE PRECISION" // Double precision floating-point (8 bytes)

	// Boolean type
	TypeBool DataType = "BOOLEAN" // Logical Boolean (true, false, or null)

	// String types
	TypeVarchar DataType = "CHARACTER VARYING" // Variable-length character string
	TypeText    DataType = "CHARACTER VARYING" // Use CHARACTER VARYING for RisingWave

	// Binary type
	TypeBytea DataType = "BYTEA" // Binary strings (hex format)

	// Date and time types
	TypeDate        DataType = "DATE"                     // Calendar date (year, month, day)
	TypeTime        DataType = "TIME"                     // Time of day (no time zone)
	TypeTimestamp   DataType = "TIMESTAMP"                // Date and time (no time zone)
	TypeTimestamptz DataType = "TIMESTAMP WITH TIME ZONE" // Timestamp with time zone
	TypeInterval    DataType = "INTERVAL"                 // Time span

	// Complex types
	TypeJsonb DataType = "JSONB" // Binary JSON value

	// RisingWave-specific semantic types
	TypeRwInt256  DataType = "rw_int256"  // RisingWave's 256-bit signed integer type
	TypeRwUint256 DataType = "rw_uint256" // RisingWave's 256-bit unsigned integer type
)

func (s DataType) String() string {
	return string(s)
}

func IsWellKnownType(fd *desc.FieldDescriptor) bool {
	switch fd.GetMessageType().GetFullyQualifiedName() {
	case "google.protobuf.Timestamp":
		return true
	default:
		return false
	}
}

// MapSemanticType maps semantic types to RisingWave-specific SQL types
func MapSemanticType(semanticType sql2.SemanticType) (string, bool) {
	switch semanticType {
	case sql2.SemanticUint256:
		return string(TypeRwUint256), true // Use new rw_uint256 for unsigned
	case sql2.SemanticInt256:
		return string(TypeRwInt256), true // Keep rw_int256 for signed
	case sql2.SemanticAddress:
		return "CHARACTER VARYING", true
	case sql2.SemanticHash:
		return "CHARACTER VARYING", true
	case sql2.SemanticSignature:
		return "CHARACTER VARYING", true
	case sql2.SemanticPubkey:
		return "CHARACTER VARYING", true
	case sql2.SemanticHex:
		return "CHARACTER VARYING", true
	case sql2.SemanticBase64:
		return "CHARACTER VARYING", true
	case sql2.SemanticJSON:
		return string(TypeJsonb), true
	case sql2.SemanticUUID:
		return "CHARACTER VARYING", true // RisingWave converts UUID to CHARACTER VARYING
	case sql2.SemanticUnixTimestamp, sql2.SemanticUnixTimestampMS, sql2.SemanticBlockTimestamp:
		return string(TypeTimestamptz), true
	default:
		return "", false // Not supported
	}
}

// SupportsSemanticType returns true if RisingWave supports the semantic type
func SupportsSemanticType(semanticType sql2.SemanticType) bool {
	_, supported := MapSemanticType(semanticType)
	return supported
}

func MapFieldType(fd *desc.FieldDescriptor) DataType {
	// Check for semantic type annotation first
	semanticType, _, hasSemanticType := proto.SemanticTypeInfo(fd)
	if hasSemanticType {
		if sqlType, supported := MapSemanticType(sql2.SemanticType(semanticType)); supported {
			return DataType(sqlType)
		}
		// Fall through to default mapping if semantic type not supported
	}

	// Default protobuf type mapping
	t := fd.GetType()
	switch t {
	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		switch fd.GetMessageType().GetFullyQualifiedName() {
		case "google.protobuf.Timestamp":
			return TypeTimestamptz // Use timestamptz for protobuf timestamps
		default:
			panic(fmt.Sprintf("Message type not supported: %s", fd.GetMessageType().GetFullyQualifiedName()))
		}
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		return TypeBool
	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_SINT32, descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		return TypeInteger
	case descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_SINT64, descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		return TypeBigInt
	case descriptor.FieldDescriptorProto_TYPE_UINT64, descriptor.FieldDescriptorProto_TYPE_FIXED64:
		return TypeNumeric // Use NUMERIC for large unsigned integers
	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_FIXED32:
		return TypeBigInt // Use BIGINT for 32-bit unsigned (to avoid overflow)
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return TypeReal // Use REAL for single precision
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return TypeDouble
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		return TypeVarchar
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		return TypeBytea // Use BYTEA for binary data
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		return TypeVarchar // Store enums as varchar
	default:
		panic(fmt.Sprintf("unsupported type: %s", t))
	}
}

func ValueToString(value any) (s string) {
	switch v := value.(type) {
	case string:
		s = "'" + strings.ReplaceAll(strings.ReplaceAll(v, "'", "''"), "\\", "\\\\") + "'"
	case int64:
		s = strconv.FormatInt(v, 10)
	case int32:
		s = strconv.FormatInt(int64(v), 10)
	case int:
		s = strconv.FormatInt(int64(v), 10)
	case uint64:
		// For large unsigned integers, use numeric literal
		s = strconv.FormatUint(v, 10)
	case uint32:
		s = strconv.FormatUint(uint64(v), 10)
	case uint:
		s = strconv.FormatUint(uint64(v), 10)
	case float64:
		s = strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		s = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case []uint8:
		// RisingWave expects hex format for bytea: '\x...'
		s = "'\\x" + strings.ToUpper(fmt.Sprintf("%x", v)) + "'"
	case bool:
		s = strconv.FormatBool(v)
	case time.Time:
		// Use RFC3339 format for timestamps
		s = "'" + v.Format(time.RFC3339) + "'"
	case *timestamppb.Timestamp:
		// Convert protobuf timestamp to timestamptz format
		s = "'" + v.AsTime().Format(time.RFC3339) + "'"
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
	return
}

// ConvertSemanticValue converts a value according to semantic type and format hint for RisingWave
func ConvertSemanticValue(semanticType sql2.SemanticType, value interface{}, formatHint string) (string, error) {
	switch semanticType {
	case sql2.SemanticUint256:
		return convertToRwUint256(value, formatHint)
	case sql2.SemanticInt256:
		return convertToRwInt256(value, formatHint)
	case sql2.SemanticAddress:
		return convertToAddress(value)
	case sql2.SemanticHash:
		return convertToHash(value)
	case sql2.SemanticSignature, sql2.SemanticPubkey, sql2.SemanticHex:
		return convertToHexString(value)
	case sql2.SemanticJSON:
		return convertToJSON(value)
	case sql2.SemanticUUID:
		return convertToUUID(value)
	case sql2.SemanticUnixTimestamp:
		return convertUnixTimestamp(value, false)
	case sql2.SemanticUnixTimestampMS:
		return convertUnixTimestamp(value, true)
	case sql2.SemanticBlockTimestamp:
		return convertUnixTimestamp(value, false)
	default:
		// Fallback to default value conversion
		return ValueToString(value), nil
	}
}

// convertToRwUint256 converts values to RisingWave's rw_uint256 type
func convertToRwUint256(value interface{}, formatHint string) (string, error) {
	switch v := value.(type) {
	case string:
		// Handle hex strings (0x...)
		if strings.HasPrefix(v, "0x") {
			return "'" + v + "'::rw_uint256", nil
		}
		// Handle decimal strings
		if formatHint == "hex" && !strings.HasPrefix(v, "0x") {
			// Add 0x prefix if missing for hex format
			return "'0x" + v + "'::rw_uint256", nil
		}
		return "'" + v + "'::rw_uint256", nil
	case []byte:
		// Convert bytes to hex for rw_uint256
		hexStr := "0x" + hex.EncodeToString(v)
		return "'" + hexStr + "'::rw_uint256", nil
	case int64, uint64, int32, uint32:
		// Convert numeric types to string
		return "'" + fmt.Sprintf("%v", v) + "'::rw_uint256", nil
	default:
		return "", fmt.Errorf("cannot convert %T to rw_uint256", value)
	}
}

// convertToRwInt256 converts values to RisingWave's rw_int256 type
func convertToRwInt256(value interface{}, formatHint string) (string, error) {
	switch v := value.(type) {
	case string:
		// Handle hex strings (0x...)
		if strings.HasPrefix(v, "0x") {
			return "'" + v + "'::rw_int256", nil
		}
		// Handle decimal strings
		if formatHint == "hex" && !strings.HasPrefix(v, "0x") {
			// Add 0x prefix if missing for hex format
			return "'0x" + v + "'::rw_int256", nil
		}
		return "'" + v + "'::rw_int256", nil
	case []byte:
		// Convert bytes to hex for rw_int256
		hexStr := "0x" + hex.EncodeToString(v)
		return "'" + hexStr + "'::rw_int256", nil
	case int64, uint64, int32, uint32:
		// Convert numeric types to string
		return "'" + fmt.Sprintf("%v", v) + "'::rw_int256", nil
	default:
		return "", fmt.Errorf("cannot convert %T to rw_int256", value)
	}
}

// convertToAddress converts values to blockchain address format
func convertToAddress(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		// Validate address format
		if strings.HasPrefix(v, "0x") {
			// Has 0x prefix - validate hex part is exactly 40 chars
			hexPart := v[2:]
			if len(hexPart) == 40 {
				return "'" + v + "'", nil
			}
			return "", fmt.Errorf("invalid address format: %s (expected 40 hex chars after 0x)", v)
		}
		// No 0x prefix - should be exactly 40 hex chars
		if len(v) == 40 {
			return "'0x" + v + "'", nil
		}
		return "", fmt.Errorf("invalid address format: %s (expected 40 or 42 chars)", v)
	case []byte:
		if len(v) == 20 {
			return "'0x" + hex.EncodeToString(v) + "'", nil
		}
		return "", fmt.Errorf("invalid address byte length: %d (expected 20)", len(v))
	default:
		return "", fmt.Errorf("cannot convert %T to address", value)
	}
}

// convertToHash converts values to hash format
func convertToHash(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		// Validate hash format
		if len(v) == 66 && strings.HasPrefix(v, "0x") {
			return "'" + v + "'", nil
		}
		if len(v) == 64 {
			// Add 0x prefix if missing
			return "'0x" + v + "'", nil
		}
		return "", fmt.Errorf("invalid hash format: %s (expected 64 or 66 chars)", v)
	case []byte:
		if len(v) == 32 {
			return "'0x" + hex.EncodeToString(v) + "'", nil
		}
		return "", fmt.Errorf("invalid hash byte length: %d (expected 32)", len(v))
	default:
		return "", fmt.Errorf("cannot convert %T to hash", value)
	}
}

// convertToHexString converts values to hex string format
func convertToHexString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		if strings.HasPrefix(v, "0x") {
			return "'" + v + "'", nil
		}
		// Add 0x prefix if missing
		return "'0x" + v + "'", nil
	case []byte:
		return "'0x" + hex.EncodeToString(v) + "'", nil
	default:
		return "", fmt.Errorf("cannot convert %T to hex string", value)
	}
}

// convertToJSON converts values to JSONB format
func convertToJSON(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		// Assume string is already valid JSON
		return "'" + strings.ReplaceAll(v, "'", "''") + "'::jsonb", nil
	default:
		return "", fmt.Errorf("cannot convert %T to JSON", value)
	}
}

// convertToUUID converts values to UUID format
func convertToUUID(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		// Basic UUID validation (length check)
		if len(v) == 36 {
			return "'" + v + "'", nil
		}
		return "", fmt.Errorf("invalid UUID format: %s (expected 36 chars)", v)
	default:
		return "", fmt.Errorf("cannot convert %T to UUID", value)
	}
}

// convertUnixTimestamp converts unix timestamps to RisingWave timestamp format
func convertUnixTimestamp(value interface{}, isMilliseconds bool) (string, error) {
	var t time.Time

	switch v := value.(type) {
	case int64:
		if isMilliseconds {
			t = time.Unix(v/1000, (v%1000)*1000000)
		} else {
			t = time.Unix(v, 0)
		}
	case uint64:
		if isMilliseconds {
			t = time.Unix(int64(v/1000), int64((v%1000)*1000000))
		} else {
			t = time.Unix(int64(v), 0)
		}
	case string:
		// Try to parse as number
		if val, err := strconv.ParseInt(v, 10, 64); err == nil {
			if isMilliseconds {
				t = time.Unix(val/1000, (val%1000)*1000000)
			} else {
				t = time.Unix(val, 0)
			}
		} else {
			return "", fmt.Errorf("cannot parse timestamp string: %s", v)
		}
	default:
		return "", fmt.Errorf("cannot convert %T to timestamp", value)
	}

	return "'" + t.UTC().Format(time.RFC3339) + "'", nil
}
