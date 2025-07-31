package clickhouse

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	protoutil "github.com/streamingfast/substreams-sink-sql/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DataType string

const (
	TypeInteger8   DataType = "Int8"
	TypeInteger16  DataType = "Int16"
	TypeInteger32  DataType = "Int32"
	TypeInteger64  DataType = "Int64"
	TypeInteger128 DataType = "Int128"
	TypeInteger256 DataType = "Int256"

	TypeUInt8   DataType = "UInt8"
	TypeUInt16  DataType = "UInt16"
	TypeUInt32  DataType = "UInt32"
	TypeUInt64  DataType = "UInt64"
	TypeUInt128 DataType = "UInt128"
	TypeUInt256 DataType = "UInt256"

	TypeFloat32 DataType = "Float32"
	TypeFloat64 DataType = "Float64"

	TypeBool       DataType = "Bool"
	TypeString     DataType = "String"
	TypeVarchar    DataType = "VARCHAR"
	TypeDateTime   DataType = "DateTime"
	TypeDateTime64 DataType = "DateTime64"

	// ClickHouse semantic type mappings
	TypeFixedString DataType = "FixedString"
	TypeDecimal32   DataType = "Decimal32"
	TypeDecimal64   DataType = "Decimal64"
	TypeDecimal128  DataType = "Decimal128"
	TypeDecimal256  DataType = "Decimal256"
)

func (s DataType) String() string {
	return string(s)
}

// MapSemanticType maps semantic types to ClickHouse-specific SQL types
func MapSemanticType(semanticType sql2.SemanticType) (string, bool) {
	switch semanticType {
	case sql2.SemanticUint256:
		return string(TypeString), true // ClickHouse stores as String (no native UInt256)
	case sql2.SemanticInt256:
		return string(TypeString), true // ClickHouse stores as String (no native Int256)
	case sql2.SemanticAddress:
		return "FixedString(42)", true // Fixed-length for blockchain addresses
	case sql2.SemanticHash:
		return "FixedString(66)", true // Fixed-length for blockchain hashes
	case sql2.SemanticSignature:
		return string(TypeString), true
	case sql2.SemanticPubkey:
		return string(TypeString), true
	case sql2.SemanticHex:
		return string(TypeString), true
	case sql2.SemanticBase64:
		return string(TypeString), true
	case sql2.SemanticJSON:
		return string(TypeString), true // ClickHouse doesn't have native JSON, use String
	case sql2.SemanticUUID:
		return string(TypeString), true // Store UUID as string in ClickHouse
	case sql2.SemanticUnixTimestamp, sql2.SemanticBlockTimestamp:
		return string(TypeDateTime), true
	case sql2.SemanticUnixTimestampMS:
		return "DateTime64(3)", true // ClickHouse DateTime64 with millisecond precision
	default:
		return "", false // Not supported
	}
}

// SupportsSemanticType returns true if ClickHouse supports the semantic type
func SupportsSemanticType(semanticType sql2.SemanticType) bool {
	_, supported := MapSemanticType(semanticType)
	return supported
}

func MapFieldType(fd *desc.FieldDescriptor) DataType {
	// Check for semantic type annotation first
	semanticType, _, hasSemanticType := protoutil.SemanticTypeInfo(fd)
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
			return TypeDateTime
		default:
			panic(fmt.Sprintf("Message type not supported: %s", fd.GetMessageType().GetFullyQualifiedName()))
		}
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		return TypeInteger32
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		return TypeBool
	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_SINT32, descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		return TypeInteger32
	case descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_SINT64, descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		return TypeInteger64
	case descriptor.FieldDescriptorProto_TYPE_UINT64, descriptor.FieldDescriptorProto_TYPE_FIXED64:
		return TypeUInt64
	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_FIXED32:
		return TypeUInt32
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return TypeFloat32
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return TypeFloat64
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		return TypeVarchar
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		return TypeVarchar
	default:
		panic(fmt.Sprintf("unsupported type: %s", t))
	}
}

func ColInputForColumn(fd *desc.FieldDescriptor) proto.ColInput {
	switch fd.GetType() {
	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		switch fd.GetMessageType().GetFullyQualifiedName() {
		case "google.protobuf.Timestamp":
			return &proto.ColDateTime{}
		default:
			panic(fmt.Sprintf("Message type not supported: %s", fd.GetMessageType().GetFullyQualifiedName()))
		}
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		return &proto.ColInt32{}
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		return &proto.ColBool{}
	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_SINT32, descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		return &proto.ColInt32{}
	case descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_SINT64, descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		return &proto.ColInt64{}
	case descriptor.FieldDescriptorProto_TYPE_UINT64, descriptor.FieldDescriptorProto_TYPE_FIXED64:
		return &proto.ColUInt64{}
	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_FIXED32:
		return &proto.ColUInt32{}
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return &proto.ColFloat32{}
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return &proto.ColFloat64{}
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		return &proto.ColStr{}
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		return &proto.ColBytes{}
	default:
		panic(fmt.Sprintf("unsupported type: %s", fd.GetType()))
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
		s = "'" + base64.StdEncoding.EncodeToString(v) + "'"
	case bool:
		s = strconv.FormatBool(v)
	case time.Time:
		s = "'" + v.Format(time.DateTime) + "'"
	case *timestamppb.Timestamp:
		s = "'" + v.AsTime().Format(time.DateTime) + "'"
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
	return
}

// ConvertSemanticValue converts a value according to semantic type and format hint for ClickHouse
func ConvertSemanticValue(semanticType sql2.SemanticType, value interface{}, formatHint string) (string, error) {
	switch semanticType {
	case sql2.SemanticUint256:
		return convertToString(value)
	case sql2.SemanticInt256:
		return convertToString(value)
	case sql2.SemanticAddress:
		return convertToFixedStringAddress(value)
	case sql2.SemanticHash:
		return convertToFixedStringHash(value)
	case sql2.SemanticSignature, sql2.SemanticPubkey, sql2.SemanticHex:
		return convertToString(value)
	case sql2.SemanticJSON:
		return convertToString(value) // ClickHouse stores JSON as String
	case sql2.SemanticUUID:
		return convertToString(value) // ClickHouse stores UUID as String
	case sql2.SemanticUnixTimestamp, sql2.SemanticBlockTimestamp:
		return convertUnixTimestamp(value, false)
	case sql2.SemanticUnixTimestampMS:
		return convertUnixTimestamp(value, true)
	default:
		// Fallback to default value conversion
		return ValueToString(value), nil
	}
}

// convertToUInt256 converts values to ClickHouse UInt256 type
func convertToUInt256(value interface{}, formatHint string) (string, error) {
	switch v := value.(type) {
	case string:
		// Handle hex strings (0x...)
		if strings.HasPrefix(v, "0x") {
			return v, nil // ClickHouse UInt256 can handle hex directly
		}
		// Handle decimal strings
		if formatHint == "hex" && !strings.HasPrefix(v, "0x") {
			// Add 0x prefix for ClickHouse UInt256
			return "0x" + v, nil
		}
		return v, nil
	case []byte:
		// Convert bytes to hex for ClickHouse UInt256
		return "0x" + hex.EncodeToString(v), nil
	case int64, uint64, int32, uint32:
		// Convert numeric types to string
		return fmt.Sprintf("%v", v), nil
	default:
		return "", fmt.Errorf("cannot convert %T to UInt256", value)
	}
}

// convertToInt256 converts values to ClickHouse Int256 type
func convertToInt256(value interface{}, formatHint string) (string, error) {
	switch v := value.(type) {
	case string:
		// Handle hex strings (0x...)
		if strings.HasPrefix(v, "0x") {
			return v, nil // ClickHouse Int256 can handle hex directly
		}
		// Handle decimal strings
		if formatHint == "hex" && !strings.HasPrefix(v, "0x") {
			// Add 0x prefix for ClickHouse Int256
			return "0x" + v, nil
		}
		return v, nil
	case []byte:
		// Convert bytes to hex for ClickHouse Int256
		return "0x" + hex.EncodeToString(v), nil
	case int64, uint64, int32, uint32:
		// Convert numeric types to string
		return fmt.Sprintf("%v", v), nil
	default:
		return "", fmt.Errorf("cannot convert %T to Int256", value)
	}
}

// convertToFixedStringAddress converts values to ClickHouse FixedString(42) format
func convertToFixedStringAddress(value interface{}) (string, error) {
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

// convertToFixedStringHash converts values to ClickHouse FixedString(66) format
func convertToFixedStringHash(value interface{}) (string, error) {
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

// convertToString converts values to ClickHouse String format
func convertToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(strings.ReplaceAll(v, "'", "''"), "\\", "\\\\") + "'", nil
	case []byte:
		// For binary data, encode as hex
		return "'0x" + hex.EncodeToString(v) + "'", nil
	default:
		return fmt.Sprintf("'%v'", value), nil
	}
}

// convertToDecimal converts values to ClickHouse decimal format
func convertToDecimal(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil // ClickHouse decimals accept string literals directly
	case float64, float32:
		return fmt.Sprintf("%v", v), nil
	case int64, uint64, int32, uint32, int, uint:
		return fmt.Sprintf("%v", v), nil
	default:
		return "", fmt.Errorf("cannot convert %T to decimal", value)
	}
}

// convertUnixTimestamp converts unix timestamps to ClickHouse timestamp format
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

	if isMilliseconds {
		// ClickHouse DateTime64 format with milliseconds
		return "'" + t.UTC().Format("2006-01-02 15:04:05.000") + "'", nil
	}
	return "'" + t.UTC().Format("2006-01-02 15:04:05") + "'", nil
}
