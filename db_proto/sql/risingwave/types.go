package risingwave

import (
	"fmt"
	"math/big"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
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
	TypeVarchar DataType = "VARCHAR" // Variable-length character string (no length limit specified)
	TypeText    DataType = "VARCHAR" // Use VARCHAR instead of TEXT for RisingWave compatibility

	// Large integer type (stored as VARCHAR for safety)
	TypeUint256 DataType = "VARCHAR" // uint256 stored as VARCHAR to prevent data loss

	// Binary type
	TypeBytea DataType = "BYTEA" // Binary strings (hex format)

	// Date and time types
	TypeDate        DataType = "DATE"                     // Calendar date (year, month, day)
	TypeTime        DataType = "TIME"                     // Time of day (no time zone)
	TypeTimestamp   DataType = "TIMESTAMP"                // Date and time (no time zone)
	TypeTimestamptz DataType = "TIMESTAMP WITH TIME ZONE" // Timestamp with time zone
	TypeInterval    DataType = "INTERVAL"                 // Time span

	// Complex types - basic definitions
	TypeStruct DataType = "STRUCT" // Nested data structure
	TypeArray  DataType = "ARRAY"  // Ordered list of elements
	TypeMap    DataType = "MAP"    // Key-value pairs
	TypeJsonb  DataType = "JSONB"  // Binary JSON value
)

// uint256 maximum value for validation
var uint256Max *big.Int

// Configuration for uint256 field mapping
type Uint256Config struct {
	// ExplicitFields contains field names that should always be treated as uint256
	ExplicitFields map[string]bool
	// DisableAutoDetection disables automatic field name detection
	DisableAutoDetection bool
}

var uint256Config *Uint256Config

func init() {
	// Initialize uint256 maximum value: 2^256 - 1
	uint256Max = new(big.Int)
	uint256Max.Exp(big.NewInt(2), big.NewInt(256), nil)
	uint256Max.Sub(uint256Max, big.NewInt(1))

	// Initialize configuration
	uint256Config = &Uint256Config{
		ExplicitFields: make(map[string]bool),
	}

	// Load configuration from environment variables
	loadUint256Config()
}

// loadUint256Config loads uint256 field configuration from environment variables
func loadUint256Config() {
	// Environment variable format: SINK_SQL_UINT256_FIELDS=field1,field2,field3
	if fieldsEnv := os.Getenv("SINK_SQL_UINT256_FIELDS"); fieldsEnv != "" {
		fields := strings.Split(fieldsEnv, ",")
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if field != "" {
				uint256Config.ExplicitFields[field] = true
			}
		}
	}

	// Environment variable to disable auto-detection: SINK_SQL_UINT256_AUTO_DETECT=false
	if autoDetectEnv := os.Getenv("SINK_SQL_UINT256_AUTO_DETECT"); autoDetectEnv != "" {
		if autoDetectEnv == "false" || autoDetectEnv == "0" {
			uint256Config.DisableAutoDetection = true
		}
	}
}

// SetUint256Fields allows programmatic configuration of uint256 fields
func SetUint256Fields(fields []string) {
	uint256Config.ExplicitFields = make(map[string]bool)
	for _, field := range fields {
		if field != "" {
			uint256Config.ExplicitFields[field] = true
		}
	}
}

// SetUint256AutoDetection enables or disables automatic field detection
func SetUint256AutoDetection(enabled bool) {
	uint256Config.DisableAutoDetection = !enabled
}

// Regular expressions for uint256 detection
var (
	// Matches pure numeric strings (decimal)
	uint256DecimalRegex = regexp.MustCompile(`^[0-9]+$`)
	// Matches hex strings (with or without 0x/0X prefix)
	uint256HexRegex = regexp.MustCompile(`^(0[xX])?[0-9a-fA-F]+$`)
	// Common Ethereum field names that typically contain uint256 values
	uint256FieldNames = regexp.MustCompile(`(?i)(amount|value|balance|supply|price|wei|gwei|ether|token|quantity|count|total|sum|fee|gas|nonce|block_number|timestamp)`)
)

func (s DataType) String() string {
	return string(s)
}

// IsUint256Field detects if a field likely contains uint256 values
func IsUint256Field(fieldName string, fieldType descriptor.FieldDescriptorProto_Type) bool {
	// Only check string fields for uint256 content
	if fieldType != descriptor.FieldDescriptorProto_TYPE_STRING {
		return false
	}

	// Check explicit configuration first
	if uint256Config.ExplicitFields[fieldName] {
		return true
	}

	// Skip auto-detection if disabled
	if uint256Config.DisableAutoDetection {
		return false
	}

	// Check if field name suggests uint256 content
	return uint256FieldNames.MatchString(fieldName)
}

// IsUint256String validates if a string represents a valid uint256 value
func IsUint256String(value string) bool {
	if value == "" {
		return false
	}

	// Handle hex format
	if strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X") {
		if !uint256HexRegex.MatchString(value) {
			return false
		}
		// Remove 0x prefix for parsing
		value = value[2:]
		if len(value) == 0 {
			return false
		}
		// Check if hex string is too long (256 bits = 64 hex chars max)
		if len(value) > 64 {
			return false
		}
		// Try to parse as hex
		num := new(big.Int)
		_, ok := num.SetString(value, 16)
		return ok && num.Cmp(uint256Max) <= 0
	}

	// Handle decimal format
	if !uint256DecimalRegex.MatchString(value) {
		return false
	}

	// Try to parse as decimal and check range
	num := new(big.Int)
	_, ok := num.SetString(value, 10)
	return ok && num.Sign() >= 0 && num.Cmp(uint256Max) <= 0
}

func IsWellKnownType(fd *desc.FieldDescriptor) bool {
	switch fd.GetMessageType().GetFullyQualifiedName() {
	case "google.protobuf.Timestamp":
		return true
	default:
		return false
	}
}

func MapFieldType(fd *desc.FieldDescriptor) DataType {
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
		// Use VARCHAR for uint64 to avoid precision loss in NUMERIC (28 digits max)
		return TypeVarchar
	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_FIXED32:
		return TypeBigInt // Use BIGINT for 32-bit unsigned (to avoid overflow)
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return TypeReal // Use REAL for single precision
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return TypeDouble
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		// Check if this string field likely contains uint256 values
		if IsUint256Field(fd.GetName(), t) {
			return TypeUint256 // Maps to VARCHAR but indicates uint256 content
		}
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
		// Check if this string represents a uint256 value
		if IsUint256String(v) {
			// For uint256 strings, ensure proper SQL escaping
			escaped := strings.ReplaceAll(strings.ReplaceAll(v, "'", "''"), "\\", "\\\\")
			s = "'" + escaped + "'"
		} else {
			// Regular string handling
			s = "'" + strings.ReplaceAll(strings.ReplaceAll(v, "'", "''"), "\\", "\\\\") + "'"
		}
	case int64:
		s = strconv.FormatInt(v, 10)
	case int32:
		s = strconv.FormatInt(int64(v), 10)
	case int:
		s = strconv.FormatInt(int64(v), 10)
	case uint64:
		// Store large unsigned integers as quoted strings to avoid NUMERIC precision loss
		s = "'" + strconv.FormatUint(v, 10) + "'"
	case uint32:
		s = strconv.FormatUint(uint64(v), 10)
	case uint:
		// For very large uint values, use string representation
		if v > 9223372036854775807 { // > max int64
			s = "'" + strconv.FormatUint(uint64(v), 10) + "'"
		} else {
			s = strconv.FormatUint(uint64(v), 10)
		}
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

// Helper functions for safe uint256 operations

// IsSafeForRwInt256 checks if a uint256 string value can be safely cast to rw_int256
// Returns true if the value is within the signed 256-bit range (-2^255 to 2^255-1)
func IsSafeForRwInt256(value string) bool {
	if !IsUint256String(value) {
		return false
	}

	// Parse the value to check if it fits in signed 256-bit range
	num := new(big.Int)
	var ok bool

	// Handle hex format
	if strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X") {
		_, ok = num.SetString(value[2:], 16)
	} else {
		_, ok = num.SetString(value, 10)
	}

	if !ok {
		return false
	}

	// Check if value is within rw_int256 range (0 to 2^255-1 for positive values)
	maxSafeValue := new(big.Int)
	maxSafeValue.Exp(big.NewInt(2), big.NewInt(255), nil)
	maxSafeValue.Sub(maxSafeValue, big.NewInt(1))

	return num.Cmp(maxSafeValue) <= 0
}

// GenerateSafeCastSQL generates SQL for safely casting uint256 VARCHAR to rw_int256
// Returns SQL that casts to rw_int256 when safe, NULL otherwise
func GenerateSafeCastSQL(columnName string) string {
	return fmt.Sprintf(`CASE 
		WHEN LENGTH(%s) <= 75 AND %s ~ '^[0-9]+$' AND %s NOT LIKE '9%%'
		THEN CAST(%s AS rw_int256)
		ELSE NULL 
	END`, columnName, columnName, columnName, columnName)
}

// GenerateHexCastSQL generates SQL for safely converting hex uint256 to rw_int256
func GenerateHexCastSQL(columnName string) string {
	return fmt.Sprintf(`CASE 
		WHEN %s ~ '^0[xX][0-9a-fA-F]+$' AND LENGTH(%s) <= 66
		THEN hex_to_int256(%s)
		ELSE NULL 
	END`, columnName, columnName, columnName)
}

// GetUint256StorageInfo returns information about how uint256 values are stored
func GetUint256StorageInfo() map[string]interface{} {
	return map[string]interface{}{
		"storage_type":       "VARCHAR",
		"max_value":          uint256Max.String(),
		"safe_rw_int256_max": "57896044618658097711785492504343953926634992332820282019728792003956564819967", // 2^255-1
		"supported_formats":  []string{"decimal", "hex (0x/0X prefix)"},
		"auto_detected_fields": []string{
			"amount", "value", "balance", "supply", "price", "wei", "gwei", "ether",
			"token", "quantity", "count", "total", "sum", "fee", "gas", "nonce", "block_number", "timestamp",
		},
		"configuration": map[string]string{
			"explicit_fields_env": "SINK_SQL_UINT256_FIELDS",
			"auto_detect_env":     "SINK_SQL_UINT256_AUTO_DETECT",
		},
	}
}

// ValidateUint256Value validates a uint256 string and returns detailed information
func ValidateUint256Value(value string) map[string]interface{} {
	result := map[string]interface{}{
		"value":     value,
		"is_valid":  false,
		"format":    "unknown",
		"safe_cast": false,
		"length":    len(value),
		"errors":    []string{},
	}

	if value == "" {
		result["errors"] = append(result["errors"].([]string), "empty value")
		return result
	}

	// Check format
	var num *big.Int
	var ok bool

	if strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X") {
		result["format"] = "hex"
		if !uint256HexRegex.MatchString(value) {
			result["errors"] = append(result["errors"].([]string), "invalid hex format")
			return result
		}
		hexValue := value[2:]
		if len(hexValue) > 64 {
			result["errors"] = append(result["errors"].([]string), "hex value too long (>64 chars)")
			return result
		}
		num = new(big.Int)
		_, ok = num.SetString(hexValue, 16)
	} else {
		result["format"] = "decimal"
		if !uint256DecimalRegex.MatchString(value) {
			result["errors"] = append(result["errors"].([]string), "invalid decimal format")
			return result
		}
		num = new(big.Int)
		_, ok = num.SetString(value, 10)
	}

	if !ok {
		result["errors"] = append(result["errors"].([]string), "failed to parse number")
		return result
	}

	// Check range
	if num.Sign() < 0 {
		result["errors"] = append(result["errors"].([]string), "negative value (uint256 must be >= 0)")
		return result
	}

	if num.Cmp(uint256Max) > 0 {
		result["errors"] = append(result["errors"].([]string), "value exceeds uint256 maximum")
		return result
	}

	result["is_valid"] = true

	// Check if safe for rw_int256
	maxSafeValue := new(big.Int)
	maxSafeValue.Exp(big.NewInt(2), big.NewInt(255), nil)
	maxSafeValue.Sub(maxSafeValue, big.NewInt(1))

	if num.Cmp(maxSafeValue) <= 0 {
		result["safe_cast"] = true
	}

	return result
}
