package sql

import "fmt"

// SemanticType represents a high-level semantic meaning for a field
// that can be mapped to optimal SQL types per database dialect
type SemanticType string

const (
	// Blockchain/Crypto types
	SemanticUint256   SemanticType = "uint256"   // 256-bit unsigned integer
	SemanticInt256    SemanticType = "int256"    // 256-bit signed integer
	SemanticAddress   SemanticType = "address"   // Blockchain address (42 chars with 0x prefix)
	SemanticHash      SemanticType = "hash"      // Cryptographic hash (66 chars with 0x prefix)
	SemanticSignature SemanticType = "signature" // Cryptographic signature
	SemanticPubkey    SemanticType = "pubkey"    // Public key

	// Precision numeric types
	SemanticDecimal18 SemanticType = "decimal18" // 18 decimal places (common in DeFi)
	SemanticDecimal6  SemanticType = "decimal6"  // 6 decimal places (USDC, etc.)
	SemanticDecimal8  SemanticType = "decimal8"  // 8 decimal places (Bitcoin)
	SemanticMoney     SemanticType = "money"     // Currency/monetary values

	// Text/Binary types
	SemanticHex    SemanticType = "hex"    // Hexadecimal string
	SemanticBase64 SemanticType = "base64" // Base64 encoded data
	SemanticJSON   SemanticType = "json"   // JSON data
	SemanticUUID   SemanticType = "uuid"   // UUID string

	// Time types
	SemanticUnixTimestamp   SemanticType = "unix_timestamp"    // Unix timestamp (seconds)
	SemanticUnixTimestampMS SemanticType = "unix_timestamp_ms" // Unix timestamp (milliseconds)
	SemanticBlockTimestamp  SemanticType = "block_timestamp"   // Blockchain timestamp
)

// SemanticTypeInfo contains metadata about a semantic type
type SemanticTypeInfo struct {
	Name        SemanticType
	Description string
	DefaultSQL  string // Fallback SQL type when dialect doesn't support it
	Validation  string // Optional validation pattern/rules
}

// SemanticTypeRegistry contains all supported semantic types with their metadata
var SemanticTypeRegistry = map[SemanticType]SemanticTypeInfo{
	SemanticUint256: {
		Name:        SemanticUint256,
		Description: "256-bit unsigned integer for large blockchain values",
		DefaultSQL:  "VARCHAR", // Safe fallback for unsupported dialects
		Validation:  "numeric",
	},
	SemanticInt256: {
		Name:        SemanticInt256,
		Description: "256-bit signed integer for large blockchain values",
		DefaultSQL:  "VARCHAR",
		Validation:  "numeric",
	},
	SemanticAddress: {
		Name:        SemanticAddress,
		Description: "Blockchain address (42 characters with 0x prefix)",
		DefaultSQL:  "VARCHAR(42)",
		Validation:  "^0x[a-fA-F0-9]{40}$",
	},
	SemanticHash: {
		Name:        SemanticHash,
		Description: "Cryptographic hash (66 characters with 0x prefix)",
		DefaultSQL:  "VARCHAR(66)",
		Validation:  "^0x[a-fA-F0-9]{64}$",
	},
	SemanticSignature: {
		Name:        SemanticSignature,
		Description: "Cryptographic signature (variable length hex)",
		DefaultSQL:  "VARCHAR",
		Validation:  "hex",
	},
	SemanticPubkey: {
		Name:        SemanticPubkey,
		Description: "Public key (variable length hex)",
		DefaultSQL:  "VARCHAR",
		Validation:  "hex",
	},
	SemanticDecimal18: {
		Name:        SemanticDecimal18,
		Description: "Decimal with 18 decimal places (DeFi standard)",
		DefaultSQL:  "NUMERIC(78,18)",
		Validation:  "decimal",
	},
	SemanticDecimal6: {
		Name:        SemanticDecimal6,
		Description: "Decimal with 6 decimal places (USDC standard)",
		DefaultSQL:  "NUMERIC(38,6)",
		Validation:  "decimal",
	},
	SemanticDecimal8: {
		Name:        SemanticDecimal8,
		Description: "Decimal with 8 decimal places (Bitcoin standard)",
		DefaultSQL:  "NUMERIC(28,8)",
		Validation:  "decimal",
	},
	SemanticMoney: {
		Name:        SemanticMoney,
		Description: "Monetary value with currency precision",
		DefaultSQL:  "NUMERIC(19,4)",
		Validation:  "decimal",
	},
	SemanticHex: {
		Name:        SemanticHex,
		Description: "Hexadecimal string data",
		DefaultSQL:  "VARCHAR",
		Validation:  "hex",
	},
	SemanticBase64: {
		Name:        SemanticBase64,
		Description: "Base64 encoded binary data",
		DefaultSQL:  "TEXT",
		Validation:  "base64",
	},
	SemanticJSON: {
		Name:        SemanticJSON,
		Description: "JSON structured data",
		DefaultSQL:  "TEXT",
		Validation:  "json",
	},
	SemanticUUID: {
		Name:        SemanticUUID,
		Description: "UUID identifier",
		DefaultSQL:  "VARCHAR(36)",
		Validation:  "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
	},
	SemanticUnixTimestamp: {
		Name:        SemanticUnixTimestamp,
		Description: "Unix timestamp in seconds",
		DefaultSQL:  "TIMESTAMP WITH TIME ZONE",
		Validation:  "unix_timestamp",
	},
	SemanticUnixTimestampMS: {
		Name:        SemanticUnixTimestampMS,
		Description: "Unix timestamp in milliseconds",
		DefaultSQL:  "TIMESTAMP WITH TIME ZONE",
		Validation:  "unix_timestamp_ms",
	},
	SemanticBlockTimestamp: {
		Name:        SemanticBlockTimestamp,
		Description: "Blockchain block timestamp",
		DefaultSQL:  "TIMESTAMP WITH TIME ZONE",
		Validation:  "unix_timestamp",
	},
}

// IsValidSemanticType checks if a semantic type is supported
func IsValidSemanticType(semanticType string) bool {
	_, exists := SemanticTypeRegistry[SemanticType(semanticType)]
	return exists
}

// GetSemanticTypeInfo returns metadata for a semantic type
func GetSemanticTypeInfo(semanticType string) (SemanticTypeInfo, error) {
	info, exists := SemanticTypeRegistry[SemanticType(semanticType)]
	if !exists {
		return SemanticTypeInfo{}, fmt.Errorf("unsupported semantic type: %s", semanticType)
	}
	return info, nil
}

// SemanticTypeMapper defines the interface for dialect-specific semantic type mapping
type SemanticTypeMapper interface {
	// MapSemanticType maps a semantic type to dialect-specific SQL type
	MapSemanticType(semanticType SemanticType) (sqlType string, supported bool)
	
	// ConvertValue converts a value according to semantic type and format hint
	ConvertValue(semanticType SemanticType, value interface{}, formatHint string) (string, error)
	
	// SupportsSemanticType returns true if the dialect supports the semantic type
	SupportsSemanticType(semanticType SemanticType) bool
}

// FormatHint provides guidance for value conversion
type FormatHint string

const (
	FormatHintHex     FormatHint = "hex"     // Hexadecimal format
	FormatHintDecimal FormatHint = "decimal" // Decimal format
	FormatHintBase64  FormatHint = "base64"  // Base64 format
	FormatHintString  FormatHint = "string"  // String format
)