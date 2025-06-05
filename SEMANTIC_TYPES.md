# Semantic Type Annotations for SQL Schema Generation

## Overview

The semantic type annotation system allows you to specify high-level semantic meanings for protobuf fields that get automatically mapped to optimal SQL types for each database dialect. This enables support for specialized types like RisingWave's `rw_int256` while maintaining compatibility across PostgreSQL, RisingWave, and ClickHouse.

## Quick Start

1. Import the schema annotations in your protobuf:
```protobuf
import "sf/substreams/sink/sql/schema/v1/schema.proto";
```

2. Add semantic type annotations to your fields:
```protobuf
message EthereumTransaction {
  option (sf.substreams.sink.sql.schema.v1.table) = { name: "eth_transactions" };
  
  string hash = 1 [(sf.substreams.sink.sql.schema.v1.field) = {
    primary_key: true,
    semantic_type: "hash"  // Optimized hash storage
  }];
  
  string value = 2 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "uint256",  // Uses RisingWave's rw_int256
    format_hint: "decimal"
  }];
  
  string from_address = 3 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "address"  // Blockchain address format
  }];
}
```

## Supported Semantic Types

### Blockchain/Crypto Types

| Semantic Type | Description | RisingWave | PostgreSQL | ClickHouse |
|---------------|-------------|------------|------------|------------|
| `uint256` | 256-bit unsigned integer | `rw_int256` | `NUMERIC(78,0)` | `String` |
| `int256` | 256-bit signed integer | `rw_int256` | `NUMERIC(78,0)` | `String` |
| `address` | Blockchain address (42 chars) | `VARCHAR(42)` | `CHAR(42)` | `FixedString(42)` |
| `hash` | Cryptographic hash (66 chars) | `VARCHAR(66)` | `CHAR(66)` | `FixedString(66)` |
| `signature` | Cryptographic signature | `VARCHAR` | `VARCHAR` | `String` |
| `pubkey` | Public key | `VARCHAR` | `VARCHAR` | `String` |

### Precision Numeric Types

| Semantic Type | Description | RisingWave | PostgreSQL | ClickHouse |
|---------------|-------------|------------|------------|------------|
| `decimal18` | 18 decimal places (DeFi standard) | `NUMERIC(78,18)` | `NUMERIC(78,18)` | `Decimal128(18)` |
| `decimal6` | 6 decimal places (USDC standard) | `NUMERIC(38,6)` | `NUMERIC(38,6)` | `Decimal64(6)` |
| `decimal8` | 8 decimal places (Bitcoin standard) | `NUMERIC(28,8)` | `NUMERIC(28,8)` | `Decimal64(8)` |
| `money` | Currency/monetary values | `NUMERIC(19,4)` | `NUMERIC(19,4)` | `Decimal64(4)` |

### Text/Binary Types

| Semantic Type | Description | RisingWave | PostgreSQL | ClickHouse |
|---------------|-------------|------------|------------|------------|
| `hex` | Hexadecimal string | `VARCHAR` | `VARCHAR` | `String` |
| `base64` | Base64 encoded data | `VARCHAR` | `TEXT` | `String` |
| `json` | JSON structured data | `JSONB` | `JSONB` | `String` |
| `uuid` | UUID identifier | `VARCHAR(36)` | `UUID` | `String` |

### Time Types

| Semantic Type | Description | RisingWave | PostgreSQL | ClickHouse |
|---------------|-------------|------------|------------|------------|
| `unix_timestamp` | Unix timestamp (seconds) | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP WITH TIME ZONE` | `DateTime` |
| `unix_timestamp_ms` | Unix timestamp (milliseconds) | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP WITH TIME ZONE` | `DateTime64(3)` |
| `block_timestamp` | Blockchain timestamp | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP WITH TIME ZONE` | `DateTime` |

## Format Hints

Format hints provide additional guidance for value conversion:

| Format Hint | Description | Usage |
|-------------|-------------|-------|
| `hex` | Hexadecimal format | For `uint256`, `int256` fields containing hex strings |
| `decimal` | Decimal format | For numeric fields containing decimal strings |
| `base64` | Base64 format | For binary data encoded as base64 |
| `string` | String format | Default string handling |

## Complete Example

```protobuf
syntax = "proto3";
import "sf/substreams/sink/sql/schema/v1/schema.proto";

message EthereumTransaction {
  option (sf.substreams.sink.sql.schema.v1.table) = { name: "eth_transactions" };
  
  // Hash fields - optimized storage
  string tx_hash = 1 [(sf.substreams.sink.sql.schema.v1.field) = {
    primary_key: true,
    semantic_type: "hash"
  }];
  
  string block_hash = 2 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "hash"
  }];
  
  // Large integers - uses rw_int256 in RisingWave
  string value = 3 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "uint256",
    format_hint: "decimal"
  }];
  
  string gas_price = 4 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "uint256",
    format_hint: "hex"
  }];
  
  // Addresses - validated format
  string from_address = 5 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "address"
  }];
  
  string to_address = 6 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "address"
  }];
  
  // Precision decimals for token amounts
  string amount_18_decimals = 7 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "decimal18"
  }];
  
  string usdc_amount = 8 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "decimal6"
  }];
  
  // Timestamps
  int64 block_timestamp = 9 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "unix_timestamp"
  }];
  
  // JSON metadata
  string metadata = 10 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "json"
  }];
  
  // UUID tracking
  string trace_id = 11 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "uuid"
  }];
}
```

## Generated SQL Examples

### RisingWave Output
```sql
CREATE TABLE eth_transactions (
  tx_hash VARCHAR(66),              -- hash semantic type
  block_hash VARCHAR(66),           -- hash semantic type
  value rw_int256,                  -- uint256 → rw_int256 (RisingWave-specific)
  gas_price rw_int256,              -- uint256 → rw_int256
  from_address VARCHAR(42),         -- address semantic type
  to_address VARCHAR(42),           -- address semantic type
  amount_18_decimals NUMERIC(78,18), -- decimal18 semantic type
  usdc_amount NUMERIC(38,6),        -- decimal6 semantic type
  block_timestamp TIMESTAMP WITH TIME ZONE, -- unix_timestamp
  metadata JSONB,                   -- json semantic type
  trace_id VARCHAR(36)              -- uuid semantic type
);

-- Sample insert with rw_int256 casting
INSERT INTO eth_transactions VALUES (
  '0x1234...abcd',
  '0x5678...efab', 
  '115792089237316195423570985008687907853269984665640564039457584007913129639935'::rw_int256,
  '0x1bc16d674ec80000'::rw_int256,
  '0x742d35cc6636C0532925a3b8D0A3e5A5F2d5De8e',
  '0x8ba1f109551bD432803012645Hac136c5ae5c9e6',
  '1000.123456789012345678',
  '1000.123456',
  '2024-01-01 00:00:00+00',
  '{"type": "transfer"}'::jsonb,
  '550e8400-e29b-41d4-a716-446655440000'
);
```

### PostgreSQL Output
```sql
CREATE TABLE eth_transactions (
  tx_hash CHAR(66),                 -- hash semantic type
  block_hash CHAR(66),              -- hash semantic type  
  value NUMERIC(78,0),              -- uint256 → NUMERIC fallback
  gas_price NUMERIC(78,0),          -- uint256 → NUMERIC fallback
  from_address CHAR(42),            -- address semantic type
  to_address CHAR(42),              -- address semantic type
  amount_18_decimals NUMERIC(78,18), -- decimal18 semantic type
  usdc_amount NUMERIC(38,6),        -- decimal6 semantic type
  block_timestamp TIMESTAMP WITH TIME ZONE, -- unix_timestamp
  metadata JSONB,                   -- json semantic type
  trace_id UUID                     -- uuid → PostgreSQL UUID type
);
```

### ClickHouse Output
```sql
CREATE TABLE eth_transactions (
  tx_hash FixedString(66),          -- hash semantic type
  block_hash FixedString(66),       -- hash semantic type
  value String,                     -- uint256 → String fallback  
  gas_price String,                 -- uint256 → String fallback
  from_address FixedString(42),     -- address semantic type
  to_address FixedString(42),       -- address semantic type
  amount_18_decimals Decimal128(18), -- decimal18 semantic type
  usdc_amount Decimal64(6),         -- decimal6 semantic type
  block_timestamp DateTime,         -- unix_timestamp
  metadata String,                  -- json → String fallback
  trace_id String                   -- uuid → String fallback
) ENGINE = ReplacingMergeTree(version);
```

## Value Conversion Examples

### RisingWave rw_int256 Conversion

**Input Values:**
```protobuf
// In your protobuf data
value: "115792089237316195423570985008687907853269984665640564039457584007913129639935"
gas_price: "0x1bc16d674ec80000"
```

**Generated SQL:**
```sql
-- Decimal format
INSERT INTO table VALUES ('115792089237316195423570985008687907853269984665640564039457584007913129639935'::rw_int256);

-- Hex format  
INSERT INTO table VALUES ('0x1bc16d674ec80000'::rw_int256);
```

### Address Validation

**Valid Formats:**
```protobuf
from_address: "0x742d35cc6636C0532925a3b8D0A3e5A5F2d5De8e"  // 42 chars with 0x
to_address: "742d35cc6636C0532925a3b8D0A3e5A5F2d5De8e"    // 40 chars, 0x added automatically
```

**Generated SQL:**
```sql
INSERT INTO table VALUES (
  '0x742d35cc6636C0532925a3b8D0A3e5A5F2d5De8e',
  '0x742d35cc6636C0532925a3b8D0A3e5A5F2d5De8e'
);
```

### Timestamp Conversion

**Input Values:**
```protobuf
block_timestamp: 1640995200        // Unix timestamp in seconds
updated_at_ms: 1640995200000      // Unix timestamp in milliseconds  
```

**Generated SQL:**
```sql
INSERT INTO table VALUES (
  '2022-01-01 00:00:00+00',        -- Converted from unix timestamp
  '2022-01-01 00:00:00+00'         -- Converted from unix timestamp ms
);
```

## Migration from Existing Schemas

### Step 1: Add Semantic Types Gradually
```protobuf
message Transaction {
  // Existing field without semantic type
  string hash = 1;
  
  // New field with semantic type
  string block_hash = 2 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "hash"
  }];
  
  // Existing large number field
  string value = 3;
  
  // Updated with semantic type for better performance
  string gas_price = 4 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "uint256",
    format_hint: "hex"
  }];
}
```

### Step 2: Update All Fields
```protobuf
message Transaction {
  // All fields now use semantic types
  string hash = 1 [(sf.substreams.sink.sql.schema.v1.field) = {
    primary_key: true,
    semantic_type: "hash"
  }];
  
  string block_hash = 2 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "hash"
  }];
  
  string value = 3 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "uint256",
    format_hint: "decimal"
  }];
  
  string gas_price = 4 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "uint256", 
    format_hint: "hex"
  }];
}
```

## Best Practices

### 1. Choose Appropriate Semantic Types
- Use `uint256`/`int256` for large blockchain values that need arithmetic operations
- Use `address` for blockchain addresses to get validation and optimal storage
- Use `hash` for fixed-length hashes (transaction hashes, block hashes)
- Use appropriate decimal precision (`decimal6` for USDC, `decimal18` for most ERC20s)

### 2. Use Format Hints Consistently
- Add `format_hint: "hex"` for fields containing hexadecimal strings
- Add `format_hint: "decimal"` for fields containing decimal number strings
- Consistent format hints help with validation and conversion

### 3. Leverage RisingWave Features
- Use `uint256`/`int256` semantic types to take advantage of RisingWave's `rw_int256` type
- This enables efficient storage and arithmetic operations on large integers
- Falls back gracefully to `NUMERIC` types in PostgreSQL and `String` in ClickHouse

### 4. Plan for Multi-Dialect Deployment
- Test your schema generation across all target dialects
- Verify that fallback types meet your precision and performance requirements
- Consider dialect-specific optimizations for high-volume data

## Troubleshooting

### Common Issues

1. **Semantic type not recognized**
   - Ensure you've imported `sf/substreams/sink/sql/schema/v1/schema.proto`
   - Check that the semantic type name matches exactly (case-sensitive)

2. **Value conversion errors**
   - Verify format hints match your data format (`hex` vs `decimal`)
   - Check that address formats are valid (40 hex chars or 42 with 0x prefix)
   - Ensure timestamp values are valid Unix timestamps

3. **Type fallback unexpected**
   - Check if the dialect supports the semantic type
   - RisingWave supports more specialized types than PostgreSQL/ClickHouse
   - Review the semantic type mapping table above

### Debug Commands

```bash
# Test schema generation
substreams-sink-sql from-proto --help

# Validate protobuf syntax
buf lint proto/

# Test with specific dialect
substreams-sink-sql from-proto "postgresql://..." manifest.yaml
substreams-sink-sql from-proto "risingwave://..." manifest.yaml  
substreams-sink-sql from-proto "clickhouse://..." manifest.yaml
```

## Performance Impact

### RisingWave Benefits
- `rw_int256` provides native 256-bit arithmetic operations
- Optimized storage for large integers compared to string fallbacks  
- Better query performance for mathematical operations on blockchain data

### Storage Optimization
- `address` and `hash` types use fixed-length storage where supported
- Precision decimal types prevent unnecessary precision overhead
- JSON types enable efficient structured data queries

### Query Performance
- Semantic types enable database-specific optimizations
- Proper type selection improves index performance
- Reduced type conversion overhead in queries

This semantic type system provides a powerful way to leverage database-specific features like RisingWave's `rw_int256` while maintaining broad compatibility across different SQL databases.