# Protobuf Annotations Used by from-proto

## Overview

The from-proto path derives your SQL schema directly from protobuf descriptors. It recognizes a set of annotations that control table/column naming, relationships, constraints, dialect-specific table options, and semantic typing of fields.

When the package includes `sf/substreams/sink/sql/schema/v1/schema.proto`, from-proto honors these annotations (useProtoOption enabled). Without it, from-proto falls back to best‑effort inference (table = message name; simple fields become columns; no explicit constraints unless added by the dialect for system integrity).

This document explains all supported annotations, how each dialect uses them, and why they matter.

---

## Message Options: table

Import the annotations definition:

```protobuf
import "sf/substreams/sink/sql/schema/v1/schema.proto";
```

Annotate any message that should materialize as a table:

```protobuf
message Orders {
  option (sf.substreams.sink.sql.schema.v1.table) = {
    name: "orders"
    child_of: "accounts on id"  // optional parent relation
    clickhouse_table_options: {  // optional, ClickHouse only
      order_by_fields: [{ name: "order_id" }]
      partition_fields: [{ name: "_block_timestamp_", function: toYYYYMM }]
      replacing_fields: [{ name: "order_id" }]
      index_fields: [{ name: "idx_product", field_name: "product", type: set, granularity: 4 }]
    }
  };
  ...
}
```

Fields:
- name: Required. The SQL table name.
- child_of: Optional. Defines a parent/child relation: `"<parent_table> on <parent_pk_field>"`.
  - Postgres: Adds a NOT NULL parent reference column to the child table plus a FK to the parent’s PK. Also every table gets a FK to `_blocks_` on `_block_number_` (ON DELETE CASCADE).
  - RisingWave: Adds the parent reference column (no FK constraints; autocommit system).
  - ClickHouse: Adds the parent reference column (no FK constraints; column used for modeling joins).
- clickhouse_table_options: Optional, ClickHouse‑only. See “ClickHouse Table Options” below.

Defaults when no table option is present:
- With proto options (schema.proto present): messages without `(table)` are ignored (no table).
- Without proto options: every message becomes a table named after the message.

System columns added to every table:
- `_block_number_` (all dialects) — tracks the originating block.
- `_block_timestamp_` (all dialects).
- `_version_`, `_deleted_` (ClickHouse only) — used by ReplacingMergeTree and retraction modeling.

Primary keys when not specified explicitly:
- Postgres: PK only if specified via column annotation; else no table PK (but FK to `_blocks_`).
- RisingWave: If no explicit PK is set, a composite PK is created: `(_block_number_, <parent keys...>)` to preserve uniqueness in streaming mode.
- ClickHouse: PRIMARY KEY/ORDER BY derived from ClickHouse options or defaults (see below).

---

## Field Options: column

Annotate fields that should map to specific columns/constraints:

```protobuf
message Orders {
  option (sf.substreams.sink.sql.schema.v1.table) = { name: "orders" };

  string order_id = 1 [(sf.substreams.sink.sql.schema.v1.field) = {
    name: "order_id",
    primary_key: true,
    unique: true,                         // adds uniqueness constraint (dialect-specific)
    semantic_type: "hash",               // see Semantic Types below
    format_hint: "hex"                   // optional value format hint
  }];

  string account_id = 2 [(sf.substreams.sink.sql.schema.v1.field) = {
    foreign_key: "accounts on id"        // FK to accounts(id) (Postgres only enforces)
  }];
}
```

Fields:
- name: Optional. Overrides the SQL column name (default is the proto field name).
- primary_key: Optional. Marks this column as the table’s primary key.
  - Postgres: Adds a PK constraint.
  - RisingWave: Declares an inline PK.
  - ClickHouse: Used for defaults in ORDER BY/PRIMARY KEY where applicable.
- unique: Optional. Enforces uniqueness.
  - Postgres: Adds a unique constraint.
  - RisingWave: Emits `UNIQUE` in column definition.
  - ClickHouse: No native unique constraint — ignored (consider indexes).
- foreign_key: Optional. Declares a FK to another table: `"<table> on <field>"`.
  - Postgres: Adds a FK constraint to the referenced table/field.
  - RisingWave/ClickHouse: Presence is validated for existence, but no constraint is created.
- semantic_type, format_hint: Optional. See “Semantic Types & Format Hints”. Affects column type selection in each dialect. Value conversion helpers exist but are not applied automatically by from‑proto inserts (see “Runtime Conversion” below).

---

## ClickHouse Table Options

ClickHouse engines need explicit ORDER/PARTITION configuration for good performance and correctness. The `clickhouse_table_options` block lets you control this per table.

Fields (repeated lists):
- order_by_fields: Required for CH. Defines the ORDER BY tuple. Each item supports:
  - name: Column to order by (e.g., `_block_number_`, your PK, other fields).
  - descending: Optional.
  - function: Optional function wrapper (e.g., `toYYYYMM` for dates).
- partition_fields: Optional additional partition keys. If none is provided, the dialect adds a default month partition on `_block_timestamp_`.
- replacing_fields: Optional extra fields in `ReplacingMergeTree(version, <replacing_fields...>)` for conflict resolution.
- index_fields: Optional skip indexes to accelerate predicates:
  - name: Index name.
  - field_name: Column to index.
  - type: One of `minmax`, `set`, `ngrambf_v1`, `tokenbf_v1`, `bloom_filter`.
  - granularity: Index granularity.
  - function: Optional function wrapper.

Defaults when options are omitted:
- Engine: `ReplacingMergeTree(_version_)`.
- PARTITION BY: `toYYYYMM(_block_timestamp_)`.
- ORDER BY: if not provided, dialect defaults to PK or `_block_number_`.

---

## Semantic Types & Format Hints

Semantic types give the dialect a clue to select the best storage type for a field (e.g., 256‑bit integers, addresses, hashes). Format hints help interpret the incoming literal representation when conversion is needed (hex vs decimal, etc.).

Supported semantic types and their column type mappings:

Note: If a semantic type is not supported by a dialect, the dialect falls back to its default mapping for the underlying protobuf type.

## Supported Semantic Types

### Blockchain/Crypto Types

| Semantic Type | Description | RisingWave | PostgreSQL | ClickHouse |
|---------------|-------------|------------|------------|------------|
| `uint256` | 256-bit unsigned integer | `rw_uint256` | `NUMERIC(78,0)` | `String` |
| `int256` | 256-bit signed integer | `rw_int256` | `NUMERIC(78,0)` | `String` |
| `address` | Blockchain address (42 chars) | `CHARACTER VARYING` | `CHAR(42)` | `FixedString(42)` |
| `hash` | Cryptographic hash (66 chars) | `CHARACTER VARYING` | `CHAR(66)` | `FixedString(66)` |
| `signature` | Cryptographic signature | `CHARACTER VARYING` | `VARCHAR` | `String` |
| `pubkey` | Public key | `CHARACTER VARYING` | `VARCHAR` | `String` |


### Text/Binary Types

| Semantic Type | Description | RisingWave | PostgreSQL | ClickHouse |
|---------------|-------------|------------|------------|------------|
| `hex` | Hexadecimal string | `CHARACTER VARYING` | `VARCHAR` | `String` |
| `base64` | Base64 encoded data | `CHARACTER VARYING` | `TEXT` | `String` |
| `json` | JSON structured data | `JSONB` | `JSONB` | `String` |
| `uuid` | UUID identifier | `CHARACTER VARYING` | `UUID` | `String` |

### Time Types

| Semantic Type | Description | RisingWave | PostgreSQL | ClickHouse |
|---------------|-------------|------------|------------|------------|
| `unix_timestamp` | Unix timestamp (seconds) | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP WITH TIME ZONE` | `DateTime` |
| `unix_timestamp_ms` | Unix timestamp (milliseconds) | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP WITH TIME ZONE` | `DateTime64(3)` |
| `block_timestamp` | Blockchain timestamp | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP WITH TIME ZONE` | `DateTime` |

### Format Hints

Format hints provide additional guidance for value conversion (when conversions are used):

| Format Hint | Description | Usage |
|-------------|-------------|-------|
| `hex` | Hexadecimal format | For `uint256`, `int256` fields containing hex strings |
| `decimal` | Decimal format | For numeric fields containing decimal strings |
| `base64` | Base64 format | For binary data encoded as base64 |
| `string` | String format | Default string handling |

---

## Runtime Conversion (advanced)

The codebase contains per‑dialect helpers to convert annotated values at insert time (e.g., converting `uint256` hex to a decimal literal for PostgreSQL, or casting to `rw_uint256` in RisingWave). Today, from‑proto uses prepared statements and passes values as they appear in your message — it does not automatically apply semantic conversions. The annotations primarily affect column type selection.

Practical guidance:
- Emit values in the “natural” format for your chosen dialect when possible (e.g., strings for `rw_uint256` or `NUMERIC`).
- If you require strict conversions, adapt your Substreams output to provide appropriately typed/encoded values. The conversion helpers in `db_proto/sql/*/types.go` show how to transform values if you build a custom inserter.

---

## End‑to‑End Example (with annotations)

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
  
  // Large token amounts - use uint256 for full precision
  string token_amount = 7 [(sf.substreams.sink.sql.schema.v1.field) = {
    semantic_type: "uint256",
    format_hint: "decimal"
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
  tx_hash CHARACTER VARYING,       -- hash semantic type
  block_hash CHARACTER VARYING,    -- hash semantic type
  value rw_uint256,                 -- uint256 → rw_uint256 (RisingWave-specific)
  gas_price rw_uint256,             -- uint256 → rw_uint256
  from_address CHARACTER VARYING,  -- address semantic type
  to_address CHARACTER VARYING,    -- address semantic type
  token_amount rw_uint256,           -- uint256 semantic type
  block_timestamp TIMESTAMP WITH TIME ZONE, -- unix_timestamp
  metadata JSONB,                   -- json semantic type
  trace_id CHARACTER VARYING       -- uuid semantic type
);

-- Sample insert with rw_int256 casting
INSERT INTO eth_transactions VALUES (
  '0x1234...abcd',
  '0x5678...efab', 
  '115792089237316195423570985008687907853269984665640564039457584007913129639935'::rw_uint256,
  '0x1bc16d674ec80000'::rw_uint256,
  '0x742d35cc6636C0532925a3b8D0A3e5A5F2d5De8e',
  '0x8ba1f109551bD432803012645Hac136c5ae5c9e6',
  '1000123456789012345678'::rw_uint256,
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
  token_amount rw_uint256,           -- uint256 semantic type
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
  value String,                     -- uint256 → String (no native UInt256)  
  gas_price String,                 -- uint256 → String (no native UInt256)
  from_address FixedString(42),     -- address semantic type
  to_address FixedString(42),       -- address semantic type
  token_amount String,              -- uint256 semantic type
  block_timestamp DateTime,         -- unix_timestamp
  metadata String,                  -- json → String fallback
  trace_id String                   -- uuid → String fallback
) ENGINE = ReplacingMergeTree(version);
```

## Value Conversion Examples

### RisingWave rw_uint256 and rw_int256 Conversion

**Input Values:**
```protobuf
// In your protobuf data
value: "115792089237316195423570985008687907853269984665640564039457584007913129639935"
gas_price: "0x1bc16d674ec80000"
```

**Generated SQL:**
```sql
-- Decimal format for uint256
INSERT INTO table VALUES ('115792089237316195423570985008687907853269984665640564039457584007913129639935'::rw_uint256);

-- Hex format for uint256  
INSERT INTO table VALUES ('0x1bc16d674ec80000'::rw_uint256);

-- Signed values use rw_int256
INSERT INTO table VALUES ('-12345'::rw_int256);
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
- Use `uint256` for large blockchain values that need full 256-bit precision

### 2. Use Format Hints Consistently
- Add `format_hint: "hex"` for fields containing hexadecimal strings
- Add `format_hint: "decimal"` for fields containing decimal number strings
- Consistent format hints help with validation and conversion

### 3. Leverage Database-Specific Features
- **RisingWave**: Use `uint256`/`int256` semantic types to leverage `rw_uint256`/`rw_int256` for efficient 256-bit arithmetic
- **PostgreSQL**: Semantic types map to optimized native types like `UUID`, `JSONB`, and `NUMERIC` with proper precision
- **ClickHouse**: Leverages native `UInt256`/`Int256`, `FixedString`, and `Decimal` types for optimal performance
- All dialects gracefully handle unsupported semantic types with appropriate fallbacks

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
- `rw_uint256` and `rw_int256` provide native 256-bit arithmetic operations
- Proper unsigned/signed type distinction for accurate mathematical operations
- Optimized storage for large integers compared to string fallbacks  
- Better query performance for mathematical operations on blockchain data

### PostgreSQL Benefits
- Native `UUID` type for efficient UUID operations and indexing
- `JSONB` for structured data queries with GIN indexing support
- Fixed-length `CHAR` types for blockchain addresses and hashes provide storage optimization
- Proper `NUMERIC` precision prevents overflow issues with large numbers

### ClickHouse Benefits
- `FixedString` types provide optimal storage for fixed-length data like addresses and hashes
- Specialized `Decimal` types with configurable precision for financial calculations
- `String` type for large integers (256-bit values) with efficient columnar compression
- Columnar storage optimizations work best with proper type selection

### Storage Optimization
- `address` and `hash` types use fixed-length storage where supported across all dialects
- Precision decimal types prevent unnecessary precision overhead
- JSON types enable efficient structured data queries (JSONB in PostgreSQL/RisingWave)

### Query Performance
- Semantic types enable database-specific optimizations across all supported dialects
- Proper type selection improves index performance
- Reduced type conversion overhead in queries

This semantic type system provides a powerful way to leverage database-specific features like RisingWave's `rw_int256` while maintaining broad compatibility across different SQL databases.
