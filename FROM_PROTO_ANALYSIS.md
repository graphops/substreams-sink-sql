# Substreams SQL Sink: From-Proto Mode Analysis

## Overview

The `from-proto` mode in substreams-sink-sql enables dynamic SQL schema generation from protobuf message definitions. This mode analyzes your substream's output protobuf messages and automatically creates corresponding SQL tables, columns, and constraints based on protobuf field types and custom schema annotations.

## Table of Contents

- [Execution Flow](#execution-flow)
- [Schema Annotations Reference](#schema-annotations-reference)
- [Schema Processing Rules](#schema-processing-rules)
- [Database Dialect Support](#database-dialect-support)
- [Usage Examples](#usage-examples)
- [Configuration Options](#configuration-options)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Execution Flow

### 1. Command Entry Point
**File**: `cmd/substreams-sink-sql/from_proto.go:58`

```bash
substreams-sink-sql from-proto <dsn> <manifest> [output-module]
```

### 2. Schema Detection Process

1. **Manifest Parsing**: Reads substreams manifest and extracts protobuf definitions
2. **Dependency Analysis**: Checks if `sf/substreams/sink/sql/schema/v1/schema.proto` is imported
3. **Proto Option Detection**: Sets `useProtoOption=true` if schema annotations are available
4. **Constraint Configuration**: Enables constraints only when proto options are detected

### 3. Schema Generation Pipeline

```
Protobuf Messages → Schema Registry → SQL Tables → Constraint Application
```

**Key Files**:
- `db_proto/sql/schema/schema.go` - Main schema orchestration
- `db_proto/sql/schema/table.go` - Table creation logic
- `db_proto/sql/schema/column.go` - Column processing
- `proto/utils.go` - Annotation extraction

## Schema Annotations Reference

### Message-Level Annotations

**Extension**: `sf.substreams.sink.sql.schema.v1.table`
**Definition**: `proto/sf/substreams/sink/sql/schema/v1/schema.proto:16-22`

```protobuf
message Table {
  string name = 1;                    // Custom table name (required)
  optional string child_of = 2;       // Parent-child relationship
}
```

#### Supported Options

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `name` | string | Yes | Custom table name | `"customers"` |
| `child_of` | string | No | Parent table relationship | `"orders on order_id"` |

### Field-Level Annotations

**Extension**: `sf.substreams.sink.sql.schema.v1.field`
**Definition**: `proto/sf/substreams/sink/sql/schema/v1/schema.proto:24-29`

```protobuf
message Column {
  optional string name = 1;           // Custom column name
  optional string foreign_key = 2;    // Foreign key reference
  bool unique = 3;                    // Unique constraint
  bool primary_key = 4;               // Primary key constraint
}
```

#### Supported Options

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `name` | string | No | Custom column name | `"customer_id"` |
| `primary_key` | bool | No | Primary key constraint | `true` |
| `unique` | bool | No | Unique constraint | `true` |
| `foreign_key` | string | No | Foreign key reference | `"customers on customer_id"` |

#### Foreign Key Format

Foreign keys use the format: `"target_table on target_field"`

Examples:
- `"customers on customer_id"`
- `"items on item_id"`
- `"orders on order_id"`

#### Child Table Format  

Child relationships use the format: `"parent_table on parent_field"`

Examples:
- `"orders on order_id"`
- `"customers on customer_id"`

## Schema Processing Rules

### Field Inclusion Rules
**Location**: `db_proto/sql/schema/table.go:76-115`

#### Included Fields ✅
- **Scalar fields**: `string`, `int32`, `int64`, `uint32`, `uint64`, `bool`, `double`, `float`, etc.
- **Timestamp fields**: `google.protobuf.Timestamp`
- **Non-repeated fields**: Single-value fields only
- **Non-oneof fields**: Regular message fields

#### Excluded Fields ❌
- **Repeated fields**: Automatically become child tables or are ignored
- **Oneof fields**: Used for polymorphic entity handling
- **Non-timestamp message fields**: Become separate tables
- **Extension fields**: Protobuf extensions are ignored

### Table Creation Logic

#### Automatic Table Names
If no `table` annotation is provided:
- Uses protobuf message name as table name
- Only created when `useProtoOption=false`

#### Constraint Processing
**Primary Keys**:
- Only one primary key per table allowed
- Generates `ALTER TABLE ... ADD CONSTRAINT pk_table_name PRIMARY KEY (field)`

**Unique Constraints**:
- Multiple unique constraints allowed per table
- Generates `ALTER TABLE ... ADD CONSTRAINT table_field_unique UNIQUE (field)`

**Foreign Keys**:
- Supports both explicit and implicit foreign keys
- Explicit: Defined via `foreign_key` annotation
- Implicit: Created for child table relationships

## Database Dialect Support

### PostgreSQL
**Files**: `db_proto/sql/postgres/`

**Features**:
- Full constraint support (PK, FK, UNIQUE)
- Separate ALTER TABLE statements for constraints
- Transaction-based constraint application

**SQL Generation Example**:
```sql
CREATE TABLE customers (customer_id TEXT, name TEXT);
ALTER TABLE customers ADD CONSTRAINT customers_pk PRIMARY KEY (customer_id);
ALTER TABLE customers ADD CONSTRAINT customers_customer_id_unique UNIQUE (customer_id);
```

### RisingWave
**Files**: `db_proto/sql/risingwave/`

**Features**:
- Same constraint pattern as PostgreSQL
- UPSERT-based data handling
- Autocommit mode for block undo operations

**SQL Generation**: Identical to PostgreSQL

### ClickHouse
**Files**: `db_proto/sql/click_house/`

**Features**:
- Primary keys embedded in CREATE TABLE
- ReplacingMergeTree engine with versioning
- Partition by block timestamp
- Limited constraint support (PK only)

**SQL Generation Example**:
```sql
CREATE TABLE customers (
  customer_id String,
  name String,
  block_timestamp DateTime,
  version UInt64
) ENGINE = ReplacingMergeTree(version) 
PARTITION BY (toYYYYMM(block_timestamp)) 
PRIMARY KEY (customer_id) 
ORDER BY (customer_id);
```

## Usage Examples

### Basic Proto Definition

```protobuf
syntax = "proto3";
import "sf/substreams/sink/sql/schema/v1/schema.proto";

message Output {
  repeated Entity entities = 1;
}

message Entity {
  oneof entity {
    Customer customer = 1;
    Order order = 2;
    OrderItem order_item = 3;
    Item item = 4;
  }
}
```

### Customer Table

```protobuf
message Customer {
  option (sf.substreams.sink.sql.schema.v1.table) = { 
    name: "customers" 
  };
  
  string customer_id = 1 [(sf.substreams.sink.sql.schema.v1.field) = { 
    primary_key: true 
  }];
  string name = 2;
  string email = 3 [(sf.substreams.sink.sql.schema.v1.field) = { 
    unique: true 
  }];
}
```

**Generated SQL**:
```sql
CREATE TABLE customers (
  customer_id TEXT,
  name TEXT,
  email TEXT
);
ALTER TABLE customers ADD CONSTRAINT customers_pk PRIMARY KEY (customer_id);
ALTER TABLE customers ADD CONSTRAINT customers_email_unique UNIQUE (email);
```

### Order Table with Foreign Key

```protobuf
message Order {
  option (sf.substreams.sink.sql.schema.v1.table) = { 
    name: "orders" 
  };
  
  string order_id = 1 [(sf.substreams.sink.sql.schema.v1.field) = { 
    primary_key: true 
  }];
  string customer_ref_id = 2 [(sf.substreams.sink.sql.schema.v1.field) = { 
    foreign_key: "customers on customer_id" 
  }];
  google.protobuf.Timestamp created_at = 3;
  repeated OrderItem items = 4;  // Becomes child table
}
```

**Generated SQL**:
```sql
CREATE TABLE orders (
  order_id TEXT,
  customer_ref_id TEXT,
  created_at TIMESTAMP
);
ALTER TABLE orders ADD CONSTRAINT orders_pk PRIMARY KEY (order_id);
ALTER TABLE orders ADD CONSTRAINT fk_customer_ref_id FOREIGN KEY (customer_ref_id) REFERENCES customers(customer_id);
```

### Child Table Relationship

```protobuf
message OrderItem {
  option (sf.substreams.sink.sql.schema.v1.table) = {
    name: "order_items",
    child_of: "orders on order_id"
  };
  
  string item_id = 1 [(sf.substreams.sink.sql.schema.v1.field) = { 
    foreign_key: "items on item_id" 
  }];
  int64 quantity = 2;
  double unit_price = 3;
}
```

**Generated SQL**:
```sql
CREATE TABLE order_items (
  item_id TEXT,
  quantity BIGINT,
  unit_price DOUBLE PRECISION,
  orders_order_id TEXT  -- Auto-generated parent reference
);
ALTER TABLE order_items ADD CONSTRAINT fk_item_id FOREIGN KEY (item_id) REFERENCES items(item_id);
ALTER TABLE order_items ADD CONSTRAINT fk_order_items FOREIGN KEY (orders_order_id) REFERENCES orders(order_id);
```

### Complete Example

```protobuf
syntax = "proto3";
import "google/protobuf/timestamp.proto";
import "sf/substreams/sink/sql/schema/v1/schema.proto";

message Output {
  repeated Entity entities = 1;
}

message Entity {
  oneof entity {
    Customer customer = 1;
    Order order = 2;
    Item item = 3;
  }
}

message Customer {
  option (sf.substreams.sink.sql.schema.v1.table) = { name: "customers" };
  
  string customer_id = 1 [(sf.substreams.sink.sql.schema.v1.field) = { primary_key: true }];
  string name = 2;
  string email = 3 [(sf.substreams.sink.sql.schema.v1.field) = { unique: true }];
}

message Order {
  option (sf.substreams.sink.sql.schema.v1.table) = { name: "orders" };
  
  string order_id = 1 [(sf.substreams.sink.sql.schema.v1.field) = { primary_key: true }];
  string customer_ref_id = 2 [(sf.substreams.sink.sql.schema.v1.field) = { foreign_key: "customers on customer_id" }];
  google.protobuf.Timestamp created_at = 3;
  repeated OrderItem items = 4;
}

message OrderItem {
  option (sf.substreams.sink.sql.schema.v1.table) = {
    name: "order_items",
    child_of: "orders on order_id"
  };
  
  string item_id = 1 [(sf.substreams.sink.sql.schema.v1.field) = { foreign_key: "items on item_id" }];
  int64 quantity = 2;
  double unit_price = 3;
}

message Item {
  option (sf.substreams.sink.sql.schema.v1.table) = { name: "items" };
  
  string item_id = 1 [(sf.substreams.sink.sql.schema.v1.field) = { primary_key: true }];
  string name = 2;
  double price = 3;
}
```

## Configuration Options

### Command Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--no-constraints` | `false` | Disable constraint generation for faster imports |
| `--block-batch-size` | `25` | Number of blocks to process at once |
| `--start-block` | `""` | Starting block number |
| `--stop-block` | `"0"` | Ending block number |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `SUBSTREAMS_ENDPOINT_<network>` | Network-specific endpoints |

### DSN Examples

```bash
# PostgreSQL
postgresql://user:password@localhost:5432/database?sslmode=disable

# RisingWave  
postgresql://root@localhost:4566/dev?sslmode=disable

# ClickHouse
clickhouse://default@localhost:9000/default
```

## Best Practices

### 1. Schema Design

**✅ Do:**
- Use meaningful table and column names
- Define primary keys for all entities
- Use foreign keys to maintain referential integrity
- Leverage child table relationships for one-to-many data
- Use unique constraints for business keys

**❌ Don't:**
- Create tables without primary keys
- Use overly complex nested message structures
- Ignore foreign key relationships
- Mix entity types in the same message

### 2. Performance Optimization

**Initial Import:**
```bash
# Disable constraints for faster initial import
substreams-sink-sql from-proto --no-constraints <dsn> <manifest>

# Apply constraints after import
substreams-sink-sql from-proto-apply-constraints <dsn> <manifest> <module>
```

**Batch Processing:**
```bash
# Increase batch size for faster processing
substreams-sink-sql from-proto --block-batch-size=100 <dsn> <manifest>
```

### 3. Migration Strategy

**Schema Changes:**
1. The system automatically detects schema changes via hash comparison
2. Creates temporary schemas with new hash suffix
3. Validates data consistency between old and new schemas
4. Promotes new schema when validation passes

**Version Control:**
- Keep protobuf definitions in version control
- Test schema changes in development environment
- Use consistent naming conventions

## Troubleshooting

### Common Issues

#### 1. Schema Detection Failure
**Problem**: Schema annotations not recognized
**Solution**: Ensure `sf/substreams/sink/sql/schema/v1/schema.proto` is imported

```protobuf
import "sf/substreams/sink/sql/schema/v1/schema.proto";
```

#### 2. Multiple Primary Keys Error
**Problem**: `multiple primary keys are not supported in message`
**Solution**: Only one field per message can have `primary_key: true`

#### 3. Foreign Key Format Error
**Problem**: `invalid foreign key format`
**Solution**: Use correct format: `"target_table on target_field"`

#### 4. Missing Table Name
**Problem**: `table name is required for message`
**Solution**: Always specify table name in table annotation

```protobuf
option (sf.substreams.sink.sql.schema.v1.table) = { name: "my_table" };
```

#### 5. Constraint Application Failures
**Problem**: Constraints fail to apply
**Solutions**:
- Check that referenced tables exist
- Verify foreign key target columns exist
- Ensure data consistency before applying constraints
- Use `--no-constraints` for problematic imports

### Debug Commands

```bash
# Check schema detection
substreams-sink-sql from-proto --help

# Validate manifest
substreams run <manifest> -t +10

# Test database connection
psql <dsn>
clickhouse-client --host <host>
```

### Log Analysis

Enable debug logging to trace schema processing:
```bash
export RUST_LOG=debug
substreams-sink-sql from-proto <dsn> <manifest>
```

Look for these log messages:
- `creating schema` - Schema initialization
- `creating table message descriptor` - Table creation
- `walking message descriptor` - Field processing
- `apply constraints` - Constraint application

## Advanced Features

### Custom Column Names

```protobuf
message Customer {
  string id = 1 [(sf.substreams.sink.sql.schema.v1.field) = { 
    name: "customer_identifier",
    primary_key: true 
  }];
}
```

### Complex Relationships

```protobuf
// Many-to-many through junction table
message CustomerOrder {
  option (sf.substreams.sink.sql.schema.v1.table) = { name: "customer_orders" };
  
  string customer_id = 1 [(sf.substreams.sink.sql.schema.v1.field) = { 
    foreign_key: "customers on customer_id" 
  }];
  string order_id = 2 [(sf.substreams.sink.sql.schema.v1.field) = { 
    foreign_key: "orders on order_id" 
  }];
}
```

### Polymorphic Entities

```protobuf
message Entity {
  oneof entity_type {
    Customer customer = 1;
    Business business = 2;
    Individual individual = 3;
  }
}
```

## File Reference

### Core Files
- `cmd/substreams-sink-sql/from_proto.go` - Main command implementation
- `proto/sf/substreams/sink/sql/schema/v1/schema.proto` - Schema annotation definitions
- `db_proto/sql/schema/schema.go` - Schema orchestration
- `db_proto/sql/schema/table.go` - Table creation logic
- `db_proto/sql/schema/column.go` - Column processing
- `proto/utils.go` - Annotation extraction utilities

### Dialect Implementations
- `db_proto/sql/postgres/` - PostgreSQL dialect
- `db_proto/sql/risingwave/` - RisingWave dialect  
- `db_proto/sql/click_house/` - ClickHouse dialect

### Example Files
- `proto/test/relations/relations.proto` - Example schema definitions
- `db_proto/test/substreams/order/` - Complete test substream

---

This documentation covers the complete from-proto functionality in substreams-sink-sql. For additional support, refer to the source code or open an issue in the project repository.