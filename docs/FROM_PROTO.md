# from-proto

## Overview

`from-proto` ingests a typed protobuf message emitted by your Substreams output module, derives and manages a relational schema automatically, and inserts entities into the target database. It can backfill ranges and continue live, with full compatibility across PostgreSQL, RisingWave, and ClickHouse (dialect-specific features apply).

Use it when your Substreams module exposes a strongly-typed data model and you want the sink to manage table definitions and relationships from the protobuf descriptors, including parent/child mappings.

## What It Manages

- Schema creation/update (when empty) from your proto descriptors (proto options if present).
- System tables:
  - PostgreSQL/RisingWave: `"<schema>"._sink_info_`, `"<schema>"._cursor_`, `"<schema>"._blocks_`
  - ClickHouse: `_blocks_` table and schema management; sink info/cursor are handled differently in CH live mode
- Cursor persistence per stream (single record in `_cursor_` with `name='cursor'`).
- Reorg handling: deletes/retracts data above the last valid block (`_blocks_` is authoritative, with ON DELETE CASCADE on Postgres).

## Usage

```bash
substreams-sink-sql from-proto \
  <dsn> <manifest> [output-module] \
  --substreams-endpoint=<endpoint> \
  --start-block=<start> --stop-block=<stop> \
  [--no-constraints] \
  [--block-batch-size=25] \
  [--clickhouse-sink-info-folder=...] \
  [--clickhouse-cursor-file-path=cursor.txt]
```

Notes:
- If `--substreams-endpoint` is omitted, it is inferred from the manifest’s network.
- `[output-module]` is optional; inferred from manifest sink config when omitted.
- `--block-batch-size` controls how many blocks are accumulated before a flush (and cursor write). Default is 25.
- Transactions are enabled by default (per batch) when supported by the driver; `parallel` per-block mode is disabled by default.

## Flags

- `--no-constraints`: Skip creating PKs/FKs/uniques. Recommended for high-throughput backfills; once backfilled, you can enforce constraints and continue live.
- `--block-batch-size`: Number of blocks per flush. Increase cautiously; large batches increase memory and may delay cursor writes.
- ClickHouse-specific:
  - `--clickhouse-sink-info-folder`: Filesystem folder for schema hash information.
  - `--clickhouse-cursor-file-path`: Path to a file where the cursor is stored (used by CH since `_cursor_` is not table-based here).

## Behavior

- Schema derivation:
  - With proto options enabled (presence of `sf/substreams/sink/sql/schema/v1/schema.proto`), table/column names and relationships are taken from options.
  - Without proto options, a best-effort schema is derived (table per message, columns per simple field).

- Live and constraints:
  - Live streaming is supported with or without constraints. Constraints may reduce write throughput; prefer `--no-constraints` for backfills and enable constraints for live integrity if needed.

- Reorgs:
  - Postgres: `_blocks_` has PK on `number` and other tables FK reference `_blocks_` with ON DELETE CASCADE. Reverts delete rows beyond the last valid block.
  - RisingWave: works in autocommit mode; tables are created without FK constraints but maintain `_blocks_` for authoritative block numbers.
  - ClickHouse: uses ReplacingMergeTree with version/deleted columns to model retractions.

- Flushing:
  - Flush happens when the batch reaches `--block-batch-size`, or immediately in live when needed. Cursor is written after a successful flush.
  - On range completion, any partial batch is flushed and the final cursor is stored.

## Differences vs `run`

- Input contract: `from-proto` consumes typed protobuf; `run` consumes DatabaseChanges.
- Schema: `from-proto` generates/manages schema; `run` expects a pre-created schema via `setup`.
- System tables: `from-proto` uses `_cursor_`, `_blocks_`, `_sink_info_`; `run` uses `cursors` and `substreams_history`.
- Handoff: If you backfilled with `from-proto`, continue live with `from-proto` to reuse cursor and system tables. Switching directly to `run` requires a migration and won’t reuse the same cursor.

## ClickHouse Notes

- `_blocks_` is created with `ReplacingMergeTree(version)` and includes `_version_` and `_deleted_` columns.
- `--clickhouse-sink-info-folder` writes a `<schema>_schema_hash.txt` file to persist the schema hash; there is no `_sink_info_` table.
- Cursor is stored to `--clickhouse-cursor-file-path` (text file with the cursor string). Ensure the agent has write access and persists this path across restarts.

## With CSV Backfill

For very large backfills, use `from-proto-generate-csv` to export schema + CSVs and inject them (PostgreSQL provides a built-in injector). Then start `from-proto` from the exported cursor for live streaming. See `docs/FROM_PROTO_GENERATE_CSV_README.md`.

