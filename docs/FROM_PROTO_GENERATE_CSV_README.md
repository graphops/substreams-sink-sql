# from-proto-generate-csv

## Overview

`from-proto-generate-csv` exports the exact SQL schema and data that `from-proto` would produce, but as files you can inspect and inject. It streams final blocks only. Use it to backfill history; then start `from-proto` to continue live streaming from the exported cursor.

## What It Produces

- `schema.sql`: DDL identical to `from-proto` (tables, types, constraints)
- `schema.json`: Metadata (column order, types, schema hash)
- `csv/<table>/<start>-<end>.csv`: Data files for each table, bundled by block range
- `csv/_cursor_/last_cursor.csv`: Final cursor file compatible with `from-proto`’s `_cursor_` table

Example layout:

```
./csv-output/
├── schema.sql
├── schema.json
├── orders/
│   ├── 0000000000-0000010000.csv
│   └── 0000010000-0000020000.csv
├── order_items/
│   └── 0000000000-0000010000.csv
└── _cursor_/
    └── last_cursor.csv
```

## Usage

```bash
substreams-sink-sql from-proto-generate-csv \
  "postgres://user:pass@localhost:5432/mydb?sslmode=disable" \
  my-substreams.spkg \
  [output-module] [start]:[stop] \
  --schema-output=./schema.sql \
  --schema-metadata=./schema.json \
  --output-dir=./csv-output \
  --working-dir=./workdir \
  --bundle-size=10000 \
  --buffer-max-size=$((128*1024*1024))
```

Notes:
- Streams final blocks only (undos are not applied in this mode).
- If `--substreams-endpoint` is omitted, it is inferred from the manifest/network.
- If `[start]:[stop]` is omitted, you can use `--start-block`/`--stop-block`.
- Default `--cursors-table` is `_cursor_` to match `from-proto`.

## CSV Details

- Column order is exactly what `from-proto` inserts:
  - `_block_number_`, `_block_timestamp_`, optional `_version_` and `_deleted_` (ClickHouse), primary key (if any), optional parent reference, then other fields in schema order.
- Every CSV file includes a header row.
- Files are bundled by block ranges; partial boundaries are flushed at completion so the last file is written.
- Binary data is formatted COPY‑friendly for each dialect (e.g., `\xHEX` for PostgreSQL/RisingWave).

## Cursor File

- Path: `csv/_cursor_/last_cursor.csv`
- Format:
  - Header: `name,cursor`
  - Row: `cursor,<blockNum>:<blockID>`
- This is the only cursor artifact you need; it matches the DB schema of `_cursor_` used by `from-proto`.

## Schema File

- `schema.sql` is the exact DDL `from-proto` would execute.
- For PostgreSQL/RisingWave, a line is appended to seed `_sink_info_` with the schema hash:
  ```sql
  INSERT INTO "<schema>"."_sink_info_" (schema_hash)
  VALUES ('<hash>')
  ON CONFLICT (schema_hash) DO NOTHING;
  ```
  This lets `from-proto` start without additional migrations.

## End‑to‑End Workflow (PostgreSQL)

1) Generate files
```bash
substreams-sink-sql from-proto-generate-csv \
  "$PSQL_DSN" "$MANIFEST" "$MODULE" "$START:$STOP" \
  --schema-output=./schema.sql \
  --schema-metadata=./schema.json \
  --output-dir=./csv-output
```

2) Apply schema
```bash
psql "$PSQL_DB" < ./schema.sql
```

3) Inject tables (can run in parallel)
```bash
for t in $(find ./csv-output -maxdepth 1 -type d -not -name "_cursor_" -not -path ./csv-output); do
  table=$(basename "$t")
  substreams-sink-sql inject-csv "$PSQL_DSN" ./csv-output "$table" "$START:$STOP"
done
```

4) Inject cursor (last)
```bash
substreams-sink-sql inject-csv "$PSQL_DSN" ./csv-output "_cursor_" ":$STOP"
```

5) Start live streaming (continues from the exported cursor)
```bash
substreams-sink-sql from-proto "$PSQL_DSN" "$MANIFEST" "$MODULE" \
  --start-block=$STOP
```

## Compatibility Guarantees

- DDL/constraints: identical to `from-proto` (same generators)
- Column order/types: identical to dialect implementation
- Parent/child relationships: identical mapping
- Cursor: compatible with `from-proto`’s `_cursor_` table (same header & row format)

## Dialect Notes

- PostgreSQL/RisingWave: `_sink_info_`, `_cursor_`, `_blocks_` tables are created in `schema.sql`. Cursor injection uses `COPY ... WITH (HEADER)`.
- ClickHouse: CSVs include dialect-specific version/deleted fields. Schema.sql includes database and `_blocks_` table; cursor/sink info are not table-based — follow FROM_PROTO.md for CH specifics.

## Operational Tips

- Increase `--buffer-max-size` to reduce local I/O when memory allows.
- Choose `--bundle-size` to balance file count vs. parallel load throughput.
- Ensure the `[start]:[stop]` you inject matches the bundles produced.

## Known Limitations

- `_blocks_` rows are not exported; `from-proto` will populate them live.
- Reorgs/undos are not represented in backfilled CSV; use `from-proto` for live reorg handling.
- ClickHouse injection is operator-managed; there is no built-in injector.

## Rationale

- We reuse the exact `from-proto` schema and field mapping logic to ensure 1:1 compatibility.
- We emit files (instead of executing) to let operators review, sequence, and scale injection.
- See also docs/FROM_PROTO.md for the live ingestion counterpart.
