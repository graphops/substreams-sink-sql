package db

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

func (l *Loader) Flush(ctx context.Context, outputModuleHash string, cursor *sink.Cursor, lastFinalBlock uint64) (rowFlushedCount int, err error) {
	ctx = clickhouse.Context(context.Background(), clickhouse.WithStdAsync(false))

	startAt := time.Now()
	l.logger.Info("RisingWave DEBUG [FLUSH] - Starting flush operation",
		zap.Stringer("block", cursor.Block()),
		zap.Uint64("last_final_block", lastFinalBlock))

	// Debug: Log what operations are queued before transaction begins
	l.logger.Info("RisingWave DEBUG [FLUSH] - Operations queued for flush",
		zap.Int("table_count", l.entries.Len()))

	for tablePair := l.entries.Oldest(); tablePair != nil; tablePair = tablePair.Next() {
		tableName := tablePair.Key
		operations := tablePair.Value
		l.logger.Info("RisingWave DEBUG [FLUSH] - Table operations",
			zap.String("table", tableName),
			zap.Int("operation_count", operations.Len()))

		// Log details of each operation
		for opPair := operations.Oldest(); opPair != nil; opPair = opPair.Next() {
			operation := opPair.Value
			l.logger.Info("RisingWave DEBUG [FLUSH] - Operation details",
				zap.String("table", tableName),
				zap.String("operation_type", string(operation.opType)),
				zap.String("primary_key", fmt.Sprintf("%v", operation.primaryKey)),
				zap.String("data", fmt.Sprintf("%v", operation.data)))
		}
	}

	tx, err := l.BeginTx(ctx, nil)
	if err != nil {
		l.logger.Error("RisingWave DEBUG [FLUSH] - BeginTx failed", zap.Error(err))
		return 0, fmt.Errorf("failed to being db transaction: %w", err)
	}
	l.logger.Info("RisingWave DEBUG [FLUSH] - BeginTx successful")

	// Check if we're using RisingWave autocommit mode
	if _, isAutocommit := tx.(*RisingWaveAutocommitTx); isAutocommit {
		l.logger.Info("RisingWave DEBUG [FLUSH] - Using autocommit mode (no real transaction)")
	} else {
		l.logger.Info("RisingWave DEBUG [FLUSH] - Using real transaction mode")
	}

	defer func() {
		if err != nil {
			l.logger.Error("RisingWave DEBUG [FLUSH] - Operation failed, rolling back", zap.Error(err))
			if err := tx.Rollback(); err != nil {
				l.logger.Warn("failed to rollback transaction", zap.Error(err))
			}
		}
	}()

	l.logger.Info("RisingWave DEBUG [FLUSH] - About to call dialect.Flush")
	rowFlushedCount, err = l.dialect.Flush(tx, ctx, l, outputModuleHash, lastFinalBlock)
	if err != nil {
		l.logger.Error("RisingWave DEBUG [FLUSH] - dialect.Flush failed", zap.Error(err))
		return 0, fmt.Errorf("dialect flush: %w", err)
	}
	l.logger.Info("RisingWave DEBUG [FLUSH] - dialect.Flush successful", zap.Int("rows_flushed", rowFlushedCount))

	rowFlushedCount += 1
	l.logger.Info("RisingWave DEBUG [FLUSH] - About to update cursor")
	if err := l.UpdateCursor(ctx, tx, outputModuleHash, cursor); err != nil {
		l.logger.Error("RisingWave DEBUG [FLUSH] - UpdateCursor failed", zap.Error(err))
		return 0, fmt.Errorf("update cursor: %w", err)
	}
	l.logger.Info("RisingWave DEBUG [FLUSH] - UpdateCursor successful")

	l.logger.Info("RisingWave DEBUG [FLUSH] - About to commit transaction")
	if err := tx.Commit(); err != nil {
		l.logger.Error("RisingWave DEBUG [FLUSH] - Commit failed", zap.Error(err))
		return 0, fmt.Errorf("failed to commit db transaction: %w", err)
	}
	l.logger.Info("RisingWave DEBUG [FLUSH] - Commit successful")

	l.reset()

	// We add + 1 to the table count because the `cursors` table is an implicit table
	l.logger.Debug("flushed table(s) rows to databaseName", zap.Int("table_count", l.entries.Len()+1), zap.Int("row_count", rowFlushedCount), zap.Duration("took", time.Since(startAt)))
	l.logger.Info("RisingWave DEBUG [FLUSH] - Flush operation completed successfully", zap.Duration("total_time", time.Since(startAt)))
	return rowFlushedCount, nil
}

func (l *Loader) Revert(ctx context.Context, outputModuleHash string, cursor *sink.Cursor, lastValidBlock uint64) error {
	tx, err := l.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to being db transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if err := tx.Rollback(); err != nil {
				l.logger.Warn("failed to rollback transaction", zap.Error(err))
			}
		}
	}()

	if err := l.dialect.Revert(tx, ctx, l, lastValidBlock); err != nil {
		return err
	}

	if err := l.UpdateCursor(ctx, tx, outputModuleHash, cursor); err != nil {
		return fmt.Errorf("update cursor after revert: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit db transaction: %w", err)
	}

	l.logger.Debug("reverted changes to databaseName", zap.Uint64("last_valid_block", lastValidBlock))
	return nil
}

func (l *Loader) reset() {
	for entriesPair := l.entries.Oldest(); entriesPair != nil; entriesPair = entriesPair.Next() {
		l.entries.Set(entriesPair.Key, NewOrderedMap[string, *Operation]())
	}
}
