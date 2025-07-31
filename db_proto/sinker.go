package db_proto

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	sink "github.com/streamingfast/substreams-sink"
	sql "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/stats"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
)

type Sinker struct {
	*sink.Sinker
	db                    sql.Database
	useTransaction        bool
	parallel              bool
	parallelWorkers       int
	blockBatchSize        uint64
	stats                 *stats.Stats
	logger                *zap.Logger
	rootMessageDescriptor *desc.MessageDescriptor
	useConstraints        bool
	flushLock             sync.Mutex
}

func NewSinker(rootMessageDescriptor *desc.MessageDescriptor, sink *sink.Sinker, db sql.Database, useTransaction bool, useConstraints bool, blockBatchSize int, parallel bool, parallelWorkers int, stats *stats.Stats, logger *zap.Logger) *Sinker {
	return &Sinker{
		db:                    db,
		rootMessageDescriptor: rootMessageDescriptor,
		useTransaction:        useTransaction,
		parallel:              parallel,
		parallelWorkers:       parallelWorkers,
		blockBatchSize:        uint64(blockBatchSize),
		stats:                 stats,
		Sinker:                sink,
		logger:                logger,
	}
}

func (s *Sinker) Run(ctx context.Context) error {
	cursor, err := s.db.FetchCursor()
	if err != nil {
		return fmt.Errorf("fetch cursor: %w", err)
	}

	//clean up the mess from running without a transaction
	if cursor != nil {
		err = s.db.HandleBlocksUndo(cursor.Block().Num())
		if err != nil {
			return fmt.Errorf("handle blocks undo from %d : %w", cursor.Block().Num(), err)
		}
	}

	s.logger.Info("fetched cursor", zap.Uint64("block_num", cursor.Block().Num()))

	s.stats.LastBlockProcessAt = time.Now()
	s.Sinker.Run(ctx, cursor, s)
	return nil
}

type Holder struct {
	output *pbsubstreamsrpc.MapModuleOutput
	data   *pbsubstreamsrpc.BlockScopedData
	isLive *bool
	cursor *sink.Cursor
}

var holding []*Holder

func (s *Sinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) (err error) {
	if (isLive != nil && *isLive) && s.useConstraints {
		return fmt.Errorf("live mode is not supported without constraints")
	}

	startAt := time.Now()
	defer func() {
		s.stats.LastBlockProcessAt = time.Now()
		s.stats.BlockProcessingDuration.Add(time.Since(startAt))
		s.stats.TotalProcessingDuration += time.Since(startAt)
	}()

	if s.stats.BlockCount > 0 {
		s.stats.WaitDurationBetweenBlocks.Add(time.Since(s.stats.LastBlockProcessAt))
		s.stats.TotalDurationBetween += time.Since(s.stats.LastBlockProcessAt)
	}
	s.stats.BlockCount++

	output := data.Output
	if output.Name != s.OutputModuleName() {
		return fmt.Errorf("received data from wrong output module, expected to received from %q but got module's output for %q", s.OutputModuleName(), output.Name)
	}

	holder := &Holder{
		output: output,
		data:   data,
		isLive: isLive,
		cursor: cursor,
	}
	holding = append(holding, holder)
	if data.Clock.Number%s.blockBatchSize == 0 || s.blockBatchSize == 1 || (isLive != nil && *isLive) {
		if s.useTransaction && !s.parallel {
			if err := s.db.BeginTransaction(); err != nil {
				return fmt.Errorf("begin tx: %w", err)
			}
		}
		if s.parallel {
			// Use worker pool pattern for better resource management
			err := s.processHoldersInParallel(holding, s.stats)
			if err != nil {
				return fmt.Errorf("parallel processing failed: %w", err)
			}

		} else {
			for _, h := range holding {
				err = s.processHolder(h, s.stats)
				if err != nil {
					if s.useTransaction {
						s.db.RollbackTransaction()
					}
					return fmt.Errorf("process holder: %w", err)
				}
			}
		}

		flushDuration, err := s.db.Flush()
		if err != nil {
			return fmt.Errorf("flushing: %w", err)
		}

		flushDurationPerBlock := flushDuration / time.Duration(len(holding))
		s.stats.FlushDuration.Add(flushDurationPerBlock)

		err = s.db.StoreCursor(cursor)
		if err != nil {
			return fmt.Errorf("inserting cursor: %w", err)
		}

		if s.useTransaction && !s.parallel {
			if err := s.db.CommitTransaction(); err != nil {
				return fmt.Errorf("commit tx: %w", err)
			}
		}
		holding = []*Holder{}
	}

	return nil
}

func (s *Sinker) processHolder(h *Holder, stats *stats.Stats) (err error) {
	if len(h.output.GetMapOutput().GetValue()) == 0 {
		return nil
	}

	unmarshalStartAt := time.Now()
	md := s.rootMessageDescriptor
	dm := dynamic.NewMessage(md)
	err = dm.Unmarshal(h.data.Output.GetMapOutput().GetValue())
	if err != nil {
		return fmt.Errorf("unmarshaling message: %w", err)
	}
	stats.UnmarshallingDuration.Add(time.Since(unmarshalStartAt))

	err = processMessage(dm, s.db, h.data.Clock.Number, h.data.Clock.Id, h.data.Clock.Timestamp.AsTime(), stats)
	if err != nil {
		return fmt.Errorf("process entity: %w", err)
	}

	return nil
}
func processMessage(dm *dynamic.Message, database sql.Database, blockNum uint64, blockHash string, blockTimestamp time.Time, stats *stats.Stats) error {
	startInsertBlock := time.Now()
	err := database.InsertBlock(blockNum, blockHash, blockTimestamp)
	if err != nil {
		return fmt.Errorf("inserting block: %w", err)
	}
	stats.BlockInsertDuration.Add(time.Since(startInsertBlock))

	sqlDuration, err := database.WalkMessageDescriptorAndInsert(dm, blockNum, blockTimestamp, nil)
	if err != nil {
		return fmt.Errorf("processing message %q: %w", dm.GetMessageDescriptor().GetFullyQualifiedName(), err)
	}

	stats.EntitiesInsertDuration.Add(sqlDuration)

	return nil
}

func (s *Sinker) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) (err error) {
	lastValidBlockNum := undoSignal.LastValidBlock.Number

	s.logger.Info("Handling undo block signal", zap.Stringer("block", cursor.Block()), zap.Stringer("cursor", cursor))

	err = s.db.HandleBlocksUndo(lastValidBlockNum)
	if err != nil {
		return fmt.Errorf("handle blocks undo from %d : %w", lastValidBlockNum, err)
	}

	err = s.db.StoreCursor(cursor)
	if err != nil {
		return fmt.Errorf("inserting cursor: %w", err)
	}

	return nil
}

// processHoldersInParallel processes multiple holders using a worker pool pattern
func (s *Sinker) processHoldersInParallel(holding []*Holder, stats *stats.Stats) error {
	if len(holding) == 0 {
		return nil
	}

	// Create worker pool with limited number of workers
	workerCount := s.parallelWorkers
	if workerCount <= 0 {
		workerCount = 4 // Default fallback
	}

	// Limit worker count to number of holders to avoid unnecessary goroutines
	if workerCount > len(holding) {
		workerCount = len(holding)
	}

	s.logger.Debug("starting parallel processing",
		zap.Int("holder_count", len(holding)),
		zap.Int("worker_count", workerCount),
	)

	// Create channels for work distribution
	holderChan := make(chan *Holder, len(holding))
	errorChan := make(chan error, len(holding))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Clone database for this worker
			db := s.db.Clone()

			// Process holders assigned to this worker
			for holder := range holderChan {
				err := s.processWorkerHolder(db, holder, stats, workerID)
				if err != nil {
					s.logger.Error("worker processing failed",
						zap.Int("worker_id", workerID),
						zap.Error(err),
					)
					errorChan <- err
				}
			}
		}(i)
	}

	// Distribute work to workers
	startTime := time.Now()
	for _, holder := range holding {
		holderChan <- holder
	}
	close(holderChan)

	// Wait for all workers to complete
	wg.Wait()
	close(errorChan)

	duration := time.Since(startTime)
	s.logger.Debug("parallel processing completed",
		zap.Duration("duration", duration),
		zap.Float64("holders_per_second", float64(len(holding))/duration.Seconds()),
	)

	// Collect errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("parallel processing errors (%d/%d failed): %v",
			len(errors), len(holding), errors)
	}

	return nil
}

// processWorkerHolder processes a single holder in a worker context
func (s *Sinker) processWorkerHolder(db sql.Database, holder *Holder, stats *stats.Stats, workerID int) error {
	// Begin transaction if supported (RisingWave will ignore this)
	err := db.BeginTransaction()
	if err != nil {
		return fmt.Errorf("worker %d begin transaction: %w", workerID, err)
	}

	// Process the holder
	err = s.processHolder(holder, stats)
	if err != nil {
		db.RollbackTransaction() // RisingWave will ignore this
		return fmt.Errorf("worker %d process holder: %w", workerID, err)
	}

	// Commit transaction if supported (RisingWave will ignore this)
	err = db.CommitTransaction()
	if err != nil {
		return fmt.Errorf("worker %d commit transaction: %w", workerID, err)
	}

	return nil
}
