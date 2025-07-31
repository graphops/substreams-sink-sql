package risingwave

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"go.uber.org/zap"
)

// ConnectionPoolManager manages multiple connection pools for parallel processing
type ConnectionPoolManager struct {
	pools      []*pgxpool.Pool
	poolCount  int
	roundRobin int
	mutex      sync.Mutex
	logger     *zap.Logger
	dsn        *db.DSN
	ctx        context.Context
}

// NewConnectionPoolManager creates a new connection pool manager for parallel processing
func NewConnectionPoolManager(ctx context.Context, dsn *db.DSN, poolCount int, logger *zap.Logger) (*ConnectionPoolManager, error) {
	if poolCount <= 0 {
		poolCount = 4 // Default to 4 pools
	}

	manager := &ConnectionPoolManager{
		pools:     make([]*pgxpool.Pool, poolCount),
		poolCount: poolCount,
		logger:    logger.Named("risingwave_pool_manager"),
		dsn:       dsn,
		ctx:       ctx,
	}

	// Create individual pools for each worker
	for i := 0; i < poolCount; i++ {
		pool, err := manager.createPool(ctx, dsn, i)
		if err != nil {
			// Clean up any pools that were successfully created
			manager.Close()
			return nil, fmt.Errorf("creating pool %d: %w", i, err)
		}
		manager.pools[i] = pool
	}

	manager.logger.Info("connection pool manager created",
		zap.Int("pool_count", poolCount),
		zap.String("dsn_host", dsn.Host),
		zap.String("database", dsn.Database),
	)

	return manager, nil
}

// createPool creates a single connection pool with optimized settings for RisingWave
func (m *ConnectionPoolManager) createPool(ctx context.Context, dsn *db.DSN, poolIndex int) (*pgxpool.Pool, error) {
	// Configure pool settings optimized for RisingWave streaming workloads
	config, err := pgxpool.ParseConfig(dsn.ConnString())
	if err != nil {
		return nil, fmt.Errorf("parsing connection config: %w", err)
	}

	// Optimize for bulk insertion scenarios
	config.MaxConns = 4                         // 4 connections per pool (total: poolCount * 4)
	config.MinConns = 1                         // Keep at least 1 connection warm
	config.MaxConnLifetime = 30 * time.Minute   // Rotate connections every 30 minutes
	config.MaxConnIdleTime = 5 * time.Minute    // Close idle connections after 5 minutes
	config.HealthCheckPeriod = 30 * time.Second // Check connection health every 30 seconds

	// RisingWave-specific connection optimizations
	config.ConnConfig.PreferSimpleProtocol = true // Use simple protocol for better bulk insert performance

	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("connecting to pool: %w", err)
	}

	// Test the connection
	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("testing pool connection: %w", err)
	}
	defer conn.Release()

	// Verify RisingWave connectivity with a simple query
	var version string
	err = conn.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("testing RisingWave connection: %w", err)
	}

	m.logger.Debug("pool created and tested",
		zap.Int("pool_index", poolIndex),
		zap.String("risingwave_version", version),
	)

	return pool, nil
}

// GetPool returns a connection pool using round-robin allocation
func (m *ConnectionPoolManager) GetPool() *pgxpool.Pool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	pool := m.pools[m.roundRobin]
	m.roundRobin = (m.roundRobin + 1) % m.poolCount
	return pool
}

// GetPoolByIndex returns a specific pool by index (for dedicated workers)
func (m *ConnectionPoolManager) GetPoolByIndex(index int) *pgxpool.Pool {
	if index < 0 || index >= m.poolCount {
		return m.GetPool() // Fallback to round-robin
	}
	return m.pools[index]
}

// GetStats returns connection pool statistics for monitoring
func (m *ConnectionPoolManager) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	totalAcquired := int32(0)
	totalIdle := int32(0)
	totalTotal := int32(0)

	for i, pool := range m.pools {
		poolStats := pool.Stat()
		stats[fmt.Sprintf("pool_%d_acquired", i)] = poolStats.AcquiredConns()
		stats[fmt.Sprintf("pool_%d_idle", i)] = poolStats.IdleConns()
		stats[fmt.Sprintf("pool_%d_total", i)] = poolStats.TotalConns()

		totalAcquired += poolStats.AcquiredConns()
		totalIdle += poolStats.IdleConns()
		totalTotal += poolStats.TotalConns()
	}

	stats["total_acquired"] = totalAcquired
	stats["total_idle"] = totalIdle
	stats["total_connections"] = totalTotal
	stats["pool_count"] = m.poolCount

	return stats
}

// LogStats logs current connection pool statistics
func (m *ConnectionPoolManager) LogStats() {
	stats := m.GetStats()
	m.logger.Info("connection pool stats",
		zap.Int("pool_count", stats["pool_count"].(int)),
		zap.Int32("total_connections", stats["total_connections"].(int32)),
		zap.Int32("total_acquired", stats["total_acquired"].(int32)),
		zap.Int32("total_idle", stats["total_idle"].(int32)),
	)
}

// Close closes all connection pools
func (m *ConnectionPoolManager) Close() {
	m.logger.Info("closing connection pool manager")

	for i, pool := range m.pools {
		if pool != nil {
			pool.Close()
			m.logger.Debug("closed pool", zap.Int("pool_index", i))
		}
	}
}

// HealthCheck performs a health check on all pools
func (m *ConnectionPoolManager) HealthCheck(ctx context.Context) error {
	for i, pool := range m.pools {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("acquiring connection from pool %d: %w", i, err)
		}

		err = conn.Ping(ctx)
		conn.Release()

		if err != nil {
			return fmt.Errorf("pinging pool %d: %w", i, err)
		}
	}

	return nil
}
