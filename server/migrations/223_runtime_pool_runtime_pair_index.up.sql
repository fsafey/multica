CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS runtime_pool_runtime_pair_unique ON runtime_pool_runtime(pool_id, runtime_id);
