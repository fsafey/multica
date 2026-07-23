CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS agent_runtime_pool_pair_unique ON agent_runtime_pool(agent_id, pool_id);
