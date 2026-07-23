CREATE INDEX CONCURRENTLY IF NOT EXISTS workflow_outbox_pending ON workflow_outbox(available_at, created_at) WHERE status = 'pending';
