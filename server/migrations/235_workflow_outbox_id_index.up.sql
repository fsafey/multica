CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS workflow_outbox_id_unique ON workflow_outbox(id);
