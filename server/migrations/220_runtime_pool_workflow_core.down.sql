ALTER TABLE agent_task_queue
    DROP COLUMN IF EXISTS workflow_law_digest,
    DROP COLUMN IF EXISTS workflow_input_digest,
    DROP COLUMN IF EXISTS workflow_claim_epoch,
    DROP COLUMN IF EXISTS workflow_attempt_id,
    DROP COLUMN IF EXISTS workflow_node_id;

DROP TABLE IF EXISTS workflow_outbox;
DROP TABLE IF EXISTS workflow_resource_claim;
DROP TABLE IF EXISTS workflow_node_resource;
DROP TABLE IF EXISTS workflow_node_result;
DROP TABLE IF EXISTS workflow_node_attempt;
DROP TABLE IF EXISTS workflow_node_dependency;
DROP TABLE IF EXISTS workflow_node;
DROP TABLE IF EXISTS workflow_run;
DROP TABLE IF EXISTS agent_runtime_pool;
DROP TABLE IF EXISTS runtime_pool_runtime;
DROP TABLE IF EXISTS runtime_pool;
