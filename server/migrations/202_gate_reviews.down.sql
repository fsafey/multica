ALTER TABLE agent_task_queue DROP COLUMN IF EXISTS gate_decision_id;
DROP TABLE IF EXISTS gate_decision_wake;
DROP TABLE IF EXISTS gate_review_decision;
DROP TABLE IF EXISTS gate_review_request;
