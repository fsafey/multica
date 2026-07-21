CREATE TABLE IF NOT EXISTS task_execution_evidence (
    task_id UUID NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    payload BYTEA NOT NULL,
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^sha256:[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION reject_task_execution_evidence_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'task_execution_evidence rows are immutable';
END;
$$;

DROP TRIGGER IF EXISTS task_execution_evidence_immutable ON task_execution_evidence;

CREATE TRIGGER task_execution_evidence_immutable
BEFORE UPDATE ON task_execution_evidence
FOR EACH ROW
EXECUTE FUNCTION reject_task_execution_evidence_mutation();
