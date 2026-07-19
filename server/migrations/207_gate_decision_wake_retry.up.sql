ALTER TABLE gate_decision_wake ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
