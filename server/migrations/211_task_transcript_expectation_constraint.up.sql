ALTER TABLE agent_task_queue
    ADD CONSTRAINT agent_task_queue_transcript_expectation_pair CHECK (
        (
            transcript_expected_message_count IS NULL
            AND transcript_expected_last_seq IS NULL
            AND transcript_delivery_confirmed IS NULL
        )
        OR (
            transcript_expected_message_count IS NOT NULL
            AND transcript_expected_last_seq IS NOT NULL
            AND transcript_delivery_confirmed IS NOT NULL
            AND transcript_expected_message_count >= 0
            AND transcript_expected_last_seq >= 0
        )
    ) NOT VALID;
