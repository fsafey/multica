ALTER TABLE task_message
    ALTER COLUMN arrival_order DROP DEFAULT;

DROP SEQUENCE IF EXISTS task_message_arrival_order_seq;
