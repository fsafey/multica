CREATE SEQUENCE IF NOT EXISTS task_message_arrival_order_seq AS BIGINT;

ALTER SEQUENCE task_message_arrival_order_seq
    OWNED BY task_message.arrival_order;

ALTER TABLE task_message
    ALTER COLUMN arrival_order SET DEFAULT nextval('task_message_arrival_order_seq');
