ALTER TABLE task_message
    ADD CONSTRAINT task_message_arrival_order_not_null
    CHECK (arrival_order IS NOT NULL) NOT VALID;
