ALTER TABLE outbox_message
    ADD COLUMN lease_token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin,
    ADD COLUMN lease_until DATETIME(6),
    ADD CONSTRAINT ck_outbox_message_lease CHECK ((lease_token IS NULL) = (lease_until IS NULL));
