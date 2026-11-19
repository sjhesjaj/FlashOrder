CREATE TABLE campaign (
    id BIGINT NOT NULL AUTO_INCREMENT,
    sku VARCHAR(64) NOT NULL,
    starts_at DATETIME(6) NOT NULL,
    ends_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT ck_campaign_window CHECK (starts_at < ends_at)
) ENGINE = InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;

CREATE TABLE inventory (
    campaign_id BIGINT NOT NULL,
    total INT NOT NULL,
    available INT NOT NULL,
    PRIMARY KEY (campaign_id),
    CONSTRAINT fk_inventory_campaign FOREIGN KEY (campaign_id) REFERENCES campaign (id),
    CONSTRAINT ck_inventory_available CHECK (available BETWEEN 0 AND total)
) ENGINE = InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;

CREATE TABLE purchase_request (
    id BIGINT NOT NULL AUTO_INCREMENT,
    request_uid CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    user_id BIGINT NOT NULL,
    campaign_id BIGINT NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    status VARCHAR(24) NOT NULL,
    reject_reason VARCHAR(64),
    order_id BIGINT,
    accepted_at DATETIME(6) NOT NULL,
    finished_at DATETIME(6),
    PRIMARY KEY (id),
    UNIQUE KEY uk_purchase_request_uid (request_uid),
    UNIQUE KEY uk_purchase_request_idempotency (user_id, idempotency_key),
    KEY idx_purchase_request_campaign_status (campaign_id, status),
    CONSTRAINT fk_purchase_request_campaign FOREIGN KEY (campaign_id) REFERENCES campaign (id)
) ENGINE = InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;

CREATE TABLE purchase_order (
    id BIGINT NOT NULL AUTO_INCREMENT,
    order_uid CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_id BIGINT NOT NULL,
    campaign_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    status VARCHAR(24) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    expire_at DATETIME(6) NOT NULL,
    paid_at DATETIME(6),
    cancelled_at DATETIME(6),
    active_flag TINYINT GENERATED ALWAYS AS (
        IF(status IN ('PENDING', 'PAID'), 1, NULL)
    ) STORED,
    PRIMARY KEY (id),
    UNIQUE KEY uk_purchase_order_uid (order_uid),
    UNIQUE KEY uk_purchase_order_request (request_id),
    UNIQUE KEY uk_purchase_order_active (campaign_id, user_id, active_flag),
    KEY idx_purchase_order_status_expire (status, expire_at),
    KEY idx_purchase_order_campaign_status (campaign_id, status),
    CONSTRAINT fk_purchase_order_request FOREIGN KEY (request_id) REFERENCES purchase_request (id),
    CONSTRAINT fk_purchase_order_campaign FOREIGN KEY (campaign_id) REFERENCES campaign (id)
) ENGINE = InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;

CREATE TABLE payment_attempt (
    id BIGINT NOT NULL AUTO_INCREMENT,
    order_id BIGINT NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    result VARCHAR(32) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_payment_attempt_idempotency (order_id, idempotency_key),
    CONSTRAINT fk_payment_attempt_order FOREIGN KEY (order_id) REFERENCES purchase_order (id)
) ENGINE = InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;

CREATE TABLE outbox_message (
    id BIGINT NOT NULL AUTO_INCREMENT,
    event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    topic VARCHAR(128) NOT NULL,
    msg_key VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSON NOT NULL,
    created_at DATETIME(6) NOT NULL,
    published_at DATETIME(6),
    PRIMARY KEY (id),
    UNIQUE KEY uk_outbox_message_event (event_id),
    KEY idx_outbox_message_published_id (published_at, id)
) ENGINE = InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
