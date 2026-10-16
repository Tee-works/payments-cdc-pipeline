-- Source database: a simplified payments gateway ledger.
-- Money is stored as integer minor units (kobo, pesewas, cents) to avoid
-- floating point and decimal-encoding surprises downstream.

CREATE DATABASE IF NOT EXISTS payments;
USE payments;

CREATE TABLE merchants (
    id          BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(120) NOT NULL,
    country     CHAR(2)      NOT NULL,
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE transactions (
    id            BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    reference     VARCHAR(64) NOT NULL UNIQUE,
    merchant_id   BIGINT      NOT NULL,
    amount_minor  BIGINT      NOT NULL,
    currency      CHAR(3)     NOT NULL,
    channel       VARCHAR(20) NOT NULL,
    status        VARCHAR(16) NOT NULL DEFAULT 'pending',
    created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    CONSTRAINT fk_transactions_merchant FOREIGN KEY (merchant_id) REFERENCES merchants (id),
    INDEX idx_transactions_created_at (created_at)
);

CREATE TABLE refunds (
    id              BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    transaction_id  BIGINT      NOT NULL,
    amount_minor    BIGINT      NOT NULL,
    status          VARCHAR(16) NOT NULL DEFAULT 'pending',
    created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    CONSTRAINT fk_refunds_transaction FOREIGN KEY (transaction_id) REFERENCES transactions (id)
);

-- Users and grants are in 02_users.sh, which takes passwords from the environment.
