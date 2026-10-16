#!/bin/bash
# Database users, created from environment variables so no password lives in
# the repository. The MySQL entrypoint runs this after 01_schema.sql.

: "${MYSQL_DEBEZIUM_PASSWORD:?MYSQL_DEBEZIUM_PASSWORD is not set}"
: "${MYSQL_APP_PASSWORD:?MYSQL_APP_PASSWORD is not set}"

mysql --protocol=socket -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
-- Debezium reads the binlog, so it needs replication privileges, not write access.
CREATE USER 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY '${MYSQL_DEBEZIUM_PASSWORD}';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES
    ON *.* TO 'debezium'@'%';

-- The application user that writes payments.
CREATE USER 'app'@'%' IDENTIFIED WITH mysql_native_password BY '${MYSQL_APP_PASSWORD}';
GRANT SELECT, INSERT, UPDATE, DELETE ON payments.* TO 'app'@'%';

FLUSH PRIVILEGES;
SQL
