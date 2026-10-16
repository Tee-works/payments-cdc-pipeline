"""Runtime settings, read from environment variables.

Defaults match the hostnames in docker-compose.yml. Passwords have no
default: they come from the environment (.env locally, secrets in Kubernetes).
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = "kafka:9092"
    consumer_group: str = "payments-cdc-history"
    source_topic: str = "gateway.payments.transactions"
    dlq_topic: str = "gateway.dlq.transactions"
    batch_size: int = 500

    mysql_host: str = "mysql"
    mysql_port: int = 3306
    mysql_db: str = "payments"
    mysql_user: str = "app"
    mysql_password: str = ""  # from MYSQL_PASSWORD; see .env.example

    pg_host: str = "postgres"
    pg_port: int = 5432
    pg_db: str = "warehouse"
    pg_user: str = "warehouse"
    pg_password: str = ""  # from PG_PASSWORD; see .env.example

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ
        d = cls()
        return cls(
            kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", d.kafka_bootstrap),
            consumer_group=env.get("CONSUMER_GROUP", d.consumer_group),
            source_topic=env.get("SOURCE_TOPIC", d.source_topic),
            dlq_topic=env.get("DLQ_TOPIC", d.dlq_topic),
            batch_size=int(env.get("BATCH_SIZE", d.batch_size)),
            mysql_host=env.get("MYSQL_HOST", d.mysql_host),
            mysql_port=int(env.get("MYSQL_PORT", d.mysql_port)),
            mysql_db=env.get("MYSQL_DB", d.mysql_db),
            mysql_user=env.get("MYSQL_USER", d.mysql_user),
            mysql_password=env.get("MYSQL_PASSWORD", d.mysql_password),
            pg_host=env.get("PG_HOST", d.pg_host),
            pg_port=int(env.get("PG_PORT", d.pg_port)),
            pg_db=env.get("PG_DB", d.pg_db),
            pg_user=env.get("PG_USER", d.pg_user),
            pg_password=env.get("PG_PASSWORD", d.pg_password),
        )

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )
