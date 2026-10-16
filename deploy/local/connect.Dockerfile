# Strimzi Kafka Connect image with the two Debezium plugins the pipeline uses.
# Same Debezium line (2.7) as the docker compose stack. Debezium 2.7 is built
# against Kafka 3.7; on Kafka 3.9 the JDBC sink task dies with NoSuchMethodError
# (InternalSinkRecord.originalRecord), so Connect stays on 3.7.
ARG STRIMZI_KAFKA_IMAGE=quay.io/strimzi/kafka:0.43.0-kafka-3.7.1
FROM ${STRIMZI_KAFKA_IMAGE}

ARG DEBEZIUM_VERSION=2.7.3.Final
ARG MAVEN=https://repo1.maven.org/maven2/io/debezium

USER root
RUN set -eux; \
    mkdir -p /opt/kafka/plugins; \
    for c in mysql jdbc; do \
      curl -fsSL "${MAVEN}/debezium-connector-${c}/${DEBEZIUM_VERSION}/debezium-connector-${c}-${DEBEZIUM_VERSION}-plugin.tar.gz" \
        | tar -xz -C /opt/kafka/plugins; \
    done
USER 1001
