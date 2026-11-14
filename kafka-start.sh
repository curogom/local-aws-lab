#!/bin/sh
set -eu
if docker inspect queue-reliability-kafka >/dev/null 2>&1; then
 test "$(docker inspect -f '{{ index .Config.Labels "queue-reliability-lab" }}' queue-reliability-kafka)" = true || { echo 'Container name is not owned by this lab.' >&2; exit 1; }
 docker start queue-reliability-kafka
else
docker run -d --name queue-reliability-kafka --label queue-reliability-lab=true --memory=1536m -p 127.0.0.1:19092:9092 \
-e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
-e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
-e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
-e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
-e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
-e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19092 \
-e KAFKA_HEAP_OPTS='-Xms256m -Xmx512m' \
-e KAFKA_LOG_RETENTION_HOURS=1 -e KAFKA_LOG_RETENTION_BYTES=67108864 apache/kafka:4.4.0
fi
cd "$(dirname "$0")"
.venv/bin/python - <<'READY'
import time
from kafka.admin import KafkaAdminClient
for attempt in range(30):
 try:
  admin=KafkaAdminClient(bootstrap_servers='127.0.0.1:19092',bootstrap_timeout_ms=1500,request_timeout_ms=2000)
  admin.list_topics(); admin.close(); print('Kafka ready at 127.0.0.1:19092'); break
 except Exception:
  time.sleep(1)
else:
 raise SystemExit('Kafka readiness timeout')
READY
