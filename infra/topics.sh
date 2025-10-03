#!/usr/bin/env bash
set -euo pipefail
BROKER="${1:-localhost:29092}"
DOCKER="docker compose -f infra/docker-compose.yml"
KAFKA_BIN="/opt/bitnami/kafka/bin/kafka-topics.sh"
create() {
  $DOCKER exec -T kafka $KAFKA_BIN --create --if-not-exists \
    --topic "$1" --bootstrap-server "$BROKER" --replication-factor 1 --partitions 3
}
create raw.ingest
create proc.text
create predictions.text
create alerts
echo "topics ensured"
