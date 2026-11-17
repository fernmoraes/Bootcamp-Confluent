#!/usr/bin/env bash
# Utilitário de consumo de alertas de fraude usando a identidade 'desafio-consumer'
# Não imprime nem expõe segredos.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

[ -n "${ENV_ID:-}" ]              || die "ENV_ID não definido no .state."
[ -n "${CLUSTER_ID:-}" ]          || die "CLUSTER_ID não definido no .state."
[ -n "${SR_URL:-}" ]              || die "SR_URL não definido no .state."
[ -n "${CONSUMER_KAFKA_KEY:-}" ]  || die "CONSUMER_KAFKA_KEY não definida no .state."
[ -n "${CONSUMER_KAFKA_SECRET:-}" ] || die "CONSUMER_KAFKA_SECRET não definida no .state."

TOPIC="${TOPIC_PREFIX}.fraud.detected"
GROUP="${CONSUMER_GROUP_PREFIX}alerts-client"

FLAGS=(
  --cluster "$CLUSTER_ID"
  --environment "$ENV_ID"
  --api-key "$CONSUMER_KAFKA_KEY"
  --api-secret "$CONSUMER_KAFKA_SECRET"
  --value-format avro
  --schema-registry-endpoint "$SR_URL"
  --schema-registry-api-key "$CONSUMER_SR_KEY"
  --schema-registry-api-secret "$CONSUMER_SR_SECRET"
  --group "$GROUP"
)

if [ "${1:-}" = "--from-beginning" ] || [ "${1:-}" = "-b" ]; then
  FLAGS+=(--from-beginning)
fi

log "Iniciando consumo de $TOPIC com identidade $CONSUMER_SA_NAME (grupo: $GROUP)..."
confluent kafka topic consume "$TOPIC" "${FLAGS[@]}"
