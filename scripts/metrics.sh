#!/usr/bin/env bash
# Coleta métricas reais do cluster Kafka via Confluent Telemetry Metrics API e CLI
# Não imprime nem salva segredos.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

[ -n "${ENV_ID:-}" ]     || die "ENV_ID não definido no .state."
[ -n "${CLUSTER_ID:-}" ] || die "CLUSTER_ID não definido no .state."

# Se não houver chave cloud no state, cria uma temporária para a query
CREATED_TEMP_KEY=false
if [ -z "${METRICS_CLOUD_KEY:-}" ] || [ -z "${METRICS_CLOUD_SECRET:-}" ]; then
  log "Criando Cloud API Key temporária para consulta à Metrics API..."
  KEY_JSON="$(confluent api-key create --resource cloud --description "Temp Metrics Key" -o json)"
  METRICS_CLOUD_KEY="$(jq -r .api_key <<<"$KEY_JSON")"
  METRICS_CLOUD_SECRET="$(jq -r .api_secret <<<"$KEY_JSON")"
  CREATED_TEMP_KEY=true
fi

# Janela de consulta: últimas 12 horas
START=$(date -u -v-12H "+%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d "12 hours ago" "+%Y-%m-%dT%H:%M:%SZ")
END=$(date -u "+%Y-%m-%dT%H:%M:%SZ")

query_metric() {
  local metric="$1"
  curl -s -u "$METRICS_CLOUD_KEY:$METRICS_CLOUD_SECRET" \
    -H "Content-Type: application/json" \
    -X POST "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query" \
    -d "{\"aggregations\":[{\"metric\":\"$metric\"}],\"filter\":{\"field\":\"resource.kafka.id\",\"op\":\"EQ\",\"value\":\"$CLUSTER_ID\"},\"granularity\":\"PT1H\",\"intervals\":[\"$START/$END\"]}"
}

echo "=== 1. Métricas de Volume no Cluster ($CLUSTER_ID) — Granularidade 1h ==="
echo
echo "--- io.confluent.kafka.server/received_bytes (Bytes Recebidos / Ingestão) ---"
query_metric "io.confluent.kafka.server/received_bytes" | jq .

echo
echo "--- io.confluent.kafka.server/sent_bytes (Bytes Enviados / Egress / Flink & Consumidores) ---"
query_metric "io.confluent.kafka.server/sent_bytes" | jq .

echo
echo "--- io.confluent.kafka.server/retained_bytes (Bytes Retidos em Disco) ---"
query_metric "io.confluent.kafka.server/retained_bytes" | jq .

echo
echo "=== 2. Grupos de Consumo e Lag ==="
confluent kafka consumer group list --cluster "$CLUSTER_ID" --environment "$ENV_ID"

if [ "$CREATED_TEMP_KEY" = true ]; then
  confluent api-key delete "$METRICS_CLOUD_KEY" --force >/dev/null 2>&1 || true
fi
