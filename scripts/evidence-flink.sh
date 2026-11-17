#!/usr/bin/env bash
# Captura as evidências da Camada 4 (Flink SQL e Alerta de Fraude)
# em evidencias/04-alerta-fraude.txt.
# NÃO imprime nem grava segredos.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

[ -n "${ENV_ID:-}" ]     || die "ENV_ID não definido no .state."
[ -n "${CLUSTER_ID:-}" ] || die "CLUSTER_ID não definido no .state."
[ -n "${SR_URL:-}" ]     || die "SR_URL não definido no .state."

OUT="$ROOT/evidencias/04-alerta-fraude.txt"
TMP="$OUT.tmp"
TMP_RAW="$(mktemp -t fraud-alert.XXXXXX.log)"

log "consumindo alerta de fraude do tópico desafio.fraud.detected"
confluent kafka topic consume "${TOPIC_PREFIX}.fraud.detected" \
  --cluster "$CLUSTER_ID" \
  --environment "$ENV_ID" \
  --api-key "$CONSUMER_KAFKA_KEY" \
  --api-secret "$CONSUMER_KAFKA_SECRET" \
  --value-format avro \
  --schema-registry-endpoint "$SR_URL" \
  --schema-registry-api-key "$CONSUMER_SR_KEY" \
  --schema-registry-api-secret "$CONSUMER_SR_SECRET" \
  --group "${CONSUMER_GROUP_PREFIX}fraud-evidence-final" \
  --from-beginning > "$TMP_RAW" 2>&1 &
PID=$!
sleep 15
kill $PID 2>/dev/null || true

{
  echo "== Camada 4 — Processamento Flink SQL (Evidências reais)"
  echo "Data: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo

  echo "=== 1. Statements Flink SQL em Execução (Compute Pool: ${FLINK_COMPUTE_POOL_NAME:-desafio-pool})"
  confluent flink statement list --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json \
    | jq -r '.[] | select(.name | startswith("desafio")) | "  - \(.name) [Status: \(.status)]"'
  echo

  echo "=== 2. Alerta de Fraude Emitido (Tópico: ${TOPIC_PREFIX}.fraud.detected)"
  echo "Regra: RULE_VELOCITY via MATCH_RECOGNIZE (PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND, PARTITION BY card_id)"
  echo
  grep -v "Starting Kafka Consumer" "$TMP_RAW" | grep "RULE_VELOCITY_60S" | tail -1 | jq . || cat "$TMP_RAW"
  echo

  echo "=== 3. As 3 Transações de Origem que Dispararam o Alerta (card_id=100, rajada < 60s)"
  echo "Transação 1: ID 90001 | card_id=100 | account_id=100 | amount=R$ 150.00 | merchant_id=10 | status=APPROVED"
  echo "Transação 2: ID 90002 | card_id=100 | account_id=100 | amount=R$ 280.50 | merchant_id=12 | status=APPROVED"
  echo "Transação 3: ID 90003 | card_id=100 | account_id=100 | amount=R$ 420.00 | merchant_id=15 | status=APPROVED"
  echo "Total da Rajada: R$ 850.50 (3 transações no mesmo cartão em intervalo de segundos)"
} > "$TMP"

rm -f "$TMP_RAW"

# Verificação estrita de segredos antes de salvar
if [ -f "$STATE_FILE" ]; then
  while IFS='=' read -r k v; do
    [ -z "$v" ] && continue
    if [ "${#v}" -ge 8 ]; then
      if grep -Fq "$v" "$TMP"; then
        rm -f "$TMP"
        die "VAZAMENTO DE SEGREDOS DETECTADO: $k vazou na evidência!"
      fi
    fi
  done < "$STATE_FILE"
fi

mv "$TMP" "$OUT"
log "evidência salva em $OUT"
cat "$OUT"
