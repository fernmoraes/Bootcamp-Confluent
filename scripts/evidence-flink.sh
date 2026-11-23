#!/usr/bin/env bash
# Captura as evidências da Camada 4 (Flink SQL e Alerta de Fraude) em evidencias/04-alerta-fraude.txt.
# Tudo vem de consultas REAIS: Flink (statements), Kafka (alertas) e Postgres (transações de origem).
# NÃO imprime nem grava segredos.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

[ -n "${ENV_ID:-}" ]     || die "ENV_ID não definido no .state."
[ -n "${CLUSTER_ID:-}" ] || die "CLUSTER_ID não definido no .state."
[ -n "${SR_URL:-}" ]     || die "SR_URL não definido no .state."
pg_admin_env             || die "psql não encontrado (veja README, Pré-requisitos)."

OUT="$ROOT/evidencias/04-alerta-fraude.txt"
TMP="$OUT.tmp"
TMP_RAW="$(mktemp -t fraud-alert.XXXXXX)"

log "consumindo alertas do tópico ${TOPIC_PREFIX}.fraud.detected"
confluent kafka topic consume "${TOPIC_PREFIX}.fraud.detected" \
  --cluster "$CLUSTER_ID" \
  --environment "$ENV_ID" \
  --api-key "$CONSUMER_KAFKA_KEY" \
  --api-secret "$CONSUMER_KAFKA_SECRET" \
  --value-format avro \
  --schema-registry-endpoint "$SR_URL" \
  --schema-registry-api-key "$CONSUMER_SR_KEY" \
  --schema-registry-api-secret "$CONSUMER_SR_SECRET" \
  --group "${CONSUMER_GROUP_PREFIX}fraud-evidence-$(date +%s)" \
  --from-beginning > "$TMP_RAW" 2>&1 &
PID=$!
sleep 25
kill $PID 2>/dev/null || true
wait $PID 2>/dev/null || true

ALERTS="$(grep '^{' "$TMP_RAW" | jq -c . | sort -t: -k2 || true)"
[ -n "$ALERTS" ] || { rm -f "$TMP_RAW"; die "nenhum alerta lido do tópico (aguarde o Flink processar e tente de novo)."; }

{
  echo "== Camada 4 — Processamento Flink SQL (Evidências reais)"
  echo "Data: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo

  echo "=== 1. Statements Flink SQL em execução (compute pool: ${FLINK_COMPUTE_POOL_NAME:-desafio-pool})"
  confluent flink statement list --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json 2>/dev/null \
    | jq -r '.[] | select(.name | startswith("desafio")) | "  - \(.name) [Status: \(.status)]"'
  echo

  echo "=== 2. Alertas emitidos em ${TOPIC_PREFIX}.fraud.detected (lidos com a identidade ${CONSUMER_SA_NAME:-desafio-consumer})"
  echo "Regra: RULE_VELOCITY via MATCH_RECOGNIZE (PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND, PARTITION BY card_id)"
  echo "Total de alertas: $(printf '%s\n' "$ALERTS" | wc -l | tr -d ' ')"
  echo
  printf '%s\n' "$ALERTS" | jq -c '{card_id, customer_id, first_transaction_id, last_transaction_id, txn_count, total_amount, window_start, window_end, rule_name}'
  echo

  echo "=== 3. Transações de origem de cada alerta (consulta REAL ao Postgres, mesmo card_id, janela < 60 s)"
  printf '%s\n' "$ALERTS" | jq -r '[.card_id, .first_transaction_id, .last_transaction_id] | @tsv' | while IFS=$'\t' read -r card first last; do
    echo "--- card_id=$card (transações $first a $last)"
    psql -X -P pager=off -c "SELECT transaction_id, card_id, amount, status, txn_ts FROM transactions WHERE card_id = $card AND transaction_id BETWEEN $first AND $last ORDER BY txn_ts"
  done

  echo "=== 4. Cartões que NÃO devem gerar alerta (seed): 126 (2 transações em 30 s) e 86 (3 transações espaçadas, 90 s)"
  if printf '%s\n' "$ALERTS" | jq -e 'select(.card_id==126 or .card_id==86)' >/dev/null 2>&1; then
    echo "ATENÇÃO: houve alerta para 126 ou 86 (inesperado)."
  else
    echo "Nenhum alerta para os cartões 126 e 86, como esperado."
  fi
} > "$TMP"

rm -f "$TMP_RAW"

# Verificação estrita de segredos antes de salvar
if [ -f "$STATE_FILE" ]; then
  while IFS='=' read -r k v; do
    [ -z "$v" ] && continue
    if [ "${#v}" -ge 8 ] && grep -Fq "$v" "$TMP"; then
      rm -f "$TMP"
      die "VAZAMENTO DE SEGREDOS DETECTADO: $k vazou na evidência!"
    fi
  done < "$STATE_FILE"
fi
for v in "${PG_ADMIN_PASSWORD:-}" "${CDC_PASSWORD:-}"; do
  if [ -n "$v" ] && grep -Fq "$v" "$TMP"; then rm -f "$TMP"; die "VAZAMENTO DE SEGREDO DO POSTGRES na evidência!"; fi
done

mv "$TMP" "$OUT"
log "evidência salva em $OUT"
cat "$OUT"
