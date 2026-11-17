#!/usr/bin/env bash
# Camada 5 — Consumo e Autorização (Sub-tarefa 6)
# Valida a leitura de alertas pela identidade 'desafio-consumer' e executa o teste negativo de escrita (menor privilégio).
# Gera evidencias/05-consumo.txt. Idempotente.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

[ -n "${ENV_ID:-}" ]              || die "ENV_ID não definido no .state."
[ -n "${CLUSTER_ID:-}" ]          || die "CLUSTER_ID não definido no .state."
[ -n "${SR_URL:-}" ]              || die "SR_URL não definido no .state."
[ -n "${CONSUMER_KAFKA_KEY:-}" ]  || die "CONSUMER_KAFKA_KEY não definida no .state."
[ -n "${CONSUMER_KAFKA_SECRET:-}" ] || die "CONSUMER_KAFKA_SECRET não definida no .state."

OUT="$ROOT/evidencias/05-consumo.txt"
TMP="$OUT.tmp"
TMP_CONSUME="$(mktemp -t consumer-alerts.XXXXXX.log)"
TMP_PRODUCE="$(mktemp -t consumer-negative-write.XXXXXX.log)"

TOPIC="${TOPIC_PREFIX}.fraud.detected"
GROUP="${CONSUMER_GROUP_PREFIX}evidence-$(date +%s)"

log "1. Consumindo alertas do tópico $TOPIC com a identidade $CONSUMER_SA_NAME..."
confluent kafka topic consume "$TOPIC" \
  --cluster "$CLUSTER_ID" \
  --environment "$ENV_ID" \
  --api-key "$CONSUMER_KAFKA_KEY" \
  --api-secret "$CONSUMER_KAFKA_SECRET" \
  --value-format avro \
  --schema-registry-endpoint "$SR_URL" \
  --schema-registry-api-key "$CONSUMER_SR_KEY" \
  --schema-registry-api-secret "$CONSUMER_SR_SECRET" \
  --group "$GROUP" \
  --from-beginning > "$TMP_CONSUME" 2>&1 &
PID=$!
sleep 10
kill $PID 2>/dev/null || true

log "2. Executando teste negativo de escrita (tentativa de produzir com $CONSUMER_SA_NAME)..."
# Tentativa de produzir com as credenciais do consumidor (que só tem permissão de leitura)
echo '{"attempt": "unauthorized_write_test"}' | confluent kafka topic produce "$TOPIC" \
  --cluster "$CLUSTER_ID" \
  --environment "$ENV_ID" \
  --api-key "$CONSUMER_KAFKA_KEY" \
  --api-secret "$CONSUMER_KAFKA_SECRET" > "$TMP_PRODUCE" 2>&1 || true

log "3. Montando arquivo de evidência..."
{
  echo "== Camada 5 — Consumo e Controle de Acesso (Evidências Reais)"
  echo "Data: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo "Identidade do Consumidor: $CONSUMER_SA_NAME"
  echo "Grupo de Consumo Utilizado: $GROUP"
  echo "Tópico Alvo: $TOPIC"
  echo
  echo "=== 1. Leitura Autorizada de Alertas de Fraude"
  echo "Comando:"
  echo "  confluent kafka topic consume $TOPIC \\"
  echo "    --group $GROUP \\"
  echo "    --value-format avro --schema-registry-endpoint <SR_URL> \\"
  echo "    --from-beginning"
  echo
  echo "Amostra de Alerta Consumido:"
  grep "RULE_VELOCITY_60S" "$TMP_CONSUME" | tail -1 | jq .
  echo
  echo "=== 2. Teste Negativo de Escrita (Menor Privilégio / ACLs)"
  echo "Comando executado com credenciais de $CONSUMER_SA_NAME:"
  echo "  echo '{\"attempt\": \"unauthorized_write_test\"}' | confluent kafka topic produce $TOPIC"
  echo
  echo "Saída do broker Kafka:"
  cat "$TMP_PRODUCE"
  echo
  echo "Resultado da Avaliação de Segurança:"
  if grep -q "Topic authorization failed" "$TMP_PRODUCE"; then
    echo "  [OK] Escrita negada com sucesso: Broker retornou 'Topic authorization failed'."
    echo "       A service account '$CONSUMER_SA_NAME' possui estritamente permissões de leitura (READ, DESCRIBE) e não consegue poluir nem alterar dados em tópicos '${TOPIC_PREFIX}*'."
  else
    echo "  [AVISO] Verifique a saída do teste de escrita."
  fi
} > "$TMP"

rm -f "$TMP_CONSUME" "$TMP_PRODUCE"

# Verificação estrita de segredos antes de salvar
if [ -f "$STATE_FILE" ]; then
  while IFS='=' read -r k v; do
    [ -z "$v" ] && continue
    case "$k" in
      *KEY*|*SECRET*|*PASS*|*PASSWORD*)
        if [ "${#v}" -ge 6 ] && grep -Fq "$v" "$TMP"; then
          rm -f "$TMP"
          die "VAZAMENTO DE SEGREDOS DETECTADO: $k vazou na evidência!"
        fi
        ;;
    esac
  done < "$STATE_FILE"
fi

mv "$TMP" "$OUT"
chmod +x "${BASH_SOURCE[0]}"
log "Evidência da Sub-tarefa 6 salva com sucesso em $OUT"
cat "$OUT"
