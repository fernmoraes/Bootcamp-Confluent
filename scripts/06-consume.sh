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
# Em qualquer saída (sucesso ou falha) apaga os temporários; a evidência anterior só é trocada pelo mv final.
trap 'rm -f "$TMP" "$TMP_CONSUME" "$TMP_PRODUCE"' EXIT

TOPIC="${TOPIC_PREFIX}.fraud.detected"
TRIES="${CONSUME_TRIES:-6}"   # cluster recém-criado: leitura e ACL podem demorar a propagar

# Sucesso = ao menos um alerta lido. Um cluster novo pode devolver só "Starting Kafka Consumer" nos primeiros segundos.
consume_alerts() {
  local try
  for try in $(seq 1 "$TRIES"); do
    GROUP="${CONSUMER_GROUP_PREFIX}evidence-$(date +%s)"
    : > "$TMP_CONSUME"
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
    local pid=$!
    sleep 10
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    grep -q "RULE_VELOCITY_60S" "$TMP_CONSUME" && return 0
    log "   tentativa $try/$TRIES: nenhum alerta lido (última linha: $(tail -1 "$TMP_CONSUME" | cut -c1-120)); tentando de novo"
    sleep 5
  done
  return 1
}

# Teste negativo. Só "Topic authorization failed" aprova (e encerra sem repetir).
# Qualquer outra saída (conexão, propagação, erro genérico) repete; esgotadas as tentativas, é FALHA.
negative_write() {
  local try
  for try in $(seq 1 "$TRIES"); do
    echo '{"attempt": "unauthorized_write_test"}' | confluent kafka topic produce "$TOPIC" \
      --cluster "$CLUSTER_ID" \
      --environment "$ENV_ID" \
      --api-key "$CONSUMER_KAFKA_KEY" \
      --api-secret "$CONSUMER_KAFKA_SECRET" > "$TMP_PRODUCE" 2>&1 || true
    grep -q "Topic authorization failed" "$TMP_PRODUCE" && return 0
    log "   tentativa $try/$TRIES: sem 'Topic authorization failed' (saída: $(tail -1 "$TMP_PRODUCE" | cut -c1-160)); tentando de novo"
    sleep 5
  done
  return 1
}

log "1. Consumindo alertas do tópico $TOPIC com a identidade $CONSUMER_SA_NAME..."
consume_alerts || die "nenhum alerta RULE_VELOCITY_60S lido em $TRIES tentativas; evidência anterior mantida."

log "2. Executando teste negativo de escrita (tentativa de produzir com $CONSUMER_SA_NAME)..."
negative_write || die "não vi 'Topic authorization failed' em $TRIES tentativas; NÃO aprovo o teste; evidência anterior mantida."

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
  echo "  [OK] Escrita negada com sucesso: Broker retornou 'Topic authorization failed'."
  echo "       A service account '$CONSUMER_SA_NAME' possui estritamente permissões de leitura (READ, DESCRIBE) e não consegue poluir nem alterar dados em tópicos '${TOPIC_PREFIX}*'."
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
