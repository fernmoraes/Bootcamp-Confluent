#!/usr/bin/env bash
# Camada 5 — Operação (Sub-tarefa 7: Os Quatro Pilares)
# Coleta dados reais de observabilidade, segurança, custos e confiabilidade.
# Gera evidencias/06-operacao.txt. Idempotente.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

[ -n "${ENV_ID:-}" ]     || die "ENV_ID não definido no .state."
[ -n "${CLUSTER_ID:-}" ] || die "CLUSTER_ID não definido no .state."

OUT="$ROOT/evidencias/06-operacao.txt"
TMP="$OUT.tmp"
TMP_METRICS="$(mktemp -t op-metrics.XXXXXX.log)"

log "1. Coletando métricas de telemetria..."
"$ROOT/scripts/metrics.sh" > "$TMP_METRICS" 2>&1 || true

log "2. Coletando dados de faturamento..."
TMP_COST="$(mktemp -t op-cost.XXXXXX.log)"
confluent billing cost list --start-date "2026-11-01" --end-date "$(date -u '+%Y-%m-%d')" > "$TMP_COST" 2>&1 || true

log "3. Montando relatório consolidado dos 4 pilares..."
{
  echo "== Camada 5 — Operação e Governança: Os Quatro Pilares (Evidências Reais)"
  echo "Data da Coleta: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo "Cluster ID: $CLUSTER_ID"
  echo "Environment ID: $ENV_ID"
  echo

  echo "=== PILAR 1: Observabilidade (Métricas e Lag) ==="
  cat "$TMP_METRICS"
  echo

  echo "=== PILAR 2: Segurança e Controle de Acesso (Matriz de Menor Privilégio) ==="
  echo "Service Accounts e ACLs Kafka configuradas:"
  confluent kafka acl list --cluster "$CLUSTER_ID" --environment "$ENV_ID"
  echo
  echo "RBAC no Schema Registry / Stream Governance (Producer: $PRODUCER_SA_NAME):"
  confluent iam rbac role-binding list --principal "User:$PRODUCER_SA" --environment "$ENV_ID" --inclusive
  echo
  echo "RBAC no Schema Registry (Consumer: $CONSUMER_SA_NAME):"
  confluent iam rbac role-binding list --principal "User:$CONSUMER_SA" --environment "$ENV_ID" --inclusive
  echo

  echo "=== PILAR 3: Custos Reais e Faturamento Confluent Cloud ==="
  cat "$TMP_COST"
  echo

  echo "=== PILAR 4: Confiabilidade Fim a Fim e Idempotência ==="
  echo "Análise de Garantias por Trecho:"
  echo "1. Postgres WAL -> CDC Debezium: At-least-once garantido via replication slot LSN."
  echo "2. Debezium -> Tópicos Kafka: At-least-once com acks=all e RF=3."
  echo "3. Kafka -> Flink SQL -> Tópicos de Alerta: Exactly-once nativo com checkpoints e transações."
  echo "4. Tópico de Alerta -> Consumidor: At-least-once."
  echo "   [ELO FRACO]: Efeito colateral do consumidor (ex: bloquear cartão / notificar)."
  echo "   Mitigação: Idempotência na aplicação consumidora chaveada por (card_id, first_transaction_id, window_start)."
} > "$TMP"

rm -f "$TMP_METRICS" "$TMP_COST"

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
log "Evidência da Sub-tarefa 7 salva com sucesso em $OUT"
cat "$OUT"
