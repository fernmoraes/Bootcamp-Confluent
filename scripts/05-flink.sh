#!/usr/bin/env bash
# Camada 4 — Processamento Flink SQL: compute pool, temporal join e regra de velocidade (MATCH_RECOGNIZE).
# Idempotente: se compute pool e statements já existirem, reaproveita.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

[ -n "${ENV_ID:-}" ]     || die "ENV_ID não definido no .state. Execute scripts/01-foundation.sh primeiro."
[ -n "${CLUSTER_ID:-}" ] || die "CLUSTER_ID não definido no .state. Execute scripts/01-foundation.sh primeiro."

POOL_NAME="${FLINK_COMPUTE_POOL_NAME:-desafio-pool}"

# ---------- 1. Compute Pool Flink ----------
log "verificando compute pool Flink $POOL_NAME ($CLUSTER_CLOUD/$CLUSTER_REGION)"
POOL_ID="$(confluent flink compute-pool list --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json 2>/dev/null \
  | jq -r --arg n "$POOL_NAME" '.[]? | select(.name==$n) | .id' | head -1 || true)"

if [ -z "$POOL_ID" ]; then
  log "criando compute pool $POOL_NAME (max-cfu 5)"
  POOL_JSON="$(confluent flink compute-pool create "$POOL_NAME" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --max-cfu 5 --environment "$ENV_ID" -o json)"
  POOL_ID="$(jq -r .id <<<"$POOL_JSON")"
fi
state_set FLINK_POOL_ID "$POOL_ID"
log "compute pool Flink: $POOL_NAME ($POOL_ID)"

log "aguardando compute pool ficar PROVISIONED ou RUNNING"
for _ in $(seq 1 30); do
  st="$(confluent flink compute-pool describe "$POOL_ID" --environment "$ENV_ID" -o json 2>/dev/null | jq -r .status)"
  [ "$st" = "PROVISIONED" ] || [ "$st" = "RUNNING" ] && break
  sleep 10
done

# ---------- 2. Tópicos de Saída / Schemas e Tabelas Flink ----------
log "garantindo tópicos de saída e schemas para o Flink"
if ! confluent kafka topic describe desafio.transactions.enriched --cluster "$CLUSTER_ID" --environment "$ENV_ID" >/dev/null 2>&1; then
  confluent schema-registry schema create --subject desafio.transactions.enriched-value --schema "$ROOT/schemas/transactions-enriched.avsc" --type avro --environment "$ENV_ID" >/dev/null 2>&1 || true
  confluent kafka topic create desafio.transactions.enriched --partitions 1 --cluster "$CLUSTER_ID" --environment "$ENV_ID" >/dev/null 2>&1 || true
fi

if ! confluent kafka topic describe desafio.fraud.detected --cluster "$CLUSTER_ID" --environment "$ENV_ID" >/dev/null 2>&1; then
  confluent schema-registry schema create --subject desafio.fraud.detected-value --schema "$ROOT/schemas/fraud-detected.avsc" --type avro --environment "$ENV_ID" >/dev/null 2>&1 || true
  confluent kafka topic create desafio.fraud.detected --partitions 1 --cluster "$CLUSTER_ID" --environment "$ENV_ID" >/dev/null 2>&1 || true
fi

# Cria tabela versionada de contas se não existir
confluent flink statement create create-accounts-versioned \
  --sql 'CREATE TABLE IF NOT EXISTS `desafio.accounts.versioned` (
    account_id INT,
    customer_id INT,
    account_type STRING,
    risk_tier STRING,
    PRIMARY KEY (account_id) NOT ENFORCED
  );' \
  --compute-pool "$POOL_ID" \
  --database "$CLUSTER_ID" \
  --environment "$ENV_ID" \
  --cloud "$CLUSTER_CLOUD" \
  --region "$CLUSTER_REGION" \
  --wait >/dev/null 2>&1 || true
confluent flink statement delete create-accounts-versioned --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true

# ---------- 3. Deploy dos Statements Flink SQL ----------
deploy_stmt() {
  local name="$1" sql_content="$2"
  log "verificando statement $name"
  local existing_st
  existing_st="$(confluent flink statement list --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json 2>/dev/null \
    | jq -r --arg n "$name" '.[]? | select(.name==$n) | .status // empty' | head -1 || true)"
  
  if [ "$existing_st" = "RUNNING" ] || [ "$existing_st" = "COMPLETED" ]; then
    log "statement $name já está $existing_st"
    return
  elif [ -n "$existing_st" ]; then
    log "removendo statement anterior $name (status=$existing_st)"
    confluent flink statement delete "$name" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
    sleep 5
  fi
  
  log "submetendo statement $name"
  confluent flink statement create "$name" \
    --sql "$sql_content" \
    --compute-pool "$POOL_ID" \
    --database "$CLUSTER_ID" \
    --environment "$ENV_ID" \
    --cloud "$CLUSTER_CLOUD" \
    --region "$CLUSTER_REGION" \
    --property sql.tables.scan.idle-timeout=5s \
    --wait
}

deploy_stmt "desafio-accounts-populate" "INSERT INTO \`desafio.accounts.versioned\` (account_id, customer_id, account_type, risk_tier) SELECT account_id, customer_id, account_type, risk_tier FROM \`desafio.public.accounts\`;"
deploy_stmt "desafio-enrichment"        "$(cat "$ROOT/sql/01-enrichment.sql")"
deploy_stmt "desafio-fraud-detection"   "$(cat "$ROOT/sql/02-fraud-detection.sql")"

log "processamento Flink SQL ativo e operacional"
