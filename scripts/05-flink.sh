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

# DDL (statements efêmeros: rodam, completam e são apagados)
# (as tabelas de saída desafio.transactions.enriched / desafio.fraud.detected são criadas pelo Flink a partir
#  dos schemas Avro registrados; enriched precisa de watermark em txn_ts para o MATCH_RECOGNIZE.)
run_ddl() {
  local name="$1" sql_content="$2"
  confluent flink statement delete "$name" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
  confluent flink statement create "$name" \
    --sql "$sql_content" \
    --compute-pool "$POOL_ID" --database "$CLUSTER_ID" \
    --environment "$ENV_ID" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" \
    --wait >/dev/null 2>&1 || true
  confluent flink statement delete "$name" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
}

# Tabela de contas com PK e tempo de evento (necessária ao temporal join por FOR SYSTEM_TIME AS OF).
run_ddl desafio-ddl-accounts "CREATE TABLE IF NOT EXISTS \`desafio.accounts.versioned\` (
    account_id INT,
    customer_id INT,
    account_type STRING,
    risk_tier STRING,
    updated_at TIMESTAMP_LTZ(3),
    PRIMARY KEY (account_id) NOT ENFORCED,
    WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
  );"

# A tabela do CDC chega como changelog (retract) e com txn_ts em STRING. Para o temporal join e o
# MATCH_RECOGNIZE: (1) só INSERTs (changelog.mode=append), (2) tempo de evento = txn_ts do Postgres.
# O ADD de coluna só roda se txn_time ainda não existir (idempotência na 2ª execução do setup).
run_ddl desafio-ddl-txn-append "ALTER TABLE \`desafio.public.transactions\` SET ('changelog.mode' = 'append');"
if ! confluent flink statement create desafio-ddl-txn-chk --sql "SELECT txn_time FROM \`desafio.public.transactions\` LIMIT 0;" \
     --compute-pool "$POOL_ID" --database "$CLUSTER_ID" --environment "$ENV_ID" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" \
     --wait >/dev/null 2>&1 \
   || [ "$(confluent flink statement describe desafio-ddl-txn-chk --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json 2>/dev/null | jq -r .status)" = "FAILED" ]; then
  run_ddl desafio-ddl-txn-time "ALTER TABLE \`desafio.public.transactions\` ADD (txn_time AS TO_TIMESTAMP_LTZ(REPLACE(SUBSTRING(txn_ts, 1, 19), 'T', ' '), 'yyyy-MM-dd HH:mm:ss', 'UTC'));"
fi
confluent flink statement delete desafio-ddl-txn-chk --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
run_ddl desafio-ddl-txn-watermark "ALTER TABLE \`desafio.public.transactions\` MODIFY WATERMARK FOR txn_time AS txn_time - INTERVAL '5' SECOND;"

run_ddl desafio-ddl-enriched-wm "ALTER TABLE \`desafio.transactions.enriched\` MODIFY WATERMARK FOR txn_ts AS txn_ts - INTERVAL '5' SECOND;"

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

deploy_stmt "desafio-accounts-populate" "INSERT INTO \`desafio.accounts.versioned\` (account_id, customer_id, account_type, risk_tier, updated_at) SELECT account_id, customer_id, account_type, risk_tier, TO_TIMESTAMP_LTZ(REPLACE(SUBSTRING(updated_at, 1, 19), 'T', ' '), 'yyyy-MM-dd HH:mm:ss', 'UTC') FROM \`desafio.public.accounts\`;"
deploy_stmt "desafio-enrichment"        "$(cat "$ROOT/sql/01-enrichment.sql")"
deploy_stmt "desafio-fraud-detection"   "$(cat "$ROOT/sql/02-fraud-detection.sql")"

log "processamento Flink SQL ativo e operacional"
