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
  local name="$1" sql_content="$2" try st
  for try in 1 2 3 4 5 6; do
    confluent flink statement delete "$name" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
    confluent flink statement create "$name" \
      --sql "$sql_content" \
      --compute-pool "$POOL_ID" --database "$CLUSTER_ID" \
      --environment "$ENV_ID" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" \
      --wait >/dev/null 2>&1 || true
    st="$(confluent flink statement describe "$name" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.status // empty')"
    confluent flink statement delete "$name" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
    if [ "$st" = "COMPLETED" ] || [ "$st" = "RUNNING" ]; then log "DDL $name: $st"; return 0; fi
    log "DDL $name: status=${st:-desconhecido} (tentativa $try/6), tentando de novo em 15s"; sleep 15
  done
  die "DDL $name falhou após 6 tentativas"
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

# O DDL acima pode terminar COMPLETED e deixar só o tópico e os subjects, sem a tabela no catálogo do Flink
# (observado em 2 ciclos de setup). Confirma com SHOW CREATE TABLE; se não existir, apaga os restos e recria.
table_visible() {
  local n=desafio-chk-tbl st
  confluent flink statement delete "$n" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
  confluent flink statement create "$n" --sql "SHOW CREATE TABLE \`$1\`;" --compute-pool "$POOL_ID" --database "$CLUSTER_ID" \
    --environment "$ENV_ID" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --wait >/dev/null 2>&1 || true
  st="$(confluent flink statement describe "$n" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.status // empty')"
  confluent flink statement delete "$n" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
  [ "$st" = "COMPLETED" ]
}
if ! table_visible desafio.accounts.versioned; then
  log "tabela desafio.accounts.versioned fora do catálogo; limpando tópico/subjects órfãos e recriando"
  confluent kafka topic delete desafio.accounts.versioned --cluster "$CLUSTER_ID" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
  for sfx in key value; do
    for perm in "" "?permanent=true"; do
      curl -s -o /dev/null -X DELETE -u "$PRODUCER_SR_KEY:$PRODUCER_SR_SECRET" "$SR_URL/subjects/desafio.accounts.versioned-$sfx$perm" || true
    done
  done
  sleep 15
  run_ddl desafio-ddl-accounts "CREATE TABLE \`desafio.accounts.versioned\` (
      account_id INT, customer_id INT, account_type STRING, risk_tier STRING, updated_at TIMESTAMP_LTZ(3),
      PRIMARY KEY (account_id) NOT ENFORCED,
      WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
    );"
  ok=0; for _ in $(seq 1 12); do table_visible desafio.accounts.versioned && { ok=1; break; }; sleep 10; done
  [ "$ok" = 1 ] || die "tabela desafio.accounts.versioned continua fora do catálogo do Flink"
fi
log "tabela desafio.accounts.versioned visível no catálogo"

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
    --wait >/dev/null 2>&1 || true
  local st
  st="$(confluent flink statement describe "$name" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.status // empty')"
  if [ "$st" != "RUNNING" ] && [ "$st" != "COMPLETED" ]; then
    die "statement $name ficou $st: $(confluent flink statement describe "$name" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.status_detail // empty')"
  fi
  log "statement $name: $st"
}

deploy_stmt "desafio-accounts-populate" "INSERT INTO \`desafio.accounts.versioned\` (account_id, customer_id, account_type, risk_tier, updated_at) SELECT account_id, customer_id, account_type, risk_tier, TO_TIMESTAMP_LTZ(REPLACE(SUBSTRING(updated_at, 1, 19), 'T', ' '), 'yyyy-MM-dd HH:mm:ss', 'UTC') FROM \`desafio.public.accounts\`;"
# O populate é um statement de streaming e nunca termina: não dá para esperar pelo status dele.
# O temporal join do enrichment e do fraud-detection precisa encontrar TODAS as contas já em accounts.versioned
# quando for avaliado. Com sql.tables.scan.idle-timeout=5s, uma pausa de ~5 s entre as levas de gravação do populate
# faz o join disparar e descartar (inner join) as transações das contas que chegam depois. Por isso esperamos a
# CONTAGEM de contas distintas chegar ao total da origem antes de subir os statements que fazem o join.
wait_accounts_versioned() {
  local timeout="${ACCOUNTS_WAIT_TIMEOUT:-600}" name=desafio-wait-accounts expected n=0 deadline org kj k sec url json page
  pg_admin_env || die "psql/credenciais PG ausentes: preciso contar as contas na origem para saber quando accounts.versioned está completa."
  expected="$(psql -X -tA -c 'SELECT count(*) FROM accounts')"
  [ -n "$expected" ] && [ "$expected" -gt 0 ] 2>/dev/null || die "não consegui contar as contas na origem (Postgres)."
  org="$(confluent organization describe -o json | jq -r .id)"
  kj="$(confluent api-key create --resource flink --environment "$ENV_ID" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" \
        --description "temporaria: espera accounts.versioned" -o json)" || die "não consegui criar a Flink API key temporária."
  k="$(jq -r .api_key <<<"$kj")"; sec="$(jq -r .api_secret <<<"$kj")"
  log "aguardando accounts.versioned ter $expected contas distintas (timeout ${timeout}s)"
  deadline=$(( $(date +%s) + timeout ))
  while :; do
    confluent flink statement delete "$name" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
    confluent flink statement create "$name" \
      --sql "SELECT COUNT(DISTINCT account_id) AS n FROM \`desafio.accounts.versioned\` /*+ OPTIONS('scan.bounded.mode'='latest-offset') */;" \
      --compute-pool "$POOL_ID" --database "$CLUSTER_ID" --environment "$ENV_ID" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" \
      --property sql.tables.scan.idle-timeout=5s --wait >/dev/null 2>&1 || true
    n=0
    url="https://flink.${CLUSTER_REGION}.${CLUSTER_CLOUD}.confluent.cloud/sql/v1/organizations/${org}/environments/${ENV_ID}/statements/${name}/results"
    while [ -n "$url" ]; do
      json="$(curl -s -u "$k:$sec" "$url" || true)"
      page="$(jq -r '[.results.data[]?.row[0] | tonumber] | max // 0' <<<"$json" 2>/dev/null || echo 0)"
      [ "${page:-0}" -gt "$n" ] 2>/dev/null && n="$page"
      url="$(jq -r '.metadata.next // empty' <<<"$json" 2>/dev/null || true)"
    done
    log "accounts.versioned: $n/$expected contas distintas"
    [ "$n" -ge "$expected" ] && break
    if [ "$(date +%s)" -ge "$deadline" ]; then
      confluent flink statement delete "$name" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
      confluent api-key delete "$k" --force >/dev/null 2>&1 || true
      die "accounts.versioned só chegou a $n/$expected contas em ${timeout}s. O desafio-accounts-populate está RUNNING? Não subo o enrichment/fraud para não perder transações no join."
    fi
    sleep 15
  done
  confluent flink statement delete "$name" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force >/dev/null 2>&1 || true
  confluent api-key delete "$k" --force >/dev/null 2>&1 || true
}
wait_accounts_versioned

deploy_stmt "desafio-enrichment"        "$(cat "$ROOT/sql/01-enrichment.sql")"
deploy_stmt "desafio-fraud-detection"   "$(cat "$ROOT/sql/02-fraud-detection.sql")"

log "processamento Flink SQL ativo e operacional"
