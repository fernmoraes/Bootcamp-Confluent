#!/usr/bin/env bash
# Camada 3 — Ingestão CDC: criação do conector PostgresCdcSourceV2 na Confluent Cloud.
# Idempotente: se o conector já existir e estiver RUNNING, reaproveita.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

[ -n "${ENV_ID:-}" ]     || die "ENV_ID não definido no .state. Execute scripts/01-foundation.sh primeiro."
[ -n "${CLUSTER_ID:-}" ] || die "CLUSTER_ID não definido no .state. Execute scripts/01-foundation.sh primeiro."
[ -n "${PRODUCER_KAFKA_KEY:-}" ] || die "PRODUCER_KAFKA_KEY não definida no .state."
[ -n "${PRODUCER_KAFKA_SECRET:-}" ] || die "PRODUCER_KAFKA_SECRET não definida no .state."

CONN_NAME="${CONNECTOR_NAME:-desafio-cdc}"

log "verificando conector $CONN_NAME no cluster $CLUSTER_ID"
CONN_ID="$(confluent connect cluster list --cluster "$CLUSTER_ID" --environment "$ENV_ID" -o json 2>/dev/null \
  | jq -r --arg n "$CONN_NAME" '.[]? | select(.name==$n or .id==$n) | .id' | head -1 || true)"

if [ -z "$CONN_ID" ]; then
  log "criando conector $CONN_NAME a partir de connectors/cdc.json"
  TMP_CFG="$(mktemp -t cdc-config.XXXXXX.json)"
  chmod 600 "$TMP_CFG"
  
  # Preenchimento seguro das variáveis de configuração
  export CONNECTOR_NAME="$CONN_NAME"
  export PRODUCER_KAFKA_KEY="$PRODUCER_KAFKA_KEY"
  export PRODUCER_KAFKA_SECRET="$PRODUCER_KAFKA_SECRET"
  export PG_HOST="$PG_HOST"
  export PG_PORT="${PG_PORT:-5432}"
  export CDC_USER="$CDC_USER"
  export CDC_PASSWORD="$CDC_PASSWORD"
  export PG_DATABASE="$PG_DATABASE"
  export PG_SSLMODE="${PG_SSLMODE:-require}"
  export TOPIC_PREFIX="${TOPIC_PREFIX:-desafio}"
  
  if command -v envsubst >/dev/null 2>&1; then
    envsubst < "$ROOT/connectors/cdc.json" > "$TMP_CFG"
  else
    python3 -c "
import os, sys
template = open(sys.argv[1]).read()
for k, v in os.environ.items():
    template = template.replace('\${' + k + '}', v)
open(sys.argv[2], 'w').write(template)
" "$ROOT/connectors/cdc.json" "$TMP_CFG"
  fi
  
  RES="$(confluent connect cluster create --config-file "$TMP_CFG" --cluster "$CLUSTER_ID" --environment "$ENV_ID" -o json)"
  rm -f "$TMP_CFG"
  CONN_ID="$(jq -r .id <<<"$RES")"
  log "conector criado com ID $CONN_ID"
else
  log "conector $CONN_NAME já existe ($CONN_ID)"
fi

state_set CONNECTOR_ID "$CONN_ID"

log "aguardando conector $CONN_ID ficar RUNNING"
for i in $(seq 1 40); do
  STATUS_JSON="$(confluent connect cluster describe "$CONN_ID" --cluster "$CLUSTER_ID" --environment "$ENV_ID" -o json 2>/dev/null || echo '{}')"
  CONN_STATUS="$(jq -r '.status.status // .status // empty' <<<"$STATUS_JSON")"
  TASKS_STATUS="$(jq -r '[.tasks[]?.status] | join(",")' <<<"$STATUS_JSON")"
  
  log "status conector: $CONN_STATUS | tasks: [${TASKS_STATUS:-pendente}] ($i/40)"
  
  if [ "$CONN_STATUS" = "RUNNING" ] && [ -n "$TASKS_STATUS" ] && [[ "$TASKS_STATUS" != *"FAILED"* ]]; then
    if [[ "$TASKS_STATUS" == *"RUNNING"* ]]; then
      log "conector $CONN_NAME está 100% RUNNING"
      break
    fi
  elif [ "$CONN_STATUS" = "FAILED" ] || [[ "$TASKS_STATUS" == *"FAILED"* ]]; then
    die "conector entrou em estado FAILED: $(jq -r '.tasks[]?.trace // empty' <<<"$STATUS_JSON")"
  fi
  sleep 15
done

log "ingestão CDC pronta: $CONN_NAME ($CONN_ID)"
