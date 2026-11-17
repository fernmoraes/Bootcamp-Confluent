#!/usr/bin/env bash
# Camada 1 — Fundação: environment, cluster Basic, Schema Registry, 2 identidades.
# Idempotente: reexecutar reaproveita o que já existe. Estado em .state (gitignored).
#
# Identidades (menor privilégio):
#   PRODUCER_SA_NAME  escreve em TOPIC_PREFIX* ; registra schemas (subjects TOPIC_PREFIX*)
#   CONSUMER_SA_NAME  lê TOPIC_PREFIX* + grupos CONSUMER_GROUP_PREFIX* ; lê schemas
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

# ---------- environment ----------
ENV_ID="$(confluent environment list -o json | jq -r --arg n "$ENV_NAME" '.[] | select(.name==$n) | .id' | head -1)"
if [ -z "$ENV_ID" ]; then
  log "criando environment $ENV_NAME"
  ENV_ID="$(confluent environment create "$ENV_NAME" --governance-package essentials -o json | jq -r .id)"
else log "environment $ENV_NAME já existe ($ENV_ID)"; fi
state_set ENV_ID "$ENV_ID"

# ---------- cluster Kafka Basic ----------
CLUSTER_ID="$(confluent kafka cluster list --environment "$ENV_ID" -o json | jq -r --arg n "$CLUSTER_NAME" '.[] | select(.name==$n) | .id' | head -1)"
if [ -z "$CLUSTER_ID" ]; then
  log "criando cluster Basic $CLUSTER_NAME ($CLUSTER_CLOUD/$CLUSTER_REGION)"
  CLUSTER_ID="$(confluent kafka cluster create "$CLUSTER_NAME" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" \
                 --type basic --availability single-zone --environment "$ENV_ID" -o json | jq -r .id)"
else log "cluster $CLUSTER_NAME já existe ($CLUSTER_ID)"; fi
state_set CLUSTER_ID "$CLUSTER_ID"

log "aguardando cluster ficar UP"
for _ in $(seq 1 40); do
  st="$(confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" -o json | jq -r .status)"
  [ "$st" = "UP" ] && break; sleep 15
done
[ "$st" = "UP" ] || die "cluster não ficou UP (status=$st)"
state_set BOOTSTRAP "$(confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" -o json | jq -r .endpoint | sed 's#^SASL_SSL://##')"
state_set KAFKA_REST "$(confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" -o json | jq -r .rest_endpoint)"

# ---------- Schema Registry (habilitado junto com o 1º cluster do environment) ----------
SR_JSON="$(retry 6 confluent schema-registry cluster describe --environment "$ENV_ID" -o json)" || die "Schema Registry indisponível"
SR_ID="$(jq -r .cluster <<<"$SR_JSON")"; SR_URL="$(jq -r .endpoint_url <<<"$SR_JSON")"
state_set SR_ID "$SR_ID"; state_set SR_URL "$SR_URL"
log "Schema Registry $SR_ID ($SR_URL)"

# ---------- service accounts ----------
sa_id() {   # sa_id NOME DESCRIÇÃO  -> imprime o ID (cria se faltar)
  local id
  id="$(confluent iam service-account list -o json | jq -r --arg n "$1" '.[] | select(.name==$n) | .id' | head -1)"
  [ -n "$id" ] || id="$(confluent iam service-account create "$1" --description "$2" -o json | jq -r .id)"
  echo "$id"
}
PRODUCER_SA="$(sa_id "$PRODUCER_SA_NAME" "Escreve nos topicos ${TOPIC_PREFIX}* (CDC e Flink)")"
CONSUMER_SA="$(sa_id "$CONSUMER_SA_NAME" "Le os topicos ${TOPIC_PREFIX}* (consumo dos alertas)")"
state_set PRODUCER_SA "$PRODUCER_SA"; state_set CONSUMER_SA "$CONSUMER_SA"
log "service accounts: $PRODUCER_SA_NAME=$PRODUCER_SA  $CONSUMER_SA_NAME=$CONSUMER_SA"

# ---------- ACLs Kafka (prefixo) ----------
acl() { retry 6 confluent kafka acl create --allow --environment "$ENV_ID" --cluster "$CLUSTER_ID" "$@" >/dev/null; }
log "ACLs do producer"
acl --service-account "$PRODUCER_SA" --operations create,write,describe --prefix --topic "$TOPIC_PREFIX"
acl --service-account "$PRODUCER_SA" --operations create,write,describe --prefix --topic "__debezium-heartbeat"
acl --service-account "$PRODUCER_SA" --operations describe,describe-configs --cluster-scope   # exigido pelo Debezium V2
log "ACLs do consumer"
acl --service-account "$CONSUMER_SA" --operations read,describe --prefix --topic "$TOPIC_PREFIX"
acl --service-account "$CONSUMER_SA" --operations read --prefix --consumer-group "$CONSUMER_GROUP_PREFIX"

# ---------- Schema Registry: RBAC por subject (SR não usa ACL) ----------
rb() { retry 6 confluent iam rbac role-binding create --environment "$ENV_ID" --schema-registry-cluster "$SR_ID" \
         --prefix --resource "Subject:${TOPIC_PREFIX}" "$@" >/dev/null; }
log "role bindings do Schema Registry"
rb --principal "User:$PRODUCER_SA" --role DeveloperWrite
rb --principal "User:$PRODUCER_SA" --role DeveloperRead
rb --principal "User:$CONSUMER_SA" --role DeveloperRead

# ---------- API keys (uma por SA e por recurso; secret só aparece na criação) ----------
mkkey() {   # mkkey VAR_PREFIX RESOURCE SA
  local p=$1 res=$2 sa=$3 j
  state_load
  if [ -n "$(eval echo "\${${p}_KEY:-}")" ]; then log "API key $p já existe no .state"; return; fi
  j="$(retry 6 confluent api-key create --resource "$res" --service-account "$sa" --environment "$ENV_ID" \
        --description "$p (gerada por setup)" -o json)" || die "falha ao criar API key $p"
  state_set "${p}_KEY" "$(jq -r .api_key <<<"$j")"; state_set "${p}_SECRET" "$(jq -r .api_secret <<<"$j")"
  log "API key $p criada"
}
mkkey PRODUCER_KAFKA "$CLUSTER_ID" "$PRODUCER_SA"
mkkey CONSUMER_KAFKA "$CLUSTER_ID" "$CONSUMER_SA"
mkkey PRODUCER_SR    "$SR_ID"      "$PRODUCER_SA"
mkkey CONSUMER_SR    "$SR_ID"      "$CONSUMER_SA"

log "fundação pronta. Estado em .state (não versionado)."
