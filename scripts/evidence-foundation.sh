#!/usr/bin/env bash
# Evidência da Camada 1: identidades, ACLs, role bindings e prova de menor privilégio.
# Saída em stdout (use: ./scripts/evidence-foundation.sh | tee evidencias/02-fundacao.txt)
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load
: "${ENV_ID:?rode scripts/01-foundation.sh antes}"

echo "# Camada 1 — Fundação ($(date -u +%Y-%m-%dT%H:%MZ))"
echo; echo "== environment e cluster"
confluent environment list | grep -E "ID|$ENV_NAME"
confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" | grep -E "ID|Name|Type|Cloud|Region|Availability|Status"
echo; echo "== Schema Registry"
confluent schema-registry cluster describe --environment "$ENV_ID" | grep -E "Cluster|Endpoint URL|Global Compatibility|Mode"

echo; echo "== confluent iam service-account list"
confluent iam service-account list | grep -E "ID|${RESOURCE_PREFIX}"

for pair in "$PRODUCER_SA_NAME:$PRODUCER_SA" "$CONSUMER_SA_NAME:$CONSUMER_SA"; do
  echo; echo "== ACLs Kafka de ${pair%%:*} (${pair##*:})"
  confluent kafka acl list --service-account "${pair##*:}" --environment "$ENV_ID" --cluster "$CLUSTER_ID"
  echo "-- role bindings (Schema Registry)"
  confluent iam rbac role-binding list --principal "User:${pair##*:}" --environment "$ENV_ID" --inclusive 2>&1 | grep -v "^$" || true
done

echo; echo "== prova de menor privilégio (comandos reais com as API keys de cada SA)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
probe() {   # probe DESCRIÇÃO KEY SECRET TÓPICO MODO(produce|consume)
  local desc=$1 key=$2 secret=$3 topic=$4 mode=$5
  printf '%-62s ' "$desc"
  if [ "$mode" = produce ]; then
    out="$(echo "k:v" | confluent kafka topic produce "$topic" --parse-key --delimiter ":" \
           --api-key "$key" --api-secret "$secret" --cluster "$CLUSTER_ID" --environment "$ENV_ID" 2>&1)" && rc=0 || rc=$?
  else
    confluent kafka topic consume "$topic" --from-beginning --group "${CONSUMER_GROUP_PREFIX}probe" \
      --api-key "$key" --api-secret "$secret" --cluster "$CLUSTER_ID" --environment "$ENV_ID" >"$tmp/c.out" 2>&1 &
    local pid=$!; sleep 15; kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null || true
    out="$(cat "$tmp/c.out")"; rc=0
  fi
  if echo "$out" | grep -qi -E "does not have the ACLs|authorization|not authorized|denied"; then echo "NEGADO"
  elif [ "$mode" = consume ] && ! echo "$out" | grep -qx "v"; then echo "FALHOU: $(echo "$out" | head -1)"
  else echo "PERMITIDO"; fi
}
T="${TOPIC_PREFIX}.probe"
confluent kafka topic create "$T" --partitions 1 --environment "$ENV_ID" --cluster "$CLUSTER_ID" >/dev/null 2>&1 || true
probe "producer escreve em ${T}"                       "$PRODUCER_KAFKA_KEY" "$PRODUCER_KAFKA_SECRET" "$T" produce
probe "consumer tenta escrever em ${T}"                "$CONSUMER_KAFKA_KEY" "$CONSUMER_KAFKA_SECRET" "$T" produce
probe "producer tenta escrever fora do prefixo (outro.topico)" "$PRODUCER_KAFKA_KEY" "$PRODUCER_KAFKA_SECRET" "outro.topico" produce
probe "consumer lê ${T}"                               "$CONSUMER_KAFKA_KEY" "$CONSUMER_KAFKA_SECRET" "$T" consume
confluent kafka topic delete "$T" --force --environment "$ENV_ID" --cluster "$CLUSTER_ID" >/dev/null 2>&1 || true
