#!/usr/bin/env bash
# Mostra o que existe agora (e portanto está gerando custo). Somente leitura.
# Uso: ./scripts/status.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
set +e

if ! confluent environment list >/dev/null 2>&1; then
  echo "Confluent: sem sessão ativa (rode ./setup.sh ou 'confluent login')."; LOGGED=0
else LOGGED=1; fi

echo "== Confluent Cloud"
if [ "$LOGGED" = 1 ]; then
  ENV_ID="$(confluent environment list -o json | jq -r --arg n "$ENV_NAME" '.[]? | select(.name==$n) | .id' | head -1)"
  if [ -z "$ENV_ID" ]; then
    echo "   environment $ENV_NAME : NÃO EXISTE  -> nada gerando custo na Confluent"
  else
    echo "   environment $ENV_NAME : $ENV_ID"
    CL="$(confluent kafka cluster list --environment "$ENV_ID" -o json | jq -r --arg n "$CLUSTER_NAME" '.[]? | select(.name==$n) | "\(.id) (\(.type), \(.status))"')"
    echo "   cluster $CLUSTER_NAME : ${CL:-NÃO EXISTE}"
    CID="$(confluent kafka cluster list --environment "$ENV_ID" -o json | jq -r --arg n "$CLUSTER_NAME" '.[]? | select(.name==$n) | .id')"
    if [ -n "$CID" ]; then
      echo "   conectores        : $(confluent connect cluster list --cluster "$CID" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '[.[]? | "\(.name)=\(.status)"] | join(", ") | if .=="" then "nenhum" else . end')"
      echo "   tópicos           : $(confluent kafka topic list --cluster "$CID" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '[.[]? | .name] | length') (use 'confluent kafka topic list' para ver)"
    fi
    echo "   flink statements  : $(confluent flink statement list --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '[.[]? | "\(.name)=\(.status)"] | join(", ") | if .=="" then "nenhum" else . end')"
    echo "   flink compute pool: $(confluent flink compute-pool list --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '[.[]? | .id] | join(", ") | if .=="" then "nenhum" else . end')"
  fi
  echo "   service accounts  : $(confluent iam service-account list -o json 2>/dev/null | jq -r --arg p "$RESOURCE_PREFIX" '[.[]? | select(.name|startswith($p)) | .name] | join(", ") | if .=="" then "nenhuma" else . end')"
fi

echo "== Estado local"
[ -f "$STATE_FILE" ] && echo "   .state presente ($(grep -c . "$STATE_FILE") chaves)" || echo "   .state ausente (setup ainda não rodou, ou o teardown já apagou)"

echo "== Neon (Postgres de origem)"
if pg_admin_env 2>/dev/null; then
  psql -X -tA -c "SELECT '   transactions: ' || count(*) FROM transactions" 2>&1 | head -1
  psql -X -tA -c "SELECT '   slot ' || slot_name || ' ativo=' || active FROM pg_replication_slots WHERE slot_name LIKE 'desafio%'" 2>&1 | grep . || echo "   nenhum slot de replicação 'desafio%'"
else echo "   psql/credenciais indisponíveis"; fi
echo
echo "Custo real: confluent billing cost list --start-date <AAAA-MM-DD> --end-date <AAAA-MM-DD>  (há defasagem de dados)"
