#!/usr/bin/env bash
# Derruba o que o pipeline criou na Confluent Cloud e o slot de replicação na Neon.
# Serve para PARAR (parar de gastar) e para o fim do desafio. Para RETOMAR: ./setup.sh
#
# Uso:
#   ./teardown.sh --dry-run      só mostra o que seria apagado (nada é alterado)
#   ./teardown.sh                pede confirmação e apaga
#   ./teardown.sh --yes          apaga sem perguntar
#   ./teardown.sh --keep-neon    não mexe no slot de replicação do Postgres
#
# Só apaga recursos com os NOMES do .env (ENV_NAME, CLUSTER_NAME, *_SA_NAME).
# Nunca toca no environment "default" nem em nada fora desses nomes.
# Não depende do .state: descobre tudo pelo nome (funciona mesmo se o .state se perdeu).
# O banco Neon e os dados NÃO são apagados (para apagar o projeto, veja o README).
source "$(dirname "${BASH_SOURCE[0]}")/scripts/lib.sh"

DRY=0; YES=0; KEEP_NEON=0
for a in "$@"; do
  case "$a" in
    --dry-run) DRY=1 ;; --yes) YES=1 ;; --keep-neon) KEEP_NEON=1 ;;
    -h|--help) sed -n '2,15p' "$0"; exit 0 ;;
    *) die "opção desconhecida: $a" ;;
  esac
done

load_env; ensure_login
set +e   # teardown é "melhor esforço": uma falha não impede os passos seguintes

run() {   # run CMD...  -> executa (ou mostra, em --dry-run) e segue em caso de erro
  if [ "$DRY" = 1 ]; then echo "   [dry-run] $*"; return 0; fi
  if "$@" >/dev/null 2>&1; then echo "   ok: $*"; else echo "   AVISO (falhou, seguindo): $*"; fi
}

# ---------- descoberta (somente leitura) ----------
ENV_ID="$(confluent environment list -o json 2>/dev/null | jq -r --arg n "$ENV_NAME" '.[]? | select(.name==$n) | .id' | head -1)"
CLUSTER_ID=""; SR_ID=""; CONNECTORS=""; STATEMENTS=""; POOLS=""
if [ -n "$ENV_ID" ]; then
  CLUSTER_ID="$(confluent kafka cluster list --environment "$ENV_ID" -o json 2>/dev/null | jq -r --arg n "$CLUSTER_NAME" '.[]? | select(.name==$n) | .id' | head -1)"
  SR_ID="$(confluent schema-registry cluster describe --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.cluster // empty')"
  [ -n "$CLUSTER_ID" ] && CONNECTORS="$(confluent connect cluster list --cluster "$CLUSTER_ID" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.[]? | .id')"
  STATEMENTS="$(confluent flink statement list --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.[]? | .name')"
  POOLS="$(confluent flink compute-pool list --region "$CLUSTER_REGION" --environment "$ENV_ID" -o json 2>/dev/null | jq -r '.[]? | .id')"
fi
SA_IDS=""; for n in "$PRODUCER_SA_NAME" "$CONSUMER_SA_NAME"; do
  id="$(confluent iam service-account list -o json 2>/dev/null | jq -r --arg n "$n" '.[]? | select(.name==$n) | .id' | head -1)"
  [ -n "$id" ] && SA_IDS="$SA_IDS $id"
done

echo "== O que será apagado$([ "$DRY" = 1 ] && echo " (dry-run)")"
echo "   environment : ${ENV_ID:-<não existe>} ($ENV_NAME)"
echo "   cluster     : ${CLUSTER_ID:-<não existe>} ($CLUSTER_NAME)"
echo "   Sch.Registry: ${SR_ID:-<n/a>} (some junto com o environment)"
echo "   conectores  : ${CONNECTORS:-<nenhum>}"
echo "   flink stmts : ${STATEMENTS:-<nenhum>}"
echo "   flink pools : ${POOLS:-<nenhum>}"
echo "   service acc.: ${SA_IDS:-<nenhuma>}"
echo "   slot Neon   : $([ "$KEEP_NEON" = 1 ] && echo 'mantido (--keep-neon)' || echo "slots 'desafio%' inativos")"
echo "   .state local: $([ -f "$STATE_FILE" ] && echo 'será removido (as API keys morrem junto)' || echo '<não existe>')"
echo

if [ "$DRY" = 0 ] && [ "$YES" = 0 ]; then
  printf 'Digite "apagar" para confirmar: '; read -r ans
  [ "$ans" = "apagar" ] || { echo "Cancelado."; exit 1; }
fi

# ---------- 1. conectores (antes de tudo: liberam o slot no Postgres) ----------
log "1/8 conectores"
for c in $CONNECTORS; do run confluent connect cluster delete "$c" --cluster "$CLUSTER_ID" --environment "$ENV_ID" --force; done

# ---------- 2. Flink: statements e compute pool ----------
log "2/8 Flink statements e compute pool"
for s in $STATEMENTS; do run confluent flink statement delete "$s" --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" --force; done
for p in $POOLS; do run confluent flink compute-pool delete "$p" --environment "$ENV_ID" --force; done

if [ -n "$ENV_ID" ] && [ "$DRY" = 0 ]; then
  echo "   -- verificação (esperado: None found.)"
  echo "   connect cluster list:";    confluent connect cluster list --cluster "$CLUSTER_ID" --environment "$ENV_ID" 2>&1 | sed 's/^/     /'
  echo "   flink statement list:";    confluent flink statement list --cloud "$CLUSTER_CLOUD" --region "$CLUSTER_REGION" --environment "$ENV_ID" 2>&1 | grep -v "No Flink endpoint" | sed 's/^/     /'
fi

# ---------- 3. Schema Registry: role bindings ----------
log "3/8 role bindings do Schema Registry"
if [ -n "$SR_ID" ]; then
  # bindings criados em scripts/01-foundation.sh: producer = Write+Read, consumer = Read
  for pair in "$PRODUCER_SA_NAME:DeveloperWrite" "$PRODUCER_SA_NAME:DeveloperRead" "$CONSUMER_SA_NAME:DeveloperRead"; do
    sa="$(confluent iam service-account list -o json 2>/dev/null | jq -r --arg n "${pair%%:*}" '.[]? | select(.name==$n) | .id' | head -1)"
    [ -n "$sa" ] && run confluent iam rbac role-binding delete --principal "User:$sa" --role "${pair##*:}" --environment "$ENV_ID" \
        --schema-registry-cluster "$SR_ID" --resource "Subject:${TOPIC_PREFIX}" --prefix --force
  done
fi

# ---------- 4. API keys das service accounts ----------
log "4/8 API keys"
for sa in $SA_IDS; do
  for k in $(confluent api-key list --service-account "$sa" -o json 2>/dev/null | jq -r '.[]? | .key'); do
    run confluent api-key delete "$k" --force
  done
done

# ---------- 5. cluster Kafka (ACLs e tópicos vão junto) ----------
log "5/8 cluster Kafka"
if [ -n "$CLUSTER_ID" ]; then
  run confluent kafka cluster delete "$CLUSTER_ID" --environment "$ENV_ID" --force
  if [ "$DRY" = 0 ]; then
    for _ in $(seq 1 60); do
      confluent kafka cluster list --environment "$ENV_ID" -o json 2>/dev/null \
        | jq -e --arg id "$CLUSTER_ID" 'map(select(.id==$id)) | length == 0' >/dev/null && break
      sleep 10
    done
  fi
fi

# ---------- 6. service accounts ----------
log "6/8 service accounts"
for sa in $SA_IDS; do run confluent iam service-account delete "$sa" --force; done

# ---------- 7. environment (leva o Schema Registry junto) ----------
log "7/8 environment"
[ -n "$ENV_ID" ] && run confluent environment delete "$ENV_ID" --force

# ---------- 8. Neon: slot de replicação órfão (retém WAL e mantém o compute ativo) ----------
log "8/8 slot de replicação no Postgres"
if [ "$KEEP_NEON" = 1 ]; then echo "   pulado (--keep-neon)"
elif ! pg_admin_env; then echo "   AVISO: psql ou credenciais PG ausentes; verifique os slots manualmente (veja README)."
elif [ "$DRY" = 1 ]; then
  psql -X -tA -c "SELECT '   [dry-run] drop slot ' || slot_name || ' (active=' || active || ')' FROM pg_replication_slots WHERE slot_name LIKE 'desafio%'"
else
  for _ in 1 2 3 4 5 6; do
    psql -X -tA -c "SELECT 'drop slot ' || slot_name FROM pg_replication_slots WHERE slot_name LIKE 'desafio%' AND NOT active" 2>&1 | sed 's/^/   /'
    psql -X -q -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'desafio%' AND NOT active" >/dev/null 2>&1
    left="$(psql -X -tA -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'desafio%'" 2>/dev/null)"
    [ "${left:-1}" = "0" ] && break; sleep 10
  done
  echo "   slots 'desafio%' restantes: ${left:-?}"
fi

# ---------- limpeza local e verificação final ----------
if [ "$DRY" = 0 ]; then
  rm -f "$STATE_FILE" "$STATE_FILE.tmp"
  echo; echo "== Verificação final (nada com o prefixo '${RESOURCE_PREFIX}' deve restar)"
  echo "-- confluent environment list"; confluent environment list 2>&1 | sed 's/^/   /'
  echo "-- confluent iam service-account list"; confluent iam service-account list 2>&1 | sed 's/^/   /'
  echo; echo "Pronto. Para retomar: ./setup.sh"
else
  echo; echo "Dry-run concluído: nada foi alterado."
fi
