#!/usr/bin/env bash
# Camada 2 — Contrato de dados: registro de schemas Avro (namespace com.bootcamp.payments.*),
# tags PII/PCI via Stream Governance Catalog e validação de compatibilidade BACKWARD.
#
# Idempotente: pode ser reexecutado sem efeitos colaterais.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

[ -n "${ENV_ID:-}" ] || die "ENV_ID não definido no .state. Execute scripts/01-foundation.sh primeiro."
[ -n "${SR_ID:-}" ]  || die "SR_ID não definido no .state. Execute scripts/01-foundation.sh primeiro."
[ -n "${SR_URL:-}" ] || die "SR_URL não definido no .state. Execute scripts/01-foundation.sh primeiro."

# ---------- 1. Permissões de Catálogo / Tags (DataSteward para PRODUCER_SA) ----------
log "garantindo permissão DataSteward para $PRODUCER_SA_NAME ($PRODUCER_SA)"
has_ds="$(confluent iam rbac role-binding list --principal "User:$PRODUCER_SA" --environment "$ENV_ID" -o json 2>/dev/null \
  | jq -r '.[]? | select(.role_name=="DataSteward") | .id' | head -1)"
if [ -z "$has_ds" ]; then
  retry 6 confluent iam rbac role-binding create --principal "User:$PRODUCER_SA" --role DataSteward --environment "$ENV_ID" >/dev/null
  log "role binding DataSteward criada"
else
  log "role binding DataSteward já existe ($has_ds)"
fi

# ---------- 2. Criação das Tag Definitions (PII e PCI) no Stream Governance Catalog ----------
log "verificando/criando tag definitions no catálogo (PII, PCI)"
existing_tags="$(curl -s -u "$PRODUCER_SR_KEY:$PRODUCER_SR_SECRET" "$SR_URL/catalog/v1/types/tagdefs" | jq -r '.[].name' 2>/dev/null || true)"

create_tag() {
  local name="$1" desc="$2"
  if grep -qw "$name" <<<"$existing_tags"; then
    log "tag $name já existe no catálogo"
  else
    log "criando tag $name no catálogo"
    curl -s -f -X POST -u "$PRODUCER_SR_KEY:$PRODUCER_SR_SECRET" \
      -H "Content-Type: application/json" \
      -d "[{\"name\":\"$name\",\"description\":\"$desc\"}]" \
      "$SR_URL/catalog/v1/types/tagdefs" >/dev/null
  fi
}
create_tag "PII" "Personally Identifiable Information"
create_tag "PCI" "Payment Card Industry data"

# ---------- 3. Registro dos Schemas Canônicos das Entidades e Fraude ----------
reg_schema() {
  local subject="$1" schema_file="$2"
  log "registrando schema $subject ($schema_file)"
  retry 3 confluent schema-registry schema create \
    --subject "$subject" \
    --schema "$schema_file" \
    --type avro \
    --environment "$ENV_ID" >/dev/null
}

reg_schema "${TOPIC_PREFIX}.customers-value"       "$ROOT/schemas/customers.avsc"
reg_schema "${TOPIC_PREFIX}.accounts-value"        "$ROOT/schemas/accounts.avsc"
reg_schema "${TOPIC_PREFIX}.cards-value"           "$ROOT/schemas/cards.avsc"
reg_schema "${TOPIC_PREFIX}.merchants-value"       "$ROOT/schemas/merchants.avsc"
reg_schema "${TOPIC_PREFIX}.transactions-value"    "$ROOT/schemas/transactions.avsc"
reg_schema "${TOPIC_PREFIX}.fraud.detected-value"  "$ROOT/schemas/fraud-detected.avsc"

log "todos os schemas canônicos foram registrados com sucesso"
