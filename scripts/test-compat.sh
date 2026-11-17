#!/usr/bin/env bash
# Demonstração e validação de compatibilidade de schemas (Evolução BACKWARD/FULL).
# Uso: ./scripts/test-compat.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

[ -n "${ENV_ID:-}" ] || die "ENV_ID não definido no .state."

SUBJECT_TEST="${TOPIC_PREFIX}.test.evolution-value"
SUBJECT_FULL="${TOPIC_PREFIX}.test.evolution-full-value"

log "== 1. Registrando versão v1 base no subject $SUBJECT_TEST =="
confluent schema-registry schema create \
  --subject "$SUBJECT_TEST" \
  --schema "$ROOT/schemas/test-evolution/transaction-v1.avsc" \
  --type avro \
  --environment "$ENV_ID"

log "== 2. Validando evolução compatível BACKWARD (novo campo opcional com default) =="
echo "$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-compatible.avsc --type avro --subject $SUBJECT_TEST --version latest"
confluent schema-registry schema compatibility validate \
  "$ROOT/schemas/test-evolution/transaction-v2-compatible.avsc" \
  --type avro \
  --subject "$SUBJECT_TEST" \
  --version latest \
  --environment "$ENV_ID"

log "== 3. Validando quebra de compatibilidade BACKWARD (novo campo obrigatório sem default) =="
echo "$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-add-required.avsc --type avro --subject $SUBJECT_TEST --version latest"
confluent schema-registry schema compatibility validate \
  "$ROOT/schemas/test-evolution/transaction-v2-add-required.avsc" \
  --type avro \
  --subject "$SUBJECT_TEST" \
  --version latest \
  --environment "$ENV_ID" || true

log "== 4. Validando quebra de compatibilidade FULL (remoção de campo obrigatório 'amount') =="
confluent schema-registry schema create \
  --subject "$SUBJECT_FULL" \
  --schema "$ROOT/schemas/test-evolution/transaction-v1.avsc" \
  --type avro \
  --environment "$ENV_ID" >/dev/null 2>&1 || true
confluent schema-registry subject update "$SUBJECT_FULL" --compatibility full --environment "$ENV_ID" >/dev/null 2>&1 || true

echo "$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-incompatible.avsc --type avro --subject $SUBJECT_FULL --version latest"
confluent schema-registry schema compatibility validate \
  "$ROOT/schemas/test-evolution/transaction-v2-incompatible.avsc" \
  --type avro \
  --subject "$SUBJECT_FULL" \
  --version latest \
  --environment "$ENV_ID" || true
