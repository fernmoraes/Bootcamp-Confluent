#!/usr/bin/env bash
# Captura as evidências da Camada 2 (Contrato de dados) em evidencias/03-contrato-de-dados.txt.
# NÃO imprime nem grava segredos.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

OUT="$ROOT/evidencias/03-contrato-de-dados.txt"
TMP="$OUT.tmp"

{
  echo "== Camada 2 — Contrato de dados (Evidências reais)"
  echo "Data: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo

  echo "=== 1. Nível de Compatibilidade Global do Schema Registry"
  echo "$ confluent schema-registry configuration describe"
  confluent schema-registry configuration describe --environment "$ENV_ID"
  echo

  echo "=== 2. Tag Definitions Registradas no Stream Governance Catalog"
  echo "Tags de governança (PII e PCI) criadas no catálogo do Schema Registry:"
  curl -s -u "$PRODUCER_SR_KEY:$PRODUCER_SR_SECRET" "$SR_URL/catalog/v1/types/tagdefs" | jq -r '.[] | "  - Tag: \(.name) | Descrição: \(.description)"'
  echo

  echo "=== 3. Schemas de Domínio Registrados (Subject Prefix: ${TOPIC_PREFIX})"
  echo "$ confluent schema-registry schema list --subject-prefix ${TOPIC_PREFIX}"
  confluent schema-registry schema list --subject-prefix "$TOPIC_PREFIX" --environment "$ENV_ID"
  echo

  echo "=== 4. Teste de Compatibilidade — Cenário 1: Aceitando campo novo com default (BACKWARD)"
  echo "$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-compatible.avsc --type avro --subject ${TOPIC_PREFIX}.test.evolution-value --version latest"
  confluent schema-registry schema compatibility validate \
    "$ROOT/schemas/test-evolution/transaction-v2-compatible.avsc" \
    --type avro \
    --subject "${TOPIC_PREFIX}.test.evolution-value" \
    --version latest \
    --environment "$ENV_ID"
  echo

  echo "=== 5. Teste de Compatibilidade — Cenário 2: REJEITANDO quebra (campo obrigatório sem default em BACKWARD)"
  echo "$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-add-required.avsc --type avro --subject ${TOPIC_PREFIX}.test.evolution-value --version latest"
  confluent schema-registry schema compatibility validate \
    "$ROOT/schemas/test-evolution/transaction-v2-add-required.avsc" \
    --type avro \
    --subject "${TOPIC_PREFIX}.test.evolution-value" \
    --version latest \
    --environment "$ENV_ID" || true
  echo

  echo "=== 6. Teste de Compatibilidade — Cenário 3: REJEITANDO remoção de campo obrigatório (FULL)"
  echo "$ confluent schema-registry schema compatibility validate schemas/test-evolution/transaction-v2-incompatible.avsc --type avro --subject ${TOPIC_PREFIX}.test.evolution-full-value --version latest"
  confluent schema-registry schema compatibility validate \
    "$ROOT/schemas/test-evolution/transaction-v2-incompatible.avsc" \
    --type avro \
    --subject "${TOPIC_PREFIX}.test.evolution-full-value" \
    --version latest \
    --environment "$ENV_ID" || true
} > "$TMP"

# Verificação estrita de segredos antes de salvar
if [ -f "$STATE_FILE" ]; then
  while IFS='=' read -r k v; do
    [ -z "$v" ] && continue
    # Se o valor tiver pelo menos 8 caracteres e não for booleano/número simples
    if [ "${#v}" -ge 8 ]; then
      if grep -Fq "$v" "$TMP"; then
        rm -f "$TMP"
        die "VAZAMENTO DE SEGREDOS DETECTADO: $k vazou na saída!"
      fi
    fi
  done < "$STATE_FILE"
fi

mv "$TMP" "$OUT"
log "evidência salva em $OUT"
cat "$OUT"
