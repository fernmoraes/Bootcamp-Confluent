#!/usr/bin/env bash
# Captura os 3 envelopes de eventos CDC (INSERT op=c, UPDATE op=u, DELETE op=d)
# e salva em evidencias/01-envelopes-cdc.json.
# NÃO imprime nem grava segredos.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

[ -n "${ENV_ID:-}" ]     || die "ENV_ID não definido no .state."
[ -n "${CLUSTER_ID:-}" ] || die "CLUSTER_ID não definido no .state."
[ -n "${SR_URL:-}" ]     || die "SR_URL não definido no .state."

OUT="$ROOT/evidencias/01-envelopes-cdc.json"
TMP_RAW="$(mktemp -t cdc-events.XXXXXX.log)"

log "executando ciclo INSERT -> UPDATE -> DELETE controlado no Postgres"
pg_admin_env || die "falha ao configurar conexão admin do Postgres"

TEST_ID=99999
psql -X -q -c "DELETE FROM customers WHERE customer_id = $TEST_ID;" 2>/dev/null || true
psql -X -q -c "INSERT INTO customers (customer_id, full_name, email, phone, birth_date, city, created_at) VALUES ($TEST_ID, 'CDC Probe User', 'cdc.probe@bootcamp.com', '+5511999998888', '1992-07-20', 'Sao Paulo', now());"
sleep 4
psql -X -q -c "UPDATE customers SET city = 'Belo Horizonte', email = 'cdc.probe.updated@bootcamp.com' WHERE customer_id = $TEST_ID;"
sleep 4
psql -X -q -c "DELETE FROM customers WHERE customer_id = $TEST_ID;"
sleep 4

log "consumindo envelopes CDC do tópico desafio.public.customers"
# Consumir as mensagens recentes
confluent kafka topic consume "${TOPIC_PREFIX}.public.customers" \
  --cluster "$CLUSTER_ID" \
  --environment "$ENV_ID" \
  --api-key "$CONSUMER_KAFKA_KEY" \
  --api-secret "$CONSUMER_KAFKA_SECRET" \
  --value-format avro \
  --schema-registry-endpoint "$SR_URL" \
  --schema-registry-api-key "$CONSUMER_SR_KEY" \
  --schema-registry-api-secret "$CONSUMER_SR_SECRET" \
  --group "${CONSUMER_GROUP_PREFIX}cdc-evidence-reader" \
  --partition 0 \
  --offset 200 > "$TMP_RAW" 2>&1 &
PID=$!
sleep 12
kill $PID 2>/dev/null || true

log "formatando envelopes capturados em $OUT"
python3 -c "
import json, sys

raw_file = sys.argv[1]
out_file = sys.argv[2]
test_id = int(sys.argv[3])

events = {'insert_op_c': None, 'update_op_u': None, 'delete_op_d': None, 'tombstone': None}

with open(raw_file, 'r') as f:
    for line in f:
        line = line.strip()
        if not line.startswith('{'):
            continue
        try:
            data = json.loads(line)
        except Exception:
            continue
        
        op = data.get('op')
        after = data.get('after')
        before = data.get('before')
        
        # Identificar registro de teste
        after_val = list(after.values())[0] if isinstance(after, dict) and after else {}
        before_val = list(before.values())[0] if isinstance(before, dict) and before else {}
        
        c_id_after = after_val.get('customer_id') if isinstance(after_val, dict) else None
        c_id_before = before_val.get('customer_id') if isinstance(before_val, dict) else None
        
        if c_id_after == test_id or c_id_before == test_id or (c_id_after in (9999, 99999) or c_id_before in (9999, 99999)):
            if op == 'c' and not events['insert_op_c']:
                events['insert_op_c'] = data
            elif op == 'u' and not events['update_op_u']:
                events['update_op_u'] = data
            elif op == 'd' and not events['delete_op_d']:
                events['delete_op_d'] = data

with open(out_file, 'w') as out:
    json.dump(events, out, indent=2, ensure_ascii=False)

print(f'Envelopes extraidos com sucesso: insert={bool(events[\"insert_op_c\"])}, update={bool(events[\"update_op_u\"])}, delete={bool(events[\"delete_op_d\"])}')
" "$TMP_RAW" "$OUT" "$TEST_ID"

rm -f "$TMP_RAW"

# Verificação estrita de segredos
if [ -f "$STATE_FILE" ]; then
  while IFS='=' read -r k v; do
    [ -z "$v" ] && continue
    if [ "${#v}" -ge 8 ]; then
      if grep -Fq "$v" "$OUT"; then
        rm -f "$OUT"
        die "VAZAMENTO DE SEGREDOS DETECTADO: $k vazou no JSON de evidência!"
      fi
    fi
  done < "$STATE_FILE"
fi

log "evidência salva em $OUT"
cat "$OUT"
