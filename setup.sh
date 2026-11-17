#!/usr/bin/env bash
# Sobe (ou RETOMA) o pipeline do zero, na ordem. Idempotente: pode rodar de novo.
# Executa, em ordem, db (se preciso) e todos os scripts/NN-*.sh existentes.
# Cada sub-tarefa nova do plano acrescenta o seu scripts/NN-*.sh e entra aqui sozinha.
#
# Uso:  ./setup.sh            sobe tudo que existe
#       ./setup.sh --reseed   recarrega os dados sintéticos no Postgres (db/apply.sh)
# Para parar/derrubar: ./teardown.sh
source "$(dirname "${BASH_SOURCE[0]}")/scripts/lib.sh"
load_env; ensure_login

RESEED=0; [ "${1:-}" = "--reseed" ] && RESEED=1

# ---- banco: só recarrega se faltarem dados (o Neon sobrevive ao teardown) ----
log "Postgres (Neon)"
need_db=1
if [ "$RESEED" = 0 ] && pg_admin_env 2>/dev/null; then
  n="$(psql -X -tA -c 'SELECT count(*) FROM transactions' 2>/dev/null || echo 0)"
  [ "${n:-0}" = "2000" ] && need_db=0 && echo "   dados já presentes (transactions=2000); use --reseed para recarregar"
fi
[ "$need_db" = 1 ] && "$ROOT/db/apply.sh"

# ---- camadas ----
for s in "$ROOT"/scripts/[0-9][0-9]-*.sh; do
  [ -x "$s" ] || continue
  log "$(basename "$s")"
  "$s"
done

echo
"$ROOT/scripts/status.sh"
