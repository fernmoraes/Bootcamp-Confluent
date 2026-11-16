#!/usr/bin/env bash
# Aplica schema + cdc_user + seed no Postgres (Neon) e verifica o resultado.
# Uso: ./db/apply.sh            (lê credenciais de ../.env)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

[ -f .env ] || { echo "ERRO: .env não encontrado. Copie .env.example e preencha." >&2; exit 1; }
set -a; . ./.env; set +a

# psql do Homebrew (libpq é keg-only)
if ! command -v psql >/dev/null 2>&1; then
  for p in /opt/homebrew/opt/libpq/bin /usr/local/opt/libpq/bin; do
    [ -x "$p/psql" ] && PATH="$p:$PATH" && break
  done
fi
command -v psql >/dev/null 2>&1 || { echo "ERRO: psql não encontrado (veja README, Pré-requisitos)." >&2; exit 1; }

: "${PG_HOST:?}" "${PG_DATABASE:?}" "${PG_ADMIN_USER:?}" "${PG_ADMIN_PASSWORD:?}" "${CDC_USER:?}" "${CDC_PASSWORD:?}"
case "$PG_HOST" in *-pooler*) echo "ERRO: use a conexão DIRETA (sem -pooler); CDC não funciona via pooler." >&2; exit 1;; esac

export PGHOST="$PG_HOST" PGPORT="${PG_PORT:-5432}" PGDATABASE="$PG_DATABASE" \
       PGUSER="$PG_ADMIN_USER" PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE="${PG_SSLMODE:-require}"
export CDC_USER CDC_PASSWORD
PSQL=(psql -v ON_ERROR_STOP=1 -X -q)

echo ">> wal_level (precisa ser 'logical')"
WAL="$("${PSQL[@]}" -tA -c 'SHOW wal_level')"
echo "   wal_level=$WAL"
[ "$WAL" = "logical" ] || { echo "ERRO: habilite logical replication (neon projects update <id> --enable-logical-replication --yes)." >&2; exit 1; }

echo ">> 01-schema.sql"
"${PSQL[@]}" -f db/01-schema.sql
echo ">> 02-cdc-user.sql"
"${PSQL[@]}" -f db/02-cdc-user.sql
echo ">> seed (SEED=${SEED:-42})"
python3 db/seed.py --seed "${SEED:-42}" 2>"${TMPDIR:-/tmp}/seed.log" | "${PSQL[@]}" -f -
grep -E '^(seed=|linhas:)' "${TMPDIR:-/tmp}/seed.log"
echo ">> verificação"
"${PSQL[@]}" -f db/03-verify.sql
