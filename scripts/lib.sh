#!/usr/bin/env bash
# Helpers compartilhados pelos scripts. Uso: source scripts/lib.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_FILE="$ROOT/.state"

log() { printf '>> %s\n' "$*"; }
die() { printf 'ERRO: %s\n' "$*" >&2; exit 1; }

load_env() {
  [ -f "$ROOT/.env" ] || die ".env não encontrado. Copie .env.example e preencha."
  set -a; . "$ROOT/.env"; set +a
  command -v confluent >/dev/null || die "Confluent CLI não encontrada (veja README, Pré-requisitos)."
  command -v jq >/dev/null || die "jq não encontrado."
}

# Estado local (IDs e API keys). Gitignored, chmod 600.
state_load() { [ -f "$STATE_FILE" ] && . "$STATE_FILE" || true; }
state_set() {   # state_set CHAVE valor
  touch "$STATE_FILE"; chmod 600 "$STATE_FILE"
  grep -v "^$1=" "$STATE_FILE" > "$STATE_FILE.tmp" || true
  printf '%s=%q\n' "$1" "$2" >> "$STATE_FILE.tmp"
  mv "$STATE_FILE.tmp" "$STATE_FILE"; chmod 600 "$STATE_FILE"
}

ensure_login() {
  if ! confluent environment list >/dev/null 2>&1; then
    log "login na Confluent Cloud"
    confluent login >/dev/null
  fi
}

# Prepara o psql (libpq do Homebrew é keg-only) e as variáveis PG* do usuário admin.
pg_admin_env() {
  if ! command -v psql >/dev/null 2>&1; then
    for p in /opt/homebrew/opt/libpq/bin /usr/local/opt/libpq/bin; do
      [ -x "$p/psql" ] && PATH="$p:$PATH" && break
    done
  fi
  command -v psql >/dev/null 2>&1 || return 1
  : "${PG_HOST:?}" "${PG_DATABASE:?}" "${PG_ADMIN_USER:?}" "${PG_ADMIN_PASSWORD:?}"
  export PGHOST="$PG_HOST" PGPORT="${PG_PORT:-5432}" PGDATABASE="$PG_DATABASE" \
         PGUSER="$PG_ADMIN_USER" PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE="${PG_SSLMODE:-require}"
}

# retry N cmd...  (consistência eventual: SA recém-criada pode demorar a aparecer)
retry() {
  local n=$1; shift; local i=1
  until "$@"; do
    [ "$i" -ge "$n" ] && return 1
    i=$((i + 1)); sleep 5
  done
}
