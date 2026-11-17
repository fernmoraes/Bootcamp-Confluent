#!/usr/bin/env bash
# Consulta faturamento real na Confluent Cloud via CLI
# Não imprime nem salva segredos.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env; ensure_login; state_load

START_DATE="${1:-2026-11-01}"
END_DATE="${2:-$(date -u '+%Y-%m-%d')}"

log "Consultando custos na Confluent Cloud de $START_DATE até $END_DATE..."
confluent billing cost list --start-date "$START_DATE" --end-date "$END_DATE"
