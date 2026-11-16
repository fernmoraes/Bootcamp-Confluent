-- Verificação da Sub-tarefa 1 (evidência para o README).
\pset footer off

\echo '== wal_level'
SHOW wal_level;

\echo '== contagens (esperado: 200 | 240 | 320 | 60 | 2000)'
SELECT (SELECT count(*) FROM customers)    AS customers,
       (SELECT count(*) FROM accounts)     AS accounts,
       (SELECT count(*) FROM cards)        AS cards,
       (SELECT count(*) FROM merchants)    AS merchants,
       (SELECT count(*) FROM transactions) AS transactions;

\echo '== REPLICA IDENTITY (f = FULL)'
SELECT c.relname AS tabela, c.relreplident AS replica_identity
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r'
ORDER BY 1;

\echo '== publication'
SELECT pubname, string_agg(tablename, ', ' ORDER BY tablename) AS tabelas
FROM pg_publication_tables WHERE pubname = 'desafio_pub' GROUP BY pubname;

\echo '== atributos do cdc_user (sem superuser/createdb/createrole)'
SELECT rolname, rolcanlogin, rolreplication, rolsuper, rolcreatedb, rolcreaterole
FROM pg_roles WHERE rolname = 'cdc_user';

\echo '== rajadas: cartões com 3+ transações em janela de 60 s (esperado: 6 cartões)'
SELECT card_id, max(n) AS max_txns_em_60s, min(txn_ts) FILTER (WHERE n >= 3) AS terceira_txn_em
FROM (
  SELECT t.card_id, t.txn_ts,
         count(*) OVER (PARTITION BY t.card_id ORDER BY t.txn_ts
                        RANGE BETWEEN INTERVAL '60 seconds' PRECEDING AND CURRENT ROW) AS n
  FROM transactions t
) s
WHERE n >= 3
GROUP BY card_id
ORDER BY card_id;
