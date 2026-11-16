-- Usuário do conector CDC, com menor privilégio.
-- Variáveis (vindas do ambiente via \getenv): CDC_USER, CDC_PASSWORD.
--
-- Roles criados por SQL na Neon NÃO herdam neon_superuser; só recebem
-- o que for concedido aqui. Privilégios concedidos:
--   LOGIN + REPLICATION  -> abrir o slot de replicação lógica
--   USAGE no schema      -> enxergar as tabelas
--   SELECT nas tabelas   -> snapshot inicial do Debezium
-- Não recebe INSERT/UPDATE/DELETE, nem DDL, nem CREATEROLE/CREATEDB.

\getenv cdc_user CDC_USER
\getenv cdc_password CDC_PASSWORD

SELECT format('CREATE ROLE %I', :'cdc_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'cdc_user') \gexec

SELECT format('ALTER ROLE %I LOGIN REPLICATION PASSWORD %L', :'cdc_user', :'cdc_password') \gexec

SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'cdc_user') \gexec
SELECT format('GRANT SELECT ON customers, accounts, cards, merchants, transactions TO %I', :'cdc_user') \gexec
