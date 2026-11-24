-- Camada 4 — Flink SQL: Detecção de fraude em tempo real (RULE_VELOCITY)
-- Padrão: 3 ou mais transações no mesmo cartão em menos de 60 segundos.
--
-- Ordem das operações (decisão de projeto):
--   1) MATCH_RECOGNIZE direto sobre as transações (tempo de EVENTO = txn_time, do txn_ts do Postgres);
--   2) só depois, temporal join com a conta para obter customer_id (FOR SYSTEM_TIME AS OF o fim da janela).
-- O temporal join grava os resultados agrupados por conta, fora da ordem de tempo de evento
-- (evidencias/07-ordem-enriched.txt: 1986 de 2000 linhas fora de ordem). Por isso o padrão roda sobre as
-- transações e só depois busca a conta.
INSERT INTO `desafio.fraud.detected` (
  card_id,
  account_id,
  customer_id,
  first_transaction_id,
  last_transaction_id,
  txn_count,
  total_amount,
  window_start,
  window_end,
  rule_name,
  alert_ts
)
SELECT
  m.card_id,
  m.account_id,
  a.customer_id,
  m.first_transaction_id,
  m.last_transaction_id,
  CAST(m.txn_count AS INT) AS txn_count,
  CAST(m.total_amount AS DECIMAL(15, 2)) AS total_amount,
  m.window_start,
  m.window_end,
  m.rule_name,
  m.alert_ts
FROM (
  SELECT card_id, account_id, first_transaction_id, last_transaction_id, txn_count, total_amount,
         window_start, window_end, rule_name, alert_ts
  FROM `desafio.public.transactions`
  MATCH_RECOGNIZE (
    PARTITION BY card_id
    ORDER BY txn_time
    MEASURES
      FIRST(T.account_id) AS account_id,
      FIRST(T.transaction_id) AS first_transaction_id,
      LAST(T.transaction_id) AS last_transaction_id,
      COUNT(T.transaction_id) AS txn_count,
      SUM(T.amount) AS total_amount,
      FIRST(T.txn_time) AS window_start,
      LAST(T.txn_time) AS window_end,
      CAST('RULE_VELOCITY_60S' AS STRING) AS rule_name,
      CURRENT_ROW_TIMESTAMP() AS alert_ts
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (T{3,}?)
    WITHIN INTERVAL '60' SECOND
    DEFINE
      T AS TRUE
  )
) m
JOIN `desafio.accounts.versioned` FOR SYSTEM_TIME AS OF m.window_end AS a
  ON m.account_id = a.account_id;
