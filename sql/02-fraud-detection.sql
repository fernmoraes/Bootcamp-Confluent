-- Camada 4 — Flink SQL: Detecção de fraude em tempo real (RULE_VELOCITY)
-- Padrão: 3 ou mais transações no mesmo cartão em menos de 60 segundos
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
  card_id,
  account_id,
  customer_id,
  first_transaction_id,
  last_transaction_id,
  CAST(txn_count AS INT) AS txn_count,
  CAST(total_amount AS DECIMAL(15, 2)) AS total_amount,
  window_start,
  window_end,
  rule_name,
  alert_ts
FROM `desafio.transactions.enriched`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY `$rowtime`
  MEASURES
    FIRST(T.account_id) AS account_id,
    FIRST(T.customer_id) AS customer_id,
    FIRST(T.transaction_id) AS first_transaction_id,
    LAST(T.transaction_id) AS last_transaction_id,
    COUNT(T.transaction_id) AS txn_count,
    SUM(T.amount) AS total_amount,
    FIRST(T.`$rowtime`) AS window_start,
    LAST(T.`$rowtime`) AS window_end,
    CAST('RULE_VELOCITY_60S' AS STRING) AS rule_name,
    CURRENT_ROW_TIMESTAMP() AS alert_ts
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?)
  WITHIN INTERVAL '60' SECOND
  DEFINE
    T AS TRUE
);
