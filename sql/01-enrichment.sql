-- Camada 4 — Flink SQL: Enriquecimento de transações (Temporal Table Join transações ⋈ contas)
-- Propriedade de execução: sql.tables.scan.idle-timeout=5s
INSERT INTO `desafio.transactions.enriched` (
  transaction_id,
  card_id,
  account_id,
  customer_id,
  merchant_id,
  amount,
  currency,
  status,
  account_type,
  risk_tier,
  txn_ts
)
SELECT
  t.transaction_id,
  t.card_id,
  t.account_id,
  a.customer_id,
  t.merchant_id,
  t.amount,
  t.currency,
  t.status,
  a.account_type,
  a.risk_tier,
  TO_TIMESTAMP_LTZ(REPLACE(SUBSTRING(t.txn_ts, 1, 19), 'T', ' '), 'yyyy-MM-dd HH:mm:ss', 'UTC') AS txn_ts
FROM `desafio.public.transactions` t
JOIN `desafio.accounts.versioned` a
  ON t.account_id = a.account_id;
