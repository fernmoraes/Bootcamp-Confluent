-- Camada 4 — Flink SQL: Enriquecimento de transações (Temporal Table Join transações ⋈ contas)
-- Propriedade de execução: sql.tables.scan.idle-timeout=5s
-- Pré-requisito (scripts/05-flink.sh): tabelas desafio.public.transactions (coluna txn_time + watermark,
-- changelog.mode=append) e desafio.accounts.versioned (PK account_id + watermark em updated_at).
-- txn_time é o relógio de EVENTO (txn_ts do Postgres), não o de ingestão.
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
  t.txn_time AS txn_ts
FROM `desafio.public.transactions` t
JOIN `desafio.accounts.versioned` FOR SYSTEM_TIME AS OF t.txn_time AS a
  ON t.account_id = a.account_id;
