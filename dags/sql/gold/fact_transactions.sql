MERGE INTO gold.fact_transactions t
USING (
  SELECT
    transaction_id,
    user_id,
    timestamp AS transaction_ts,
    CAST(timestamp AS DATE) AS transaction_date,
    transaction_type,
    status,
    payment_method,
    tax,
    total,
    subtotal,
    bill_country AS billing_country,
    bill_city AS billing_city,
    bill_state AS billing_state
  FROM silver.stg_transactions
  WHERE timestamp IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY transaction_id
    ORDER BY processed_at DESC
  ) = 1
) s
ON t.transaction_id = s.transaction_id

WHEN MATCHED THEN UPDATE SET
  t.user_id = s.user_id,
  t.transaction_ts = s.transaction_ts,
  t.transaction_date = s.transaction_date,
  t.transaction_type = s.transaction_type,
  t.status = s.status,
  t.payment_method = s.payment_method,
  t.tax = s.tax,
  t.total = s.total,
  t.subtotal = s.subtotal,
  t.billing_country = s.billing_country,
  t.billing_city = s.billing_city,
  t.billing_state = s.billing_state,
  t.processed_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
  transaction_id,
  user_id,
  transaction_ts,
  transaction_date,
  transaction_type,
  status,
  payment_method,
  tax,
  total,
  subtotal,
  billing_country,
  billing_city,
  billing_state,
  processed_at
) VALUES (
  s.transaction_id,
  s.user_id,
  s.transaction_ts,
  s.transaction_date,
  s.transaction_type,
  s.status,
  s.payment_method,
  s.tax,
  s.total,
  s.subtotal,
  s.billing_country,
  s.billing_city,
  s.billing_state,
  CURRENT_TIMESTAMP()
);