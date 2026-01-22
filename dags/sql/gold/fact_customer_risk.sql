MERGE INTO GOLD.FACT_CUSTOMER_RISK t
USING (
  WITH daily_counts AS (
    SELECT
      user_id,
      CAST(timestamp AS DATE) AS event_date,
      COUNT_IF(transaction_type = 'refund')     AS num_refunds,
      COUNT_IF(transaction_type = 'chargeback') AS num_chargebacks
    FROM SILVER.STG_TRANSACTIONS
    WHERE status = 'completed'
      AND transaction_type IN ('refund', 'chargeback')
    GROUP BY 1, 2
  ),
  daily_location AS (
    SELECT
      user_id,
      CAST(timestamp AS DATE) AS event_date,
      bill_city,
      bill_country,
      bill_state
    FROM SILVER.STG_TRANSACTIONS
    WHERE status = 'completed'
      AND transaction_type IN ('refund', 'chargeback')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY user_id, CAST(timestamp AS DATE)
      ORDER BY timestamp DESC
    ) = 1
  )
  SELECT
    c.user_id,
    c.event_date,

    d.full_name AS customer_name,
    d.email     AS email,

    l.bill_city,
    l.bill_country,
    l.bill_state,
    c.num_refunds,
    c.num_chargebacks
  FROM daily_counts c
  LEFT JOIN daily_location l
    ON l.user_id = c.user_id
   AND l.event_date = c.event_date
  LEFT JOIN GOLD.DIM_CUSTOMER d
    ON d.user_id = c.user_id
) s
ON t.user_id = s.user_id
AND t.event_date = s.event_date

WHEN MATCHED THEN UPDATE SET
  t.customer_name  = s.customer_name,
  t.email          = s.email,
  t.bill_city      = s.bill_city,
  t.bill_country   = s.bill_country,
  t.bill_state     = s.bill_state,
  t.num_refunds    = s.num_refunds,
  t.num_chargebacks= s.num_chargebacks,
  t.processed_at   = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT
  (user_id, event_date, customer_name, email, bill_country, bill_city, bill_state, num_refunds, num_chargebacks)
VALUES
  (s.user_id, s.event_date, s.customer_name, s.email, s.bill_country, s.bill_city, s.bill_state, s.num_refunds, s.num_chargebacks);
