MERGE INTO GOLD.FACT_CUSTOMER_DAILY_SPEND t
USING (
  SELECT
    f.transaction_date AS sales_date,
    f.user_id,

    d.full_name       AS customer_name,
    d.email           AS email,

    COUNT(*) AS orders,
    SUM(f.total) AS spend
  FROM GOLD.FACT_TRANSACTIONS f
  LEFT JOIN GOLD.DIM_CUSTOMER d
    ON f.user_id = d.user_id
  WHERE f.status = 'completed'
  GROUP BY
    sales_date,
    f.user_id,
    d.full_name,
    d.email
) s
ON t.sales_date = s.sales_date
AND t.user_id = s.user_id

WHEN MATCHED THEN UPDATE SET
  t.customer_name = s.customer_name,
  t.email         = s.email,
  t.orders        = s.orders,
  t.spend         = s.spend,
  t.processed_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
  sales_date, user_id, customer_name, email, orders, spend
)
VALUES (
  s.sales_date, s.user_id, s.customer_name, s.email, s.orders, s.spend
);
