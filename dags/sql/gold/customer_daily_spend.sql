MERGE INTO GOLD.FACT_CUSTOMER_DAILY_SPEND t
USING (
  SELECT
    CAST(timestamp AS DATE) AS sales_date,
    user_id,
    COUNT(*) AS orders,
    SUM(total) AS spend
  FROM SILVER.STG_TRANSACTIONS
  WHERE status = 'completed'
  GROUP BY sales_date, user_id
) s
ON t.sales_date = s.sales_date AND t.user_id = s.user_id
WHEN MATCHED THEN UPDATE SET
  t.orders = s.orders,
  t.spend = s.spend,
  t.processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (sales_date, user_id, orders, spend)
VALUES (s.sales_date, s.user_id, s.orders, s.spend);
