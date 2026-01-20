MERGE INTO GOLD.FACT_DAILY_REVENUE t
USING (
  SELECT
    CAST(timestamp AS DATE) AS sales_date,
    COUNT(*) AS orders,
    SUM(total) AS revenue,
    SUM(tax) AS tax,
    ROUND(SUM(total) / NULLIF(COUNT(*), 0), 2) AS avg_order_value
  FROM SILVER.STG_TRANSACTIONS
  WHERE status = 'completed'
  GROUP BY sales_date
) s
ON t.sales_date = s.sales_date
WHEN MATCHED THEN UPDATE SET
  t.orders = s.orders,
  t.revenue = s.revenue,
  t.tax = s.tax,
  t.avg_order_value = s.avg_order_value,
  t.processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (sales_date, orders, revenue, tax, avg_order_value)
VALUES (s.sales_date, s.orders, s.revenue, s.tax, s.avg_order_value);
