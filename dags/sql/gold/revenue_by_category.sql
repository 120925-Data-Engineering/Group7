MERGE INTO GOLD.FACT_DAILY_REVENUE_BY_CATEGORY t
USING (
  SELECT
    CAST(timestamp AS DATE) AS sales_date,
    category,
    SUM(quantity) AS units_sold,
    SUM(item_subtotal) AS revenue
  FROM SILVER.STG_TRANSACTION_ITEMS
  GROUP BY sales_date, category
) s
ON t.sales_date = s.sales_date AND t.category = s.category
WHEN MATCHED THEN UPDATE SET
  t.units_sold = s.units_sold,
  t.revenue = s.revenue,
  t.processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (sales_date, category, units_sold, revenue)
VALUES (s.sales_date, s.category, s.units_sold, s.revenue);
