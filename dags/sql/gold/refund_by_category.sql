MERGE INTO GOLD.FACT_DAILY_REFUNDS_BY_CATEGORY t
USING (
  SELECT
    CAST(tx.timestamp AS DATE) AS sales_date,
    i.category,
    COUNT(DISTINCT tx.transaction_id) AS refund_count,
    SUM(ABS(i.item_subtotal)) AS refund_amount
  FROM SILVER.STG_TRANSACTION_ITEMS i
  JOIN SILVER.STG_TRANSACTIONS tx
    ON tx.transaction_id = i.transaction_id
  WHERE tx.transaction_type = 'refund'
  GROUP BY sales_date, i.category
) s
ON t.sales_date = s.sales_date AND t.category = s.category
WHEN MATCHED THEN UPDATE SET
  t.refund_count = s.refund_count,
  t.refund_amount = s.refund_amount,
  t.processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (sales_date, category, refund_count, refund_amount)
VALUES (s.sales_date, s.category, s.refund_count, s.refund_amount);
