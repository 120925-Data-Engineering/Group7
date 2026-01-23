MERGE INTO GOLD.FACT_REFUND_CHARGEBACK_ITEMS t
USING (
  SELECT
      tr.user_id,
      tr.transaction_id,
      tr.timestamp,
      tr.bill_country,
      tr.bill_city,
      tr.bill_state,
      COALESCE(
        ARRAY_AGG(
          OBJECT_CONSTRUCT(
            'product_id', it.product_id,
            'product_name', it.product_name,
            'category', it.category,
            'brand', it.brand,
            'quantity', it.quantity,
            'unit_price', it.unit_price,
            'item_subtotal', it.item_subtotal,
            'currency', it.currency,
            'line_item_index', it.line_item_index
          )
        ) WITHIN GROUP (ORDER BY it.line_item_index),
        ARRAY_CONSTRUCT()
      ) AS purchase_items
  FROM SILVER.stg_transactions_stream tr
  LEFT JOIN SILVER.stg_transaction_items_stream it
    ON it.transaction_id = tr.transaction_id
  WHERE tr.status = 'completed'
    AND tr.transaction_type IN ('refund', 'chargeback')
  GROUP BY
      tr.user_id,
      tr.transaction_id,
      tr.timestamp,
      tr.bill_country,
      tr.bill_city,
      tr.bill_state
) s
ON  t.user_id = s.user_id
AND t.transaction_id = s.transaction_id

WHEN MATCHED THEN UPDATE SET
  t.timestamp      = s.timestamp,
  t.bill_country   = s.bill_country,
  t.bill_city      = s.bill_city,
  t.bill_state     = s.bill_state,
  t.purchase_items = s.purchase_items,
  t.processed_at   = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
  user_id, transaction_id, timestamp,
  bill_country, bill_city, bill_state,
  purchase_items
) VALUES (
  s.user_id, s.transaction_id, s.timestamp,
  s.bill_country, s.bill_city, s.bill_state,
  s.purchase_items
);