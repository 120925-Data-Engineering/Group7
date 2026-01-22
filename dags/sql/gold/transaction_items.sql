MERGE INTO gold.transaction_items t
USING (
  SELECT
    transaction_id,
    user_id,
    timestamp,
    product_id,
    product_name,
    category,
    brand,
    quantity,
    unit_price,
    item_subtotal,
    currency,
    line_item_index
  FROM silver.stg_transaction_items
  WHERE transaction_id IS NOT NULL
    AND line_item_index IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY transaction_id, line_item_index
    ORDER BY processed_at DESC
  ) = 1
) s
ON t.transaction_id = s.transaction_id
AND t.line_item_index = s.line_item_index

WHEN MATCHED THEN UPDATE SET
  t.user_id = s.user_id,
  t.timestamp = s.timestamp,
  t.product_id = s.product_id,
  t.product_name = s.product_name,
  t.category = s.category,
  t.brand = s.brand,
  t.quantity = s.quantity,
  t.unit_price = s.unit_price,
  t.item_subtotal = s.item_subtotal,
  t.currency = s.currency,
  t.processed_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
  transaction_id,
  user_id,
  timestamp,
  product_id,
  product_name,
  category,
  brand,
  quantity,
  unit_price,
  item_subtotal,
  currency,
  line_item_index,
  processed_at
) VALUES (
  s.transaction_id,
  s.user_id,
  s.timestamp,
  s.product_id,
  s.product_name,
  s.category,
  s.brand,
  s.quantity,
  s.unit_price,
  s.item_subtotal,
  s.currency,
  s.line_item_index,
  CURRENT_TIMESTAMP()
);
