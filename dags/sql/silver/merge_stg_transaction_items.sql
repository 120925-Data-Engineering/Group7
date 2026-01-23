MERGE INTO SILVER.STG_TRANSACTION_ITEMS t
USING (
  SELECT
    b.raw:"transaction_id"::STRING                          AS transaction_id,
    b.raw:"user_id"::STRING                                 AS user_id,
    TRY_TO_TIMESTAMP_NTZ(b.raw:"timestamp"::STRING)         AS event_ts,
    b.raw:"currency"::STRING                                AS currency,

    f.index                                                 AS line_item_index,

    f.value:"product_id"::STRING                            AS product_id,
    f.value:"product_name"::STRING                          AS product_name,
    f.value:"category"::STRING                              AS category,
    f.value:"brand"::STRING                                 AS brand,
    TRY_TO_NUMBER(f.value:"quantity"::STRING)               AS quantity,
    TRY_TO_NUMBER(f.value:"unit_price"::STRING, 18, 2)      AS unit_price,
    TRY_TO_NUMBER(f.value:"quantity"::STRING) 
      * TRY_TO_NUMBER(f.value:"unit_price"::STRING, 18, 2)  AS item_subtotal
  FROM BRONZE.RAW_TRANSACTIONS_STREAM b,
  LATERAL FLATTEN(input => b.raw:"line_items") f
) s
ON t.transaction_id = s.transaction_id
AND t.line_item_index = s.line_item_index
WHEN MATCHED THEN UPDATE SET
  user_id = s.user_id,
  event_ts = s.event_ts,
  currency = s.currency,
  product_id = s.product_id,
  product_name = s.product_name,
  category = s.category,
  brand = s.brand,
  quantity = s.quantity,
  unit_price = s.unit_price,
  item_subtotal = s.item_subtotal,
  processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  transaction_id, user_id, event_ts, currency,
  line_item_index,
  product_id, product_name, category, brand,
  quantity, unit_price, item_subtotal
) VALUES (
  s.transaction_id, s.user_id, s.event_ts, s.currency,
  s.line_item_index,
  s.product_id, s.product_name, s.category, s.brand,
  s.quantity, s.unit_price, s.item_subtotal
);
