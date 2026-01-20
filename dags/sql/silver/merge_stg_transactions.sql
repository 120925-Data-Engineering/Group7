merge into silver.stg_transactions t
using(
    select
        raw:"transaction_id"::string as transaction_id,
        raw:"user_id"::string as user_id,
        raw:"transaction_type"::string as transaction_type,
        try_to_timestamp_ntz(raw:"timestamp"::string) as timestamp,
        raw:"status"::string as status,
        raw:"payment_method"::STRING                              AS payment_method,
        raw:"currency"::STRING                                    AS currency,
        COALESCE(raw:"original_transaction_id"::STRING, 'None')   AS original_transaction_id,
        TRY_TO_NUMBER(raw:"subtotal"::STRING, 18, 2)              AS subtotal,
        TRY_TO_NUMBER(raw:"tax"::STRING, 18, 2)                   AS tax,
        TRY_TO_NUMBER(raw:"total"::STRING, 18, 2)                 AS total,
        raw:"billing_address":"city"::STRING                      AS bill_city,
        raw:"billing_address":"state"::STRING                     AS bill_state,
        raw:"billing_address":"country"::STRING                   AS bill_country,
        raw:"shipping_address":"city"::STRING                     AS ship_city,
        raw:"shipping_address":"country"::STRING                  AS ship_country
    from bronze.raw_transactions_stream
) s
on t.transaction_id = s.transaction_id
when matched then update set
  t.user_id                 = s.user_id,
  t.transaction_type        = s.transaction_type,
  t.timestamp               = s.timestamp,
  t.status                  = s.status,
  t.payment_method          = s.payment_method,
  t.currency                = s.currency,
  t.original_transaction_id = s.original_transaction_id,
  t.subtotal                = s.subtotal,
  t.tax                     = s.tax,
  t.total                   = s.total,
  t.bill_city               = s.bill_city,
  t.bill_state              = s.bill_state,
  t.bill_country            = s.bill_country,
  t.ship_city               = s.ship_city,
  t.ship_country            = s.ship_country,
  t.processed_at            = CURRENT_TIMESTAMP()
when not matched then insert (
  transaction_id, user_id, transaction_type, timestamp, status, payment_method, currency,
  original_transaction_id, subtotal, tax, total,
  bill_city, bill_state, bill_country,
  ship_city, ship_country
) values (
  s.transaction_id, s.user_id, s.transaction_type, s.timestamp, s.status, s.payment_method, s.currency,
  s.original_transaction_id, s.subtotal, s.tax, s.total,
  s.bill_city, s.bill_state, s.bill_country,
  s.ship_city, s.ship_country
);

MERGE INTO SILVER.STG_TRANSACTION_ITEMS t
USING (
  SELECT
    b.raw:"transaction_id"::STRING                          AS transaction_id,
    b.raw:"user_id"::STRING                                 AS user_id,
    TRY_TO_TIMESTAMP_NTZ(b.raw:"timestamp"::STRING)         AS timestamp,
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
  timestamp = s.timestamp,
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
  transaction_id, user_id, timestamp, currency,
  line_item_index,
  product_id, product_name, category, brand,
  quantity, unit_price, item_subtotal
) VALUES (
  s.transaction_id, s.user_id, s.timestamp, s.currency,
  s.line_item_index,
  s.product_id, s.product_name, s.category, s.brand,
  s.quantity, s.unit_price, s.item_subtotal
);
