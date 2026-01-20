MERGE INTO SILVER.STG_PRODUCTS t
USING (
  SELECT
    raw:"product_id"::STRING                            AS product_id,
    raw:"product_name"::STRING                          AS product_name,
    raw:"category"::STRING                              AS category,
    raw:"subcategory"::STRING                           AS subcategory,
    raw:"brand"::STRING                                 AS brand,
    raw:"manufacturer"::STRING                          AS manufacturer,
    TRY_TO_NUMBER(raw:"msrp"::STRING, 18, 2)            AS msrp,
    TRY_TO_NUMBER(raw:"cost_price"::STRING, 18, 2)      AS cost_price,
    TRY_TO_DATE(raw:"created_date"::STRING)             AS created_date,
    COALESCE(raw:"is_active"::BOOLEAN, TRUE)            AS is_active
  FROM BRONZE.RAW_PRODUCTS_STREAM
) s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET
  t.product_name  = s.product_name,
  t.category      = s.category,
  t.subcategory   = s.subcategory,
  t.brand         = s.brand,
  t.manufacturer  = s.manufacturer,
  t.msrp          = s.msrp,
  t.cost_price    = s.cost_price,
  t.created_date  = s.created_date,
  t.is_active     = s.is_active,
  t.processed_at  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  product_id, product_name, category, subcategory, brand, manufacturer,
  msrp, cost_price, created_date, is_active
) VALUES (
  s.product_id, s.product_name, s.category, s.subcategory, s.brand, s.manufacturer,
  s.msrp, s.cost_price, s.created_date, s.is_active
);
