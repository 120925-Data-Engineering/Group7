MERGE INTO gold.dim_product t
USING (
  SELECT
    product_id,
    product_name,
    category,
    subcategory,
    manufacturer,
    brand,
    msrp,
    cost_price,
    TRY_TO_DATE(created_date) AS created_date,
    is_active,
    CURRENT_TIMESTAMP() AS processed_at
  FROM silver.stg_products
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY processed_at DESC) = 1
) s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET
  t.product_name = s.product_name,
  t.category = s.category,
  t.subcategory = s.subcategory,
  t.manufacturer = s.manufacturer,
  t.brand = s.brand,
  t.msrp = s.msrp,
  t.cost_price = s.cost_price,
  t.created_date = s.created_date,
  t.is_active = s.is_active,
  t.processed_at = s.processed_at
WHEN NOT MATCHED THEN INSERT (
  product_id, product_name, category, subcategory, manufacturer, brand,
  msrp, cost_price, created_date, is_active, processed_at
) VALUES (
  s.product_id, s.product_name, s.category, s.subcategory, s.manufacturer, s.brand,
  s.msrp, s.cost_price, s.created_date, s.is_active, s.processed_at
);