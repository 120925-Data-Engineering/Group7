MERGE INTO gold.fact_user_events t
USING (
  SELECT
    event_id,
    event_type,
    timestamp,
    user_id,
    session_id,
    page,
    device,
    browser,
    country,
    city,
    product_id,
    quantity
  FROM silver.stg_user_events_stream
  WHERE event_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY processed_at DESC
  ) = 1
) s
ON t.event_id = s.event_id

WHEN MATCHED THEN UPDATE SET
  t.event_type = s.event_type,
  t.timestamp = s.timestamp,
  t.user_id = s.user_id,
  t.session_id = s.session_id,
  t.page = s.page,
  t.device = s.device,
  t.browser = s.browser,
  t.country = s.country,
  t.city = s.city,
  t.product_id = s.product_id,
  t.quantity = s.quantity,
  t.processed_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
  event_id,
  event_type,
  timestamp,
  user_id,
  session_id,
  page,
  device,
  browser,
  country,
  city,
  product_id,
  quantity,
  processed_at
) VALUES (
  s.event_id,
  s.event_type,
  s.timestamp,
  s.user_id,
  s.session_id,
  s.page,
  s.device,
  s.browser,
  s.country,
  s.city,
  s.product_id,
  s.quantity,
  CURRENT_TIMESTAMP()
);