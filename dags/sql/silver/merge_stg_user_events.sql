MERGE INTO SILVER.STG_USER_EVENTS t
USING (
  SELECT
    raw:"event_id"::STRING                            AS event_id,
    raw:"event_type"::STRING                          AS event_type,
    TRY_TO_TIMESTAMP_NTZ(raw:"timestamp"::STRING)     AS timestamp,
    raw:"user_id"::STRING                             AS user_id,
    raw:"session_id"::STRING                          AS session_id,
    raw:"page"::STRING                                AS page,
    raw:"device"::STRING                              AS device,
    raw:"browser"::STRING                             AS browser,
    raw:"country"::STRING                             AS country,
    raw:"city"::STRING                                AS city,
    COALESCE(raw:"product_id"::STRING, 'None')        AS product_id,
    COALESCE(TRY_TO_NUMBER(raw:"quantity"::STRING), 0)::INTEGER AS quantity
  FROM BRONZE.RAW_USER_EVENTS_STREAM
) s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
  t.event_type   = s.event_type,
  t.timestamp    = s.timestamp,
  t.user_id      = s.user_id,
  t.session_id   = s.session_id,
  t.page         = s.page,
  t.device       = s.device,
  t.browser      = s.browser,
  t.country      = s.country,
  t.city         = s.city,
  t.product_id   = s.product_id,
  t.quantity     = s.quantity,
  t.processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  event_id, event_type, timestamp, user_id, session_id, page, device, browser, country, city, product_id, quantity
) VALUES (
  s.event_id, s.event_type, s.timestamp, s.user_id, s.session_id, s.page, s.device, s.browser, s.country, s.city, s.product_id, s.quantity
);
