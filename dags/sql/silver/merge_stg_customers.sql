MERGE INTO SILVER.STG_CUSTOMERS t
USING (
  SELECT
    raw:"user_id"::STRING                              AS user_id,
    raw:"email"::STRING                                AS email,
    raw:"first_name"::STRING                           AS first_name,
    raw:"last_name"::STRING                            AS last_name,
    TRY_TO_DATE(raw:"registration_date"::STRING)       AS registration_date,
    raw:"account_type"::STRING                         AS account_type,
    TRY_TO_DATE(raw:"date_of_birth"::STRING)           AS date_of_birth,
    COALESCE(TRY_TO_NUMBER(raw:"loyalty_points"::STRING), 0) AS loyalty_points,
    raw:"state"::STRING                                AS state
  FROM BRONZE.RAW_USERS_STREAM
) s
ON t.user_id = s.user_id
WHEN MATCHED THEN UPDATE SET
  t.email             = s.email,
  t.first_name        = s.first_name,
  t.last_name         = s.last_name,
  t.registration_date = s.registration_date,
  t.account_type      = s.account_type,
  t.date_of_birth     = s.date_of_birth,
  t.loyalty_points    = s.loyalty_points,
  t.state             = s.state,
  t.processed_at      = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  user_id, email, first_name, last_name, registration_date, account_type, date_of_birth, loyalty_points, state
) VALUES (
  s.user_id, s.email, s.first_name, s.last_name, s.registration_date, s.account_type, s.date_of_birth, s.loyalty_points, s.state
);
