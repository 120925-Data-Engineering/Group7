MERGE INTO gold.dim_customer t
USING (
  SELECT
    user_id,
    first_name,
    last_name,
    first_name || ' ' || last_name AS full_name,
    email,
    account_type,
    TRY_TO_DATE(registration_date) AS registration_date,
    TRY_TO_DATE(date_of_birth) AS date_of_birth,
    loyalty_points,
    state,
    TRUE AS is_active,
    CURRENT_TIMESTAMP() AS processed_at
  FROM silver.stg_customers
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY processed_at DESC) = 1
) s
ON t.user_id = s.user_id
WHEN MATCHED THEN UPDATE SET
  t.first_name = s.first_name,
  t.last_name = s.last_name,
  t.full_name = s.full_name,
  t.email = s.email,
  t.account_type = s.account_type,
  t.registration_date = s.registration_date,
  t.date_of_birth = s.date_of_birth,
  t.loyalty_points = s.loyalty_points,
  t.state = s.state,
  t.is_active = s.is_active,
  t.processed_at = s.processed_at
WHEN NOT MATCHED THEN INSERT (
  user_id, first_name, last_name, full_name, email, account_type,
  registration_date, date_of_birth, loyalty_points, state, is_active, processed_at
) VALUES (
  s.user_id, s.first_name, s.last_name, s.full_name, s.email, s.account_type,
  s.registration_date, s.date_of_birth, s.loyalty_points, s.state, s.is_active, s.processed_at
);