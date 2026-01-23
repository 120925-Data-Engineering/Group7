INSERT OVERWRITE INTO gold.dim_date
WITH bounds AS (
  SELECT
    DATEADD(day, -30, MIN(CAST(timestamp AS DATE))) AS min_d,
    DATEADD(day,  30, MAX(CAST(timestamp AS DATE))) AS max_d
  FROM silver.stg_transactions
  WHERE timestamp IS NOT NULL
),
span AS (
  SELECT DATEADD(day, SEQ4(), (SELECT min_d FROM bounds)) AS d
  FROM TABLE(GENERATOR(ROWCOUNT => 50000))
)
SELECT
  TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')) AS date_key,
  d AS date_value,
  YEAR(d) AS year,
  QUARTER(d) AS quarter,
  MONTH(d) AS month,
  TO_CHAR(d, 'MMMM') AS month_name,
  DAY(d) AS day,
  WEEKOFYEAR(d) AS week_of_year,
  DAYOFWEEKISO(d) AS day_of_week,
  TO_CHAR(d, 'DY') AS day_name,
  IFF(DAYOFWEEKISO(d) IN (6,7), TRUE, FALSE) AS is_weekend
FROM span
WHERE d <= (SELECT max_d FROM bounds);