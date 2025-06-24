CREATE OR REPLACE TABLE `gcp-project-463802.trusted.customers` AS
SELECT
  customer_id,
  -- if your raw.became_member_on is already DATETIME:
  DATE(became_member_on) AS became_member_on,
  gender,
  CAST(age    AS INT64)   AS age,
  CAST(income AS FLOAT64) AS income
FROM `gcp-project-463802.raw.customers`
WHERE income IS NOT NULL;
