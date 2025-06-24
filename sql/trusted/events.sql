-- sql/trusted/events.sql
CREATE OR REPLACE TABLE `gcp-project-463802.trusted.events` AS
SELECT
  customer_id,
  event,
  -- extract the offer_id from the JSON payload when it exists
  JSON_EXTRACT_SCALAR(value, '$.offer_id') AS offer_id,
  -- extract the transaction amount (for ‘transaction’ events)
  SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.amount') AS FLOAT64) AS value_amount,
  SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.reward') AS FLOAT64) AS reward,
  time
FROM
  `gcp-project-463802.raw.events`
;
