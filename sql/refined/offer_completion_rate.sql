CREATE OR REPLACE TABLE `gcp-project-463802.refined.offer_compensation_rate` AS

WITH
  exploded_offers AS (
    SELECT
      offer_id,
      channel
    FROM
      `gcp-project-463802.trusted.offers`,
      UNNEST(channels) AS channel
  ),

  completed_offers AS (
    SELECT
      DISTINCT JSON_EXTRACT_SCALAR(value, '$.offer_id') AS offer_id
    FROM
      `gcp-project-463802.raw.events`    -- use raw.events here
    WHERE
      event = 'offer completed'
  )

SELECT
  o.channel,
  ROUND(
    COUNT(DISTINCT c.offer_id)
    / COUNT(DISTINCT o.offer_id)
    , 4
  ) AS completion_rate
FROM
  exploded_offers o
LEFT JOIN
  completed_offers c
USING
  (offer_id)
GROUP BY
  o.channel
ORDER BY
  completion_rate DESC;
