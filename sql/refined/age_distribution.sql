CREATE OR REPLACE TABLE `gcp-project-463802.refined.age_distribution` AS
WITH completed_customers AS (
  -- get every customer who has completed at least one offer
  SELECT DISTINCT
    customer_id
  FROM
    `gcp-project-463802.raw.events`
  WHERE
    event = 'offer completed'
)

SELECT
  c.age,
  COUNTIF(cc.customer_id IS NOT NULL)         AS completed_count,
  COUNTIF(cc.customer_id IS NULL)             AS not_completed_count,
  ROUND(
    100 * COUNTIF(cc.customer_id IS NOT NULL) / COUNT(*),
    2
  )                                           AS percent_completed
FROM
  `gcp-project-463802.trusted.customers` c
LEFT JOIN
  completed_customers cc
ON
  c.customer_id = cc.customer_id
GROUP BY
  c.age
ORDER BY
  c.age;
