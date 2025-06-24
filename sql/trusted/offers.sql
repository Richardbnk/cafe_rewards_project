CREATE OR REPLACE TABLE `gcp-project-463802.trusted.offers` AS
SELECT
  offer_id,
  offer_type,
  difficulty,
  reward,
  duration,
  -- turn "['web','email',...]" into a JSON array, then extract each element
  ARRAY(
    SELECT
      JSON_VALUE(elem, '$')
    FROM
      UNNEST(
        JSON_EXTRACT_ARRAY(
          REGEXP_REPLACE(channels, r"'([^']*)'", r'"\1"')
        )
      ) AS elem
  ) AS channels
FROM
  `gcp-project-463802.raw.offers`;
