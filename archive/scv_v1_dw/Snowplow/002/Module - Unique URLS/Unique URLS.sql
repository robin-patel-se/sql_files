USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

-- Combine page and referrer url and create distinct list.
CREATE OR REPLACE TABLE MODULE_UNIQUE_URLS
(
    URL          VARCHAR NOT NULL,
    PARSED_URL   OBJECT,
    IS_VALID_URL BOOLEAN,
    HAS_QUERY    BOOLEAN,
    UPDATED_AT   TIMESTAMP_LTZ
);

MERGE INTO MODULE_UNIQUE_URLS AS TARGET
    USING (
        WITH list_of_urls AS (
            --combine page urls and referrer urls into single stream of urls
            SELECT PAGE_URL               AS URL,
                   PARSE_URL(PAGE_URL, 1) AS PARSED_URL

            FROM EVENT_STREAM
            WHERE PAGE_URL IS NOT NULL
--                 AND UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
            UNION

            SELECT PAGE_REFERRER               AS URL,
                   PARSE_URL(PAGE_REFERRER, 1) AS PARSED_URL

            FROM EVENT_STREAM
            WHERE PAGE_REFERRER IS NOT NULL
--                 AND UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
        )

        SELECT DISTINCT URL,
                        PARSED_URL,
                        CASE
                            WHEN PARSED_URL['error'] IS NULL
                                THEN TRUE
                            ELSE FALSE
                            END           AS IS_VALID_URL, --url structure makes parsing fail.
                        CASE
                            WHEN PARSED_URL['query'] != 'null'
                                THEN TRUE
                            ELSE FALSE
                            END           AS HAS_QUERY,
                        CURRENT_TIMESTAMP AS updated_at    --TODO: replace with '{schedule_tstamp}'

        FROM list_of_urls

    ) AS BATCH ON TARGET.URL = BATCH.URL
    WHEN NOT MATCHED
        THEN INSERT (
                     URL,
                     PARSED_URL,
                     IS_VALID_URL,
                     HAS_QUERY,
                     UPDATED_AT
        ) VALUES (BATCH.URL,
                  BATCH.PARSED_URL,
                  BATCH.IS_VALID_URL,
                  BATCH.HAS_QUERY,
                  BATCH.UPDATED_AT);


------------------------------------------------------------------------------------------------------------------------
--assertions
--check that urls are unique
SELECT CASE WHEN COUNT(*) > 0 THEN FALSE ELSE TRUE END UNIQUE_URLS
FROM (
         SELECT URL,
                COUNT(*)
         FROM MODULE_UNIQUE_URLS
         GROUP BY 1
         HAVING COUNT(*) > 1
     )
;

--check that all page urls have been inserted correctly.
SELECT CASE
           WHEN
                   (-- count of page urls that can't be joined
                       SELECT COUNT(*)
                       FROM EVENT_STREAM e
                                LEFT JOIN MODULE_UNIQUE_URLS u ON e.PAGE_URL = u.URL
                       WHERE u.URL IS NULL
                         AND e.PAGE_URL IS NOT NULL)
                   > 0 THEN FALSE
           ELSE TRUE END AS ALL_PAGE_URLS_INSERTED;

--check that all referrer urls have been inserted correctly.
SELECT CASE
           WHEN
                   (-- count of referrer urls that can't be joined
                       SELECT COUNT(*)
                       FROM EVENT_STREAM e
                                LEFT JOIN MODULE_UNIQUE_URLS u ON e.PAGE_REFERRER = u.URL
                       WHERE u.URL IS NULL
                         AND e.PAGE_REFERRER IS NOT NULL)
                   > 0 THEN FALSE
           ELSE TRUE END AS ALL_REFERRER_URLS_INSERTED;

------------------------------------------------------------------------------------------------------------------------
DROP SCHEMA DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG;
DROP TABLE HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM;