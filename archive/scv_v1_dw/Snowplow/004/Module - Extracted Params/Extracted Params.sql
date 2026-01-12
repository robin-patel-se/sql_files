USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

--Take last extracted parameters and harmonise the relevant utms
--Found URL queries that have the same utm parameters in there twice.

CREATE OR REPLACE TABLE MODULE_EXTRACTED_PARAMS
(
    URL                VARCHAR,
    UTM_CAMPAIGN       VARCHAR,
    UTM_MEDIUM         VARCHAR,
    UTM_SOURCE         VARCHAR,
    UTM_TERM           VARCHAR,
    UTM_CONTENT        VARCHAR,
    CLICK_ID           VARCHAR,
    SUB_AFFILIATE_NAME VARCHAR,
    FROM_APP           VARCHAR,
    SNOWPLOW_ID        VARCHAR,
    AFFILIATE          VARCHAR,
    AWCAMPAIGNID       VARCHAR,
    AWADGROUPID        VARCHAR,
    ACCOUNT_VERIFIED   VARCHAR,
    UPDATED_AT         VARCHAR
);

MERGE INTO MODULE_EXTRACTED_PARAMS AS TARGET
    USING (
        WITH pivot AS (
            --pivot and harmonise parameters into columns
            SELECT u.url,
                   p.PARAMETER_INDEX,
                   CASE
                       WHEN PARAMETER IN ('utm_campaign', 'cid', 'legacy_campaign', 'campaign')
                           THEN PARAMETER_VALUE END AS utm_campaign,
                   CASE
                       WHEN PARAMETER IN ('utm_medium', 'medium')
                           THEN PARAMETER_VALUE END AS utm_medium,
                   CASE
                       WHEN PARAMETER IN ('utm_source', 'source')
                           THEN PARAMETER_VALUE END AS utm_source,
                   CASE
                       WHEN PARAMETER IN ('utm_term', 'term')
                           THEN PARAMETER_VALUE END AS utm_term,
                   CASE
                       WHEN PARAMETER = 'utm_content'
                           THEN PARAMETER_VALUE END AS utm_content,
                   CASE
                       WHEN PARAMETER IN ('gclid', 'msclkid', 'dclid', 'clickid')
                           THEN PARAMETER_VALUE END AS click_id,
                   CASE
                       WHEN PARAMETER = 'saff'
                           THEN PARAMETER_VALUE END AS sub_affiliate_name,
                   CASE
                       WHEN PARAMETER = 'fromApp'
                           THEN PARAMETER_VALUE END AS from_app,
                   CASE
                       WHEN PARAMETER = 'Snowplow'
                           THEN PARAMETER_VALUE END AS snowplow_id,
                   CASE
                       WHEN PARAMETER = 'affiliate'
                           THEN PARAMETER_VALUE END AS affiliate,
                   CASE
                       WHEN PARAMETER = 'awcampaignid'
                           THEN PARAMETER_VALUE END AS awcampaignid,
                   CASE
                       WHEN PARAMETER = 'awadgroupid'
                           THEN PARAMETER_VALUE END AS awadgroupid,
                   CASE
                       WHEN PARAMETER = 'accountVerified'
                           THEN PARAMETER_VALUE END AS account_verified
            FROM MODULE_UNIQUE_URLS u
                     INNER JOIN MODULE_URL_PARAMS p ON u.URL = p.URL
            WHERE u.IS_VALID_URL = TRUE
              AND u.HAS_QUERY = TRUE
            -- AND u.UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load

        )
             -- select the last versions of the utm params in any query (found cases where there are duplicates)
        SELECT DISTINCT URL,
                        LAST_VALUE(utm_campaign)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS utm_campaign,
                        LAST_VALUE(utm_medium)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS utm_medium,
                        LAST_VALUE(utm_source)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS utm_source,
                        LAST_VALUE(utm_term)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS utm_term,
                        LAST_VALUE(utm_content)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS utm_content,
                        LAST_VALUE(click_id)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS click_id,
                        LAST_VALUE(sub_affiliate_name)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS sub_affiliate_name,
                        LAST_VALUE(from_app)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS from_app,
                        LAST_VALUE(snowplow_id)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS snowplow_id,
                        LAST_VALUE(affiliate)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS affiliate,
                        LAST_VALUE(awcampaignid)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS awcampaignid,
                        LAST_VALUE(awadgroupid)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS awadgroupid,
                        LAST_VALUE(account_verified)
                                   IGNORE NULLS OVER (PARTITION BY URL ORDER BY PARAMETER_INDEX) AS account_verified,
                        current_timestamp                                                        AS updated_at --TODO: replace with '{schedule_tstamp}'
        FROM pivot
    ) AS BATCH ON TARGET.URL = BATCH.URL
    WHEN NOT MATCHED
        THEN INSERT (
                     URL,
                     UTM_CAMPAIGN,
                     UTM_MEDIUM,
                     UTM_SOURCE,
                     UTM_TERM,
                     UTM_CONTENT,
                     CLICK_ID,
                     SUB_AFFILIATE_NAME,
                     FROM_APP,
                     SNOWPLOW_ID,
                     AFFILIATE,
                     AWCAMPAIGNID,
                     AWADGROUPID,
                     ACCOUNT_VERIFIED,
                     UPDATED_AT
        ) VALUES (BATCH.URL,
                  BATCH.UTM_CAMPAIGN,
                  BATCH.UTM_MEDIUM,
                  BATCH.UTM_SOURCE,
                  BATCH.UTM_TERM,
                  BATCH.UTM_CONTENT,
                  BATCH.CLICK_ID,
                  BATCH.SUB_AFFILIATE_NAME,
                  BATCH.FROM_APP,
                  BATCH.SNOWPLOW_ID,
                  BATCH.AFFILIATE,
                  BATCH.AWCAMPAIGNID,
                  BATCH.AWADGROUPID,
                  BATCH.ACCOUNT_VERIFIED,
                  BATCH.UPDATED_AT);

------------------------------------------------------------------------------------------------------------------------
--assertions
--check all urls within module have a unique set of params extracted to them.
SELECT CASE WHEN COUNT(*) > 1 THEN FALSE ELSE TRUE END AS UNIQUE_URL
FROM (
         SELECT URL,
                COUNT(*)
         FROM MODULE_EXTRACTED_PARAMS
         GROUP BY 1
         HAVING COUNT(*) > 1
     );


--check that all unique page urls that have a query have extracted url params associated to them.
SELECT CASE
           WHEN
                   (SELECT COUNT(*)
                    FROM MODULE_UNIQUE_URLS
                    WHERE HAS_QUERY = TRUE)
                   =
                   (SELECT COUNT(*)
                    FROM MODULE_EXTRACTED_PARAMS)
               THEN TRUE
           ELSE FALSE END AS ALL_UNIQUE_PAGE_URLS_HAVE_EXTRACTED_PARAMS
;