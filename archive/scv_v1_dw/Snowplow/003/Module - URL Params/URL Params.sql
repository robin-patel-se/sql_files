USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

--flatten and separate url parameters against a url.
--snowflake's parse_url:parameters function doesn't accommodate for multiple of the same utms appearing in the same url string.

CREATE OR REPLACE TABLE MODULE_URL_PARAMS
(
    URL             VARCHAR,
    PARAMETER_INDEX INT,
    PARAMETER       VARCHAR,
    PARAMETER_VALUE VARCHAR,
    UPDATED_AT      TIMESTAMP_LTZ
) cluster by (URL, PARAMETER)
;

MERGE INTO MODULE_URL_PARAMS AS TARGET
    USING (
        SELECT DISTINCT URL,
--        params.VALUE::varchar,
                        params.INDEX::varchar                                                                 AS PARAMETER_INDEX,
                        --index necessary to extract latest utm params if duplicates appear in the same url
                        LEFT(params.VALUE::varchar,
                             REGEXP_INSTR(params.VALUE::varchar, '=') - 1)                                    AS parameter,
                        CASE
                            WHEN LENGTH(REGEXP_SUBSTR(params.VALUE::varchar, '=.*', 1, 1, 'e')) = 1
                                THEN NULL --empty param
                            ELSE
                                RIGHT(REGEXP_SUBSTR(params.VALUE::varchar, '=.*', 1, 1, 'e'),
                                      LENGTH(REGEXP_SUBSTR(params.VALUE::varchar, '=.*', 1, 1, 'e')) - 1) END AS parameter_value,
                        CURRENT_TIMESTAMP                                                                     AS updated_at --TODO: replace with '{schedule_tstamp}'
        FROM MODULE_UNIQUE_URLS,
             LATERAL FLATTEN(INPUT => SPLIT(PARSE_URL(URL, 1)['query']::varchar, '&'), outer => true) params
        WHERE IS_VALID_URL = TRUE
          AND HAS_QUERY = TRUE
          -- AND UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
    ) AS BATCH ON TARGET.URL = BATCH.URL
    WHEN NOT MATCHED
        THEN INSERT (
                     URL,
                     PARAMETER_INDEX,
                     PARAMETER,
                     PARAMETER_VALUE,
                     UPDATED_AT
        ) VALUES (BATCH.URL,
                  BATCH.PARAMETER_INDEX,
                  BATCH.PARAMETER,
                  BATCH.PARAMETER_VALUE,
                  BATCH.UPDATED_AT);

------------------------------------------------------------------------------------------------------------------------
--assertions
--check that all page urls with a query from unique urls that have parameters that have been extracted
SELECT CASE
           WHEN
                   (SELECT COUNT(*)
                    FROM (
                             SELECT DISTINCT p.URL
                             FROM MODULE_URL_PARAMS p
                         )) --165328943
                   =
                   (SELECT COUNT(*)
                    FROM MODULE_UNIQUE_URLS
                    WHERE HAS_QUERY = TRUE
                      AND IS_VALID_URL = TRUE)--167465990
               THEN TRUE
           ELSE FALSE END AS PARAMS_FOR_ALL_URL_QUERIES
;
