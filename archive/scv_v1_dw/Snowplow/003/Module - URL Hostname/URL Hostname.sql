USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

--Extract the hostname from referrer urls so this can be used to classify traffic that doesn't have query utm params

CREATE OR REPLACE TABLE MODULE_URL_HOSTNAME
(
    URL          VARCHAR,
    URL_HOSTNAME VARCHAR,
    URL_MEDIUM   VARCHAR,
    UPDATED_AT   TIMESTAMP_LTZ
)
    CLUSTER BY (URL, URL_HOSTNAME, URL_MEDIUM);

MERGE INTO MODULE_URL_HOSTNAME AS TARGET
    USING (
        WITH parse_hostname AS (
            SELECT URL,
                   PARSE_URL(URL, 1)['host']::varchar AS url_hostname
            FROM MODULE_UNIQUE_URLS
            WHERE IS_VALID_URL = TRUE
            -- AND UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
        )

        SELECT DISTINCT URL,
                        url_hostname,
                        -- internal and payment gateway flag required to identify which referrers to ignore in touchification
                        -- internal defined as hostnames that SE track in Snowplow
                        CASE
                            WHEN url_hostname LIKE 'webmail.%' OR
                                 url_hostname LIKE '%.email' OR
                                 url_hostname LIKE 'email.%' OR
                                 url_hostname LIKE '%.email.%'
                                THEN 'email'
                            WHEN url_hostname LIKE '%.secretescapes.%' OR
                                 url_hostname LIKE '%.evasionssecretes.%' OR
                                 url_hostname = 'escapes.travelbook.de' OR
                                 url_hostname = 'travelbird.de' OR
                                 url_hostname = 'api.secretescapes.com' OR
                                 url_hostname LIKE '%.fs-staging.escapes.tech' OR
                                 url_hostname = 'www.optimizelyedit.com' OR
                                 url_hostname = 'cdn.secretescapes.com' OR
                                 url_hostname = 'secretescapes--c.eu12.visual.force.com' OR
                                 url_hostname = 'secretescapes.my.salesforce.com' OR
                                 url_hostname = 'cms.secretescapes.com' OR
                                 (url_hostname = '%.facebook.%' AND url_hostname like '%/oauth/%') --fb oauth logins
--                                  url_hostname = 'optimizely' -- TODO: expand on optimizely
                                THEN 'internal' -- TODO: expand on internal definitions
                            WHEN url_hostname = 'www.paypal.com' OR
                                 url_hostname = 'secure.worldpay.com' OR
                                 url_hostname = 'secure.bidverdrd.com' OR
                                 url_hostname = '3d-secure.pluscard.de' OR
                                 url_hostname = 'mastercardsecurecode.sparkassen-kreditkarten.de' OR
                                 url_hostname = '3d-secure.postbank.de' OR
                                 url_hostname = 'german-3dsecure.wlp-acs.com' OR
                                 url_hostname = '3d-secure-code.de' OR
                                 url_hostname = 'search.f-secure.com'
                                THEN 'payment_gateway'
                            WHEN url_hostname LIKE '%.google.%' OR
                                 url_hostname LIKE '%.bing.%'
                                THEN 'search'

                            WHEN url_hostname LIKE '%.pinterest.%' OR
                                 url_hostname LIKE '%.facebook.%'
                                OR url_hostname = 'instagram.com'
                                THEN 'social'
                            ELSE 'unknown'
                            END           AS URL_MEDIUM,
                        CURRENT_TIMESTAMP AS UPDATED_AT --TODO: replace with '{schedule_tstamp}'
        FROM parse_hostname
        WHERE URL_HOSTNAME IS NOT NULL
    ) AS BATCH ON TARGET.URL = BATCH.URL
    WHEN NOT MATCHED
        THEN INSERT (
                     URL,
                     URL_HOSTNAME,
                     URL_MEDIUM,
                     UPDATED_AT
        ) VALUES (BATCH.URL,
                  BATCH.URL_HOSTNAME,
                  BATCH.URL_MEDIUM,
                  BATCH.UPDATED_AT)
    WHEN MATCHED AND TARGET.URL_MEDIUM != BATCH.URL_MEDIUM --If we reclassify the url medium grouping
        THEN UPDATE SET
        TARGET.URL_MEDIUM = BATCH.URL_MEDIUM,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT;

------------------------------------------------------------------------------------------------------------------------
--assertions
--unique urls
SELECT CASE WHEN COUNT(*) > 0 THEN FALSE ELSE TRUE END AS UNIQUE_URLS
FROM (SELECT URL,
             COUNT(*)
      FROM MODULE_URL_HOSTNAME
      GROUP BY 1
      HAVING COUNT(*) > 1);

--hostnames are not null
SELECT CASE WHEN COUNT(*) > 0 THEN FALSE ELSE TRUE END AS NO_NULL_HOSTNAMES
FROM MODULE_URL_HOSTNAME
WHERE url_hostname IS NULL;