USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;


CREATE OR REPLACE TABLE MODULE_TOUCH_UTM_REFERRER
(
    TOUCH_ID               VARCHAR,
    ATTRIBUTED_USER_ID     VARCHAR,
    STITCHED_IDENTITY_TYPE VARCHAR,
    TOUCH_LANDING_PAGE     VARCHAR,
    TOUCH_HOSTNAME         VARCHAR,
    UTM_CAMPAIGN           VARCHAR,
    UTM_MEDIUM             VARCHAR,
    UTM_SOURCE             VARCHAR,
    UTM_TERM               VARCHAR,
    UTM_CONTENT            VARCHAR,
    CLICK_ID               VARCHAR,
    SUB_AFFILIATE_NAME     VARCHAR,
    AFFILIATE              VARCHAR,
    AWADGROUPID            VARCHAR,
    AWCAMPAIGNID           VARCHAR,
    REFERRER_HOSTNAME      VARCHAR,
    REFERRER_MEDIUM        VARCHAR,
    UPDATED_AT             TIMESTAMP_LTZ
);

--retract touches that have been re-touchified

MERGE INTO MODULE_TOUCH_UTM_REFERRER AS TARGET
    USING (
        SELECT EVENT_HASH
        FROM MODULE_TOUCHIFICATION
--      WHERE UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load

    ) AS BATCH ON TARGET.TOUCH_ID = BATCH.EVENT_HASH
    WHEN MATCHED THEN DELETE;


--insert new or retouched touch utm referrers

MERGE INTO MODULE_TOUCH_UTM_REFERRER AS TARGET
    USING (
        SELECT b.TOUCH_ID,
               b.ATTRIBUTED_USER_ID,
               b.STITCHED_IDENTITY_TYPE,
               b.TOUCH_LANDING_PAGE,
               b.TOUCH_HOSTNAME,
               p.UTM_CAMPAIGN,
               p.UTM_MEDIUM,
               p.UTM_SOURCE,
               p.UTM_TERM,
               p.UTM_CONTENT,
               p.CLICK_ID,
               p.SUB_AFFILIATE_NAME,
               p.AFFILIATE,
               p.AWADGROUPID,
               p.AWCAMPAIGNID,
               r.URL_HOSTNAME    AS REFERRER_HOSTNAME,
               r.URL_MEDIUM      AS REFERRER_MEDIUM,
               CURRENT_TIMESTAMP AS UPDATED_AT --TODO: replace with '{schedule_tstamp}'
        FROM MODULE_TOUCH_BASIC_ATTRIBUTES b
                 LEFT JOIN MODULE_EXTRACTED_PARAMS p ON b.TOUCH_LANDING_PAGE = p.URL
                 LEFT JOIN MODULE_URL_HOSTNAME r ON b.TOUCH_REFERRER_URL = r.URL
        --      WHERE p.UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load

    ) AS BATCH ON TARGET.TOUCH_ID = BATCH.TOUCH_ID
    WHEN NOT MATCHED
        THEN INSERT (
                     TOUCH_ID,
                     ATTRIBUTED_USER_ID,
                     STITCHED_IDENTITY_TYPE,
                     TOUCH_LANDING_PAGE,
                     TOUCH_HOSTNAME,
                     UTM_CAMPAIGN,
                     UTM_MEDIUM,
                     UTM_SOURCE,
                     UTM_TERM,
                     UTM_CONTENT,
                     CLICK_ID,
                     SUB_AFFILIATE_NAME,
                     AFFILIATE,
                     AWADGROUPID,
                     AWCAMPAIGNID,
                     REFERRER_HOSTNAME,
                     REFERRER_MEDIUM,
                     UPDATED_AT
        ) VALUES (BATCH.TOUCH_ID,
                  BATCH.ATTRIBUTED_USER_ID,
                  BATCH.STITCHED_IDENTITY_TYPE,
                  BATCH.TOUCH_LANDING_PAGE,
                  BATCH.TOUCH_HOSTNAME,
                  BATCH.UTM_CAMPAIGN,
                  BATCH.UTM_MEDIUM,
                  BATCH.UTM_SOURCE,
                  BATCH.UTM_TERM,
                  BATCH.UTM_CONTENT,
                  BATCH.CLICK_ID,
                  BATCH.SUB_AFFILIATE_NAME,
                  BATCH.AFFILIATE,
                  BATCH.AWADGROUPID,
                  BATCH.AWCAMPAIGNID,
                  BATCH.REFERRER_HOSTNAME,
                  BATCH.REFERRER_MEDIUM,
                  BATCH.UPDATED_AT)
    WHEN MATCHED
        THEN UPDATE SET
        TARGET.TOUCH_ID = BATCH.TOUCH_ID,
        TARGET.ATTRIBUTED_USER_ID = BATCH.ATTRIBUTED_USER_ID,
        TARGET.STITCHED_IDENTITY_TYPE = BATCH.STITCHED_IDENTITY_TYPE,
        TARGET.TOUCH_LANDING_PAGE = BATCH.TOUCH_LANDING_PAGE,
        TARGET.TOUCH_HOSTNAME = BATCH.TOUCH_HOSTNAME,
        TARGET.UTM_CAMPAIGN = BATCH.UTM_CAMPAIGN,
        TARGET.UTM_MEDIUM = BATCH.UTM_MEDIUM,
        TARGET.UTM_SOURCE = BATCH.UTM_SOURCE,
        TARGET.UTM_TERM = BATCH.UTM_TERM,
        TARGET.UTM_CONTENT = BATCH.UTM_CONTENT,
        TARGET.CLICK_ID = BATCH.CLICK_ID,
        TARGET.SUB_AFFILIATE_NAME = BATCH.SUB_AFFILIATE_NAME,
        TARGET.AFFILIATE = BATCH.AFFILIATE,
        TARGET.AWADGROUPID = BATCH.AWADGROUPID,
        TARGET.AWCAMPAIGNID = BATCH.AWCAMPAIGNID,
        TARGET.REFERRER_HOSTNAME = BATCH.REFERRER_HOSTNAME,
        TARGET.REFERRER_MEDIUM = BATCH.REFERRER_MEDIUM,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT
;

------------------------------------------------------------------------------------------------------------------------
--assertions
--correct number of touches
SELECT CASE
           WHEN
                       (SELECT COUNT(distinct TOUCH_ID) FROM MODULE_TOUCHIFICATION)
                   =
                       (SELECT COUNT(*) FROM MODULE_TOUCH_UTM_REFERRER) THEN TRUE
           ELSE FALSE END AS CORRECT_NUMBER_OF_UTM_REFERRERS;

--unique touches
SELECT CASE
           WHEN (SELECT COUNT(*)
                 FROM (
                          SELECT TOUCH_ID
                          FROM MODULE_TOUCH_UTM_REFERRER
                          GROUP BY 1
                          HAVING COUNT(*) > 1)) > 0 THEN FALSE
           ELSE TRUE END AS UNIQUE_TOUCH_IDS;