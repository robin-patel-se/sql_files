USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TABLE MODULE_TOUCH_MARKETING_CHANNEL
(
    TOUCH_ID           VARCHAR,
    TOUCH_MKT_CHANNEL  VARCHAR,
    TOUCH_LANDING_PAGE VARCHAR,
    ATTRIBUTED_USER_ID VARCHAR,
    UTM_CAMPAIGN       VARCHAR,
    UTM_MEDIUM         VARCHAR,
    UTM_SOURCE         VARCHAR,
    UTM_TERM           VARCHAR,
    UTM_CONTENT        VARCHAR,
    CLICK_ID           VARCHAR,
    SUB_AFFILIATE_NAME VARCHAR,
    AFFILIATE          VARCHAR,
    AWADGROUPID        VARCHAR,
    AWCAMPAIGNID       VARCHAR,
    REFERRER_HOSTNAME  VARCHAR,
    REFERRER_MEDIUM    VARCHAR,
    UPDATED_AT         TIMESTAMP_LTZ
);

--retract touches that have been re-touchified

MERGE INTO MODULE_TOUCH_MARKETING_CHANNEL AS TARGET
    USING (
        SELECT EVENT_HASH
        FROM MODULE_TOUCHIFICATION
--      WHERE UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load

    ) AS BATCH ON TARGET.TOUCH_ID = BATCH.EVENT_HASH
    WHEN MATCHED THEN DELETE;


--insert new or retouched touch channels

MERGE INTO MODULE_TOUCH_MARKETING_CHANNEL AS TARGET
    USING (
        SELECT t.TOUCH_ID,
               CASE
                   --no utm or referrer data

                   WHEN
                           (
                                   t.UTM_CAMPAIGN IS NULL AND
                                   t.UTM_CONTENT IS NULL AND
                                   t.UTM_TERM IS NULL AND
                                   t.UTM_MEDIUM IS NULL AND
                                   t.UTM_SOURCE IS NULL AND
                                   t.CLICK_ID IS NULL AND
                                   (
                                           t.REFERRER_MEDIUM IS NULL
                                           OR
                                           REFERRER_HOSTNAME = 'm.facebook.com' -- to handle facebook oauth logins for direct traffic
                                       )
                               )
                           OR
                           (
                                   t.REFERRER_MEDIUM = 'internal'
                                   AND
                                   t.UTM_MEDIUM IS DISTINCT FROM 'email' --to handle autocomms coming through as internal
                               )
                       THEN 'Direct'

                   --when the utm or gclid params aren't all null

                   WHEN t.UTM_MEDIUM = 'email'
                       THEN
                       CASE
                           WHEN t.UTM_SOURCE = 'newsletter' THEN 'Email - Newsletter'
                           WHEN t.UTM_SOURCE = 'ame' THEN 'Email - Triggers'
                           --might have more
                           ELSE 'Email - Other'
                           END


                   WHEN t.UTM_MEDIUM IN ('display', 'tpemail', 'native', 'gdn')
                       THEN 'Display'

                   WHEN UTM_MEDIUM = 'affiliateprogramme'
                       THEN 'Affiliate Program'

                   WHEN UTM_MEDIUM = 'SE_media' THEN 'Media'

                   WHEN UTM_MEDIUM = 'blog' THEN 'Blog'

                   WHEN UTM_SOURCE = 'youtube' THEN 'YouTube' --not in place yet but will be

                   WHEN t.CLICK_ID IS NOT NULL
                       THEN CASE -- Rumi to assist in expanding definition between PPC - Brand, PPC - Non Brand CPA, PPC - Non Brand CPL
                                WHEN TOUCH_LANDING_PAGE REGEXP
                                     '.*(secret-twosquirrel|secret-clover|secret-squirrel|secret-threesquirrel|secret-foursquirrel|secret-fivesquirrel|yahooppc|yahooppclink).*'
                                    --this query is in the page path
                                    THEN 'PPC - Non Brand'
                                ELSE 'PPC - Brand'
                       END

                   WHEN t.UTM_MEDIUM = 'facebookads' THEN 'Paid Social'

                   WHEN t.UTM_MEDIUM = 'organic-social'
                       OR (UTM_MEDIUM = 'social' AND UTM_SOURCE LIKE 'whatsapp%') --whatsapp shares
                       OR (UTM_MEDIUM = 'social' AND UTM_SOURCE LIKE 'fbshare%') --facebook shares
                       OR (UTM_MEDIUM = 'social' AND UTM_SOURCE LIKE 'tweet%') --twitter shares
                       THEN 'Organic Social'

                   WHEN t.REFERRER_MEDIUM = 'search' THEN 'Organic Search'

                   -- no utm or glcid params (but there are referrer details)
                   WHEN
                           t.UTM_CAMPAIGN IS NULL AND
                           t.UTM_CONTENT IS NULL AND
                           t.UTM_TERM IS NULL AND
                           t.UTM_MEDIUM IS NULL AND
                           t.UTM_SOURCE IS NULL AND
                           t.CLICK_ID IS NULL
                       THEN
                       CASE
                           WHEN (
                                   (t.REFERRER_MEDIUM = 'internal')
                                   OR
                                   (
                                           t.REFERRER_MEDIUM = 'unknown' AND
                                           (
                                                   t.REFERRER_HOSTNAME LIKE '%secretescapes.%' OR
                                                   t.REFERRER_HOSTNAME LIKE 'evasionssecretes.%' OR
                                                   t.REFERRER_HOSTNAME LIKE 'travelbird.%' OR
                                                   t.REFERRER_HOSTNAME LIKE '%.travelist.%' OR
                                                   t.REFERRER_HOSTNAME LIKE '%.pigsback.%'
                                               )
                                       )
                               )
                               THEN 'Direct'

                           WHEN t.REFERRER_MEDIUM = 'unknown' AND
                                (t.REFERRER_HOSTNAME LIKE '%urlaub%' OR
                                 t.REFERRER_HOSTNAME LIKE '%butterholz%' OR
                                 t.REFERRER_HOSTNAME LIKE '%mydealz%' OR
                                 t.REFERRER_HOSTNAME LIKE '%travel-dealz%' OR
                                 t.REFERRER_HOSTNAME LIKE '%travel-dealz%' OR
                                 t.REFERRER_HOSTNAME LIKE '%discountvouchers%'
                                    ) THEN 'Partner'
                           ELSE 'Other'
                           END
                   ELSE 'Other'
                   END
                                 AS TOUCH_MKT_CHANNEL,
               t.TOUCH_LANDING_PAGE,
               t.UTM_CAMPAIGN,
               T.ATTRIBUTED_USER_ID,
               t.UTM_MEDIUM,
               t.UTM_SOURCE,
               t.UTM_TERM,
               t.UTM_CONTENT,
               t.CLICK_ID,
               t.SUB_AFFILIATE_NAME,
               t.AFFILIATE,
               t.AWADGROUPID,
               t.AWCAMPAIGNID,
               t.REFERRER_HOSTNAME,
               t.REFERRER_MEDIUM,
               CURRENT_TIMESTAMP AS updated_at --TODO: replace with '{schedule_tstamp}'
        FROM MODULE_TOUCH_UTM_REFERRER t
        --      WHERE t.UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load

    ) AS BATCH ON TARGET.TOUCH_ID = BATCH.TOUCH_ID
    WHEN NOT MATCHED
        THEN INSERT (
                     TOUCH_ID,
                     TOUCH_MKT_CHANNEL,
                     TOUCH_LANDING_PAGE,
                     ATTRIBUTED_USER_ID,
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
                  BATCH.TOUCH_MKT_CHANNEL,
                  BATCH.TOUCH_LANDING_PAGE,
                  BATCH.ATTRIBUTED_USER_ID,
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
    WHEN MATCHED AND TARGET.TOUCH_MKT_CHANNEL != BATCH.TOUCH_MKT_CHANNEL
        THEN UPDATE SET
        TARGET.TOUCH_MKT_CHANNEL = BATCH.TOUCH_MKT_CHANNEL,
        TARGET.TOUCH_LANDING_PAGE = BATCH.TOUCH_LANDING_PAGE,
        TARGET.ATTRIBUTED_USER_ID = BATCH.ATTRIBUTED_USER_ID,
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
        TARGET.UPDATED_AT = BATCH.UPDATED_AT;

------------------------------------------------------------------------------------------------------------------------
--assertions

SELECT CASE
           WHEN
                       (SELECT COUNT(*) FROM MODULE_TOUCH_MARKETING_CHANNEL)
                   =
                       (SELECT COUNT(*) FROM MODULE_TOUCH_UTM_REFERRER)
               THEN TRUE
           ELSE FALSE END AS ALL_TOUCHES_CHANNELLED;
