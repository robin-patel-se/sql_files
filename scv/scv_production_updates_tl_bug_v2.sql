USE WAREHOUSE pipe_4xlarge;
-- *shifty eyes*
-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp.snowplow.event_stream_20220125 CLONE hygiene_vault_mvp.snowplow.event_stream;

UPDATE hygiene_vault_mvp.snowplow.event_stream es
SET es.se_user_id = NULL,
    es.updated_at = CURRENT_TIMESTAMP
WHERE es.contexts_com_secretescapes_product_display_context_1[0]['se_group_brand'] = 'Travelist Brand';

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.contexts_com_secretescapes_product_display_context_1[0]['se_group_brand'] = 'Travelist Brand'
  AND es.event_tstamp >= CURRENT_DATE - 1
  AND es.booking_id IS NOT NULL;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.travelist_unique_browser_ids AS (
    SELECT DISTINCT es.unique_browser_id
    FROM hygiene_vault_mvp.snowplow.event_stream es
    WHERE es.contexts_com_secretescapes_product_display_context_1[0]['se_group_brand'] = 'Travelist Brand'
);

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_identity_associations mia;
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_identity_stitching mis;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_identity_associations_20220125 CLONE data_vault_mvp.single_customer_view_stg.module_identity_associations;


DELETE
FROM data_vault_mvp.single_customer_view_stg.module_identity_associations
WHERE unique_browser_id IN (
    SELECT DISTINCT unique_browser_id
    FROM data_vault_mvp.single_customer_view_stg.travelist_unique_browser_ids
);

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching_20220125 CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;


DELETE
FROM data_vault_mvp.single_customer_view_stg.module_identity_stitching
WHERE unique_browser_id IN (
    SELECT DISTINCT unique_browser_id
    FROM data_vault_mvp.single_customer_view_stg.travelist_unique_browser_ids
);

-- reinsert identity associations:
MERGE INTO data_vault_mvp.single_customer_view_stg.module_identity_associations AS target
    USING (
        SELECT se_user_id,
               email_address,
               booking_id,

               unique_browser_id,
               cookie_id,
               session_userid,

               MIN(event_tstamp) AS earliest_event_tstamp, --needed to handle duplicate event user identifiers matching to secret escapes user identifier
               MAX(event_tstamp) AS latest_event_tstamp
        FROM hygiene_vault_mvp.snowplow.event_stream
        WHERE updated_at >= TO_DATE(TIMESTAMPADD('day', -1, '2022-01-24 03:00:00'::TIMESTAMP))
          AND COALESCE(unique_browser_id, cookie_id, session_userid) IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6
    ) AS batch ON
        --merge in new distinct associations
                target.se_user_id IS NOT DISTINCT FROM batch.se_user_id AND
                target.email_address IS NOT DISTINCT FROM batch.email_address AND
                target.booking_id IS NOT DISTINCT FROM batch.booking_id AND
                target.unique_browser_id IS NOT DISTINCT FROM batch.unique_browser_id AND
                target.cookie_id IS NOT DISTINCT FROM batch.cookie_id AND
                target.session_userid IS NOT DISTINCT FROM batch.session_userid
    WHEN NOT MATCHED
        THEN INSERT (schedule_tstamp,
                     run_tstamp,
                     operation_id,
                     created_at,
                     updated_at,
                     se_user_id,
                     email_address,
                     booking_id,
                     unique_browser_id,
                     cookie_id,
                     session_userid,
                     earliest_event_tstamp,
                     latest_event_tstamp
        )
        VALUES ('2022-01-24 03:00:00',
                '2022-01-25 03:55:16',
                'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events/02_identity_stitching/01_module_identity_associations.py__20220124T030000__daily_at_03h00',
                CURRENT_TIMESTAMP()::TIMESTAMP,
                CURRENT_TIMESTAMP()::TIMESTAMP,
                batch.se_user_id,
                batch.email_address,
                batch.booking_id,
                batch.unique_browser_id,
                batch.cookie_id,
                batch.session_userid,
                batch.earliest_event_tstamp,
                batch.latest_event_tstamp)
    --When a late arriving event has come in that updates the earliest time we have seen this association
    WHEN MATCHED AND target.earliest_event_tstamp > batch.earliest_event_tstamp
        THEN UPDATE SET
        target.earliest_event_tstamp = batch.earliest_event_tstamp,
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP
    --When another association has come in that updates the latest timestamp we have seen this association
    WHEN MATCHED AND target.latest_event_tstamp < batch.latest_event_tstamp
        THEN UPDATE SET
        target.latest_event_tstamp = batch.latest_event_tstamp,
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP

--check user is inserted without a se_user_id
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_identity_associations
WHERE module_identity_associations.unique_browser_id = 'a2cf6910-3e1a-4c69-904d-5f64e081b6e9';

--rerun identity stitching
MERGE INTO data_vault_mvp.single_customer_view_stg.module_identity_stitching AS target
    USING (

        -- get a distinct list of the unknown identifiers coalesced by importance (identity fragment) that have had a new association.
        -- The identity associations table only inserts new rows if a new combination of identifiers has appeared that was not currently
        -- in the table.
        WITH new_associations AS (
            SELECT DISTINCT
                   COALESCE(unique_browser_id,
                            cookie_id,
                            session_userid) AS client_id
            FROM data_vault_mvp.single_customer_view_stg.module_identity_associations
            WHERE created_at >= TIMESTAMPADD('day', -1, '2022-01-24 03:00:00'::TIMESTAMP)
        ),

             --reprocess all associations for any association that match the coalesced client id
             last_value AS (
                 --for each distinct combination of known identifiers get the last (non null) version of known identifiers
                 --Cian confirmed that we should associate single unknown identities to multiple known identities to the most
                 --the recent association.
                 SELECT DISTINCT

                        LAST_VALUE(se_user_id)
                                   IGNORE NULLS OVER (PARTITION BY COALESCE(unique_browser_id, cookie_id, session_userid)
                                       ORDER BY latest_event_tstamp, earliest_event_tstamp, updated_at)
                                                                                                        AS attributed_se_user_id,
                        LAST_VALUE(email_address)
                                   IGNORE NULLS OVER (PARTITION BY COALESCE(unique_browser_id, cookie_id, session_userid)
                                       ORDER BY latest_event_tstamp, earliest_event_tstamp, updated_at) AS attributed_email_address,

                        LAST_VALUE(booking_id)
                                   IGNORE NULLS OVER (PARTITION BY COALESCE(unique_browser_id, cookie_id, session_userid)
                                       ORDER BY latest_event_tstamp, earliest_event_tstamp, updated_at) AS attributed_booking_id,

                        LAST_VALUE(unique_browser_id)
                                   IGNORE NULLS OVER (PARTITION BY COALESCE(unique_browser_id, cookie_id, session_userid)
                                       ORDER BY latest_event_tstamp, earliest_event_tstamp, updated_at) AS attributed_unique_browser_id,

                        LAST_VALUE(cookie_id)
                                   IGNORE NULLS OVER (PARTITION BY COALESCE(unique_browser_id, cookie_id, session_userid)
                                       ORDER BY latest_event_tstamp, earliest_event_tstamp, updated_at) AS attributed_cookie_id,

                        LAST_VALUE(session_userid)
                                   IGNORE NULLS OVER (PARTITION BY COALESCE(unique_browser_id, cookie_id, session_userid)
                                       ORDER BY latest_event_tstamp, earliest_event_tstamp, updated_at) AS attributed_session_userid

                 FROM data_vault_mvp.single_customer_view_stg.module_identity_associations
                 WHERE COALESCE(unique_browser_id,
                                cookie_id,
                                session_userid) IN
                       (
                           SELECT client_id
                           FROM new_associations
                       )
             )

        SELECT
            --enforce hierarchy of identifiers to associate with the most recent of a certain type
            COALESCE(attributed_se_user_id,
                     attributed_email_address,
                     attributed_booking_id,
                     attributed_unique_browser_id,
                     attributed_cookie_id,
                     attributed_session_userid) AS attributed_user_id,
            CASE
                WHEN attributed_se_user_id IS NOT NULL THEN 'se_user_id'
                WHEN attributed_email_address IS NOT NULL THEN 'email_address'
                WHEN attributed_booking_id IS NOT NULL THEN 'booking_id'
                WHEN attributed_unique_browser_id IS NOT NULL THEN 'unique_browser_id'
                WHEN attributed_cookie_id IS NOT NULL THEN 'cookie_id'
                WHEN attributed_session_userid IS NOT NULL THEN 'session_userid'
                END
                                                AS stitched_identity_type,
            attributed_unique_browser_id        AS unique_browser_id,
            attributed_cookie_id                AS cookie_id,
            attributed_session_userid           AS session_userid

        FROM last_value
    ) AS batch ON COALESCE(batch.unique_browser_id, batch.cookie_id, batch.session_userid) =
                  COALESCE(target.unique_browser_id, target.cookie_id, target.session_userid)
    WHEN NOT MATCHED
        THEN INSERT (
                     schedule_tstamp,
                     run_tstamp,
                     operation_id,
                     created_at,
                     updated_at,
                     attributed_user_id,
                     stitched_identity_type,
                     unique_browser_id,
                     cookie_id,
                     session_userid
        )
        VALUES ('2022-01-24 03:00:00',
                '2022-01-25 03:59:12',
                'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events/02_identity_stitching/02_module_identity_stitching.py__20220124T030000__daily_at_03h00',
                CURRENT_TIMESTAMP()::TIMESTAMP,
                CURRENT_TIMESTAMP()::TIMESTAMP,
                batch.attributed_user_id,
                batch.stitched_identity_type,
                batch.unique_browser_id,
                batch.cookie_id,
                batch.session_userid)
    WHEN MATCHED AND target.attributed_user_id != batch.attributed_user_id
        THEN UPDATE SET
        target.attributed_user_id = batch.attributed_user_id,
        target.stitched_identity_type = batch.stitched_identity_type,
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP
;

/* unique browser id's from travelist events
'7207600d-7404-472c-8e83-d4b660cde926',
'a062d255-b2a6-464c-a096-51e19783e203',
'2b872be9-cc2d-41ed-ae64-24c523064c68',
'b2eb39b6-d619-45f0-81f4-0a8aa2c43729',
'86579270-bb38-45b7-a81a-b1a74af3eae8',
'6c099aea-b43b-4f1e-93b7-de719aeb8b4b',
'f285107a-d760-436d-a826-6d2a5050233b',
'08f79c09-b4da-4449-bbce-c10799046c36',
'3633e0b7-759e-4969-a9df-ae9a5b119222',
'832d72ce-9881-494b-b19e-6f3f76c58b2a',
*/

--check user is inserted without a se_user_id and see how they have now been stitched
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_identity_stitching
WHERE unique_browser_id = 'a2cf6910-3e1a-4c69-904d-5f64e081b6e9';

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_identity_stitching_20220125
WHERE unique_browser_id = 'a2cf6910-3e1a-4c69-904d-5f64e081b6e9';
--attributed user id prior to fix 651125

------------------------------------------------------------------------------------------------------------------------
-- RERUN SESSIONISATION TASKS, TOUCHIFIABLE EVENTS, TIME DIFF MARKER, UTM MARKER AND TOUCHIFICATION
------------------------------------------------------------------------------------------------------------------------

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touchification_20220125 CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
USE WAREHOUSE pipe_4xlarge;
--list of event hashes that are associated to a travelist user
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchifiable_events mte
WHERE mte.unique_browser_id = 'a2cf6910-3e1a-4c69-904d-5f64e081b6e9';

USE WAREHOUSE pipe_4xlarge;
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
WHERE mt.event_hash IN ('0ca5a6c6e923ac0152b07a686891063c4cc92ecb345b852ad4df6c256af0fa45',
                        '2796b10956f5f6be51ccd7b561aea9f31f5dd622015fbccef9701b5bd25b7ea2',
                        'ffc48cd0b48fac06364cb99588dc2d1f5a7764e6a7d50dffc7b43cb96a8ac4c0',
                        'b9f6a677ff28fc5e20beb953469de89f3c5775f42048af85f627db6a526a9654',
                        '324a9c85e314114033445c03e600da5f7f5c9615b0ffcc63719867edc8fb0f65',
                        'ce5ed2f7e5d11462580f2e480eda7773549baade135c1806b7976c05a4fb41b8',
                        '73e7eb1238ded2b1e53c4972b9d0ac04df2dd74aba8fde329f1cda5a6277c152',
                        '2b12103f52133418046304948b3f63aa6ac9a4236aee46c60d2d91aff82fd58d',
                        'e8094bdcbac387e70286ca3239242ba8611638c1469252e961cdca4f02cf6fde',
                        '7dcabd966ced2c103b7d816d118761f55f9fe899111cece5eae5a9b375a476aa'
    );

USE WAREHOUSE pipe_4xlarge;
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_hostname IN (
                              'admin.oferty.travelist.pl',
                              'livetest.oferty.travelist.pl',
                              'oferty.travelist.pl',
                              'partner.travelist.pl',
                              'staging.travelist.pl',
                              'travelist.hu',
                              'travelist.pl',
                              'vision.travelist.pl',
                              'zagranica.travelist.pl')
  AND mtba.stitched_identity_type = 'se_user_id'
;
USE WAREHOUSE pipe_4xlarge;

--update the basic attributes identity information
UPDATE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes target
SET target.stitched_identity_type = batch.stitched_identity_type,
    target.attributed_user_id     = batch.attributed_user_id
FROM (
    SELECT DISTINCT
           mt.touch_id,
           mt.attributed_user_id,
           mt.stitched_identity_type
    FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
) AS batch
WHERE target.touch_id = batch.touch_id;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_id IN ('7dcabd966ced2c103b7d816d118761f55f9fe899111cece5eae5a9b375a476aa',
                        '555731db39068c667cb4ad3be3dc2f7e192a9aa44654d05cb19834ba1f76eee7',
                        '7dcabd966ced2c103b7d816d118761f55f9fe899111cece5eae5a9b375a476aa',
                        'b6b805cec54bbdb77f0b6a32818f48865cb736616701acca47b07444b5ea84ab',
                        'b6b805cec54bbdb77f0b6a32818f48865cb736616701acca47b07444b5ea84ab',
                        '6487157cab0feb3f23fda13cfdd7c018dea15deb0df65417114e29ffedef1dad',
                        '7dcabd966ced2c103b7d816d118761f55f9fe899111cece5eae5a9b375a476aa',
                        'a479fde4935e51d57a355345070c893d844aa8348638db8dcd8fc6f98d2117a6',
                        'fc86da2bf7e2b22bc7177c065859e5836d5a75267ad8d7b97936a88f8bada729',
                        '6487157cab0feb3f23fda13cfdd7c018dea15deb0df65417114e29ffedef1dad'
    );


------------------------------------------------------------------------------------------------------------------------
-- attribution

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20220125 CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

USE WAREHOUSE pipe_4xlarge;

CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution
(
    -- (lineage) metadata for the current job
    schedule_tstamp     TIMESTAMP,
    run_tstamp          TIMESTAMP,
    operation_id        VARCHAR,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,

    touch_id            VARCHAR,
    attributed_touch_id VARCHAR,
    attribution_model   VARCHAR,
    attributed_weight   FLOAT
)
    CLUSTER BY (attribution_model);

MERGE INTO data_vault_mvp.single_customer_view_stg.module_touch_attribution AS target
    USING (
        WITH users_with_new_touches AS (
            --get users who've had a new touch
            SELECT DISTINCT
                   attributed_user_id
            FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
--             WHERE updated_at >= TIMESTAMPADD('day', -1, '2022-01-13 03:00:00'::TIMESTAMP)
        ),
             all_touches_from_users AS (
                 --create a proxy touch id and touch tstamp and nullify it if the touch is mkt channel direct
                 SELECT c.touch_id,
                        b.touch_start_tstamp,
                        c.touch_mkt_channel,
                        c.attributed_user_id,
                        CASE
                            --don't nullify if first touch
                            WHEN LAG(c.touch_mkt_channel)
                                     OVER (PARTITION BY c.attributed_user_id ORDER BY b.touch_start_tstamp) IS NULL
                                THEN c.touch_id
                            --nullify if is a direct channel
                            WHEN c.touch_mkt_channel = 'Direct'
                                THEN NULL
                            ELSE c.touch_id
                            END AS nullify_touch_id,
                        --we will also bring the touch date down so we can compare the date of the attributed
                        --touch to the current touch
                        CASE
                            --don't nullify if first touch
                            WHEN LAG(c.touch_mkt_channel)
                                     OVER (PARTITION BY c.attributed_user_id ORDER BY b.touch_start_tstamp) IS NULL
                                THEN b.touch_start_tstamp
                            --nullify if is a direct channel
                            WHEN c.touch_mkt_channel = 'Direct'
                                THEN NULL
                            ELSE b.touch_start_tstamp
                            END AS nullify_touch_start_tstamp
                 FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                     INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b ON c.touch_id = b.touch_id
                                    -- get all touches from users who have had a new touch
                     INNER JOIN users_with_new_touches uwnt ON c.attributed_user_id = uwnt.attributed_user_id
             ),
             last_value AS (
                 --use proxy touch id and touch tstamp to back fill nulls
                 SELECT touch_id,
                        touch_start_tstamp,
                        touch_mkt_channel,
                        attributed_user_id,
                        LAST_VALUE(nullify_touch_id) IGNORE NULLS OVER
                            (PARTITION BY attributed_user_id ORDER BY touch_start_tstamp
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS persisted_touch_id,
                        LAST_VALUE(nullify_touch_start_tstamp) IGNORE NULLS OVER
                            (PARTITION BY attributed_user_id ORDER BY touch_start_tstamp
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS persisted_touch_start_tstamp
                 FROM all_touches_from_users
             )
             --check that the back fills don't persist longer than 6months
        SELECT touch_id,
               --        touch_start_tstamp,
               --        touch_mkt_channel,
               --        attributed_user_id,
               --        persisted_touch_id,
               --        persisted_touch_start_tstamp,
               CASE
                   WHEN touch_id != persisted_touch_id AND
                       -- if a different non direct touch id exists AND its within 6 months then use it
                        DATEDIFF(DAY, persisted_touch_start_tstamp, touch_start_tstamp) <= 30
                       THEN persisted_touch_id
                   ELSE touch_id END AS attributed_touch_id,
               'last non direct'     AS attribution_model,
               1                     AS attributed_weight
        FROM last_value

    ) AS batch ON target.touch_id = batch.touch_id
        AND target.attributed_touch_id = batch.attributed_touch_id
        AND target.attribution_model = batch.attribution_model
    WHEN NOT MATCHED
        THEN INSERT VALUES ('2022-01-24 03:00:00',
                            CURRENT_TIMESTAMP::TIMESTAMP,
                            'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py__20220113T030000__daily_at_03h00',
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            batch.touch_id,
                            batch.attributed_touch_id,
                            batch.attribution_model,
                            batch.attributed_weight)
    WHEN MATCHED THEN UPDATE SET
        target.schedule_tstamp = '2022-01-13 03:00:00',
        target.run_tstamp = '2022-01-14 05:47:15',
        target.operation_id = 'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py__20220113T030000__daily_at_03h00',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        target.touch_id = batch.touch_id,
        target.attributed_touch_id = batch.attributed_touch_id,
        target.attribution_model = batch.attribution_model,
        target.attributed_weight = batch.attributed_weight
;

MERGE INTO data_vault_mvp.single_customer_view_stg.module_touch_attribution AS target
    USING (
        WITH users_with_new_touches AS (
            --get users who've had a new touch
            SELECT DISTINCT
                   attributed_user_id
            FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
--                     WHERE updated_at >= TIMESTAMPADD('day', -1, '2022-01-13 03:00:00'::TIMESTAMP)
        ),
             all_touches_from_users AS (
                 --create a proxy touch id and touch tstamp and nullify it if the touch is mkt channel is not paid
                 SELECT c.touch_id,
                        b.touch_start_tstamp,
                        c.touch_mkt_channel,
                        c.attributed_user_id,
                        --channels to nullify
                        IFF(c.touch_mkt_channel NOT IN (
                                                        'PPC - Brand',
                                                        'PPC - Non Brand CPA',
                                                        'PPC - Non Brand CPL',
                                                        'PPC - Undefined',
                                                        'Display CPA',
                                                        'Display CPL',
                                                        'Paid Social CPA',
                                                        'Paid Social CPL',
                                                        'Affiliate Program'), TRUE, FALSE) AS is_nullify_channel,
                        CASE
                            --don't nullify if first touch
                            WHEN LAG(c.touch_mkt_channel)
                                     OVER (PARTITION BY c.attributed_user_id ORDER BY b.touch_start_tstamp) IS NULL
                                THEN c.touch_id
                            --nullify if is a not a paid channel
                            WHEN is_nullify_channel THEN NULL
                            ELSE c.touch_id
                            END                                                            AS nullify_touch_id,
                        CASE
                            --don't nullify if first touch
                            WHEN LAG(c.touch_mkt_channel)
                                     OVER (PARTITION BY c.attributed_user_id ORDER BY b.touch_start_tstamp) IS NULL
                                THEN b.touch_start_tstamp
                            --nullify if is a not a paid channel
                            WHEN is_nullify_channel THEN NULL
                            ELSE b.touch_start_tstamp
                            END                                                            AS nullify_touch_start_tstamp
                 FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                     INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b ON c.touch_id = b.touch_id
                                    -- get all touches from users who have had a new touch
                     INNER JOIN users_with_new_touches uwnt ON c.attributed_user_id = uwnt.attributed_user_id
             ),
             last_value AS (
                 --use proxy touch id and touch tstamp to back fill nulls
                 SELECT touch_id,
                        touch_start_tstamp,
                        touch_mkt_channel,
                        attributed_user_id,
                        LAST_VALUE(nullify_touch_id) IGNORE NULLS OVER
                            (PARTITION BY attributed_user_id ORDER BY touch_start_tstamp
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS persisted_touch_id,
                        LAST_VALUE(nullify_touch_start_tstamp) IGNORE NULLS OVER
                            (PARTITION BY attributed_user_id ORDER BY touch_start_tstamp
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS persisted_touch_start_tstamp
                 FROM all_touches_from_users
             )
             --check that the back fills don't persist longer than 30 days
        SELECT touch_id,
               --        touch_start_tstamp,
               --        touch_mkt_channel,
               --        attributed_user_id,
               --        persisted_touch_id,
               --        persisted_touch_start_tstamp,
               CASE
                   WHEN touch_id != persisted_touch_id AND
                       -- if a different paid touch id exists AND its within 30 days then use it
                        DATEDIFF(DAY, persisted_touch_start_tstamp, touch_start_tstamp) <= 30
                       THEN persisted_touch_id
                   ELSE touch_id END AS attributed_touch_id,
               'last paid'           AS attribution_model,
               1                     AS attributed_weight
        FROM last_value
    ) AS batch ON target.touch_id = batch.touch_id
        AND target.attributed_touch_id = batch.attributed_touch_id
        AND target.attribution_model = batch.attribution_model
    WHEN NOT MATCHED
        THEN INSERT VALUES ('2022-01-24 03:00:00',
                            CURRENT_TIMESTAMP::TIMESTAMP,
                            'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py__20220113T030000__daily_at_03h00',
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            batch.touch_id,
                            batch.attributed_touch_id,
                            batch.attribution_model,
                            batch.attributed_weight)
    WHEN MATCHED THEN UPDATE SET
        target.schedule_tstamp = '2022-01-13 03:00:00',
        target.run_tstamp = '2022-01-14 05:47:15',
        target.operation_id = 'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py__20220113T030000__daily_at_03h00',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        target.touch_id = batch.touch_id,
        target.attributed_touch_id = batch.attributed_touch_id,
        target.attribution_model = batch.attribution_model,
        target.attributed_weight = batch.attributed_weight
;

SELECT mta.attribution_model,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
GROUP BY 1;

SELECT mta.attribution_model,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution_20220125 mta
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------
--last page view

SELECT *
FROM data_vault_mvp.dwh.user_last_pageview;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_last_pageview_20220125 CLONE data_vault_mvp.dwh.user_last_pageview;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_last_spv_20220125 CLONE data_vault_mvp.dwh.user_last_spv;

USE WAREHOUSE pipe_4xlarge;
DROP TABLE data_vault_mvp.dwh.user_last_pageview;
DROP TABLE data_vault_mvp.dwh.user_last_spv;

CREATE TABLE IF NOT EXISTS data_vault_mvp.dwh.user_last_pageview
(

    -- (lineage) metadata for the current job
    schedule_tstamp      TIMESTAMP,
    run_tstamp           TIMESTAMP,
    operation_id         VARCHAR,
    created_at           TIMESTAMP,
    updated_at           TIMESTAMP,

    shiro_user_id        INT,
    last_pageview_tstamp TIMESTAMP
)
;

MERGE INTO data_vault_mvp.dwh.user_last_pageview AS target
    USING (
        SELECT t.attributed_user_id::INT AS shiro_user_id,
               MAX(t.event_tstamp)       AS last_pageview_tstamp
        FROM data_vault_mvp.single_customer_view_stg.module_touchification t
            INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON t.event_hash = e.event_hash
        WHERE e.event_name = 'page_view'
          AND t.stitched_identity_type = 'se_user_id'
        GROUP BY 1
    ) AS batch
    ON target.shiro_user_id = batch.shiro_user_id
    WHEN MATCHED AND target.last_pageview_tstamp < batch.last_pageview_tstamp
        THEN UPDATE SET
        target.schedule_tstamp = '2022-01-24 03:00:00',
        target.run_tstamp = CURRENT_TIMESTAMP,
        target.operation_id = 'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/user_attributes/user_last_pageview.py__20220124T030000__daily_at_03h00',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        target.last_pageview_tstamp = batch.last_pageview_tstamp
    WHEN NOT MATCHED
        THEN INSERT VALUES ('2022-01-24 03:00:00',
                            CURRENT_TIMESTAMP,
                            'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/user_attributes/user_last_pageview.py__20220124T030000__daily_at_03h00',
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            batch.shiro_user_id,
                            batch.last_pageview_tstamp);

CREATE TABLE IF NOT EXISTS data_vault_mvp.dwh.user_last_spv
(

    -- (lineage) metadata for the current job
    schedule_tstamp           TIMESTAMP,
    run_tstamp                TIMESTAMP,
    operation_id              VARCHAR,
    created_at                TIMESTAMP,
    updated_at                TIMESTAMP,

    shiro_user_id             INT,
    last_sale_pageview_tstamp TIMESTAMP
)
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_last_spv__step01__model_spvs AS (
    SELECT t.attributed_user_id::INT AS shiro_user_id,
           MAX(s.event_tstamp)       AS last_sale_pageview_tstamp
    FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs s
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification t ON s.touch_id = t.touch_id
    WHERE t.stitched_identity_type = 'se_user_id'
      AND LEFT(s.se_sale_id, 3) IS DISTINCT FROM 'TVL'
    GROUP BY 1
);


MERGE INTO data_vault_mvp.dwh.user_last_spv AS target
    USING data_vault_mvp.dwh.user_last_spv__step01__model_spvs AS batch ON target.shiro_user_id = batch.shiro_user_id
    WHEN MATCHED AND target.last_sale_pageview_tstamp < batch.last_sale_pageview_tstamp
        THEN UPDATE SET
        target.schedule_tstamp = '2022-01-24 03:00:00',
        target.run_tstamp = CURRENT_TIMESTAMP,
        target.operation_id = 'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/user_attributes/user_last_spv.py__20220124T030000__daily_at_03h00',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        target.last_sale_pageview_tstamp = batch.last_sale_pageview_tstamp
    WHEN NOT MATCHED
        THEN INSERT VALUES ('2022-01-24 03:00:00',
                            CURRENT_TIMESTAMP,
                            'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/user_attributes/user_last_spv.py__20220124T030000__daily_at_03h00',
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            batch.shiro_user_id,
                            batch.last_sale_pageview_tstamp);

DROP TABLE data_vault_mvp.dwh.user_last_spv__step01__model_spvs;

------------------------------------------------------------------------------------------------------------------------
-- update user activity table for se affected users

USE WAREHOUSE pipe_xlarge;

SELECT tubi.unique_browser_id
FROM data_vault_mvp.single_customer_view_stg.travelist_unique_browser_ids tubi;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.se_users_misaligned_with_travelist AS
SELECT mis.attributed_user_id::INT AS se_user_id
FROM data_vault_mvp.single_customer_view_stg.module_identity_stitching_20220125 mis
WHERE mis.unique_browser_id IN (
    SELECT DISTINCT tubi.unique_browser_id
    FROM data_vault_mvp.single_customer_view_stg.travelist_unique_browser_ids tubi
)
  AND mis.stitched_identity_type = 'se_user_id';
-- 3086 user ids

--find a user with a lot of activity
SELECT ua.shiro_user_id,
       SUM(ua.app_sessions_90d + web_sessions_90d + emails_90d)
FROM data_vault_mvp.dwh.user_activity ua
WHERE ua.shiro_user_id IN (
    SELECT DISTINCT sumwt.se_user_id
    FROM data_vault_mvp.single_customer_view_stg.se_users_misaligned_with_travelist sumwt
)
GROUP BY 1
ORDER BY 2 DESC;

USE WAREHOUSE pipe_4xlarge;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_activity_20220126 CLONE data_vault_mvp.dwh.user_activity;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_activity_update AS (
    WITH grain AS (
        --create a grain table that is for every user, a date since they we create to current date
        SELECT sc.date_value AS date,
               ua.shiro_user_id
        FROM data_vault_mvp.dwh.user_attributes ua
            LEFT JOIN data_vault_mvp.dwh.se_calendar sc
                      ON ua.signup_tstamp::DATE <= sc.date_value
                          AND sc.date_value >= ua.signup_tstamp::DATE
                          AND sc.date_value <= CURRENT_DATE - 1
                          AND sc.date_value >= '2018-01-01'
        WHERE ua.shiro_user_id IN (
--users that were misaligned with travelist users
            SELECT DISTINCT sumwt.se_user_id
            FROM data_vault_mvp.single_customer_view_stg.se_users_misaligned_with_travelist sumwt
        )
--           AND ua.shiro_user_id = 4626597 -- TODO remove
    )
            ,
         web_sessions AS (
             SELECT g.date,
                    g.shiro_user_id,
                    SUM(CASE WHEN web.touch_start_tstamp >= DATEADD(DAY, -1, g.date) THEN 1 ELSE 0 END)  AS web_sessions_1d,
                    SUM(CASE WHEN web.touch_start_tstamp >= DATEADD(DAY, -7, g.date) THEN 1 ELSE 0 END)  AS web_sessions_7d,
                    SUM(CASE WHEN web.touch_start_tstamp >= DATEADD(DAY, -14, g.date) THEN 1 ELSE 0 END) AS web_sessions_14d,
                    SUM(CASE WHEN web.touch_start_tstamp >= DATEADD(DAY, -30, g.date) THEN 1 ELSE 0 END) AS web_sessions_30d,
                    SUM(CASE WHEN web.touch_start_tstamp IS NOT NULL THEN 1 ELSE 0 END)                  AS web_sessions_90d
             FROM grain g
                 LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes web
                           ON
                                       g.shiro_user_id = TRY_TO_NUMBER(web.attributed_user_id)
                                   AND web.stitched_identity_type = 'se_user_id'
                                   AND web.touch_start_tstamp::DATE < g.date
                                   AND web.touch_start_tstamp >= (g.date - 90)
                                   AND LOWER(se.data.platform_from_touch_experience(web.touch_experience)) IS DISTINCT
                                   FROM 'native app'
             GROUP BY 1, 2
         ),
         app_sessions AS (
             SELECT g.date,
                    g.shiro_user_id,
                    SUM(CASE WHEN app.touch_start_tstamp >= DATEADD(DAY, -1, g.date) THEN 1 ELSE 0 END)  AS app_sessions_1d,
                    SUM(CASE WHEN app.touch_start_tstamp >= DATEADD(DAY, -7, g.date) THEN 1 ELSE 0 END)  AS app_sessions_7d,
                    SUM(CASE WHEN app.touch_start_tstamp >= DATEADD(DAY, -14, g.date) THEN 1 ELSE 0 END) AS app_sessions_14d,
                    SUM(CASE WHEN app.touch_start_tstamp >= DATEADD(DAY, -30, g.date) THEN 1 ELSE 0 END) AS app_sessions_30d,
                    SUM(CASE WHEN app.touch_start_tstamp IS NOT NULL THEN 1 ELSE 0 END)                  AS app_sessions_90d
             FROM grain g
                 LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes app
                           ON
                                       g.shiro_user_id = TRY_TO_NUMBER(app.attributed_user_id)
                                   AND app.stitched_identity_type = 'se_user_id'
                                   AND app.touch_start_tstamp::DATE < g.date
                                   AND app.touch_start_tstamp >= (g.date - 90)
                                   AND LOWER(se.data.platform_from_touch_experience(app.touch_experience)) = 'native app'
             GROUP BY 1, 2
         ),
         emails AS (
             SELECT g.date,
                    g.shiro_user_id,
                    SUM(CASE WHEN emails.date >= DATEADD(DAY, -1, g.date) THEN 1 ELSE 0 END)  AS emails_1d,
                    SUM(CASE WHEN emails.date >= DATEADD(DAY, -7, g.date) THEN 1 ELSE 0 END)  AS emails_7d,
                    SUM(CASE WHEN emails.date >= DATEADD(DAY, -14, g.date) THEN 1 ELSE 0 END) AS emails_14d,
                    SUM(CASE WHEN emails.date >= DATEADD(DAY, -30, g.date) THEN 1 ELSE 0 END) AS emails_30d,
                    SUM(CASE WHEN emails.date IS NOT NULL THEN 1 ELSE 0 END)                  AS emails_90d
             FROM grain g
                 LEFT JOIN data_vault_mvp.dwh.user_emails emails
                           ON
                                       g.shiro_user_id = emails.shiro_user_id
                                   AND emails.date < g.date
                                   AND emails.date >= (g.date - 90)
                                   AND emails.opens > 0
             GROUP BY 1, 2
         ),
         model_data AS (
             SELECT g.date,
                    g.shiro_user_id,
                    COALESCE(ws.web_sessions_1d, 0)   AS web_sessions_1d,
                    COALESCE(ws.web_sessions_7d, 0)   AS web_sessions_7d,
                    COALESCE(ws.web_sessions_14d, 0)  AS web_sessions_14d,
                    COALESCE(ws.web_sessions_30d, 0)  AS web_sessions_30d,
                    COALESCE(ws.web_sessions_90d, 0)  AS web_sessions_90d,
                    COALESCE(aps.app_sessions_1d, 0)  AS app_sessions_1d,
                    COALESCE(aps.app_sessions_7d, 0)  AS app_sessions_7d,
                    COALESCE(aps.app_sessions_14d, 0) AS app_sessions_14d,
                    COALESCE(aps.app_sessions_30d, 0) AS app_sessions_30d,
                    COALESCE(aps.app_sessions_90d, 0) AS app_sessions_90d,
                    COALESCE(e.emails_1d, 0)          AS emails_1d,
                    COALESCE(e.emails_7d, 0)          AS emails_7d,
                    COALESCE(e.emails_14d, 0)         AS emails_14d,
                    COALESCE(e.emails_30d, 0)         AS emails_30d,
                    COALESCE(e.emails_90d, 0)         AS emails_90d
             FROM grain g
                 LEFT JOIN web_sessions ws ON g.shiro_user_id = ws.shiro_user_id AND g.date = ws.date
                 LEFT JOIN app_sessions aps ON g.shiro_user_id = aps.shiro_user_id AND g.date = aps.date
                 LEFT JOIN emails e ON g.shiro_user_id = e.shiro_user_id AND g.date = e.date
         )
         --job will only insert data if user has activity
    SELECT *
    FROM model_data md
    WHERE md.web_sessions_90d + md.app_sessions_90d + md.emails_90d > 0
)
;

SELECT *
FROM data_vault_mvp.dwh.user_activity_update;

DELETE
FROM data_vault_mvp.dwh.user_activity ua
WHERE ua.shiro_user_id IN (
    SELECT DISTINCT sumwt.se_user_id
    FROM data_vault_mvp.single_customer_view_stg.se_users_misaligned_with_travelist sumwt
);

INSERT INTO data_vault_mvp.dwh.user_activity
SELECT CURRENT_TIMESTAMP,
       CURRENT_TIMESTAMP,
       'backfill after travelist activity issue',
       CURRENT_TIMESTAMP,
       CURRENT_TIMESTAMP,
       user_activity_update.date,
       user_activity_update.shiro_user_id,
       user_activity_update.web_sessions_1d,
       user_activity_update.web_sessions_7d,
       user_activity_update.web_sessions_14d,
       user_activity_update.web_sessions_30d,
       user_activity_update.web_sessions_90d,
       user_activity_update.app_sessions_1d,
       user_activity_update.app_sessions_7d,
       user_activity_update.app_sessions_14d,
       user_activity_update.app_sessions_30d,
       user_activity_update.app_sessions_90d,
       user_activity_update.emails_1d,
       user_activity_update.emails_7d,
       user_activity_update.emails_14d,
       user_activity_update.emails_30d,
       user_activity_update.emails_90d
FROM data_vault_mvp.dwh.user_activity_update
;

SELECT COUNT(*)
FROM data_vault_mvp.dwh.user_activity ua;

SELECT ua.shiro_user_id, ua.date
FROM data_vault_mvp.dwh.user_activity ua
GROUP BY 1, 2
HAVING COUNT(*) > 1;

SELECT *
FROM data_vault_mvp.dwh.user_activity_20220125 u
WHERE u.shiro_user_id = 2498087
  AND (u.web_sessions_90d > 0 OR u.app_sessions_90d > 0);

SELECT *
FROM data_vault_mvp.dwh.user_activity u
WHERE u.shiro_user_id = 2498087
  AND (u.web_sessions_90d > 0 OR u.app_sessions_90d > 0);

SELECT ua.date,
       ua.shiro_user_id,
       ua.web_sessions_1d,
       ua.web_sessions_7d,
       ua.web_sessions_14d,
       ua.web_sessions_30d,
       ua.web_sessions_90d,
       ua.app_sessions_1d,
       ua.app_sessions_7d,
       ua.app_sessions_14d,
       ua.app_sessions_30d,
       ua.app_sessions_90d,
       ua.emails_1d,
       ua.emails_7d,
       ua.emails_14d,
       ua.emails_30d,
       ua.emails_90d
FROM data_vault_mvp.dwh.user_activity ua
    EXCEPT
SELECT u.date,
       u.shiro_user_id,
       u.web_sessions_1d,
       u.web_sessions_7d,
       u.web_sessions_14d,
       u.web_sessions_30d,
       u.web_sessions_90d,
       u.app_sessions_1d,
       u.app_sessions_7d,
       u.app_sessions_14d,
       u.app_sessions_30d,
       u.app_sessions_90d,
       u.emails_1d,
       u.emails_7d,
       u.emails_14d,
       u.emails_30d,
       u.emails_90d
FROM data_vault_mvp.dwh.user_activity_20220126 u
------------------------------------------------------------------------------------------------------------------------
--need to repopulate demand model because its incremental
--clone the tables
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.clicks_20220125 CLONE data_vault_mvp.bi.clicks;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.opens_20220125 CLONE data_vault_mvp.bi.opens;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.sends_20220125 CLONE data_vault_mvp.bi.sends;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.event_grain_20220125 CLONE data_vault_mvp.bi.event_grain;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.grain_20220125 CLONE data_vault_mvp.bi.grain;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.monthly_active_users_20220125 CLONE data_vault_mvp.bi.monthly_active_users;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.weekly_active_users_20220125 CLONE data_vault_mvp.bi.weekly_active_users;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.session_grain_20220125 CLONE data_vault_mvp.bi.session_grain;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.sign_ups_20220125 CLONE data_vault_mvp.bi.sign_ups;

--drop prod tables
DROP TABLE data_vault_mvp.bi.clicks;
DROP TABLE data_vault_mvp.bi.opens;
DROP TABLE data_vault_mvp.bi.sends;

DROP TABLE data_vault_mvp.bi.event_grain;
DROP TABLE data_vault_mvp.bi.grain;
DROP TABLE data_vault_mvp.bi.monthly_active_users;
DROP TABLE data_vault_mvp.bi.weekly_active_users;
DROP TABLE data_vault_mvp.bi.session_grain;
DROP TABLE data_vault_mvp.bi.sign_ups;

-- Run backfill for demand model
airflow backfill --start_date '2018-01-01 00:00:00' --end_date '2018-01-02 00:00:00' --reset_dagruns --task_regex '.*' tableau__demand_model__daily_at_04h00


SELECT updated_at::DATE, COUNT(*)
FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa
GROUP BY 1

------------------------------------------------------------------------------------------------------------------------

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_segmentation_20220126 CLONE data_vault_mvp.dwh.user_segmentation;
USE WAREHOUSE pipe_2xlarge;

MERGE INTO data_vault_mvp.dwh.user_segmentation target
    USING (
        SELECT ua.shiro_user_id,
               ua.date,
               ua.web_sessions_1d + ua.app_sessions_1d + ua.emails_1d    AS activity_1d,
               ua.web_sessions_7d + ua.app_sessions_7d + ua.emails_7d    AS activity_7d,
               ua.web_sessions_14d + ua.app_sessions_14d + ua.emails_14d AS activity_14d,
               ua.web_sessions_30d + ua.app_sessions_30d + ua.emails_30d AS activity_30d,
               ua.web_sessions_90d + ua.app_sessions_90d + ua.emails_90d AS activity_90d,
               CASE
                   WHEN activity_1d > 0 THEN 'last_active_1d'
                   WHEN activity_7d > 0 THEN 'last_active_7d'
                   WHEN activity_14d > 0 THEN 'last_active_14d'
                   WHEN activity_30d > 0 THEN 'last_active_30d'
                   WHEN activity_90d > 0 THEN 'last_active_90d'
                   ELSE 'last_active_90d+'
                   END                                                   AS engagement_segment
        FROM data_vault_mvp.dwh.user_activity ua
        WHERE ua.shiro_user_id IN
              (
                  --subset of users that need to be updated
                  SELECT DISTINCT sumwt.se_user_id
                  FROM data_vault_mvp.single_customer_view_stg.se_users_misaligned_with_travelist sumwt
              )
    ) batch
    ON target.shiro_user_id = batch.shiro_user_id
        AND target.date = batch.date
    WHEN MATCHED
        THEN UPDATE SET
        target.engagement_segment = batch.engagement_segment
;

