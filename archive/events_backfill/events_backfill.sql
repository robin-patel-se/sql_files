self_describing_task --include 'dv/dwh_rec/events/00_artificial_transaction_insert/artificial_transaction_insert_se'  --method 'run' --start '2018-01-02 00:00:00' --end '2018-01-02 00:00:00'


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE SCHEMA hygiene_vault_mvp_dev_robin.cms_mongodb;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_vault_mvp.cms_mongodb.booking_summary;
CREATE SCHEMA hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;



DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
LIMIT 10;

USE WAREHOUSE pipe_large;

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE event_tstamp::DATE = '2018-07-01'
  AND useragent = 'data_team_artificial_insemination_transactions';

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE event_tstamp >= '2018-07-01 00:40:00'
  AND event_tstamp <= '2018-07-01 01:00:00'
  AND event_name IN ('page_view', 'screen_view', 'transaction', 'transaction_item');
DATA)
SELECT updated_at,
       event_tstamp,
       event_name,
       event_hash,
       se_user_id,
       cookie_id,
       booking_id,
       page_url,
       useragent
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE event_tstamp >= '2018-07-01 00:00:00'
  AND event_tstamp <= '2018-07-01 06:00:00'
  AND event_name IN ('page_view', 'screen_view', 'transaction', 'transaction_item');


------------------------------------------------------------------------------------------------------------------------
--test single customer view with new events
SELECT *
FROM data_vault_mvp.information_schema.tables
WHERE table_schema = 'SINGLE_CUSTOMER_VIEW_STG';

-- drop and recreate tables
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations CLONE data_vault_mvp.single_customer_view_stg.module_identity_associations;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params CLONE data_vault_mvp.single_customer_view_stg.module_url_params;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker;

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream_bkup CLONE hygiene_vault_mvp_dev_robin.snowplow.event_stream;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;

------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM data_vault_mvp.information_schema.tables
WHERE table_schema = 'DWH';


DROP TABLE IF EXISTS data_vault_mvp_dev_robin.dwh.se_booking;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.dwh.se_sale;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.dwh.tb_booking;
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.dwh.tb_offer;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_offer CLONE data_vault_mvp.dwh.tb_offer;


SELECT updated_at::DATE,
       event_name,
       count(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
GROUP BY 1, 2;

airflow backfill --start_date '2020-03-13 00:00:00' --end_date '2020-03-13 00:00:00' --task_regex '.*' hygiene__snowplow__events__daily
airflow backfill --start_date '2020-03-13 00:00:00' --end_date '2020-03-13 00:00:00' --task_regex '.*' snowplow__backfill_booking_events__daily
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' snowplow__backfill_booking_events__daily
airflow backfill --start_date '2020-03-13 00:00:00' --end_date '2020-03-13 00:00:00' --task_regex '.*' single_customer_view__daily
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' single_customer_view__daily -t 02_identity_stitching.01_module_identity_associations.py



SELECT DISTINCT e.booking_id
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
WHERE ( -- client side transactions
        e.collector_tstamp < '2020-02-28 00:00:00'
        AND e.event_name IN ('transaction_item', 'transaction')
        AND e.ti_orderid IS NOT NULL
    )
   OR ( -- server side transactions
        ( -- SE, we are using booking confirmation page view events due to latency of
            --update events not always able to be fired at time of the session
                e.collector_tstamp >= '2020-02-28 00:00:00'
                AND v_tracker LIKE 'java-%' --SE
                AND
                contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM
                'transaction complete'
            )
        OR
        ( -- TB
                e.collector_tstamp >= '2020-02-28 00:00:00'
                AND v_tracker LIKE 'py-%' --TB
                AND
                unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM
                'booking confirmed'
            )
    );



SELECT e.event_tstamp,
       e.useragent,
       t.booking_id,
       t.touch_id
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
WHERE e.useragent = 'data_team_artificial_insemination_transactions'
LIMIT 30;


SELECT e.event_tstamp,
       e.event_name,
       e.page_url,
       e.useragent,
       e.booking_id,
       e.se_user_id,
       t.touch_id
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
WHERE t.touch_id IN ('3eeec048d0bd0a95d2eab2fa95a6a40ca4695b7cb1d1e94295b51ddf6b83ed16',
                     '302ddc94aec6d809069c735127811ae2d246a904e959d7cd90ce0bcb36b5277c',
                     '5e059e0e26e2e2dbae19fcf15c3f2e1053fb9a4c3fea236549a8d7b9c9427cc9',
                     '62c293273e76e51a8612725cd690eab1748ee3eefd52a2b9565968a9afa1a7ec',
                     '26f9fc247000a66d8edde08915a656cb15bb6e9222b6a774959685358c2bcc26',
                     '73cb4e7bd222ab473527145134aaf8acda3a4500a53e8d1fb1eceab0332913b6',
                     '546bc235235f59b473017b182927c8a14636f7f545b7b50e77c424cff4d2c1a9',
                     'ff222a43743c0a56c86106d06d560c6d4eee38ddd8174456a701fa3cb2720b68',
                     'f2dca4cf647504d9d1accdd9a985479f0cc49706535388b851a4ece528efb0a1',
                     '039d1cc2cc5adb15c211ab8fe8426a03be0eeac8ee70955beeca8ed1aa4ba015',
                     'f05e374c348ec15012c41aac7bf826b888ac1d84f1662823eecf028af9ad2e16',
                     '4b75005bf5d0104eb547969b2118a6edb9b3f652a798763601b5e9bff23d8590',
                     '38a82cd91c32f29d18640a41577478e3fffbeb5e3d9ef3ad29e5a46306da2766',
                     'bfbca0237fa9e7dee51163b1584da01e4017fbcf7794ae103a2b4704fb51bdcd',
                     'f50eb533dfe06a228aae4980d5bde7729980b6829b8b3ef58fa3d99eb0ec334c',
                     'db3dae68490d0c855d679c5faec6b530e2fed26f51adc2ca1144ed71db41a73f',
                     'd41cc234c1d05b68c45598a1d238c87cd6899fffb274293680f74d24a320493c',
                     '5679c0a5f95863a4b94f2f0d4651a4881f85b5a3a798c6fc2aa95a5af41fdcd8',
                     '60c9f2cbbcf075a75ca2133c44537c505e1ced8022a45eb7c50ef568d762a72c',
                     'cfe27a50ed8bfe194126d94b3782db9088f51c8b27a2114fb221a8fddf61f27e',
                     '1a367d87c5be26e43e930aafe483b2a43f68f1ddeef41248536ea122790d527a',
                     '7521e4a6f8989a7f98d566b800aae15653ca3f8927ae813e54323c0965abdb87',
                     '7cd58adcbd0615e32d00eeafabc6698646b823578a3942309ad7f3b728be19b6',
                     '999c4f98acc0538a90b4d2bec55b679391100f684e3f35512ed09bbd6be1078a',
                     'c52d20929e508cdadebc4afca70300831f8759e93d41280ef760be6e7d9df91c',
                     '65db2bf75ac35d2b17ad1174f2dac3807b29c35d57fee1c29cb9533156dc7ff4',
                     'a67b796616470b56b8c50f38884e3f2147d885e10d6d8b7fb2e9f86ef328fc33',
                     '5c357ea363c15485f24eb4d1a0acbdce43adc1bb5dc42e4f384268970e7aed71',
                     'e8c707c8ff8e39525a7843e0172bc9fa02458fa57f633bc2bf56e9cd0162a1a3',
                     '4f736092e5d1078409e0154f15383effc800dec7bcab8f8f4ea759d80d8ec3b9'
    )
ORDER BY e.se_user_id, e.event_tstamp;

SELECT DATE_PART(MONTH, event_tstamp),
       DATE_PART(YEAR, event_tstamp),
       COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions t
GROUP BY 1, 2
ORDER BY 2, 1;


SELECT DATE_PART(MONTH, event_tstamp),
       DATE_PART(YEAR, event_tstamp),
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions t
GROUP BY 1, 2
ORDER BY 2, 1;

self_describing_task --include 'dv/dwh_rec/events/00_artificial_transaction_insert/artificial_transaction_insert_se'  --method 'run' --start '2020-03-13 00:00:00' --end '2020-03-13 00:00:00'

SELECT *
FROM se.data.fact_complete_booking;

SELECT t.updated_at::DATE,
       e.event_name,
       COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

SELECT date_trunc(MONTH, event_tstamp) AS month,
       count(*)

FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
WHERE event_name IN ('page_view', 'screen_view', 'transaction_item', 'transaction',
                     'booking_update_event')                                   -- explicitly define the events we want to touchify
  AND COALESCE(e.unique_browser_id, e.cookie_id, e.session_userid) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
  AND e.is_robot_spider_event = FALSE                                          -- remove extra computation required to resessionise robot events
  AND e.useragent = 'data_team_artificial_insemination_transactions'
GROUP BY 1;


--in event stream?
SELECT e.event_tstamp::DATE,
       count(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
WHERE e.useragent = 'data_team_artificial_insemination_transactions'
GROUP BY 1;
-- yes

--in touchification output?
SELECT date_trunc(MONTH, e.event_tstamp) AS month,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
WHERE e.useragent = 'data_team_artificial_insemination_transactions'
GROUP BY 1;
-- no

--in touchifiable events output?
SELECT date_trunc(MONTH, e.event_tstamp) AS month,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
WHERE e.useragent = 'data_team_artificial_insemination_transactions'
GROUP BY 1; -- no

airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '01_module_identity_associations.py' single_customer_view__daily
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '02_module_identity_stitching.py' single_customer_view__daily

MERGE INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations AS target
    USING (
        SELECT se_user_id,
               email_address,
               booking_id,

               unique_browser_id,
               cookie_id,
               session_userid,

               MIN(event_tstamp) AS earliest_event_tstamp, --needed to handle duplicate event user identifiers matching to secret escapes user identifier
               MAX(event_tstamp) AS latest_event_tstamp
        FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
        WHERE schedule_tstamp >= TIMESTAMPADD('day', -1, '2018-01-02 00:00:00'::TIMESTAMP)
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
        VALUES ('2020-03-13 00:00:00',
                '2020-03-17 14:26:10',
                'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh_rec/events/02_identity_stitching/01_module_identity_associations.py__20200313T000000__daily',
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
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP;

MERGE INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching AS target
    USING (

        -- get a distinct list of the unknown identifiers coalesced by importance (identity fragment) that have had a new association.
        -- The identity associations table only inserts new rows if a new combination of identifiers has appeared that was not currently
        -- in the table.
        WITH new_associations AS (
            SELECT DISTINCT COALESCE(unique_browser_id,
                                     cookie_id,
                                     session_userid) AS client_id
            FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations
            WHERE created_at >= TIMESTAMPADD('day', -1, '2018-01-02 00:00:00'::TIMESTAMP)
        ),

             --reprocess all associations for any association that match the coalesced client id
             last_value AS (
                 --for each distinct combination of known identifiers get the last (non null) version of known identifiers
                 --Cian confirmed that we should associate single unknown identities to multiple known identities to the most
                 --the recent association.
                 SELECT DISTINCT LAST_VALUE(se_user_id)
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

                 FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations
                 WHERE COALESCE(unique_browser_id,
                                cookie_id,
                                session_userid) IN
                       (SELECT client_id FROM new_associations)
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
        VALUES ('2020-03-13 00:00:00',
                '2020-03-17 14:27:50',
                'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh_rec/events/02_identity_stitching/02_module_identity_stitching.py__20200313T000000__daily',
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
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP;

------------------------------------------------------------------------------------------------------------------------
--how many transactions
SELECT count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
WHERE e.useragent = 'data_team_artificial_insemination_transactions';--479054

USE WAREHOUSE pipe_xlarge;

--check how many are orfans
WITH touches_with_art_trans AS (
    SELECT touch_id
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
             INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
    WHERE e.useragent = 'data_team_artificial_insemination_transactions'
)
   , max_events AS (
    SELECT t.touch_id,
           MAX(event_index_within_touch) AS events_in_touch
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
    WHERE t.touch_id IN (SELECT touch_id FROM touches_with_art_trans)
    GROUP BY 1)
SELECT COUNT(*)
FROM max_events
WHERE events_in_touch = 1;
;

-- touch id's for touches with only 1 orfan event
WITH touches_with_art_trans AS (
    SELECT touch_id
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
             INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
    WHERE e.useragent = 'data_team_artificial_insemination_transactions'
)
   , max_events AS (
    SELECT t.touch_id,
           MAX(event_index_within_touch) AS events_in_touch
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
    WHERE t.touch_id IN (SELECT touch_id FROM touches_with_art_trans)
    GROUP BY 1)
SELECT touch_id
FROM max_events
WHERE events_in_touch = 1;


--investigate orfans
SELECT e.event_tstamp,
       e.event_name,
       e.page_url,
       e.useragent,
       e.booking_id,
       e.se_user_id,
       t.touch_id
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash

WHERE t.touch_id IN (WITH touches_with_art_trans AS (
    SELECT touch_id
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
             INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
    WHERE e.useragent = 'data_team_artificial_insemination_transactions'
)
                        , max_events AS (
        SELECT t.touch_id,
               MAX(event_index_within_touch) AS events_in_touch
        FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
        WHERE t.touch_id IN (SELECT touch_id FROM touches_with_art_trans)
        GROUP BY 1)
                     SELECT touch_id
                     FROM max_events
                     WHERE events_in_touch = 1
);

--looking at three users on the 18th jan 2020
SELECT e.event_tstamp,
       e.event_name,
       e.page_url,
       e.useragent,
       e.booking_id,
       e.se_user_id,
       t.touch_id,
       e.*
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
         LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
                   ON t.event_hash = e.event_hash
WHERE e.se_user_id IN ('362154',
                       '362136',
                       '30485007'
    )
  AND e.event_tstamp::DATE = '2020-01-18';

--looking at five users on the 19th nov 2019
SELECT e.event_tstamp,
       e.event_name,
       e.page_url,
       e.useragent,
       e.booking_id,
       e.se_user_id,
       t.touch_id,
       e.*
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
         LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
                   ON t.event_hash = e.event_hash
WHERE e.se_user_id IN ('38076025',
                       '26563866',
                       '66135113',
                       '356920',
                       '67980952',
                       '72037801'
    )
  AND e.event_tstamp::DATE = '2019-11-19';

--looking at four users on the 15th jan 2018
SELECT e.event_tstamp,
       e.event_name,
       e.page_url,
       e.useragent,
       e.booking_id,
       e.se_user_id,
       t.touch_id,
       e.*
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
         LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
                   ON t.event_hash = e.event_hash
WHERE e.se_user_id IN ('40663036',
                       '33991119',
                       '43091729',
                       '48586786'
    )
  AND e.event_tstamp::DATE = '2018-01-15';

--looking at three users on the 6th aug 2018
SELECT e.event_tstamp,
       e.event_name,
       e.page_url,
       e.useragent,
       e.booking_id,
       e.se_user_id,
       t.touch_id,
       e.*
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
         LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
                   ON t.event_hash = e.event_hash
WHERE e.se_user_id IN ('38034736',
                       '27207035',
                       '40695333'
    )
  AND e.event_tstamp::DATE = '2018-08-06';

------------------------------------------------------------------------------------------------------------------------
--investigate orfans transactions how many will be handled by attribution.
SELECT c.touch_mkt_channel,
       lc.touch_mkt_channel,
       count(*)

FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel c
                    ON t.touch_id = c.touch_id
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution a
                    ON t.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel lc
                    ON a.attributed_touch_id = lc.touch_id

WHERE t.touch_id IN (
    WITH touches_with_art_trans AS (
        SELECT touch_id
        FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
                 INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
        WHERE e.useragent = 'data_team_artificial_insemination_transactions'
    )

    SELECT t.touch_id
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
    WHERE t.touch_id IN (SELECT touch_id FROM touches_with_art_trans)
    GROUP BY 1
    HAVING MAX(event_index_within_touch) = 1
)
GROUP BY 1, 2;


SELECT count(*)

FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel c
                    ON t.touch_id = c.touch_id
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution a
                    ON t.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel lc
                    ON a.attributed_touch_id = lc.touch_id

WHERE t.touch_id IN (
    WITH touches_with_art_trans AS (
        SELECT touch_id
        FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
                 INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
        WHERE e.useragent = 'data_team_artificial_insemination_transactions'
    )

    SELECT t.touch_id
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
    WHERE t.touch_id IN (SELECT touch_id FROM touches_with_art_trans)
    GROUP BY 1
    HAVING MAX(event_index_within_touch) = 1
)
  AND c.touch_mkt_channel != lc.touch_mkt_channel;

------------------------------------------------------------------------------------------------------------------------
--with backfill
WITH users AS (
    SELECT t.event_tstamp::DATE                 AS date,
           c.touch_mkt_channel                  AS last_non_direct_channel,
           count(DISTINCT t.attributed_user_id) AS users
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
             INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution a
                        ON t.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
             INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel c
                        ON a.attributed_touch_id = c.touch_id
    WHERE t.event_tstamp >= '2018-01-01'
    GROUP BY 1, 2
),
     bookings AS (
         SELECT t.event_tstamp::DATE AS date,
                c.touch_mkt_channel  AS last_non_direct_channel,
                count(*)             AS bookings
         FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions t
                  INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution a
                             ON t.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
                  INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel c
                             ON a.attributed_touch_id = c.touch_id
         GROUP BY 1, 2
     )
SELECT u.date,
       u.last_non_direct_channel,
       u.users,
       b.bookings,
       b.bookings / u.users AS user_conversion
FROM users u
         LEFT JOIN bookings b ON u.date = b.date AND u.last_non_direct_channel = b.last_non_direct_channel
ORDER BY 1, 2;

------------------------------------------------------------------------------------------------------------------------
--without backfill

WITH users AS (
    SELECT t.event_tstamp::DATE                 AS date,
           c.touch_mkt_channel                  AS last_non_direct_channel,
           count(DISTINCT t.attributed_user_id) AS users
    FROM data_vault_mvp.single_customer_view_stg.module_touchification t
             INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution a
                        ON t.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
             INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                        ON a.attributed_touch_id = c.touch_id
    WHERE t.event_tstamp >= '2018-01-01'
    GROUP BY 1, 2
),
     bookings AS (
         SELECT t.event_tstamp::DATE AS date,
                c.touch_mkt_channel  AS last_non_direct_channel,
                count(*)             AS bookings
         FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions t
                  INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution a
                             ON t.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
                  INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                             ON a.attributed_touch_id = c.touch_id
         GROUP BY 1, 2
     )
SELECT u.date,
       u.last_non_direct_channel,
       u.users,
       b.bookings,
       b.bookings / u.users AS user_conversion
FROM users u
         LEFT JOIN bookings b ON u.date = b.date AND u.last_non_direct_channel = b.last_non_direct_channel
ORDER BY 1, 2;

airflow backfill --start_date '2020-03-13 00:00:00' --end_date '2020-03-13 00:00:00' --dry_run --task_regex '.*' single_customer_view__daily


Dry run OF DAG single_customer_view__daily ON 2020-03-13T00:00:00+00:00
TASK SelfDescribingOperation__dv.dwh.events.single_customer_view.py
TASK SelfDescribingOperation__dv.dwh.events.07_events_of_interest.01_module_touched_spvs.py
TASK SelfDescribingOperation__dv.dwh.events.00_artificial_transaction_insert.artificial_transaction_insert_se.py
TASK wait_for_hygiene__snowplow__events__daily.SelfDescribingOperation__staging.hygiene.snowplow.events.py
TASK wait_for_dwh__transactional__booking__hourly.SelfDescribingOperation__dv.dwh.transactional.booking.py
TASK SelfDescribingOperation__dv.dwh.events.01_url_manipulation.03_module_extracted_params.py
TASK SelfDescribingOperation__dv.dwh.events.01_url_manipulation.01_module_unique_urls.py
TASK SelfDescribingOperation__dv.dwh.events.01_url_manipulation.02_02_module_url_params.py
TASK SelfDescribingOperation__dv.dwh.events.03_touchification.03_touchification.py
TASK SelfDescribingOperation__dv.dwh.events.03_touchification.01_touchifiable_events.py
TASK SelfDescribingOperation__dv.dwh.events.02_identity_stitching.02_module_identity_stitching.py
TASK SelfDescribingOperation__dv.dwh.events.02_identity_stitching.01_module_identity_associations.py
TASK SelfDescribingOperation__dv.dwh.events.03_touchification.02_01_utm_or_referrer_hostname_marker.py
TASK SelfDescribingOperation__dv.dwh.events.01_url_manipulation.02_01_module_url_hostname.py
TASK SelfDescribingOperation__dv.dwh.events.03_touchification.02_02_time_diff_marker.py
TASK SelfDescribingOperation__dv.dwh.events.07_events_of_interest.02_module_touched_transactions.py
TASK SelfDescribingOperation__dv.dwh.events.06_touch_attribution.01_module_touch_attribution.py
TASK SelfDescribingOperation__dv.dwh.events.04_touch_basic_attributes.01_module_touch_basic_attributes.py
TASK SelfDescribingOperation__dv.dwh.events.05_touch_channelling.02_module_touch_marketing_channel.py
TASK SelfDescribingOperation__dv.dwh.events.05_touch_channelling.01_module_touch_utm_referrer.py

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp.snowplow.event_stream__step03__replicate_event_data AS (
    SELECT
        -- (lineage) metadata for the current job
        '2020-03-21 00:00:00'                                                                                                                                                          AS schedule_tstamp,
        '2020-03-22 00:43:08'                                                                                                                                                          AS run_tstamp,
        'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh_rec/events/00_artificial_transaction_insert/artificial_transaction_insert_se.py__20200321T000000__daily' AS operation_id,
        CURRENT_TIMESTAMP()::TIMESTAMP                                                                                                                                                 AS created_at,
        CURRENT_TIMESTAMP()::TIMESTAMP                                                                                                                                                 AS updated_at,
        se.booking_id,
        --TODO: tech debt push these fields to hygiene and snapshot and remove join here.
        trim(bs.record__o['platformName'])                                                                                                                                             AS platform_extract,
        coalesce(se.shiro_user_id,
                 trim(bs.record__o['currentUserId'])::NUMBER)                                                                                                                          AS se_user_id,
        --End tech debt
        se.booking_completed_timestamp                                                                                                                                                 AS derived_tstamp,
        se.booking_completed_timestamp                                                                                                                                                 AS collector_tstamp,
        se.booking_completed_timestamp                                                                                                                                                 AS event_tstamp,
        SHA2(CONCAT(coalesce(se.booking_id, ''), coalesce(se_user_id, 0),
                    coalesce(derived_tstamp, '1970-01-01 00:00:00')))                                                                                                                  AS cookie_id,
        cookie_id                                                                                                                                                                      AS event_hash,
        'transaction'                                                                                                                                                                  AS event_name,
        'transaction'                                                                                                                                                                  AS event,
        FALSE                                                                                                                                                                          AS is_robot_spider_event,
        FALSE                                                                                                                                                                          AS is_internal_ip_address_event,
        FALSE                                                                                                                                                                          AS is_server_side_event,
        CASE
            WHEN platform_extract = 'IOS_APP' THEN 'native app'
            WHEN platform_extract = 'WEB' THEN 'web'
            WHEN platform_extract = 'TABLET_WEB' THEN 'tablet web'
            WHEN platform_extract = 'MOBILE_WEB' THEN 'mobile web'
            WHEN platform_extract = 'MOBILE_WRAP_IOS' THEN 'mobile wrap ios'
            WHEN platform_extract = 'MOBILE_WRAP_ANDROID' THEN 'mobile wrap android'
            WHEN platform_extract = 'ANDROID_APP' THEN 'mobile wrap android'
            WHEN platform_extract = 'IOS_APP_V3' THEN 'native app'
            ELSE 'not specified'
            END                                                                                                                                                                        AS device_platform,
        se.territory                                                                                                                                                                   AS app_id,
        'data_team_artificial_insemination_transactions'                                                                                                                               AS useragent,
        CASE
            WHEN territory = 'US' THEN 'us.secretescapes.com'
            WHEN territory = 'UK' THEN 'www.secretescapes.com'
            WHEN territory = 'SK' THEN 'sk.secretescapes.com'
            WHEN territory = 'SG' THEN 'sg.secretescapes.com'
            WHEN territory = 'SE' THEN 'www.secretescapes.se'
            WHEN territory = 'NO' THEN 'no.secretescapes.com'
            WHEN territory = 'NL' THEN 'nl.secretescapes.com'
            WHEN territory = 'MY' THEN 'my.secretescapes.com'
            WHEN territory = 'IT' THEN 'it.secretescapes.com'
            WHEN territory = 'IE' THEN 'ie.secretescapes.com'
            WHEN territory = 'ID' THEN 'id.secretescapes.com'
            WHEN territory = 'HU' THEN 'hu.secretescapes.com'
            WHEN territory = 'HK' THEN 'hk.secretescapes.com'
            WHEN territory = 'FR' THEN 'www.evasionssecretes.fr'
            WHEN territory = 'ES' THEN 'es.secretescapes.com'
            WHEN territory = 'DK' THEN 'dk.secretescapes.com'
            WHEN territory = 'DE' THEN 'www.secretescapes.de'
            WHEN territory = 'CZ' THEN 'cz.secretescapes.com'
            WHEN territory = 'CH' THEN 'ch.secretescapes.com'
            WHEN territory = 'BE' THEN 'be.secretescapes.com'
            WHEN territory = 'PL' THEN 'travelist.pl'
            WHEN territory = 'SE_TEMP' THEN 'dev.secretescapes.com'
            WHEN territory = 'TB_BE-FR' THEN 'be.secretescapes.com'
            WHEN territory = 'TB-NL' THEN 'nl.secretescapes.com'
            WHEN territory = 'TB-BE_NL' THEN 'be.secretescapes.com'
            WHEN territory = 'TB-BE_FR' THEN 'be.secretescapes.com'
            ELSE 'www.secretescapes.com'
            END                                                                                                                                                                        AS page_url
    FROM hygiene_vault_mvp.snowplow.event_stream__step02__missing_bookings src_batch
             INNER JOIN data_vault_mvp.dwh.se_booking se ON src_batch.booking_id = se.booking_id
             LEFT JOIN hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs ON src_batch.booking_id = bs.booking_id
);

self_describing_task --include 'dv/dwh_rec/events/00_artificial_transaction_insert/artificial_transaction_insert_se'  --method 'run' --start '2020-03-22 00:00:00' --end '2020-03-22 00:00:00'




airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' dwh__transactional__booking__hourly

airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' incoming__cms_mysql__booking__hourly
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking__hourly

airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' incoming__cms_mysql__reservation__hourly
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__reservation__hourly

airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' incoming__cms_mongodb__booking_summary__hourly
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mongodb__booking_summary__hourly

airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' incoming__travelbird_mysql__django_content_type__hourly
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__django_content_type__hourly

airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' incoming__travelbird_mysql__offers_offer__hourly
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offer__hourly

airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' incoming__travelbird_mysql__orders_order__hourly
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__orders_order__hourly

airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' incoming__travelbird_mysql__orders_orderitembase__hourly
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__orders_orderitembase__hourly

airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' incoming__travelbird_mysql__currency_exchangerateupdate__hourly
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__currency_exchangerateupdate__hourly



airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' dwh__transactional__booking__hourly

airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '01_module_identity_associations.py' single_customer_view__daily
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '02_02_module_url_params.py' single_customer_view__daily
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '02_02_module_url_params.py' single_customer_view__daily
airflow backfill --start_date '2018-01-02 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '02_02_module_url_params.py' single_customer_view__daily

USE WAREHOUSE pipe_xlarge;
SELECT CASE
           WHEN stitched_identity_type = 'email_address' THEN
               SHA2(attributed_user_id)
           ELSE attributed_user_id END AS attributed_user_id_hash
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
GROUP BY 1;

SELECT *
FROM data_vault_mvp.information_schema.columns
WHERE column_name = 'ATTRIBUTED_USER_ID'
  AND table_schema = 'SINGLE_CUSTOMER_VIEW_STG';


------------------------------------------------------------------------------------------------------------------------
--going to manually backfill

CREATE OR REPLACE VIEW hygiene_vault_mvp.snowplow.event_stream__step01__get_source_batch
AS
SELECT
    /*
        Only consider bookings which were completed at some point i.e. could be refunded now
        Minimum limit of 2018-01-01 because we don't want old sessions
    */

    booking_id
FROM data_vault_mvp.dwh.se_booking
WHERE updated_at >= TIMESTAMPADD('day', -1, '2018-01-02 00:00:00'::TIMESTAMP)
  AND booking_status IN ('COMPLETE', 'REFUNDED', 'HOLD_BOOKED')
  AND booking_completed_date >= '2018-01-01'
GROUP BY 1
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp.snowplow.event_stream__step02__missing_bookings
AS (


    -- Filter booking ids which do not exist in the event stream


    WITH booking_events AS (
        -- confirmed booking events from the event stream
        SELECT DISTINCT CASE
                            --TB send booking ids with their own internal prefix, so replacing it with TB-
                            WHEN v_tracker LIKE 'py-%' THEN
                                'TB-' || REGEXP_SUBSTR(e.booking_id, '-(.*)', 1, 1, 'e')
                            WHEN e.page_url LIKE '%reservation?id=%' THEN 'A' || e.booking_id
                            ELSE e.booking_id END AS booking_id

        FROM hygiene_vault_mvp.snowplow.event_stream e
        WHERE e.updated_at >= TIMESTAMPADD('day', -1, '2018-01-02 00:00:00'::TIMESTAMP)
          AND (
                ( -- client side transactions
                        e.collector_tstamp < '2020-02-28 00:00:00'
                        AND e.event_name IN ('transaction_item', 'transaction')
                        AND e.ti_orderid IS NOT NULL
                    )
                OR ( -- server side transactions
                        ( -- SE, we are using booking confirmation page view events due to latency of
                            --update events not always able to be fired at time of the session
                                e.collector_tstamp >= '2020-02-28 00:00:00'
                                AND v_tracker LIKE 'java-%' --SE
                                AND
                                contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM
                                'transaction complete'
                            )
                        OR
                        ( -- TB
                                e.collector_tstamp >= '2020-02-28 00:00:00'
                                AND v_tracker LIKE 'py-%' --TB
                                AND
                                unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM
                                'booking confirmed'
                            )
                    )
            )
    )

    SELECT src_batch.booking_id

    FROM hygiene_vault_mvp.snowplow.event_stream__step01__get_source_batch src_batch
             LEFT JOIN booking_events AS be ON src_batch.booking_id = be.booking_id
    WHERE be.booking_id IS NULL
);

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp.snowplow.event_stream__step03__replicate_event_data AS (
    SELECT

        -- (lineage) metadata for the current job
        '2018-01-02 00:00:00'                                                                                                                                                          AS schedule_tstamp,
        '2018-01-03 01:00:24'                                                                                                                                                          AS run_tstamp,
        'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh_rec/events/00_artificial_transaction_insert/artificial_transaction_insert_se.py__20180102T000000__daily' AS operation_id,
        CURRENT_TIMESTAMP()::TIMESTAMP                                                                                                                                                 AS created_at,
        CURRENT_TIMESTAMP()::TIMESTAMP                                                                                                                                                 AS updated_at,

        se.booking_id,

        --TODO: tech debt push these fields to hygiene and snapshot and remove join here.
        trim(bs.record__o['platformName'])                                                                                                                                             AS platform_extract,
        coalesce(se.shiro_user_id,
                 trim(bs.record__o['currentUserId'])::NUMBER)                                                                                                                          AS se_user_id,
        --End tech debt

        se.booking_completed_timestamp                                                                                                                                                 AS derived_tstamp,
        se.booking_completed_timestamp                                                                                                                                                 AS collector_tstamp,
        se.booking_completed_timestamp                                                                                                                                                 AS event_tstamp,
        SHA2(CONCAT(coalesce(se.booking_id, ''), coalesce(se_user_id, 0),
                    coalesce(derived_tstamp, '1970-01-01 00:00:00')))                                                                                                                  AS cookie_id,
        cookie_id                                                                                                                                                                      AS event_hash,
        'transaction'                                                                                                                                                                  AS event_name,
        'transaction'                                                                                                                                                                  AS event,
        FALSE                                                                                                                                                                          AS is_robot_spider_event,
        FALSE                                                                                                                                                                          AS is_internal_ip_address_event,
        FALSE                                                                                                                                                                          AS is_server_side_event,
        CASE
            WHEN platform_extract = 'IOS_APP' THEN 'native app'
            WHEN platform_extract = 'WEB' THEN 'web'
            WHEN platform_extract = 'TABLET_WEB' THEN 'tablet web'
            WHEN platform_extract = 'MOBILE_WEB' THEN 'mobile web'
            WHEN platform_extract = 'MOBILE_WRAP_IOS' THEN 'mobile wrap ios'
            WHEN platform_extract = 'MOBILE_WRAP_ANDROID' THEN 'mobile wrap android'
            WHEN platform_extract = 'ANDROID_APP' THEN 'mobile wrap android'
            WHEN platform_extract = 'IOS_APP_V3' THEN 'native app'
            ELSE 'not specified'
            END                                                                                                                                                                        AS device_platform,
        se.territory                                                                                                                                                                   AS app_id,
        'data_team_artificial_insemination_transactions'                                                                                                                               AS useragent,

        CASE
            WHEN se.territory = 'US' THEN 'us.secretescapes.com'
            WHEN se.territory = 'UK' THEN 'www.secretescapes.com'
            WHEN se.territory = 'SK' THEN 'sk.secretescapes.com'
            WHEN se.territory = 'SG' THEN 'sg.secretescapes.com'
            WHEN se.territory = 'SE' THEN 'www.secretescapes.se'
            WHEN se.territory = 'NO' THEN 'no.secretescapes.com'
            WHEN se.territory = 'NL' THEN 'nl.secretescapes.com'
            WHEN se.territory = 'MY' THEN 'my.secretescapes.com'
            WHEN se.territory = 'IT' THEN 'it.secretescapes.com'
            WHEN se.territory = 'IE' THEN 'ie.secretescapes.com'
            WHEN se.territory = 'ID' THEN 'id.secretescapes.com'
            WHEN se.territory = 'HU' THEN 'hu.secretescapes.com'
            WHEN se.territory = 'HK' THEN 'hk.secretescapes.com'
            WHEN se.territory = 'FR' THEN 'www.evasionssecretes.fr'
            WHEN se.territory = 'ES' THEN 'es.secretescapes.com'
            WHEN se.territory = 'DK' THEN 'dk.secretescapes.com'
            WHEN se.territory = 'DE' THEN 'www.secretescapes.de'
            WHEN se.territory = 'CZ' THEN 'cz.secretescapes.com'
            WHEN se.territory = 'CH' THEN 'ch.secretescapes.com'
            WHEN se.territory = 'BE' THEN 'be.secretescapes.com'
            WHEN se.territory = 'PL' THEN 'travelist.pl'
            WHEN se.territory = 'SE_TEMP' THEN 'dev.secretescapes.com'
            WHEN se.territory = 'TB_BE-FR' THEN 'be.secretescapes.com'
            WHEN se.territory = 'TB-NL' THEN 'nl.secretescapes.com'
            WHEN se.territory = 'TB-BE_NL' THEN 'be.secretescapes.com'
            WHEN se.territory = 'TB-BE_FR' THEN 'be.secretescapes.com'
            ELSE 'www.secretescapes.com'
            END                                                                                                                                                                        AS page_url
    FROM hygiene_vault_mvp.snowplow.event_stream__step02__missing_bookings src_batch
             INNER JOIN data_vault_mvp.dwh.se_booking se ON src_batch.booking_id = se.booking_id
             LEFT JOIN hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs ON src_batch.booking_id = bs.booking_id
);

MERGE INTO hygiene_vault_mvp.snowplow.event_stream AS target
    USING hygiene_vault_mvp.snowplow.event_stream__step03__replicate_event_data AS batch
    ON target.event_hash = batch.event_hash
    WHEN MATCHED
        THEN UPDATE SET
        target.schedule_tstamp = batch.schedule_tstamp,
        target.run_tstamp = batch.run_tstamp,
        target.operation_id = batch.operation_id,
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        target.booking_id = batch.booking_id,
        target.se_user_id = batch.se_user_id,
        target.derived_tstamp = batch.derived_tstamp,
        target.collector_tstamp = batch.collector_tstamp,
        target.event_tstamp = batch.event_tstamp,
        target.cookie_id = batch.cookie_id,
        target.event_hash = batch.event_hash,
        target.event_name = batch.event_name,
        target.event = batch.event,
        target.is_robot_spider_event = batch.is_robot_spider_event,
        target.is_internal_ip_address_event = batch.is_internal_ip_address_event,
        target.is_server_side_event = batch.is_server_side_event,
        target.device_platform = batch.device_platform,
        target.app_id = batch.app_id,
        target.useragent = batch.useragent,
        target.page_url = batch.page_url
    WHEN NOT MATCHED
        THEN INSERT (
                     schedule_tstamp,
                     run_tstamp,
                     operation_id,
                     created_at,
                     updated_at,
                     booking_id,
                     se_user_id,
                     derived_tstamp,
                     collector_tstamp,
                     event_tstamp,
                     cookie_id,
                     event_hash,
                     event_name,
                     event,
                     is_robot_spider_event,
                     is_internal_ip_address_event,
                     is_server_side_event,
                     device_platform,
                     app_id,
                     useragent,
                     page_url
        ) VALUES (batch.schedule_tstamp,
                  batch.run_tstamp,
                  batch.operation_id,
                  CURRENT_TIMESTAMP()::TIMESTAMP,
                  CURRENT_TIMESTAMP()::TIMESTAMP,
                  batch.booking_id,
                  batch.se_user_id,
                  batch.derived_tstamp,
                  batch.collector_tstamp,
                  batch.event_tstamp,
                  batch.cookie_id,
                  batch.event_hash,
                  batch.event_name,
                  batch.event,
                  batch.is_robot_spider_event,
                  batch.is_internal_ip_address_event,
                  batch.is_server_side_event,
                  batch.device_platform,
                  batch.app_id,
                  batch.useragent,
                  batch.page_url);

DROP VIEW hygiene_vault_mvp.snowplow.event_stream__step01__get_source_batch;
DROP TABLE hygiene_vault_mvp.snowplow.event_stream__step02__missing_bookings;
DROP TABLE hygiene_vault_mvp.snowplow.event_stream__step03__replicate_event_data;

--clone of event stream
CREATE OR REPLACE TABLE scratch.robinpatel.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE scratch.robinpatel.module_unique_urls CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;
MERGE INTO data_vault_mvp.single_customer_view_stg.module_unique_urls AS target
    USING (
        WITH list_of_urls AS (
            --combine page urls and referrer urls into single stream of urls
            SELECT page_url               AS url,
                   PARSE_URL(page_url, 1) AS parsed_url

            FROM hygiene_vault_mvp.snowplow.event_stream
            WHERE page_url IS NOT NULL
              AND schedule_tstamp >= TIMESTAMPADD('day', -1, '2018-01-02 00:00:00'::TIMESTAMP)
            UNION ALL

            SELECT page_referrer               AS url,
                   PARSE_URL(page_referrer, 1) AS parsed_url

            FROM hygiene_vault_mvp.snowplow.event_stream
            WHERE page_referrer IS NOT NULL
              AND schedule_tstamp >= TIMESTAMPADD('day', -1, '2018-01-02 00:00:00'::TIMESTAMP)
        )

        SELECT DISTINCT url,
                        parsed_url,
                        parsed_url['error'] IS NULL   AS is_valid_url, --url structure makes url parsing fail
                        parsed_url['query'] != 'null' AS has_query
        FROM list_of_urls
    ) AS batch ON target.url = batch.url
    WHEN NOT MATCHED
        THEN INSERT (
                     schedule_tstamp,
                     run_tstamp,
                     operation_id,
                     created_at,
                     updated_at,
                     url,
                     parsed_url,
                     is_valid_url,
                     has_query
        ) VALUES ('2018-01-02 00:00:00',
                  '2018-01-03 01:11:06',
                  'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh_rec/events/01_url_manipulation/01_module_unique_urls.py__20180102T000000__daily',
                  CURRENT_TIMESTAMP()::TIMESTAMP,
                  CURRENT_TIMESTAMP()::TIMESTAMP,
                  batch.url,
                  batch.parsed_url,
                  batch.is_valid_url,
                  batch.has_query);

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE scratch.robinpatel.module_url_hostname CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;
MERGE INTO data_vault_mvp.single_customer_view_stg.module_url_hostname AS target
    USING (
        WITH extract_hostname AS (
            SELECT url,
                   parsed_url['host']::VARCHAR AS url_hostname
            FROM data_vault_mvp.single_customer_view_stg.module_unique_urls
            WHERE is_valid_url = TRUE
              AND schedule_tstamp >= TIMESTAMPADD('day', -1, '2018-01-02 00:00:00'::TIMESTAMP)
        )

        SELECT DISTINCT url,
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
                                 url_hostname = 'api.secretescapes.com' OR
                                 url_hostname LIKE '%.fs-staging.escapes.tech' OR
                                 url_hostname = 'www.optimizelyedit.com' OR
                                 url_hostname = 'cdn.secretescapes.com' OR
                                 url_hostname = 'secretescapes--c.eu12.visual.force.com' OR
                                 url_hostname = 'secretescapes.my.salesforce.com' OR
                                 url_hostname = 'cms.secretescapes.com' OR
                                 url_hostname = 'escapes.jetsetter.com' OR
                                 url_hostname LIKE '%travelbird.%' OR
                                 url_hostname LIKE '%travelist.pl' OR
                                 url_hostname = 'holidays.pigsback.com' OR
                                 url_hostname = 'www.travista.de' OR
                                 (url_hostname = '%.facebook.%' AND url_hostname LIKE '%/oauth/%') --fb oauth logins
--                                  url_hostname = 'optimizely' -- TODO: expand on optimizely
                                THEN 'internal' -- TODO: expand on internal definitions

                            WHEN url_hostname = 'www.guardianescapes.com' OR
                                 url_hostname = 'www.gilttravel.com' OR
                                 url_hostname = 'www.hand-picked.telegraph.co.uk' OR
                                 url_hostname = 'escapes.radiotimes.com' OR
                                 url_hostname = 'escapes.timeout.com' OR
                                 url_hostname = 'www.independentescapes.com' OR
                                 url_hostname = 'www.confidentialescapes.co.uk' OR
                                 url_hostname = 'www.eveningstandardescapes.com' OR
                                 url_hostname = 'asap.shermanstravel.com' OR
                                 url_hostname = 'www.lateluxury.com' OR
                                 url_hostname = 'secretescapes.urlaubsguru.de'
                                THEN 'whitelabel'

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
                            END AS url_medium
        FROM extract_hostname
        WHERE url_hostname IS NOT NULL
    ) AS batch ON target.url = batch.url
    WHEN NOT MATCHED
        THEN INSERT (schedule_tstamp,
                     run_tstamp,
                     operation_id,
                     created_at,
                     updated_at,
                     url,
                     url_hostname,
                     url_medium
        ) VALUES ('2018-01-02 00:00:00',
                  '2018-01-03 01:41:35',
                  'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh_rec/events/01_url_manipulation/02_01_module_url_hostname.py__20180102T000000__daily',
                  CURRENT_TIMESTAMP()::TIMESTAMP,
                  CURRENT_TIMESTAMP()::TIMESTAMP,
                  batch.url,
                  batch.url_hostname,
                  batch.url_medium)
;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE scratch.robinpatel.module_url_params CLONE data_vault_mvp.single_customer_view_stg.module_url_params;
MERGE INTO data_vault_mvp.single_customer_view_stg.module_url_params AS target
    USING (
        SELECT DISTINCT url,
                        --index necessary to extract latest utm params if duplicates appear in the same url
                        params.index::VARCHAR                                                AS parameter_index,

                        --separate the parameter from the lateral flatten.
                        REGEXP_SUBSTR(params.value::VARCHAR, '(.*)=', 1, 1, 'e')             AS parameter,

                        --separate the parameter value from the lateral flatten
                        NULLIF(REGEXP_SUBSTR(params.value::VARCHAR, '=(.*)', 1, 1, 'e'), '') AS parameter_value

        FROM data_vault_mvp.single_customer_view_stg.module_unique_urls,
             LATERAL FLATTEN(INPUT => SPLIT(parsed_url['query']::VARCHAR, '&'), OUTER => TRUE) params
        WHERE is_valid_url = TRUE
          AND has_query = TRUE
          AND schedule_tstamp >= TIMESTAMPADD('day', -1, '2018-01-02 00:00:00'::TIMESTAMP)
    ) AS batch ON target.url = batch.url
    WHEN NOT MATCHED
        THEN INSERT (schedule_tstamp,
                     run_tstamp,
                     operation_id,
                     created_at,
                     updated_at,
                     url,
                     parameter_index,
                     parameter,
                     parameter_value
        ) VALUES ('2018-01-02 00:00:00',
                  '2018-01-03 01:41:35',
                  'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh_rec/events/01_url_manipulation/02_02_module_url_params.py__20180102T000000__daily',
                  CURRENT_TIMESTAMP()::TIMESTAMP,
                  CURRENT_TIMESTAMP()::TIMESTAMP,
                  batch.url,
                  batch.parameter_index,
                  batch.parameter,
                  batch.parameter_value);

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE scratch.robinpatel.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
MERGE INTO data_vault_mvp.single_customer_view_stg.module_extracted_params AS target
    USING (
        WITH pivot AS (
            -- pivot and harmonise parameters into columns
            SELECT url,
                   parameter_index,
                   CASE
                       WHEN parameter = 'utm_campaign'
                           THEN parameter_value END AS utm_campaign,
                   CASE
                       WHEN parameter = 'utm_medium'
                           THEN parameter_value END AS utm_medium,
                   CASE
                       WHEN parameter = 'utm_source'
                           THEN parameter_value END AS utm_source,
                   CASE
                       WHEN parameter = 'utm_term'
                           THEN parameter_value END AS utm_term,
                   CASE
                       WHEN parameter = 'utm_content'
                           THEN parameter_value END AS utm_content,
                   CASE
                       WHEN parameter IN ('gclid', 'msclkid', 'dclid', 'clickid', 'fbclid')
                           THEN parameter_value END AS click_id,
                   CASE
                       WHEN parameter = 'saff'
                           THEN parameter_value END AS sub_affiliate_name,
                   CASE
                       WHEN parameter = 'fromApp'
                           THEN parameter_value END AS from_app,
                   CASE
                       WHEN parameter = 'Snowplow'
                           THEN parameter_value END AS snowplow_id,
                   CASE
                       WHEN parameter = 'affiliate'
                           THEN parameter_value END AS affiliate,
                   CASE
                       WHEN parameter = 'awcampaignid'
                           THEN parameter_value END AS awcampaignid,
                   CASE
                       WHEN parameter = 'awadgroupid'
                           THEN parameter_value END AS awadgroupid,
                   CASE
                       WHEN parameter = 'accountVerified'
                           THEN parameter_value END AS account_verified
            FROM data_vault_mvp.single_customer_view_stg.module_url_params
            WHERE schedule_tstamp >= TIMESTAMPADD('day', -1, '2018-01-02 00:00:00'::TIMESTAMP)
        )
             -- create a distinct list and select the last versions of the utm params in any query
             -- (found cases where there are duplicates)

        SELECT DISTINCT url,
                        LAST_VALUE(utm_campaign)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS utm_campaign,
                        LAST_VALUE(utm_medium)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS utm_medium,
                        LAST_VALUE(utm_source)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS utm_source,
                        LAST_VALUE(utm_term)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS utm_term,
                        LAST_VALUE(utm_content)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS utm_content,
                        LAST_VALUE(click_id)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS click_id,
                        LAST_VALUE(sub_affiliate_name)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS sub_affiliate_name,
                        LAST_VALUE(from_app)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS from_app,
                        LAST_VALUE(snowplow_id)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS snowplow_id,
                        LAST_VALUE(affiliate)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS affiliate,
                        LAST_VALUE(awcampaignid)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS awcampaignid,
                        LAST_VALUE(awadgroupid)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS awadgroupid,
                        LAST_VALUE(account_verified)
                                   IGNORE NULLS OVER (PARTITION BY url ORDER BY parameter_index) AS account_verified
        FROM pivot
    ) AS batch ON target.url = batch.url
    WHEN NOT MATCHED
        THEN INSERT (schedule_tstamp,
                     run_tstamp,
                     operation_id,
                     created_at,
                     updated_at,
                     url,
                     utm_campaign,
                     utm_medium,
                     utm_source,
                     utm_term,
                     utm_content,
                     click_id,
                     sub_affiliate_name,
                     from_app,
                     snowplow_id,
                     affiliate,
                     awcampaignid,
                     awadgroupid,
                     account_verified
        ) VALUES ('2018-01-02 00:00:00',
                  '2018-01-03 01:59:39',
                  'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh_rec/events/01_url_manipulation/03_module_extracted_params.py__20180102T000000__daily',
                  CURRENT_TIMESTAMP()::TIMESTAMP,
                  CURRENT_TIMESTAMP()::TIMESTAMP,
                  batch.url,
                  batch.utm_campaign,
                  batch.utm_medium,
                  batch.utm_source,
                  batch.utm_term,
                  batch.utm_content,
                  batch.click_id,
                  batch.sub_affiliate_name,
                  batch.from_app,
                  batch.snowplow_id,
                  batch.affiliate,
                  batch.awcampaignid,
                  batch.awadgroupid,
                  batch.account_verified);

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE scratch.robinpatel.module_identity_associations CLONE data_vault_mvp.single_customer_view_stg.module_identity_associations;
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
        WHERE schedule_tstamp >= TIMESTAMPADD('day', -1, '2018-01-02 00:00:00'::TIMESTAMP)
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
        VALUES ('2018-01-02 00:00:00',
                '2018-01-03 01:11:05',
                'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh_rec/events/02_identity_stitching/01_module_identity_associations.py__20180102T000000__daily',
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
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP;


------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE scratch.robinpatel.module_identity_stitching CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;
MERGE INTO data_vault_mvp.single_customer_view_stg.module_identity_stitching AS target
    USING (

        -- get a distinct list of the unknown identifiers coalesced by importance (identity fragment) that have had a new association.
        -- The identity associations table only inserts new rows if a new combination of identifiers has appeared that was not currently
        -- in the table.
        WITH new_associations AS (
            SELECT DISTINCT COALESCE(unique_browser_id,
                                     cookie_id,
                                     session_userid) AS client_id
            FROM data_vault_mvp.single_customer_view_stg.module_identity_associations
            WHERE created_at >= TIMESTAMPADD('day', -1, '2018-01-02 00:00:00'::TIMESTAMP)
        ),

             --reprocess all associations for any association that match the coalesced client id
             last_value AS (
                 --for each distinct combination of known identifiers get the last (non null) version of known identifiers
                 --Cian confirmed that we should associate single unknown identities to multiple known identities to the most
                 --the recent association.
                 SELECT DISTINCT LAST_VALUE(se_user_id)
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
                       (SELECT client_id FROM new_associations)
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
        VALUES ('2018-01-02 00:00:00',
                '2018-01-03 01:12:36',
                'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh_rec/events/02_identity_stitching/02_module_identity_stitching.py__20100102T000000__daily',
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

------------------------------------------------------------------------------------------------------------------------

SELECT updated_at, count(*)
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE useragent = 'data_team_artificial_insemination_transactions'
GROUP BY 1;

SELECT event_tstamp::DATE AS date, COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions
GROUP BY 1
ORDER BY 1;

SELECT event_tstamp::DATE AS date, COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
GROUP BY 1
ORDER BY 1;

SELECT updated_at, count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchifiable_events
GROUP BY 1;