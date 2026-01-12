CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
USE WAREHOUSE pipe_xlarge;


SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.useragent = 'data_team_artificial_insemination_transactions';

SELECT
    es.device_platform,
    COUNT(*)
FROM collab.data.event_stream_two_weeks_old es
WHERE es.useragent = 'data_team_artificial_insemination_transactions'
GROUP BY 1;

SELECT
    es.device_platform,
    COUNT(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.useragent = 'data_team_artificial_insemination_transactions'
GROUP BY 1;


SELECT
    COUNT(*)
FROM collab.data.event_stream_two_weeks_old es
WHERE es.useragent = 'data_team_artificial_insemination_transactions';

SELECT
    COUNT(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.useragent = 'data_team_artificial_insemination_transactions';



USE WAREHOUSE pipe_xlarge;
SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.booking_id = 'A1355579';


SELECT
    touch_experience,
    COUNT(*)
FROM collab.data.module_touch_basic_attributes mtba
GROUP BY 1;


SELECT
    touch_experience,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
GROUP BY 1;


SELECT
    (( -- client side transactions
                 es.collector_tstamp < '2020-02-28 00:00:00'
             AND es.event_name IN ('transaction_item', 'transaction')
             AND es.ti_orderid IS NOT NULL
         )
        OR
     ( -- server side transactions
             ( -- SE, we are using booking confirmation page view events due to latency of
                 --update events not always able to be fired at time of the session
                         es.collector_tstamp >= '2020-02-28 00:00:00'
                     AND es.event_name = 'page_view'
                     AND es.v_tracker LIKE 'java-%' --SE
                     AND
                         es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM 'transaction complete'
                 )
             OR
             ( -- TB
                         es.collector_tstamp >= '2020-02-28 00:00:00'
                     AND es.event_name = 'booking_update_event'
                     AND es.v_tracker LIKE 'py-%' --TB
                     AND
                         es.unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM 'booking confirmed'
                 )
         )
        OR
     ( -- transaction events for transactions that weren't tracked.
         es.useragent = 'data_team_artificial_insemination_transactions'
         )) AS booking_transaction,
    *
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mt.event_hash = es.event_hash AND es.event_tstamp::DATE = '2020-06-25'
WHERE mt.event_tstamp::DATE = '2020-06-25'
  AND mt.event_hash IN (
                        'c29be1f3ba8c0f89e48af28cd1a590bff2dce75654ed8d7d9df8ac305b6b7a46',
                        '978ce65f65cf2a1c1a809d9c7a5e40507489cb4e39396b82c066eb1e38297704',
                        '5f59b6676e5db51b0984277e4013097832bbe8850075760ea32696f5bc8cdf28',
                        '038058daf800c783452335265ccc28513db994746e9aa8371b366b7066fd769e'
    );

USE WAREHOUSE pipe_xlarge;


-- theory is that some how in the update of the event stream these events werent updated to the correct device platform, or perhaps when donald did a fix to branch events

-- updating the device platform will rerun single customer view sessionisation to see if it fixes the issue
UPDATE hygiene_vault_mvp_dev_robin.snowplow.event_stream target
SET target.device_platform = batch.device_platform
FROM data_vault_mvp.dwh.fact_booking batch
WHERE target.booking_id = batch.booking_id
  AND target.useragent = 'data_team_artificial_insemination_transactions';


SELECT
    es.device_platform,
    COUNT(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.useragent = 'data_team_artificial_insemination_transactions'
GROUP BY 1;

SHOW TABLES IN SCHEMA data_vault_mvp.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations CLONE data_vault_mvp.single_customer_view_stg.module_identity_associations;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params CLONE data_vault_mvp.single_customer_view_stg.module_url_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;


-- re-run sessionisation modules
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/03_touchification/01_touchifiable_events.py'  --method 'run' --start '2022-09-13 00:00:00' --end '2022-09-13 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/03_touchification/02_01_utm_or_referrer_hostname_marker.py'  --method 'run' --start '2022-09-13 00:00:00' --end '2022-09-13 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/03_touchification/02_02_time_diff_marker.py'  --method 'run' --start '2022-09-13 00:00:00' --end '2022-09-13 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/03_touchification/03_touchification.py'  --method 'run' --start '2022-09-13 00:00:00' --end '2022-09-13 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2022-09-13 00:00:00' --end '2022-09-13 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/07_events_of_interest/02_module_touched_transactions.py'  --method 'run' --start '2022-09-13 00:00:00' --end '2022-09-13 00:00:00'



-- prod sessions by touch experience
SELECT
    touch_experience,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
GROUP BY 1;

-- dev sessions by touch experience
SELECT
    touch_experience,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
GROUP BY 1;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.updated_at >= '2022-09-14 11:00:00.000000000'
;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.useragent = 'data_team_artificial_insemination_transactions'
--   AND mtba.touch_event_count = 1
  AND mtba.touch_experience = 'not specified';


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_experience = 'not specified';



SELECT
    mtba.touch_id,
    fb.device_platform
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
    INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions mtt ON mtba.touch_id = mtt.touch_id
    INNER JOIN data_vault_mvp.dwh.fact_booking fb ON mtt.booking_id = fb.booking_id
WHERE mtt.event_subcategory = 'backfill_booking'
;


-- run update script on sessions that consist of a single artificial insemination event, as these won't be resessionised
UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS target
SET target.touch_experience = batch.device_platform
FROM (
    SELECT
        mtba.touch_id,
        fb.device_platform
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
        INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions mtt ON mtba.touch_id = mtt.touch_id
        INNER JOIN data_vault_mvp.dwh.fact_booking fb ON mtt.booking_id = fb.booking_id
    WHERE mtt.event_subcategory = 'backfill_booking'
) AS batch
WHERE target.touch_id = batch.touch_id
  AND target.useragent = 'data_team_artificial_insemination_transactions'
  AND target.touch_experience = 'not specified';


-- recheck dev sessions by touch experience
SELECT
    touch_experience,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
GROUP BY 1;

-- pre change sessions by touch experience
SELECT
    touch_experience,
    COUNT(*)
FROM collab.data.module_touch_basic_attributes mtba
GROUP BY 1;


-- recheck dev sessions by touch experience by month
SELECT
    touch_experience,
    DATE_TRUNC(MONTH, mtba.touch_start_tstamp) AS month,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
GROUP BY 1, 2;

-- pre change sessions by touch experience by month
SELECT
    touch_experience,
    DATE_TRUNC(MONTH, mtba.touch_start_tstamp) AS month,
    COUNT(*)
FROM collab.data.module_touch_basic_attributes mtba
GROUP BY 1, 2;

--found canibalisation of sessions from from mobile web into other experiences. Changing approach to refill entire scv

------------------------------------------------------------------------------------------------------------------------

-- drop all scv tables
-- drop hygiene

-- rerun hygiene  -- started at 3.18 UTC 14th September 2022

-- rerun scv manually
-- comment out code in basic attributes that will append to anomalous
-- clone anomalous from prod
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker;

DROP SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg;
CREATE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg;
DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;

airflow dags backfill --start-date '2018-01-01 00:00:00' --end-date '2018-01-02 00:00:00' hygiene__snowplow__event_stream__hourly




-- need to rerun event stream hygiene from 3pm UTC to current time

SELECT DISTINCT
    device_platform
FROM data_vault_mvp.dwh.fact_booking fb;

-- adjust 4xl timeout to 2h
-- ALTER WAREHOUSE pipe_4xlarge SET STATEMENT_TIMEOUT_IN_SECONDS=7200;

/*DAG ID                                Task ID                                                                                                                                     Run ID                                 Try number
------------------------------------  ------------------------------------------------------------------------------------------------------------------------------------------  -----------------------------------  ------------
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.00_artificial_transaction_insert.artificial_transaction_insert.py                                    backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.01_url_manipulation.01_module_unique_urls.py                                                         backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.01_url_manipulation.02_01_module_url_hostname.py                                                     backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.01_url_manipulation.02_02_module_url_params.py                                                       backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.01_url_manipulation.03_module_extracted_params.py                                                    backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.02_identity_stitching.01_module_identity_associations.py                                             backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.02_identity_stitching.02_module_identity_stitching.py                                                backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.03_touchification.01_touchifiable_events.py                                                          backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.03_touchification.02_01_utm_or_referrer_hostname_marker.py                                           backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.03_touchification.02_02_time_diff_marker.py                                                          backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.03_touchification.03_touchification.py                                                               backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.04_touch_basic_attributes.01_module_touch_basic_attributes.py                                        backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.05_touch_channelling.01_module_touch_utm_referrer.py                                                 backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.05_touch_channelling.02_module_touch_marketing_channel.py                                            backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.06_touch_attribution.01_module_touch_attribution.py                                                  backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.07_events_of_interest.01_module_touched_spvs.py                                                      backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.07_events_of_interest.02_module_touched_transactions.py                                              backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.07_events_of_interest.03_module_touched_searches.py                                                  backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.07_events_of_interest.04_module_touched_app_installs.py                                              backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  SelfDescribingOperation__dv.dwh.events.single_customer_view.py                                                                              backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  wait_for_dwh__transactional__fact_booking__daily_at_03h00.SelfDescribingOperation__dv.dwh.transactional.fact_booking.py                     backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  wait_for_hygiene__snowplow__event_stream__hourly.SelfDescribingOperation__staging.hygiene.snowplow.event_stream.py                          backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  wait_for_hygiene_snapshots__cms_mysql__territory__daily_at_01h00.SelfDescribingOperation__staging.hygiene_snapshots.cms_mysql.territory.py  backfill__2018-01-01T03:00:00+00:00             1
single_customer_view__daily_at_03h00  wait_for_incoming__cms_mysql__affiliate__daily_at_00h30.LatestRecordsOperation__incoming__cms_mysql__affiliate                              backfill__2018-01-01T03:00:00+00:00             1
usage: airflow [-h] GROUP_OR_COMMAND ...*/

-- clone prod tables so up to date in prod
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.affiliate CLONE latest_vault.cms_mysql.affiliate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.territory;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;

airflow dags backfill --start-date '2018-01-01 00:00:00' --end-date '2018-01-02 00:00:00' --task-regex '.*artificial_transaction_insert.py' single_customer_view__daily_at_03h00

SELECT
    es.device_platform,
    COUNT(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
GROUP BY 1;

SELECT
    es.device_platform,
    COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es
GROUP BY 1;


airflow dags backfill --start-date '2018-01-01 00:00:00' --end-date '2018-01-02 00:00:00' --task-regex '03_touchification.py' single_customer_view__daily_at_03h00

--dev
SELECT
    mis.stitched_identity_type,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching mis
GROUP BY 1;

--prod
SELECT
    mis.stitched_identity_type,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
GROUP BY 1;

SELECT
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls muu;

SELECT
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_unique_urls muu;

--dev
SELECT
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events mte;
--3,513,654,402
--prod
SELECT
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchifiable_events mte;
--3,505,947,096

-- dev
SELECT
    COUNT(DISTINCT touch_id)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt;
--771,783,477

-- prod
SELECT
    COUNT(DISTINCT touch_id)
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt;
--772,190,226

--dev
SELECT
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt;
--3,513,654,402

--prod
SELECT
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt;
--3,505,947,096

--dev
SELECT
    mt.stitched_identity_type,
    COUNT(DISTINCT touch_id)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt
GROUP BY 1;

--prod
SELECT
    mt.stitched_identity_type,
    COUNT(DISTINCT touch_id)
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
GROUP BY 1;


airflow dags backfill --start-date '2018-01-01 00:00:00' --end-date '2018-01-02 00:00:00' --task-regex '01_module_touch_basic_attributes.py' single_customer_view__daily_at_03h00

-- dev
SELECT
    mts.event_category,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs mts
GROUP BY 1;

-- prod
SELECT
    mts.event_category,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
GROUP BY 1;

-- dev spvs by month
SELECT
    DATE_TRUNC(MONTH, event_tstamp) AS month,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs mts
GROUP BY 1
;

-- prod spvs by month
SELECT
    DATE_TRUNC(MONTH, event_tstamp) AS month,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
GROUP BY 1
;


-- dev spvs by month by event category
SELECT
    DATE_TRUNC(MONTH, event_tstamp) AS month,
    mts.event_category,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs mts
GROUP BY 1, 2
;

-- prod spvs by month by event category
SELECT
    DATE_TRUNC(MONTH, event_tstamp) AS month,
    mts.event_category,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
GROUP BY 1, 2
;

--look for spv event hashes that don't exist in dev but do in prod
SELECT
    event_hash
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
WHERE DATE_TRUNC(MONTH, event_tstamp) = '2021-08-01'
EXCEPT
SELECT
    event_hash
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs mts
WHERE DATE_TRUNC(MONTH, event_tstamp) = '2021-08-01';

USE WAREHOUSE pipe_xlarge;

--look at the information for this event hash and see if there's a reason its not in touched spvs
SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE DATE_TRUNC(MONTH, event_tstamp) = '2021-08-01'
  AND es.event_hash = '6de9a80909c25a60bed6ed9bfe0b9d23166f9d609f7122d44406ea59c66ed7e1'
;


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE DATE_TRUNC(MONTH, event_tstamp) = '2021-08-01'
  AND es.event_hash = '6de9a80909c25a60bed6ed9bfe0b9d23166f9d609f7122d44406ea59c66ed7e1'
;

SELECT *
FROM snowplow.atomic.events e
WHERE e.event_id = '80b7f9bd-3f54-445a-be77-5a0343efc623'
  AND e.etl_tstamp = '2021-08-09 18:53:25.946000000';

--found as a cause of janky updating of android the se sale id is not always populated where we expect for android
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream_20220914 CLONE hygiene_vault_mvp_dev_robin.snowplow.event_stream;
USE WAREHOUSE pipe_4xlarge;

UPDATE hygiene_vault_mvp_dev_robin.snowplow.event_stream target
SET target.se_sale_id = SPLIT_PART(target.contexts_com_secretescapes_screen_context_1[0]['screen_id']::VARCHAR, ' page ', -1)
WHERE target.se_sale_id IS NULL
  AND target.contexts_com_secretescapes_screen_context_1 IS NOT NULL
;
531,270,416

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.collector_tstamp <= '2020-02-28'
  AND es.event_name = 'screen_view';


USE WAREHOUSE pipe_xlarge;



-- adjust 4xl timeout to 1h
-- ALTER WAREHOUSE pipe_4xlarge SET STATEMENT_TIMEOUT_IN_SECONDS=3600;
-- adjust 4xl warehouse back down to 4xl

BEGIN TRANSACTION;
UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
SET touch_hostname_territory = 'ANOMALOUS'
WHERE attributed_user_id IN (
    SELECT DISTINCT attributed_user_id FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker
);
COMMIT;

airflow dags backfill --start-date '2018-01-01 00:00:00' --end-date '2018-01-02 00:00:00' --task-regex 'single_customer_view.py' single_customer_view__daily_at_03h00

SELECT
    mtba.touch_hostname_territory,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
GROUP BY 1;

-- dev
SELECT
    mtba.touch_experience,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
GROUP BY 1;

-- prod
SELECT
    mtba.touch_experience,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
GROUP BY 1;

--dev
SELECT
    mtba.touch_experience,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions mtt
    INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba ON mtt.touch_id = mtba.touch_id
GROUP BY 1;

--prod
SELECT
    mtba.touch_experience,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions mtt
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba ON mtt.touch_id = mtba.touch_id
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--test with ben query
--dev
WITH sess_bookings AS (
--aggregate bookings up to session, because sessions _can_ have multiple bookings
    SELECT
        stt.touch_id,
        COUNT(*)                          AS bookings,
        SUM(fcb.margin_gross_of_toms_gbp) AS margin
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions stt
        LEFT JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
    WHERE stt.event_tstamp::DATE = '2021-01-01'
    GROUP BY 1
)
   , sess_spvs AS (
    SELECT
        s.touch_id,
        COUNT(*)                                                          AS spvs,
        COUNT(DISTINCT s.se_sale_id)                                      AS unique_spvs,
        SUM(CASE WHEN ds.product_configuration = 'Hotel' THEN 1 END)      AS ho_spvs,
        SUM(CASE WHEN ds.product_configuration = 'Hotel Plus' THEN 1 END) AS hp_spvs,
        SUM(CASE WHEN ds.product_configuration = 'Package' THEN 1 END)    AS p_spvs,
        SUM(CASE WHEN ds.product_configuration = '3PP' THEN 1 END)        AS "3pp_spvs"
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs s
        LEFT JOIN se.data.dim_sale ds ON s.se_sale_id = ds.se_sale_id
    WHERE s.event_tstamp::DATE = '2021-02-01'
    GROUP BY 1
)
SELECT
    stba.touch_start_tstamp::DATE                            AS day,
    stmc.touch_mkt_channel,
    stba.touch_experience,
    stba.touch_hostname_territory,
    stmc.touch_affiliate_territory,
    COUNT(DISTINCT stba.touch_id)                            AS sessions,
    COUNT(DISTINCT stba.attributed_user_id)                  AS users,
    COUNT(DISTINCT CASE
                       WHEN stba.stitched_identity_type = 'se_user_id'
                           THEN stba.attributed_user_id END) AS logged_in_users,
    COALESCE(SUM(b.bookings), 0)                             AS bookings,
    COALESCE(SUM(b.margin), 0)                               AS margin,
    COALESCE(SUM(s.spvs), 0)                                 AS spvs,
    COALESCE(SUM(s.unique_spvs), 0)                          AS unique_spvs,
    COALESCE(SUM(s.ho_spvs), 0)                              AS ho_spvs,
    COALESCE(SUM(s.hp_spvs), 0)                              AS hp_spvs,
    COALESCE(SUM(s.p_spvs), 0)                               AS p_spvs,
    COALESCE(SUM(s."3pp_spvs"), 0)                           AS "3pp_spvs"
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes stba
    INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
    LEFT JOIN  sess_bookings b ON stba.touch_id = b.touch_id
    LEFT JOIN  sess_spvs s ON stba.touch_id = s.touch_id
WHERE stba.touch_start_tstamp::DATE = '2021-01-01'
GROUP BY 1, 2, 3, 4, 5;

--prod
WITH sess_bookings AS (
--aggregate bookings up to session, because sessions _can_ have multiple bookings
    SELECT
        stt.touch_id,
        COUNT(*)                          AS bookings,
        SUM(fcb.margin_gross_of_toms_gbp) AS margin
    FROM se.data.scv_touched_transactions stt
        LEFT JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
    WHERE stt.event_tstamp::DATE = '2021-01-01'
    GROUP BY 1
)
   , sess_spvs AS (
    SELECT
        s.touch_id,
        COUNT(*)                                                          AS spvs,
        COUNT(DISTINCT s.se_sale_id)                                      AS unique_spvs,
        SUM(CASE WHEN ds.product_configuration = 'Hotel' THEN 1 END)      AS ho_spvs,
        SUM(CASE WHEN ds.product_configuration = 'Hotel Plus' THEN 1 END) AS hp_spvs,
        SUM(CASE WHEN ds.product_configuration = 'Package' THEN 1 END)    AS p_spvs,
        SUM(CASE WHEN ds.product_configuration = '3PP' THEN 1 END)        AS "3pp_spvs"
    FROM se.data.scv_touched_spvs s
        LEFT JOIN se.data.dim_sale ds ON s.se_sale_id = ds.se_sale_id
    WHERE s.event_tstamp::DATE = '2021-02-01'
    GROUP BY 1
)
SELECT
    stba.touch_start_tstamp::DATE                            AS day,
    stmc.touch_mkt_channel,
    stba.touch_experience,
    stba.touch_hostname_territory,
    stmc.touch_affiliate_territory,
    COUNT(DISTINCT stba.touch_id)                            AS sessions,
    COUNT(DISTINCT stba.attributed_user_id)                  AS users,
    COUNT(DISTINCT CASE
                       WHEN stba.stitched_identity_type = 'se_user_id'
                           THEN stba.attributed_user_id END) AS logged_in_users,
    COALESCE(SUM(b.bookings), 0)                             AS bookings,
    COALESCE(SUM(b.margin), 0)                               AS margin,
    COALESCE(SUM(s.spvs), 0)                                 AS spvs,
    COALESCE(SUM(s.unique_spvs), 0)                          AS unique_spvs,
    COALESCE(SUM(s.ho_spvs), 0)                              AS ho_spvs,
    COALESCE(SUM(s.hp_spvs), 0)                              AS hp_spvs,
    COALESCE(SUM(s.p_spvs), 0)                               AS p_spvs,
    COALESCE(SUM(s."3pp_spvs"), 0)                           AS "3pp_spvs"
FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
    LEFT JOIN  sess_bookings b ON stba.touch_id = b.touch_id
    LEFT JOIN  sess_spvs s ON stba.touch_id = s.touch_id
WHERE stba.touch_start_tstamp::DATE = '2021-01-01'
GROUP BY 1, 2, 3, 4, 5;


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE - 1
  AND es.contexts_com_secretescapes_sale_page_context_1 IS NULL
  AND es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page';


SELECT
    event_tstamp::DATE,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
WHERE mts.event_tstamp >= CURRENT_DATE - 30
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.new_spvs AS (
    SELECT
        mts.event_hash
    FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
    WHERE DATE_TRUNC(MONTH, mts.event_tstamp) = '2022-07-01'

    EXCEPT

    SELECT
        mts.event_hash
    FROM data_vault_mvp.single_customer_view_stg_bkup_20220914.module_touched_spvs mts
    WHERE DATE_TRUNC(MONTH, mts.event_tstamp) = '2022-07-01'
);


SELECT
    es.device_platform,
    COUNT(*)
FROM scratch.robinpatel.new_spvs ns
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON ns.event_hash = es.event_hash AND DATE_TRUNC(MONTH, es.event_tstamp) = '2022-07-01'
GROUP BY 1;



SELECT
    *
FROM scratch.robinpatel.new_spvs ns
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON ns.event_hash = es.event_hash AND DATE_TRUNC(MONTH, es.event_tstamp) = '2022-07-01';




