SELECT e.v_tracker, count(*)
FROM snowplow.atomic.events e
WHERE e.etl_tstamp >= current_date - 1
GROUP BY 1;

DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;

self_describing_task --include 'staging/hygiene/snowplow/events.py'  --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'



SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.v_tracker LIKE 'andr-%';

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE device_platform = 'native app android'
  AND es.booking_id IS NOT NULL;

-- things to update:
-- event_stream - device_platform need to update existing native app to native app ios
-- event_stream - device_platform on artificially inseminated bookings to native app ios
-- basic touch attributes - need to update device platform
-- rerun udf se.data.platform_from_touch_experience
-- rerun booking summary and downstream jobs

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
SELECT bs.booking_id,
       bs.device_platform,
       bs.*
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
         LEFT JOIN hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs ON sb.booking_id = bs.booking_id
WHERE sb.transaction_id = 'A16957-15319-2705664'
;

self_describing_task --include 'dv/dwh/events/00_artificial_transaction_insert/artificial_transaction_insert_se.py'  --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'

SELECT
    /*
        Only consider bookings which were completed at some point i.e. could be refunded now
        Minimum limit of 2018-01-01 because we don't want old sessions
    */

    booking_id
FROM data_vault_mvp_dev_robin.dwh.se_booking
WHERE booking_completed_date >= TIMESTAMPADD('day', -1, '2020-10-31 03:00:00'::TIMESTAMP)
  AND booking_status IN ('COMPLETE', 'REFUNDED', 'HOLD_BOOKED')
  AND booking_completed_date >= '2018-01-01'
GROUP BY 1;


SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE device_platform = 'native app android'
  AND es.booking_id IS NOT NULL;

self_describing_task --include 'dv/dwh/events/01_url_manipulation/01_module_unique_urls.py' --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/02_01_module_url_hostname.py' --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/02_02_module_url_params.py' --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/03_module_extracted_params.py' --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'

self_describing_task --include 'dv/dwh/events/02_identity_stitching/01_module_identity_associations.py' --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'
self_describing_task --include 'dv/dwh/events/02_identity_stitching/02_module_identity_stitching.py' --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'

self_describing_task --include 'dv/dwh/events/03_touchification/01_touchifiable_events.py' --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/02_01_utm_or_referrer_hostname_marker.py' --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/02_02_time_diff_marker.py' --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/03_touchification.py' --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs;

self_describing_task --include 'dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py' --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'
self_describing_task --include 'dv/dwh/events/07_events_of_interest/01_module_touched_spvs.py' --method 'run' --start '2020-11-01 00:00:00' --end '2020-11-01 00:00:00'

SELECT DISTINCT touch_experience
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba;

DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;

SELECT DISTINCT device_platform
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es;



CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.device_platform = 'native app';

--update existing appli
UPDATE hygiene_vault_mvp_dev_robin.snowplow.event_stream target
SET target.device_platform = 'native app ios'
WHERE target.device_platform = 'native app';

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.v_tracker LIKE 'andr-%'
   OR app_id LIKE 'android_app%'-- native events
   OR useragent LIKE '%mobile_native_v3:{platform:android%'-- webkit wrapped forwarded via native app
   OR UPPER(contexts_com_secretescapes_environment_context_1[0]['device_platform']::VARCHAR) =
      'ANDROID_APP_V3' -- SS platform categorisation
;
SELECT min(es.etl_tstamp)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.v_tracker LIKE 'andr-%'
   OR app_id LIKE 'android_app%'-- native events
   OR useragent LIKE '%mobile_native_v3:{platform:android%'-- webkit wrapped forwarded via native app
   OR UPPER(contexts_com_secretescapes_environment_context_1[0]['device_platform']::VARCHAR) =
      'ANDROID_APP_V3' -- SS platform categorisation

;

USE WAREHOUSE pipe_xlarge;

UPDATE hygiene_vault_mvp_dev_robin.snowplow.event_stream target
SET target.device_platform = 'native app android'
WHERE target.etl_tstamp >= '2020-06-16'
  AND (target.v_tracker LIKE 'andr-%'
    OR target.app_id LIKE 'android_app%'-- native events
    OR target.useragent LIKE '%mobile_native_v3:{platform:android%'-- webkit wrapped forwarded via native app
    OR UPPER(target.contexts_com_secretescapes_environment_context_1[0]['device_platform']::VARCHAR) = 'ANDROID_APP_V3'
    );

UPDATE hygiene_vault_mvp_dev_robin.snowplow.event_stream target
SET target.posa_territory = regexp_replace(app_id, 'android_app ')
WHERE target.device_platform = 'native app android';

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.device_platform = 'native app android';

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
SET target.touch_experience = 'native app ios'
WHERE target.touch_experience = 'native app';

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mtba.touch_id = es.event_hash
WHERE es.device_platform = 'native app android';

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
SET target.touch_experience         = batch.device_platform,
    target.touch_posa_territory     = regexp_replace(touch_posa_territory, 'android_app '),
    target.touch_hostname_territory = regexp_replace(target.touch_hostname_territory, 'android_app ')
FROM hygiene_vault_mvp.snowplow.event_stream batch
WHERE target.touch_id = batch.event_hash
  AND batch.device_platform = 'native app android';

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_experience = 'native app android';

SELECT DISTINCT device_platform
FROM data_vault_mvp.dwh.se_booking sb;

SELECT *
FROM hygiene_vault_mvp.cms_mongodb.booking_summary bs
WHERE bs.device_platform = 'native app';

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_vault_mvp.cms_mongodb.booking_summary;

USE WAREHOUSE pipe_xlarge;

UPDATE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary target
SET target.device_platform = 'native app ios'
WHERE target.device_platform = 'native app';

UPDATE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary target
SET target.device_platform = 'native app android'
WHERE target.transaction_id = 'A16957-15319-2705664';

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;

UPDATE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary target
SET target.device_platform = 'native app ios'
WHERE target.device_platform = 'native app';

UPDATE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary target
SET target.device_platform = 'native app android'
WHERE target.transaction_id = 'A16957-15319-2705664';

SELECT device_platform, count(*)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary bs
GROUP BY 1;

SELECT device_platform, count(*)
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
GROUP BY 1;

SELECT device_platform
     , count(*)
FROM se.data.se_booking sb
GROUP BY 1

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions mtt ON mtba.touch_id = mtt.touch_id
LEFT JOIN se.data.fact_complete_booking fcb ON mtt.booking_id = fcb.booking_id
WHERE mtba.touch_start_tstamp >= '2020-11-02'
  AND mtba.touch_experience = 'native app android';