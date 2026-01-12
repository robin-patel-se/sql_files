CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;

SELECT *
FROM data_vault_mvp.information_schema.tables t
WHERE t.table_schema = 'SINGLE_CUSTOMER_VIEW_STG';



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_basic_touch_attributes CLONE data_vault_mvp.single_customer_view_stg.module_basic_touch_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations CLONE data_vault_mvp.single_customer_view_stg.module_identity_associations;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs_bkup CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs_bkup;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params CLONE data_vault_mvp.single_customer_view_stg.module_url_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker;


airflow backfill --start_date '2021-06-14 00:00:00' --end_date '2021-06-14 00:00:00' --task_regex '.*' -m hygiene__snowplow__events__hourly
airflow backfill --start_date '2021-06-14 00:00:00' --end_date '2021-06-14 00:00:00' --task_regex '.*' -m single_customer_view__hourly
airflow backfill --start_date '2021-06-14 03:00:00' --end_date '2021-06-14 03:00:00' --task_regex '.*' -m dwh__transactional__booking__daily_at_03h00
airflow backfill --start_date '2021-06-14 01:00:00' --end_date '2021-06-14 01:00:00' --task_regex '.*' -m hygiene_snapshots__cms_mysql__affiliate__daily_at_01h00
airflow backfill --start_date '2021-06-14 01:00:00' --end_date '2021-06-14 01:00:00' --task_regex '.*' -m hygiene_snapshots__cms_mysql__territory__daily_at_01h00
airflow backfill --start_date '2021-06-13 03:00:00' --end_date '2021-06-13 03:00:00' --task_regex '.*' -m dwh__transactional__booking__daily_at_03h00
airflow backfill --start_date '2021-06-13 01:00:00' --end_date '2021-06-13 01:00:00' --task_regex '.*' -m hygiene_snapshots__cms_mysql__affiliate__daily_at_01h00
airflow backfill --start_date '2021-06-13 01:00:00' --end_date '2021-06-13 01:00:00' --task_regex '.*' -m hygiene_snapshots__cms_mysql__territory__daily_at_01h00

self_describing_task --include 'staging/hygiene/snowplow/events.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/00_artificial_transaction_insert/artificial_transaction_insert_se.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/01_module_unique_urls.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/02_01_module_url_hostname.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/02_02_module_url_params.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/03_module_extracted_params.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/02_identity_stitching/01_module_identity_associations.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/02_identity_stitching/02_module_identity_stitching.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include '/dv/dwh/events/03_touchification/01_touchifiable_events.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/02_01_utm_or_referrer_hostname_marker.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/02_02_time_diff_marker.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/03_touchification.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/05_touch_channelling/01_module_touch_utm_referrer.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/05_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/07_events_of_interest/01_module_touched_spvs.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'
self_describing_task --include 'dv/dwh/events/07_events_of_interest/02_module_touched_transactions.py'  --method 'run' --start '2021-06-14 00:00:00' --end '2021-06-14 00:00:00'




CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.affiliate CLONE hygiene_snapshot_vault_mvp.cms_mysql.affiliate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.territory;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking clone data_vault_mvp.dwh.se_booking;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.territory t;


WITH ref AS (
    SELECT CURRENT_DATE AS d
    FROM dual
),
     bookings AS (
         SELECT saleid, transaction_id
         FROM se.data.master_se_booking_list b
                  JOIN ref
         WHERE sale_dimension LIKE 'Hotel%'
           AND date_booked > DATEADD('days', -7, ref.d)
           AND date_booked <= ref.d
           AND booking_status = 'COMPLETE'
     )
SELECT DAYOFWEEK(sal.start_date), COUNT(*)
FROM bookings b
         JOIN se.data.se_sale_attributes sal ON b.saleid = sal.se_sale_id
GROUP BY 1
;



airflow backfill --start_date '2021-06-15 04:00:00' --end_date '2021-06-15 04:00:00' --task_regex '.*' single_customer_view__hourly

------------------------------------------------------------------------------------------------------------------------

--hourly job was not showing great improvements

--test on empty tables.
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_basic_touch_attributes;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs_bkup;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker;

airflow backfill --start_date '2021-06-15 05:00:00' --end_date '2021-06-15 05:00:00' --task_regex '.*' single_customer_view__hourly

--on empty tables scv takes 36 mins in an hour batch.