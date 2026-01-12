DROP VIEW hygiene_vault_mvp_dev_robin.snowplow.event_stream
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.feature_toggle CLONE latest_vault.cms_mysql.feature_toggle
;


DROP SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/00_event_stream_modelling/01_artificial_transaction_insert.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/00_event_stream_modelling/02_page_screen_enrichment.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/00_event_stream_modelling/03_app_push_enhancement.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/01_url_manipulation/01_module_unique_urls.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/01_url_manipulation/02_01_module_url_hostname.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/01_url_manipulation/02_02_module_url_params.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/01_url_manipulation/03_module_extracted_params.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/02_identity_stitching/01_module_identity_associations.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/02_identity_stitching/02_module_identity_stitching.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/03_touchification/01_touchifiable_events.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/03_touchification/02_01_utm_or_referrer_hostname_marker.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/03_touchification/02_02_time_diff_marker.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/03_touchification/03_touchification.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/07_events_of_interest/01_module_touched_spvs.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/04_touch_basic_attributes/00_anomalous_user_dates.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/05_touch_channelling/01_module_touch_utm_referrer.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/05_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/07_events_of_interest/02_module_touched_transactions.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/07_events_of_interest/03_module_touched_searches.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/07_events_of_interest/04_module_touched_app_installs.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/07_events_of_interest/05_module_touched_feature_flags.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/07_events_of_interest/07_module_touched_booking_form_views.py'  --method 'run' --start '2023-09-19 00:00:00' --end '2023-09-19 00:00:00'


CREATE OR REPLACE VIEW hygiene_vault_mvp_dev_robin.snowplow.event_stream AS
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream
;

DROP VIEW hygiene_vault_mvp_dev_robin.snowplow.event_stream;


SELECT
*
FROM dbt.bi_data_science. dssl