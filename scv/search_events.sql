SELECT *
FROM snowplow.atomic.events e
WHERE e.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND e.collector_tstamp >= CURRENT_DATE - 30;

self_describing_task --include 'bi__daily_spv_weight__daily_at_04h00'  --method 'run' --start '2018-01-01 00:00:00' --end '2018-01-01 00:00:00'

airflow backfill --start_date '2018-01-01 00:00:00' --end_date '2018-01-02 00:00:00' --task_regex '.*' bi__daily_spv_weight__daily_at_04h00
airflow backfill --start_date '2021-06-23 00:00:00' --end_date '2021-06-24 00:00:00' --task_regex '.*' -m bi__daily_spv_weight__daily_at_04h00
airflow backfill --start_date '2021-06-23 00:00:00' --end_date '2021-06-24 00:00:00' --task_regex '.*' se_bi_object_creation__daily_at_07h00


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

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;

ALTER TABLE hygiene_vault_mvp.snowplow.event_stream
    ADD COLUMN contexts_com_secretescapes_search_context_1 ARRAY;
ALTER TABLE hygiene_vault_mvp.snowplow.event_stream
    ADD COLUMN contexts_com_google_analytics_cookies_1 ARRAY;
ALTER TABLE hygiene_vault_mvp.snowplow.event_stream
    ADD COLUMN unstruct_event_com_snowplowanalytics_snowplow_application_error_1 OBJECT;
ALTER TABLE hygiene_vault_mvp.snowplow.event_stream
    ADD COLUMN contexts_com_secretescapes_content_element_interaction_context_1 ARRAY;
ALTER TABLE hygiene_vault_mvp.snowplow.event_stream
    ADD COLUMN contexts_com_secretescapes_content_elements_rendered_context_1 ARRAY;
ALTER TABLE hygiene_vault_mvp.snowplow.event_stream
    ADD COLUMN contexts_com_secretescapes_content_element_viewed_context_1 ARRAY;
ALTER TABLE hygiene_vault_mvp.snowplow.event_stream
    ADD COLUMN contexts_com_secretescapes_searched_with_refinement_event_1 ARRAY;


self_describing_task --include 'staging/hygiene/snowplow/events.py'  --method 'run' --start '2021-03-06 00:00:00' --end '2021-03-06 00:00:00'

--first search event
SELECT MIN(e.etl_tstamp)
FROM snowplow.atomic.events e
WHERE e.contexts_com_secretescapes_search_context_1 IS NOT NULL
--2020-03-06 14:50:34.651000000

--NEED TO BACKFILL HYGIENE to this point;


SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND es.event_tstamp >= CURRENT_DATE - 1;

self_describing_task --include '/dv/dwh/events/03_touchification/01_touchifiable_events.py'  --method 'run' --start '2021-06-23 00:00:00' --end '2021-06-23 00:00:00'




    flexible_search BOOLEAN,
    had_results BOOLEAN,
    location VARCHAR,
    location_search BOOLEAN,
    MONTHS ARRAY,
    months_search BOOLEAN,
    num_results NUMBER,
    refine_by_travel_type_search BOOLEAN,
    refine_by_trip_type_search BOOLEAN,
    specific_dates_search BOOLEAN,
    travel_types ARRAY,
    trip_types ARRAY,
    weekend_only_search BOOLEAN


SELECT t.event_hash,
       t.touch_id,
       t.event_tstamp,
       'search'                                                                                AS category,
       '',
       es.contexts_com_secretescapes_search_context_1,
       es.contexts_com_secretescapes_search_context_1[0]:flexible_search::BOOLEAN              AS flexible_search,
       es.contexts_com_secretescapes_search_context_1[0]:had_results::BOOLEAN                  AS had_results,
       es.contexts_com_secretescapes_search_context_1[0]:location::VARCHAR                     AS location,
       es.contexts_com_secretescapes_search_context_1[0]:location_search::BOOLEAN              AS location_search,
       es.contexts_com_secretescapes_search_context_1[0]: MONTHS [0]::VARCHAR                  AS months,
       es.contexts_com_secretescapes_search_context_1[0]:months_search::BOOLEAN                AS months_search,
       es.contexts_com_secretescapes_search_context_1[0]:num_results::NUMBER                   AS num_results,
       es.contexts_com_secretescapes_search_context_1[0]:refine_by_travel_type_search::BOOLEAN AS refine_by_travel_type_search,
       es.contexts_com_secretescapes_search_context_1[0]:refine_by_trip_type_search::BOOLEAN   AS refine_by_trip_type_search,
       es.contexts_com_secretescapes_search_context_1[0]:specific_dates_search::BOOLEAN        AS specific_dates_search,
       es.contexts_com_secretescapes_search_context_1[0]:travel_types[0]::VARCHAR              AS travel_types,
       es.contexts_com_secretescapes_search_context_1[0]:trip_types[0]::VARCHAR                AS trip_types,
       es.contexts_com_secretescapes_search_context_1[0]:weekend_only_search::BOOLEAN          AS weekend_only_search

FROM data_vault_mvp.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream es ON t.event_hash = es.event_hash
WHERE es.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND es.event_tstamp >= CURRENT_DATE - 1



SELECT es.contexts_com_secretescapes_search_context_1,
       es.contexts_com_secretescapes_search_context_1[0]:flexible_search::BOOLEAN              AS flexible_search,
       es.contexts_com_secretescapes_search_context_1[0]:had_results::BOOLEAN                  AS had_results,
       es.contexts_com_secretescapes_search_context_1[0]:location::VARCHAR                     AS location,
       es.contexts_com_secretescapes_search_context_1[0]:location_search::BOOLEAN              AS location_search,
       es.contexts_com_secretescapes_search_context_1[0]: MONTHS [0]::VARCHAR                  AS months,
       es.contexts_com_secretescapes_search_context_1[0]:months_search::BOOLEAN                AS months_search,
       es.contexts_com_secretescapes_search_context_1[0]:num_results::NUMBER                   AS num_results,
       es.contexts_com_secretescapes_search_context_1[0]:refine_by_travel_type_search::BOOLEAN AS refine_by_travel_type_search,
       es.contexts_com_secretescapes_search_context_1[0]:refine_by_trip_type_search::BOOLEAN   AS refine_by_trip_type_search,
       es.contexts_com_secretescapes_search_context_1[0]:specific_dates_search::BOOLEAN        AS specific_dates_search,
       es.contexts_com_secretescapes_search_context_1[0]:travel_types[0]::VARCHAR              AS travel_types,
       es.contexts_com_secretescapes_search_context_1[0]:trip_types[0]::VARCHAR                AS trip_types,
       es.contexts_com_secretescapes_search_context_1[0]:weekend_only_search::BOOLEAN          AS weekend_only_search
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND es.event_tstamp >= CURRENT_DATE - 1;
self_describing_task --include 'staging/hygiene/snowplow/events.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/00_artificial_transaction_insert/artificial_transaction_insert_se.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/01_module_unique_urls.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/02_01_module_url_hostname.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/02_02_module_url_params.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/01_url_manipulation/03_module_extracted_params.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/02_identity_stitching/01_module_identity_associations.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/02_identity_stitching/02_module_identity_stitching.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include '/dv/dwh/events/03_touchification/01_touchifiable_events.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/02_01_utm_or_referrer_hostname_marker.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/02_02_time_diff_marker.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/03_touchification.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/05_touch_channelling/01_module_touch_utm_referrer.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/05_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/07_events_of_interest/01_module_touched_spvs.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/07_events_of_interest/02_module_touched_transactions.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'
self_describing_task --include 'dv/dwh/events/07_events_of_interest/03_module_touched_searches.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'


SELECT es.event_tstamp,
       es.event_hash,
       es.page_url,
       es.contexts_com_secretescapes_search_context_1
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND es.event_tstamp >= CURRENT_DATE - 5;

SELECT es.event_tstamp,
       es.event_hash,
       es.page_url,
       es.contexts_com_secretescapes_search_context_1,
       TRY_TO_BOOLEAN(es.contexts_com_secretescapes_search_context_1[0]:flexible_search::VARCHAR)              AS flexible_search,
       TRY_TO_BOOLEAN(es.contexts_com_secretescapes_search_context_1[0]:had_results::VARCHAR)                  AS had_results,
       es.contexts_com_secretescapes_search_context_1[0]:location::VARCHAR                                     AS location,
       TRY_TO_BOOLEAN(es.contexts_com_secretescapes_search_context_1[0]:location_search::VARCHAR)              AS location_search,
       es.contexts_com_secretescapes_search_context_1[0]: MONTHS [0]::VARCHAR                                  AS months,
       TRY_TO_BOOLEAN(es.contexts_com_secretescapes_search_context_1[0]:months_search::VARCHAR)                AS months_search,
       TRY_TO_NUMBER(es.contexts_com_secretescapes_search_context_1[0]:num_results::VARCHAR)                   AS num_results,
       TRY_TO_BOOLEAN(es.contexts_com_secretescapes_search_context_1[0]:refine_by_travel_type_search::VARCHAR) AS refine_by_travel_type_search,
       TRY_TO_BOOLEAN(es.contexts_com_secretescapes_search_context_1[0]:refine_by_trip_type_search::VARCHAR)   AS refine_by_trip_type_search,
       TRY_TO_BOOLEAN(es.contexts_com_secretescapes_search_context_1[0]:specific_dates_search::VARCHAR)        AS specific_dates_search,
       es.contexts_com_secretescapes_search_context_1[0]:travel_types[0]::VARCHAR                              AS travel_types,
       es.contexts_com_secretescapes_search_context_1[0]:trip_types[0]::VARCHAR                                AS trip_types,
       TRY_TO_BOOLEAN(es.contexts_com_secretescapes_search_context_1[0]:weekend_only_search::VARCHAR)          AS weekend_only_search
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND es.event_tstamp >= CURRENT_DATE - 5;



flexible_search FALSE,
    had_results TRUE,
    location "Rhodes, Greece",
    location_search TRUE,
    MONTHS [],
    months_search FALSE,
    num_results 12,
    refine_by_travel_type_search FALSE,
    refine_by_trip_type_search FALSE,
    specific_dates_search FALSE,
    travel_types [],
    trip_types [],
    weekend_only_search FALSE


DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches;

SELECT event_tstamp::date,
       COUNT(*)
FROM se.data.scv_touched_searches sts
GROUP BY 1;

self_describing_task --include 'se/data/scv/scv_touched_searches.py'  --method 'run' --start '2021-07-04 00:00:00' --end '2021-07-04 00:00:00'

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND es.event_tstamp >= CURRENT_DATE - 10

SELECT *
FROM se.data.scv_touched_searches;

self_describing_task --include 'dv/dwh/events/07_events_of_interest/03_module_touched_searches.py'  --method 'run' --start '2021-07-05 00:00:00' --end '2021-07-05 00:00:00'

SELECT mt.updated_at::date, COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mt.event_hash = es.event_hash
WHERE mt.updated_at >= CURRENT_DATE - 30
  AND es.contexts_com_secretescapes_search_context_1 IS NOT NULL
GROUP BY 1;


SELECT event_tstamp::date,
       COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND es.event_tstamp >= CURRENT_DATE - 30
GROUP BY 1;


SELECT *
FROM snowplow.atomic.events e
WHERE e.etl_tstamp::DATE = CURRENT_DATE - 5
  AND e.contexts_com_secretescapes_search_context_1 IS NOT NULL;


SELECT scv_touched_searches.event_tstamp::date,
       COUNT(*)
FROM se.data.scv_touched_searches
GROUP BY 1;

SELECT *
FROM se.data.scv_touched_searches sts;


SELECT *
FROM data_vault_mvp.dwh.email_performance
    QUALIFY COUNT(*) OVER (PARTITION BY send_id) > 1

DELETE
FROM data_vault_mvp.dwh.email_performance ep
WHERE ep.send_id = 1215166
  AND ep.sessions != 2977
  AND ep.spvs != 4621;


SELECT * FROM se.data.se_user_attributes ua;

SELECT * FROM se.data.scv_touched_searches sts;