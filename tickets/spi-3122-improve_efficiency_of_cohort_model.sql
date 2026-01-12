SELECT
    qh.query_id,
    qh.query_text,
    qh.query_type,
    qh.user_name,
    qh.role_name,
    qh.warehouse_name,
    qh.warehouse_size,
    qh.execution_status,
    qh.start_time,
    qh.end_time,
    qh.total_elapsed_time,
    TIMEDIFF('min', qh.start_time, qh.end_time) AS total_elapsed_time_minutes,
    total_elapsed_time / 1000 *
    CASE warehouse_size
        WHEN 'X-Small' THEN 1 / 60 / 60
        WHEN 'Small' THEN 2 / 60 / 60
        WHEN 'Medium' THEN 4 / 60 / 60
        WHEN 'Large' THEN 8 / 60 / 60
        WHEN 'X-Large' THEN 16 / 60 / 60
        WHEN '2X-Large' THEN 32 / 60 / 60
        WHEN '3X-Large' THEN 64 / 60 / 60
        WHEN '4X-Large' THEN 128 / 60 / 60
        ELSE 0
        END                                     AS estimated_credits,
    qh.bytes_spilled_to_local_storage,
    qh.bytes_spilled_to_remote_storage,
    qh.bytes_scanned,
    qh.percentage_scanned_from_cache,
    qh.bytes_written,
    qh.partitions_scanned,
    qh.partitions_total,
    qh.bytes_sent_over_the_network,
    qh.execution_time,
    qh.transaction_blocked_time,
    qh.list_external_files_time,
    qh.credits_used_cloud_services,
    qh.queued_provisioning_time
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time::DATE >= CURRENT_DATE - 15
  AND qh.user_name = 'ROBINPATEL'
  AND qh.query_text LIKE 'CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics%'
  AND qh.execution_status = 'SUCCESS'
ORDER BY estimated_credits DESC;


SELECT
    qh.query_id,
    qh.query_text,
    qh.query_type,
    qh.user_name,
    qh.role_name,
    qh.warehouse_name,
    qh.warehouse_size,
    qh.execution_status,
    qh.start_time,
    qh.end_time,
    qh.total_elapsed_time,
    TIMEDIFF('min', qh.start_time, qh.end_time) AS total_elapsed_time_minutes,
    total_elapsed_time / 1000 *
    CASE warehouse_size
        WHEN 'X-Small' THEN 1 / 60 / 60
        WHEN 'Small' THEN 2 / 60 / 60
        WHEN 'Medium' THEN 4 / 60 / 60
        WHEN 'Large' THEN 8 / 60 / 60
        WHEN 'X-Large' THEN 16 / 60 / 60
        WHEN '2X-Large' THEN 32 / 60 / 60
        WHEN '3X-Large' THEN 64 / 60 / 60
        WHEN '4X-Large' THEN 128 / 60 / 60
        ELSE 0
        END                                     AS estimated_credits,
    qh.bytes_spilled_to_local_storage,
    qh.bytes_spilled_to_remote_storage,
    qh.bytes_scanned,
    qh.percentage_scanned_from_cache,
    qh.bytes_written,
    qh.partitions_scanned,
    qh.partitions_total,
    qh.bytes_sent_over_the_network,
    qh.execution_time,
    qh.transaction_blocked_time,
    qh.list_external_files_time,
    qh.credits_used_cloud_services,
    qh.queued_provisioning_time
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time::DATE = CURRENT_DATE
  AND qh.user_name = 'PIPELINERUNNER'
  AND qh.execution_status = 'SUCCESS'
  AND qh.query_text LIKE 'CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.chrt_fact_cohort_metrics%'
ORDER BY estimated_credits DESC;

USE WAREHOUSE pipe_large;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_activity CLONE data_vault_mvp.dwh.user_activity;


SELECT *
FROM se.data.harmonised_sale_calendar_view hscv;

self_describing_task --include 'biapp/task_catalogue/dv/bi/tableau/cohort_model/chrt_fact_cohort_metrics.py'  --method 'run' --start '2022-12-05 00:00:00' --end '2022-12-05 00:00:00'


DROP TABLE data_vault_mvp_dev_robin.dwh.user_attributes;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;
DROP TABLE data_vault_mvp_dev_robin.dwh.user_activity;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.harmonised_offer_calendar_view_snapshot CLONE data_vault_mvp.dwh.harmonised_offer_calendar_view_snapshot;

SELECT *
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics__step04__generate_grain
WHERE TRY_TO_NUMBER(shiro_user_id) IS NULL;



SELECT *
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics__model_data
    QUALIFY COUNT(*)
                  OVER (PARTITION BY event_month, sign_up_month, first_booking_month, original_affiliate_territory, current_affiliate_territory, acquisition_platform,member_original_affiliate_classification, current_affiliate_name) >
            1;

SELECT *
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics__step09__model_fact_cohort_mau
    QUALIFY COUNT(*)
                  OVER (PARTITION BY event_month, sign_up_month, first_booking_month, original_affiliate_territory, current_affiliate_territory, acquisition_platform,member_original_affiliate_classification, current_affiliate_name) >
            1;

SELECT *
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics__step08__model_fact_cohort_spvs
    QUALIFY COUNT(*)
                  OVER (PARTITION BY event_month, sign_up_month, first_booking_month, original_affiliate_territory, current_affiliate_territory, acquisition_platform,member_original_affiliate_classification, current_affiliate_name) >
            1

SELECT *
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics__step07__model_fact_cohort_bookings
    QUALIFY COUNT(*)
                  OVER (PARTITION BY event_month, sign_up_month, first_booking_month, original_affiliate_territory, current_affiliate_territory, acquisition_platform,member_original_affiliate_classification, current_affiliate_name) >
            1;

SELECT
    COUNT(*)
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics;
SELECT
    COUNT(*)
FROM data_vault_mvp.bi.chrt_fact_cohort_metrics;


SELECT
    SUM(gross_trx)
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics;
SELECT
    SUM(gross_trx)
FROM data_vault_mvp.bi.chrt_fact_cohort_metrics;

SELECT
    SUM(net_margin)
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics;
SELECT
    SUM(net_margin)
FROM data_vault_mvp.bi.chrt_fact_cohort_metrics;

SELECT
    SUM(spvs)
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics;
SELECT
    SUM(spvs)
FROM data_vault_mvp.bi.chrt_fact_cohort_metrics;


SELECT
    SUM(net_margin)
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics
WHERE current_affiliate_territory = 'UK';
SELECT
    SUM(net_margin)
FROM data_vault_mvp.bi.chrt_fact_cohort_metrics
WHERE current_affiliate_territory = 'UK';


SELECT
    SUM(spvs)
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics
WHERE current_affiliate_territory = 'UK';
SELECT
    SUM(spvs)
FROM data_vault_mvp.bi.chrt_fact_cohort_metrics
WHERE current_affiliate_territory = 'UK';

SELECT
    SUM(net_margin)
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics
WHERE current_affiliate_territory = 'DE';
SELECT
    SUM(net_margin)
FROM data_vault_mvp.bi.chrt_fact_cohort_metrics
WHERE current_affiliate_territory = 'DE';


SELECT
    SUM(spvs)
FROM data_vault_mvp_dev_robin.bi.chrt_fact_cohort_metrics
WHERE current_affiliate_territory = 'DE';
SELECT
    SUM(spvs)
FROM data_vault_mvp.bi.chrt_fact_cohort_metrics
WHERE current_affiliate_territory = 'DE';


SELECT * FROM snowplow.atomic.events e