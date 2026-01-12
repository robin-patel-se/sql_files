-- Warehouse Credits over time

SELECT
    TO_CHAR(start_time, 'YYYY-MM') AS month
  , SUM(credits_used)
FROM snowflake.account_usage.warehouse_metering_history wmh
WHERE wmh.start_time >= DATEADD(MONTH, -12, CURRENT_DATE())
GROUP BY TO_CHAR(start_time, 'YYYY-MM')
ORDER BY 1;


-- Monthly Credits By Type

SELECT
    TO_CHAR(usage_date, 'YYYYMM') AS month,
    SUM(credits_billed)           AS total
FROM snowflake.account_usage.metering_daily_history wmh
WHERE wmh.usage_date >= DATEADD(MONTH, -12, CURRENT_DATE())
GROUP BY TO_CHAR(usage_date, 'YYYYMM');


-- top 10 users
SELECT
    user_name,
    COUNT(*),
    SUM(total_elapsed_time / 1000 *
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
            END) AS estimated_credits
FROM snowflake.account_usage.query_history
WHERE start_time >= CURRENT_DATE - 30
GROUP BY user_name
ORDER BY 3 DESC
LIMIT 10;


SELECT
    qh.query_id,
    qh.query_text,
    qh.database_id,
    qh.database_name,
    qh.schema_id,
    qh.schema_name,
    qh.query_type,
    qh.session_id,
    qh.user_name,
    qh.role_name,
    qh.warehouse_id,
    qh.warehouse_name,
    qh.warehouse_size,
    qh.warehouse_type,
    qh.cluster_number,
    qh.query_tag,
    qh.execution_status,
    qh.error_code,
    qh.error_message,
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
    qh.bytes_scanned,
    qh.percentage_scanned_from_cache,
    qh.bytes_written,
    qh.bytes_written_to_result,
    qh.bytes_read_from_result,
    qh.rows_produced,
    qh.rows_inserted,
    qh.rows_updated,
    qh.rows_deleted,
    qh.rows_unloaded,
    qh.bytes_deleted,
    qh.partitions_scanned,
    qh.partitions_total,
    qh.bytes_spilled_to_local_storage,
    qh.bytes_spilled_to_remote_storage,
    qh.bytes_sent_over_the_network,
    qh.compilation_time,
    qh.execution_time,
    qh.queued_provisioning_time,
    qh.queued_repair_time,
    qh.queued_overload_time,
    qh.transaction_blocked_time,
    qh.outbound_data_transfer_cloud,
    qh.outbound_data_transfer_region,
    qh.outbound_data_transfer_bytes,
    qh.inbound_data_transfer_cloud,
    qh.inbound_data_transfer_region,
    qh.inbound_data_transfer_bytes,
    qh.list_external_files_time,
    qh.credits_used_cloud_services,
    qh.release_version,
    qh.external_function_total_invocations,
    qh.external_function_total_sent_rows,
    qh.external_function_total_received_rows,
    qh.external_function_total_sent_bytes,
    qh.external_function_total_received_bytes,
    qh.query_load_percent,
    qh.is_client_generated_statement,
    qh.query_acceleration_bytes_scanned,
    qh.query_acceleration_partitions_scanned,
    qh.query_acceleration_upper_limit_scale_factor
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time >= CURRENT_DATE
  AND qh.user_name = 'PIPELINERUNNER'
ORDER BY estimated_credits DESC;

-- Credits by Warehouse

SELECT
    warehouse_name,
    SUM(credits_used) AS credits_used
FROM snowflake.account_usage.warehouse_metering_history wmh
GROUP BY warehouse_name
ORDER BY 2 DESC


-- Credits by hour of the day

SELECT
    TO_CHAR(start_time, 'HH24') AS hour,
    SUM(credits_used)
FROM snowflake.account_usage.warehouse_metering_history wmh
WHERE wmh.start_time >= DATEADD(MONTH, -1, CURRENT_DATE())
GROUP BY TO_CHAR(start_time, 'HH24')
ORDER BY 1;


-- Data Storage by Month and Type

SELECT
    TO_CHAR(usage_date, 'YYYYMM')   AS sort_month,
    TO_CHAR(usage_date, 'Mon-YYYY') AS month,
    AVG(storage_bytes)              AS storage,
    AVG(stage_bytes)                AS stage,
    AVG(failsafe_bytes)             AS failsafe
FROM snowflake.account_usage.storage_usage
GROUP BY month, sort_month
ORDER BY sort_month;


SELECT *
FROM snowflake.account_usage.table_storage_metrics tsm
WHERE tsm.table_catalog = 'DATA_VAULT_MVP'
ORDER BY active_bytes DESC;


SELECT *
FROM dbt.information_schema.tables t
;
USE ROLE pipelinerunner;


SELECT
    signup_month,
    event_month,
    territory,
    CASE
        WHEN territory = 'UK' THEN 'UK'
        WHEN territory = 'DE' THEN 'DE'
        WHEN territory = 'Other' THEN 'Other'
        ELSE 'ROW' END AS territory_grouped,
    affiliate_category_group,
    channel,
    bookings,
    margin_gbp
FROM dbt.bi_data_platform.dp_cohort_monthly_last_paid_bookings dcmlpb;

SELECT
    signup_month,
    affiliate_category_group,
    territory,
    CASE
        WHEN territory = 'UK' THEN 'UK'
        WHEN territory = 'DE' THEN 'DE'
        WHEN territory = 'Other' THEN 'Other'
        ELSE 'ROW' END AS territory_grouped,
    members
FROM dbt.bi_data_platform.dp_cohort_monthly_member_signups dcmms;

USE ROLE personal_role__robinpatel;

USE WAREHOUSE pipe_xlarge;

SELECT
    es.event_tstamp::DATE,
    SUM(IFF(es.unique_browser_id IS NULL, 1, 0)) AS no_ubid,
    COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp::DATE >= '2020-02-28'
  AND es.is_server_side_event = FALSE
  AND es.event_name = 'page_view'
GROUP BY 1;



SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp::DATE >= '2020-01-01'
  AND es.is_server_side_event = FALSE
  AND es.event_name = 'page_view';


SELECT *
FROM data_vault_mvp.bi.total_marketing_costs;


SELECT *
FROM latest_vault.sfsc.case c
WHERE id = '5006900004tyBiFAAU';
SELECT *
FROM latest_vault.sfsc.case c
WHERE c.casenumber = 03664029;
SELECT *
FROM latest_vault.sfsc.case c
WHERE c.parentid = '5006900004tyBiFAAU';


------------------------------------------------------------------------------------------------------------------------


SELECT
    qh.query_id,
    qh.query_text,
    qh.database_id,
    qh.database_name,
    qh.schema_id,
    qh.schema_name,
    qh.query_type,
    qh.session_id,
    qh.user_name,
    qh.role_name,
    qh.warehouse_id,
    qh.warehouse_name,
    qh.warehouse_size,
    qh.warehouse_type,
    qh.cluster_number,
    qh.query_tag,
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
    qh.bytes_written_to_result,
    qh.bytes_read_from_result,
    qh.partitions_scanned,
    qh.partitions_total,
    qh.bytes_sent_over_the_network,
    qh.compilation_time,
    qh.execution_time,
    qh.queued_provisioning_time,
    qh.queued_repair_time,
    qh.queued_overload_time,
    qh.transaction_blocked_time,
    qh.list_external_files_time,
    qh.credits_used_cloud_services,
    qh.release_version,
    qh.query_load_percent,
    qh.is_client_generated_statement,
    qh.query_acceleration_bytes_scanned,
    qh.query_acceleration_partitions_scanned,
    qh.query_acceleration_upper_limit_scale_factor
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time >= CURRENT_DATE - 1
  AND qh.user_name = 'PIPELINERUNNER'
ORDER BY estimated_credits DESC;


SELECT
    qh.query_id,
    qh.query_text,
    qh.database_id,
    qh.database_name,
    qh.schema_id,
    qh.schema_name,
    qh.query_type,
    qh.session_id,
    qh.user_name,
    qh.role_name,
    qh.warehouse_id,
    qh.warehouse_name,
    qh.warehouse_size,
    qh.warehouse_type,
    qh.cluster_number,
    qh.query_tag,
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
    qh.bytes_written_to_result,
    qh.bytes_read_from_result,
    qh.partitions_scanned,
    qh.partitions_total,
    qh.bytes_sent_over_the_network,
    qh.compilation_time,
    qh.execution_time,
    qh.queued_provisioning_time,
    qh.queued_repair_time,
    qh.queued_overload_time,
    qh.transaction_blocked_time,
    qh.list_external_files_time,
    qh.credits_used_cloud_services,
    qh.release_version,
    qh.query_load_percent,
    qh.is_client_generated_statement,
    qh.query_acceleration_bytes_scanned,
    qh.query_acceleration_partitions_scanned,
    qh.query_acceleration_upper_limit_scale_factor
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time >= CURRENT_DATE - 10
  AND qh.user_name = 'PIPELINERUNNER'
ORDER BY qh.bytes_spilled_to_remote_storage DESC;


SELECT
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
            TIMEDIFF('min', qh.start_time, qh.end_time) AS total_elapsed_time_minutes,
            *
FROM snowflake.account_usage.query_history qh
WHERE qh.query_id IN ('01a84eec-3202-061e-0000-02ddd04dcfba', '01a8347d-3202-0004-0000-02ddd0196586', '01a85435-3202-0725-0000-02ddd05a7c42');



SELECT
    dcmau.event_month,
    SUM(dcmau.member_mau)
FROM dbt.bi_data_platform.dp_cohort_monthly_active_users dcmau
GROUP BY 1 airflow dags backfill
--start-date '2022-11-16 15:00:00' --end-date '2022-11-16 15:00:00' dwh__trimmed_event_stream__hourly


------------------------------------------------------------------------------------------------------------------------

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
    qh.queued_provisioning_time,
    qh.query_tag
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time::DATE = CURRENT_DATE - 1
  AND qh.user_name = 'PIPELINERUNNER'
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
    qh.queued_provisioning_time,
    qh.query_tag
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time::DATE = CURRENT_DATE - 1
ORDER BY estimated_credits DESC;



SELECT
    PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR,
    *
FROM snowflake.account_usage.query_history qh
WHERE qh.user_name = 'PIPELINERUNNER'
  AND qh.start_time >= CURRENT_DATE - 10
  AND qh.query_tag IS DISTINCT FROM ''
  AND PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR LIKE '/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events%' -- single customer view
;


SELECT
    qh.query_type,
    COUNT(*)
FROM snowflake.account_usage.query_history qh
WHERE qh.user_name = 'PIPELINERUNNER'
  AND qh.start_time >= CURRENT_DATE - 10
  AND qh.query_tag IS DISTINCT FROM ''
  AND PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR LIKE '/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events%' -- single customer view
GROUP BY 1
;


SELECT
    PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR,
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
    *
FROM snowflake.account_usage.query_history qh
WHERE qh.user_name = 'PIPELINERUNNER'
  AND qh.start_time >= CURRENT_DATE - 10
  AND qh.query_tag IS DISTINCT FROM ''
  AND PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR LIKE '/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events%' -- single customer view
  AND qh.query_type IN (
                        'CREATE',
                        'CREATE_TABLE_AS_SELECT',
                        'CREATE_VIEW',
                        'DELETE',
                        'INSERT',
                        'MERGE',
                        'SELECT'
-- 'ALTER',
-- 'ALTER_SESSION',
-- 'CREATE_TABLE',
-- 'COMMIT',
-- 'DROP',
-- 'ROLLBACK',
-- 'UNKNOWN',
-- 'USE',
    )
;


WITH scv_queries AS (
    SELECT
        PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR,
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
        *
    FROM snowflake.account_usage.query_history qh
    WHERE qh.user_name = 'PIPELINERUNNER'
      AND qh.start_time >= CURRENT_DATE - 30
      AND qh.query_tag IS DISTINCT FROM ''
      AND PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR LIKE '/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events%' -- single customer view
      AND qh.query_type IN (
                            'CREATE',
                            'CREATE_TABLE_AS_SELECT',
                            'CREATE_VIEW',
                            'DELETE',
                            'INSERT',
                            'MERGE',
                            'SELECT'
        )
)
SELECT
    sq.start_time::DATE,
    SUM(sq.estimated_credits)          AS estimated_credit_usage,
    SUM(sq.total_elapsed_time_minutes) AS estimated_elapsed_minutes
FROM scv_queries sq
GROUP BY 1
;


SELECT
    mdh.service_type,
    mdh.usage_date,
    mdh.credits_used_compute,
    mdh.credits_used_cloud_services,
    mdh.credits_used,
    mdh.credits_adjustment_cloud_services,
    mdh.credits_billed,
    mdh.credits_billed * 2.18 AS compute_cost
FROM snowflake.account_usage.metering_daily_history mdh;


SELECT *
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.query_group = 'warehouse jobs'
  AND s.query_text LIKE '%data_vault_mvp.dwh.trimmed_event_stream%';


SELECT
    start_time::DATE     AS day,
    LEFT(query_text, 75) AS query_text,
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
        END              AS estimated_credits,
    *
FROM collab.muse.snowflake_query_history_v2
WHERE query_text LIKE '%data_vault_mvp.dwh.trimmed_event_stream%'



SELECT
    mdh.service_type,
    mdh.usage_date,
    mdh.credits_used_compute,
    mdh.credits_used_cloud_services,
    mdh.credits_used,
    mdh.credits_adjustment_cloud_services,
    mdh.credits_billed,
    mdh.credits_billed * 2.18 AS compute_cost
FROM snowflake.account_usage.metering_daily_history mdh;

------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.start_time::DATE = '2023-01-19'
  AND s.query_group = 'other'
ORDER BY cost__query_duration DESC;



SELECT
    DATE_TRUNC(MONTH, mdh.usage_date) AS month,
    SUM(mdh.credits_used)             AS credits_used
FROM snowflake.account_usage.metering_daily_history mdh
GROUP BY 1
;


WITH storage AS (
    SELECT
        su.usage_date,
        su.storage_bytes,
        su.storage_bytes / POWER(1024, 4)         AS storage_tb,
        su.storage_bytes * 23 / POWER(1024, 4)    AS storage_cost,
        su.stage_bytes,
        su.stage_bytes / POWER(1024, 4)           AS stage_tb,
        su.stage_bytes * 23 / POWER(1024, 4)      AS stage_cost,
        su.failsafe_bytes,
        su.failsafe_bytes / POWER(1024, 4)        AS failsafe_tb,
        su.failsafe_bytes * 23 / POWER(1024, 4)   AS failsafe_cost,
        storage_tb + failsafe_tb + stage_tb       AS total_tb,
        storage_cost + stage_cost + failsafe_cost AS total_cost
    FROM snowflake.account_usage.storage_usage su
)
SELECT
    DATE_TRUNC(MONTH, su.usage_date) AS month,
    AVG(su.total_tb)                 AS total_tb,
    AVG(su.total_cost)               AS total_cost
FROM storage su
GROUP BY 1;


-- compute
SELECT
    DATE_TRUNC(MONTH, mdh.usage_date) AS month,
    SUM(mdh.credits_used)             AS credits_used
FROM snowflake.account_usage.metering_daily_history mdh
GROUP BY 1
;

-- storage
WITH storage AS (
    SELECT
        su.usage_date,
        su.storage_bytes,
        su.storage_bytes / POWER(1024, 4)         AS storage_tb,
        su.storage_bytes * 23 / POWER(1024, 4)    AS storage_cost,
        su.stage_bytes,
        su.stage_bytes / POWER(1024, 4)           AS stage_tb,
        su.stage_bytes * 23 / POWER(1024, 4)      AS stage_cost,
        su.failsafe_bytes,
        su.failsafe_bytes / POWER(1024, 4)        AS failsafe_tb,
        su.failsafe_bytes * 23 / POWER(1024, 4)   AS failsafe_cost,
        storage_tb + failsafe_tb + stage_tb       AS total_tb,
        storage_cost + stage_cost + failsafe_cost AS total_cost
    FROM snowflake.account_usage.storage_usage su
)
SELECT
    DATE_TRUNC(MONTH, su.usage_date) AS month,
    AVG(su.total_tb)                 AS total_tb,
    AVG(su.total_cost)               AS total_cost
FROM storage su
GROUP BY 1;


SELECT *
FROM collab.muse.snowflake_query_history_v2 s
SELECT GET_DDL('table', 'collab.muse.snowflake_query_history_v2');



SELECT *
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.start_time >= CURRENT_DATE - 1
  AND s.role_name = 'PERSONAL_ROLE__TABLEAU'
  AND LOWER(s.query_text_full) LIKE '%harmonised_sale_calendar_view_snapshot%';


SELECT *
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.start_time >= CURRENT_DATE - 1
  AND s.role_name = 'PERSONAL_ROLE__TABLEAU'
  AND s.warehouse_group = 'DATA_SCIENCE'
;