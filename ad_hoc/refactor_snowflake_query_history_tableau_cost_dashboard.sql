SELECT *
FROM dbt.bi_data_platform.dp_athena_new_flash_deal_category;


SELECT GET_DDL('table', 'collab.muse.SNOWFLAKE_QUERY_HISTORY');

/*
create or replace view SNOWFLAKE_QUERY_HISTORY(
	COST__QUERY_DURATION,
	CREDITS_USED__QUERY_DURATION,
	COST__CREDITS_USED_CLOUD_SERVICES,
	PER_CREDIT_COST,
	QUERY_GROUP,
	QUERY_TEXT,
	QUERY_TEXT_FULL,
	CREDITS_PER_HOUR,
	TOTAL_ELAPSED_TIME_SEC,
	WAREHOUSE_GROUP,
	ROLE_GROUP,
	ROLE_NAME,
	USER_NAME,
	WAREHOUSE_NAME,
	WAREHOUSE_TYPE,
	WAREHOUSE_SIZE,
	TOTAL_ELAPSED_TIME,
	START_TIME,
	END_TIME,
	CREDITS_USED_CLOUD_SERVICES,
	EXECUTION_STATUS,
	TEAM,
	POSITION,
	QUERY_TAG
) as
select
    (
      (credits_per_hour * per_credit_cost) / (60 * 60) -- per second cost
    ) * total_elapsed_time_sec as cost__query_duration,
    (
      credits_per_hour / (60 * 60) -- per second credits
    ) * total_elapsed_time_sec as credits_used__query_duration,
    credits_used_cloud_services * per_credit_cost AS cost__credits_used_cloud_services,
    *
from (
    select
      2.18::decimal(10,2) AS per_credit_cost, -- cost per credit

        case
            WHEN LOWER(query_text) LIKE ('%create or replace view hygiene_vault_mvp.snowplow.event_stream__step01__get_source_batch%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table hygiene_vault_mvp.snowplow.event_stream__step02__missing_bookings%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table hygiene_vault_mvp.snowplow.event_stream__step03__replicate_event_data%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into hygiene_vault_mvp.snowplow.event_stream%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_unique_urls%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_unique_urls%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_identity_associations%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_identity_associations%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%data_vault_mvp.single_customer_view_stg.module_identity_associations%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%data_vault_mvp.single_customer_view_stg.module_identity_associations%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%data_vault_mvp.single_customer_view_stg.module_identity_associations%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%data_vault_mvp.single_customer_view_stg.module_identity_associations%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_url_params%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_url_params%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_url_hostname%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_url_hostname as target%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_identity_stitching%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_identity_stitching%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_extracted_params%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_extracted_params as target%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_touchifiable_events%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_touchification%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_touchifiable_events%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%insert into data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_time_diff_marker%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%insert into data_vault_mvp.single_customer_view_stg.module_time_diff_marker%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_touchification%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_touched_searches%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_touched_searches__model_data%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace view data_vault_mvp.single_customer_view_stg.module_touched_searches__filter_empty_searches%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_touched_searches%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_touched_transactions%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_touched_transactions%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%data_vault_mvp.single_customer_view_stg.module_touched_transactions%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_touched_spvs%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_touched_spvs%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%delete from data_vault_mvp.single_customer_view_stg.module_touched_spvs%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracke%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%delete from data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone__model_agg_values%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone__model_basic_attributes%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone__model_current_anomalous_attributed_users%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone__model_data%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg_bak.module_touch_basic_attributes__%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%alter table data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone swap with data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone;%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg_bak.module_touch_basic_attributes__%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone__model_agg_values;%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone__model_basic_attributes;%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone__model_data;%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_clone__model_current_anomalous_attributed_users;%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%delete from data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%alter table data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer_clone swap with data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer_clone;%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg_bak.module_touch_utm_referrer__%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%delete from data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.affiliate_snapshot%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_clone__model_data%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg_bak.module_touch_marketing_channel__%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%alter table data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_clone;%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg_bak.module_touch_marketing_channel__%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg.affiliate_snapshot;%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_clone__model_data;%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.module_touch_attribution%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.module_touch_attribution_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%delete from data_vault_mvp.single_customer_view_stg.module_touch_attribution_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.module_touch_attribution_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg_bak.module_touch_attribution__%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%alter table data_vault_mvp.single_customer_view_stg.module_touch_attribution_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg.module_touch_attribution_clone%') THEN 'single_customer_view'
            WHEN LOWER(query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg_bak.module_touch_attribution__%') THEN 'single_customer_view'
            when lower(query_text) like '%sum("active user%' then 'tableau_analytical'
            when lower(query_text) like '%sum("sessions%' then 'tableau_analytical'
            when lower(query_text) like '%"active user base"%' then 'tableau_analytical'
            when lower(query_text) like '%"data"."user_segmentation" "user_segmentation"%' then 'tableau_analytical'
            when lower(query_text) like '%"tdy:date:ok"%' then 'tableau_analytical'
            when lower(query_text) like '%"custom sql query"%' then 'tableau_analytical'
            when lower(query_text) like '%) "sessions"%' then 'tableau_analytical'
            when role_name LIKE 'PERSONAL_ROLE__TABLEAU' then 'tableau_analytical'
            when lower(query_text) like '%grant %' then 'uac'
            when lower(query_text) like '%revoke %' then 'uac'
            when lower(query_text) like '%describe %' then 'admin'
            when lower(query_text) like 'show %' then 'admin'
            when lower(query_text) like '%information_schema.%' then 'admin'
            when lower(query_text) like '%snowflake.account_usage.%' then 'admin'
            when lower(query_text) like '%collab.muse.snowflake_%' then 'admin'
            when lower(query_text) like '%fails_quality_control%' then 'assertions'
            when lower(query_text) like '%fails___%' then 'assertions'
            when lower(query_text) like '%pipeline_gsheets.snowflake%' then 'uac'
            when lower(query_text) like '%call scratch.robinpatel.backfill_active_users_loop%' then 'active_user_base'
            when lower(query_text) like '%insert into se_dev_robin.data.active_user_base%' then 'active_user_base'
            when lower(query_text) like '%insert into collab.muse_data_modelling.active_user_base%' then 'active_user_base'
            when lower(query_text) like '%se.data.active_user_base%' then 'active_user_base'
            when lower(query_text) like '%se_dev_robin.data.active_user_base%' then 'active_user_base'
            when lower(query_text) like '%se.data.user_segmentation%' then 'user_segmentation'
            when lower(query_text) like '%single_customer_view%' then 'single_customer_view'
            when lower(query_text) like '%snowplow.event_stream%' then 'single_customer_view'
            when lower(query_text) like '%data_vault_mvp.engagement_stg%' then 'engagement_stg.user_snapshot'
            when lower(query_text) like '%data_vault.engagement_stg%' then 'engagement_stg.user_snapshot'
            when lower(query_text) like '%se.data.scv_%' then 'single_customer_view'
            when lower(query_text) like '%se.data%' then 'se.data other'
            when lower(query_text) like '%customer_model%' then 'customer_model'
            when lower(query_text) like '%data_vault_mvp.dwh.%' then 'dwh'
            when lower(query_text) like '%data_vault_mvp.%' then 'other data_vault'
            when lower(query_text) like '%hygiene_vault_mvp.%' then 'hygiene_vault'
            when lower(query_text) like '%hygiene_snapshot_vault_mvp.%' then 'hygiene_vault'
            when lower(query_text) like '%dev_robin%' then 'modelling_dev'
            when lower(query_text) like '%dev_andy%' then 'modelling_dev'
            when lower(query_text) like '%file format%' then 'regular jobs'
            when lower(query_text) like '%stage%' then 'regular jobs'
            when lower(query_text) like '%insert into raw_vault.%' then 'regular jobs'
            when lower(query_text) like '%insert into raw_vault_mvp.%' then 'regular jobs'
            when lower(query_text) like '%create or replace transient table raw_vault_mvp.%' then 'regular jobs'
            when lower(query_text) like '%create or replace transient table raw_vault.%' then 'regular jobs'
            when lower(query_text) like '%truncate table if exists raw_vault_mvp.%' then 'regular jobs'
            when lower(query_text) like '%create  table if not exists raw_vault.%' then 'regular jobs'
            when lower(query_text) like '%create table if not exists raw_vault_mvp.%' then 'regular jobs'
            when role_name = 'PIPELINERUNNER' and lower(query_text) like '%count(*)%' then 'regular jobs'
            when lower(query_text) like '%delete from raw_vault.%' then 'regular jobs'
            when lower(query_text) like '%delete from raw_vault_mvp.%' then 'regular jobs'
            when lower(query_text) like '%drop table if exists raw_vault_mvp.raw__%' then 'regular jobs'
            when lower(query_text) like '%drop table if exists raw_vault.raw__%' then 'regular jobs'
            when lower(query_text) like '%raw_vault%add primary key%' then 'regular jobs'
            when lower(query_text) like '%unload_vault_mvp%' then 'regular jobs'
            when lower(query_text) like '%se-data-science%' then 'data science'
            when lower(query_text) like '%data_science.%' then 'data science'
            when lower(query_text) like '%merge into%' then 'other merge into'
            when lower(query_text) like '%insert into%' then 'other insert into'
            when lower(query_text) like '%create or replace view%' then 'views'
            when lower(query_text) in ('commit', 'rollback') then 'system'
            when lower(query_text) like '%plain returns%' then 'system'
            when lower(query_text) like '%unicode returns%' then 'system'
            when lower(query_text) like '%desc table /*%' then 'system'
            else 'other'
        end as query_group,

      left(query_text, 100) AS query_text,
      query_text AS query_text_full,
      CASE warehouse_size
                WHEN 'X-Small' THEN 1
                WHEN 'Small' THEN 2
                WHEN 'Medium' THEN 4
                WHEN 'Large' THEN 8
                WHEN 'X-Large' THEN 16
                WHEN '2X-Large' THEN 32
                WHEN '3X-Large' THEN 64
                WHEN '4X-Large' THEN 128
                ELSE 0
                END  as credits_per_hour,
      total_elapsed_time / 1000 as total_elapsed_time_sec,
      case
          when warehouse_name LIKE 'DATA_SCIENCE_PIPE%' THEN 'DATA_SCIENCE'
          when warehouse_name LIKE 'MARKETING_PIPE%' THEN 'MARKETING'
          when warehouse_name = 'SNOWPLOW_WH' THEN 'SNOWPLOW'
          when warehouse_name = 'PIPE_4XLARGE' THEN 'DATA_PLATFORM_MODELLING'
          when warehouse_name = 'PIPE_2XLARGE' THEN 'DATA_PLATFORM_MODELLING'
          when warehouse_name = 'PIPE_XLARGE' THEN 'DATA_PLATFORM_MODELLING'
          when warehouse_name = 'PIPE_LARGE' THEN 'DATA_PLATFORM_MODELLING'
          when warehouse_name = 'PIPE_HYGIENE_LARGE'  THEN 'DATA_PLATFORM_HYGIENE_LARGE'
          when warehouse_name = 'PIPE_MEDIUM' THEN 'DATA_PLATFORM_MODELLING'
          when warehouse_name = 'PIPE_XSMALL' THEN 'DATA_PLATFORM_DEFAULT'
          when warehouse_name = 'PIPE_DEFAULT' THEN 'DATA_PLATFORM_DEFAULT'
          else 'DATA_PLATFORM_OTHERS'
      end as warehouse_group,
      case
        when role_name IN ('PERSONAL_ROLE__ROBINPATEL', 'PERSONAL_ROLE__TABLEAU') then role_name
        when role_name LIKE 'PERSONAL_ROLE__%' then 'PERSONAL ROLES'
        when role_name = 'PIPELINERUNNER' then 'PIPELINERUNNER'
        when role_name = 'DATASCIENCERUNNER' then 'DATASCIENCERUNNER'
        when role_name = 'DATASCIENCEAPI' then 'DATASCIENCEAPI'
        when role_name = 'SECURITYADMIN' then 'SECURITYADMIN'
        else 'OTHER ROLES'
      end as role_group,
      role_name,
      user_name,
      warehouse_name,
      warehouse_type,
      warehouse_size,
      total_elapsed_time,
      start_time,
      end_time,
      credits_used_cloud_services,
      execution_status,
      COALESCE(u.team,'unclassified') as team,
      u.position,
    qh.query_tag
    from snowflake.account_usage.query_history qh
    LEFT JOIN (SELECT * FROM raw_vault_mvp.snowflake_uac.users
QUALIFY ROW_NUMBER() OVER (PARTITION BY snowflake_user ORDER BY loaded_at DESC) = 1) u ON 'PERSONAL_ROLE__' || upper(u.snowflake_user) = qh.role_name
    -- from first day of previous month
    where start_time >= dateadd(month, -12, date_trunc('month', current_date()))
);
*/
 */

CREATE OR REPLACE VIEW collab.muse.snowflake_query_history_v2 AS
WITH dedupe_uac AS (
    SELECT *
    FROM raw_vault_mvp.snowflake_uac.users u
        QUALIFY ROW_NUMBER() OVER (PARTITION BY u.snowflake_user ORDER BY u.loaded_at DESC) = 1
)
SELECT

    --cost calcs
    2.18::decimal(10, 2)                             AS per_credit_cost, -- cost per credit

    CASE qh.warehouse_size
        WHEN 'X-Small' THEN 1
        WHEN 'Small' THEN 2
        WHEN 'Medium' THEN 4
        WHEN 'Large' THEN 8
        WHEN 'X-Large' THEN 16
        WHEN '2X-Large' THEN 32
        WHEN '3X-Large' THEN 64
        WHEN '4X-Large' THEN 128
        ELSE 0
        END                                          AS credits_per_hour,
    qh.total_elapsed_time / 1000                     AS total_elapsed_time_sec,

    ((credits_per_hour * per_credit_cost) / (60 * 60)) -- per second cost
        * total_elapsed_time_sec                     AS cost__query_duration,

    (credits_per_hour / (60 * 60)) -- per second credits
        * total_elapsed_time_sec                     AS credits_used__query_duration,

    qh.credits_used_cloud_services * per_credit_cost AS cost__credits_used_cloud_services,

    qh.query_type,
    --query text grouping
    CASE

        WHEN qh.user_name = 'SNOWPLOW' THEN 'snowplow'

        --single customer view
        WHEN LOWER(qh.query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.%') THEN 'single_customer_view'
        WHEN LOWER(qh.query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.%') THEN 'single_customer_view'
        WHEN LOWER(qh.query_text) LIKE ('%create or replace view hygiene_vault_mvp.snowplow.%') THEN 'single_customer_view'
        WHEN LOWER(qh.query_text) LIKE ('%create or replace transient table hygiene_vault_mvp.snowplow.%') THEN 'single_customer_view'
        WHEN LOWER(qh.query_text) LIKE ('%merge into hygiene_vault_mvp.snowplow.event_stream%') THEN 'single_customer_view'
        WHEN LOWER(qh.query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.%') THEN 'single_customer_view'
        WHEN LOWER(qh.query_text) LIKE ('%insert into data_vault_mvp.single_customer_view_stg.%') THEN 'single_customer_view'
        WHEN LOWER(qh.query_text) LIKE ('%create or replace view data_vault_mvp.single_customer_view_stg.module_touched_searches__filter_empty_searches%') THEN 'single_customer_view'
        WHEN LOWER(qh.query_text) LIKE ('%delete from data_vault_mvp.single_customer_view_stg.%') THEN 'single_customer_view'
        WHEN LOWER(qh.query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg_bak.%') THEN 'single_customer_view'
        WHEN LOWER(qh.query_text) LIKE ('%alter table data_vault_mvp.single_customer_view_stg.%') THEN 'single_customer_view'
        WHEN LOWER(qh.query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg.%') THEN 'single_customer_view'
        WHEN LOWER(qh.query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg_bak.%') THEN 'single_customer_view'

        --tableau
        WHEN qh.role_name LIKE 'PERSONAL_ROLE__TABLEAU' THEN 'tableau_analytical'

        --uac
        WHEN LOWER(qh.query_text) LIKE '%grant %' THEN 'uac'
        WHEN LOWER(qh.query_text) LIKE '%revoke %' THEN 'uac'
        WHEN LOWER(qh.query_text) LIKE '%pipeline_gsheets.snowflake%' THEN 'uac'

        --admin
        WHEN LOWER(qh.query_text) LIKE '%describe %' THEN 'admin'
        WHEN LOWER(qh.query_text) LIKE 'show %' THEN 'admin'
        WHEN LOWER(qh.query_text) LIKE '%information_schema.%' THEN 'admin'
        WHEN LOWER(qh.query_text) LIKE '%snowflake.account_usage.%' THEN 'admin'
        WHEN LOWER(qh.query_text) LIKE '%collab.muse.snowflake_%' THEN 'admin'
        WHEN LOWER(qh.query_text) LIKE '%drop user if exists%' THEN 'admin'
        WHEN LOWER(qh.query_text) = 'use role pipelinerunner' AND qh.role_name = 'SECURITYADMIN' THEN 'admin'

        --assertions
        WHEN LOWER(qh.query_text) LIKE '%fails_quality_control%' THEN 'assertions'
        WHEN LOWER(qh.query_text) LIKE '%fails___%' THEN 'assertions'

        --data science
        WHEN LOWER(qh.query_text) LIKE '%data_vault_mvp.engagement_stg%' THEN 'engagement_stg.user_snapshot'
        WHEN LOWER(qh.query_text) LIKE '%data_vault.engagement_stg%' THEN 'engagement_stg.user_snapshot'

        WHEN LOWER(qh.query_text) LIKE '%data_science.%' THEN 'data science'
        WHEN LOWER(qh.query_text) LIKE '%se-data-science%' THEN 'data science'
        WHEN qh.role_name = 'DATASCIENCERUNNER' THEN 'data science'

        --hygiene
        WHEN LOWER(qh.query_text) LIKE '%hygiene_snapshot_vault_mvp.%' THEN 'hygiene_vault'
        WHEN LOWER(qh.query_text) LIKE '%hygiene_vault_mvp.%' THEN 'hygiene_vault'
        WHEN LOWER(qh.query_text) LIKE '%hygiene_vault.%' THEN 'hygiene_vault'

        -- personal ide
        WHEN qh.query_type = 'DESCRIBE' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'personal ide'
        WHEN LOWER(qh.query_text) LIKE 'select system$%' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'personal ide'
        WHEN LOWER(qh.query_text) LIKE 'desc function%' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'personal ide'
        WHEN LOWER(qh.query_text) LIKE 'use role "%' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'personal ide'
        WHEN LOWER(qh.query_text) LIKE 'select current_available_roles()%' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'personal ide'
        WHEN LOWER(qh.query_text) LIKE 'select 1%' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'personal ide'

        --modelling dev
        WHEN LOWER(qh.query_text) LIKE '%_dev_%' THEN 'warehouse development'

        --regular jobs
        WHEN LOWER(qh.query_text) LIKE '%file format%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%stage%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%insert into raw_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%insert into raw_vault_mvp.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%insert into data_vault_mvp.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create schema if not exists%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create or replace transient table raw_vault_mvp.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create or replace transient table raw_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create or replace transient table hygiene_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create or replace transient table latest_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create or replace table raw_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create table if not exists raw_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create table if not exists hygiene_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create table if not exists latest_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%truncate table if exists raw_vault_mvp.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create table if not exists raw_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create table if not exists raw_vault_mvp.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create table if not exists hygiene_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create table if not exists hygiene_vault_mvp.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create table if not exists latest_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%create table if not exists hygiene_snapshot_vault_mvp.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%update raw_vault_mvp.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%count(*)%' AND qh.role_name = 'PIPELINERUNNER' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%delete from raw_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%delete from raw_vault_mvp.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%drop table if exists raw_vault_mvp.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%drop table if exists raw_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%drop table if exists hygiene_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%drop table if exists latest_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%drop view if exists raw_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%drop view if exists latest_vault.%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%raw_vault%add primary key%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%unload_vault_mvp%' THEN 'regular jobs'
        WHEN LOWER(qh.query_text) LIKE '%merge into latest_vault.%' THEN 'regular jobs'
        WHEN qh.role_name = 'WORKSHEETS_APP_RL' THEN 'regular jobs'


        WHEN LOWER(qh.query_text) LIKE '%create view if not exists data_vault_mvp.%' THEN 'warehouse jobs'
        WHEN LOWER(qh.query_text) LIKE '%insert into data_vault_mvp.%' THEN 'warehouse jobs'
        WHEN LOWER(qh.query_text) LIKE '%create or replace transient table data_vault_mvp.%' THEN 'warehouse jobs'
        WHEN LOWER(qh.query_text) LIKE '%create table if not exists data_vault_mvp.%' THEN 'warehouse jobs'
        WHEN LOWER(qh.query_text) LIKE '%create or replace table data_vault_mvp.%' THEN 'warehouse jobs'
        WHEN LOWER(qh.query_text) LIKE '%drop table data_vault_mvp.%' THEN 'warehouse jobs'
        WHEN LOWER(qh.query_text) LIKE '%drop view if exists data_vault_mvp.%' THEN 'warehouse jobs'
        WHEN LOWER(qh.query_text) LIKE '%merge into data_vault_mvp.%' THEN 'warehouse jobs'
        WHEN LOWER(qh.query_text) LIKE '%drop table if exists data_vault_mvp.%' THEN 'warehouse jobs'
        WHEN LOWER(qh.query_text) LIKE '%drop view if exists data_vault_mvp.%' THEN 'warehouse jobs'

        WHEN LOWER(qh.query_text) LIKE '%create or replace view%' THEN 'views'

        WHEN LOWER(qh.query_text) IN ('commit', 'rollback') THEN 'system'
        WHEN LOWER(qh.query_text) LIKE '%plain returns%' THEN 'system'
        WHEN LOWER(qh.query_text) LIKE '%unicode returns%' THEN 'system'
        WHEN LOWER(qh.query_text) LIKE '%desc table /*%' THEN 'system'
        WHEN LOWER(qh.query_text) LIKE '%use database%' THEN 'system'
        WHEN LOWER(qh.query_text) LIKE '%use warehouse%' THEN 'system'
        WHEN LOWER(qh.query_text) LIKE '%PUT%' AND qh.role_name = 'PIPELINERUNNER' THEN 'system'
        WHEN LOWER(qh.query_text) LIKE 'select current_available_roles()%' AND qh.role_name = 'PUBLIC' THEN 'system'
        WHEN LOWER(qh.query_text) LIKE 'select system$list_outbound_shares_details();%' AND qh.role_name = 'PUBLIC' THEN 'system'


        --user query
        WHEN LOWER(qh.query_text) LIKE 'select%' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'user query'
        WHEN LOWER(qh.query_text) LIKE 'with%' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'user query'
        WHEN LOWER(qh.query_text) LIKE 'set%' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'user query'

        --shared role
        WHEN qh.role_name = 'SE_BASIC' THEN 'shared role'

        WHEN LOWER(qh.query_text) LIKE '%merge into%' THEN 'other merge into'
        WHEN LOWER(qh.query_text) LIKE '%insert into%' THEN 'other insert into'

        --pipeline catch all
        WHEN qh.role_name = 'PIPELINERUNNER' THEN 'regular jobs'

        ELSE 'other'
        END                                          AS query_group,


    LEFT(qh.query_text, 100)                         AS query_text,
    qh.query_text                                    AS query_text_full,

    --warehouse grouping
    CASE
        WHEN qh.warehouse_name LIKE 'DATA_SCIENCE_PIPE%' THEN 'DATA_SCIENCE'
        WHEN qh.warehouse_name LIKE 'MARKETING_PIPE%' THEN 'MARKETING'
        WHEN qh.warehouse_name = 'SNOWPLOW_WH' THEN 'SNOWPLOW'
        WHEN qh.warehouse_name LIKE 'DBT%' THEN 'DBT'
        WHEN qh.warehouse_name LIKE 'TABLEAU%' THEN 'TABLEAU'
        WHEN qh.warehouse_name
            IN ('PIPE_4XLARGE',
                'PIPE_4XLARGE',
                'PIPE_2XLARGE',
                'PIPE_XLARGE',
                'PIPE_LARGE',
                'PIPE_MEDIUM') THEN 'DATA_PLATFORM_MODELLING'
        WHEN qh.warehouse_name = 'PIPE_HYGIENE%' THEN 'DATA_PLATFORM_HYGIENE'
        WHEN qh.warehouse_name = 'PIPE_XSMALL' THEN 'DATA_PLATFORM_DEFAULT'
        WHEN qh.warehouse_name = 'PIPE_DEFAULT' THEN 'DATA_PLATFORM_DEFAULT'
        ELSE 'DATA_PLATFORM_OTHERS'
        END                                          AS warehouse_group,

    --role grouping
    CASE
        WHEN qh.role_name
            IN ('PIPELINERUNNER',
                'DATASCIENCERUNNER',
                'DATASCIENCEAPI',
                'SECURITYADMIN',
                'PERSONAL_ROLE__TABLEAU', 'TABLEAU') THEN qh.role_name
        WHEN qh.role_name IN ('DBT_ANALYST', 'DBT_PRODUCTION') THEN 'DBT'
        WHEN qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'PERSONAL ROLES'
        ELSE 'OTHER ROLES'
        END                                          AS role_group,

    qh.role_name,
    qh.user_name,
    qh.warehouse_name,
    qh.warehouse_type,
    qh.warehouse_size,
    qh.total_elapsed_time,
    qh.start_time,
    qh.end_time,
    qh.credits_used_cloud_services,
    qh.execution_status,
    COALESCE(u.team, 'unclassified')                 AS team,
    u.position,
    qh.query_tag
FROM snowflake.account_usage.query_history qh
    LEFT JOIN dedupe_uac u ON 'PERSONAL_ROLE__' || UPPER(u.snowflake_user) = qh.role_name
    -- from first day of previous month
WHERE qh.start_time >= DATEADD(MONTH, -12, DATE_TRUNC('month', CURRENT_DATE()))
  -- TODO remove
  AND query_group NOT IN (
                          'regular jobs',
                          'system',
                          'hygiene_vault',
                          'admin',
                          'uac',
                          'assertions',
                          'tableau_analytical',
                          'personal ide',
                          'data science',
                          'warehouse jobs',
                          'snowplow',
                          'single_customer_view'
    )
;



SELECT *
FROM collab.muse.snowflake_query_history_v2 sqh2
WHERE query_group = 'warehouse development'
  AND sqh2.start_time BETWEEN CURRENT_DATE - 10 AND CURRENT_DATE
AND sqh2.user_name = 'DARSHANASRIDHAR';


