SELECT
	su.usage_date,
	su.storage_bytes,
	su.storage_bytes / POWER(1024, 4)       AS storage_tb,
	su.storage_bytes * 23 / POWER(1024, 4)  AS storage_cost,
	su.stage_bytes,
	su.stage_bytes / POWER(1024, 4)         AS stage_tb,
	su.stage_bytes * 23 / POWER(1024, 4)    AS stage_cost,
	su.failsafe_bytes,
	su.failsafe_bytes / POWER(1024, 4)      AS failsafe_tb,
	su.failsafe_bytes * 23 / POWER(1024, 4) AS failsafe_cost
FROM snowflake.account_usage.storage_usage su
;

SELECT
	TO_CHAR(usage_date, 'YYYYMM')   AS sort_month,
	TO_CHAR(usage_date, 'Mon-YYYY') AS month,
	AVG(storage_bytes)              AS storage,
	AVG(stage_bytes)                AS stage,
	AVG(failsafe_bytes)             AS failsafe
FROM snowflake.account_usage.storage_usage
GROUP BY month, sort_month
ORDER BY sort_month
;


SELECT *
FROM snowflake.account_usage.table_storage_metrics tsm
WHERE tsm.table_catalog = 'DATA_VAULT_MVP'
ORDER BY active_bytes DESC
;

SELECT *
FROM data_vault_mvp.information_schema.tables
;

SELECT *
FROM scratch.information_schema.tables
WHERE table_schema = 'NIROSHANBALAKUMAR'
;

USE ROLE pipelinerunner
;

USE WAREHOUSE pipe_xlarge
;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage', 'scratch.niroshanbalakumar.activity,
scratch.niroshanbalakumar.ash_sf_filter,
scratch.niroshanbalakumar.csat_members,
scratch.niroshanbalakumar.members,
scratch.niroshanbalakumar.member_engagement,
scratch.niroshanbalakumar.member_summary,
scratch.niroshanbalakumar.sales,
scratch.niroshanbalakumar.sales_booked_full,
scratch.niroshanbalakumar.uk_spvs_v2,
scratch.niroshanbalakumar.unique_members,
scratch.niroshanbalakumar.import_test_15072020,
scratch.niroshanbalakumar.member_activity,
scratch.niroshanbalakumar.member_booking_history,
scratch.niroshanbalakumar.test_2,
scratch.niroshanbalakumar.activation_members,
scratch.niroshanbalakumar.csat_test_members,
scratch.niroshanbalakumar.cumulative_member_booking,
scratch.niroshanbalakumar.dach_spvs,
scratch.niroshanbalakumar.de_user_cvr,
scratch.niroshanbalakumar.sfmc_opens,
scratch.niroshanbalakumar.snowplow,
scratch.niroshanbalakumar.stiching_table,
scratch.niroshanbalakumar.test,
scratch.niroshanbalakumar.uk_booking_history,
scratch.niroshanbalakumar.uk_user_cvr,
scratch.niroshanbalakumar.de_user_cvr_v2,
scratch.niroshanbalakumar.member_spvs,
scratch.niroshanbalakumar.repeat_bookings,
scratch.niroshanbalakumar.test_3,
scratch.niroshanbalakumar.cohort_summary,
scratch.niroshanbalakumar.cumulative_member_booking_v2,
scratch.niroshanbalakumar.dach_spvs_v2,
scratch.niroshanbalakumar.engagement,
scratch.niroshanbalakumar.member_activity_cohort,
scratch.niroshanbalakumar.snowplow_activity,
scratch.niroshanbalakumar.sales_booked_v2,
scratch.niroshanbalakumar.sales_members,
scratch.niroshanbalakumar.spv_bucketing,
scratch.niroshanbalakumar.sfmc_clicks,
scratch.niroshanbalakumar.unique_member_activity,
scratch.niroshanbalakumar.cms_booking_activity,
scratch.niroshanbalakumar.cm_region_mapping,
scratch.niroshanbalakumar.count_,
scratch.niroshanbalakumar.cumulative_member_booking_history,
scratch.niroshanbalakumar.customer_data,
scratch.niroshanbalakumar.dach_session_bucketing,
scratch.niroshanbalakumar.final_table,
scratch.niroshanbalakumar.sales_booked,
scratch.niroshanbalakumar.sales_booked_full_v2,
scratch.niroshanbalakumar.spv_activity,
scratch.niroshanbalakumar.import_test_2_15072020,
scratch.niroshanbalakumar.nps,
scratch.niroshanbalakumar.sessions_table'
	 )
;

SELECT *
FROM scratch.robinpatel.table_usage
;

CALL data_vault_mvp.dwh.table_reference_in_view('scratch.robinpatel.table_reference_in_view', 'scratch.niroshanbalakumar.activity,
scratch.niroshanbalakumar.ash_sf_filter,
scratch.niroshanbalakumar.csat_members,
scratch.niroshanbalakumar.members,
scratch.niroshanbalakumar.member_engagement,
scratch.niroshanbalakumar.member_summary,
scratch.niroshanbalakumar.sales,
scratch.niroshanbalakumar.sales_booked_full,
scratch.niroshanbalakumar.uk_spvs_v2,
scratch.niroshanbalakumar.unique_members,
scratch.niroshanbalakumar.import_test_15072020,
scratch.niroshanbalakumar.member_activity,
scratch.niroshanbalakumar.member_booking_history,
scratch.niroshanbalakumar.test_2,
scratch.niroshanbalakumar.activation_members,
scratch.niroshanbalakumar.csat_test_members,
scratch.niroshanbalakumar.cumulative_member_booking,
scratch.niroshanbalakumar.dach_spvs,
scratch.niroshanbalakumar.de_user_cvr,
scratch.niroshanbalakumar.sfmc_opens,
scratch.niroshanbalakumar.snowplow,
scratch.niroshanbalakumar.stiching_table,
scratch.niroshanbalakumar.test,
scratch.niroshanbalakumar.uk_booking_history,
scratch.niroshanbalakumar.uk_user_cvr,
scratch.niroshanbalakumar.de_user_cvr_v2,
scratch.niroshanbalakumar.member_spvs,
scratch.niroshanbalakumar.repeat_bookings,
scratch.niroshanbalakumar.test_3,
scratch.niroshanbalakumar.cohort_summary,
scratch.niroshanbalakumar.cumulative_member_booking_v2,
scratch.niroshanbalakumar.dach_spvs_v2,
scratch.niroshanbalakumar.engagement,
scratch.niroshanbalakumar.member_activity_cohort,
scratch.niroshanbalakumar.snowplow_activity,
scratch.niroshanbalakumar.sales_booked_v2,
scratch.niroshanbalakumar.sales_members,
scratch.niroshanbalakumar.spv_bucketing,
scratch.niroshanbalakumar.sfmc_clicks,
scratch.niroshanbalakumar.unique_member_activity,
scratch.niroshanbalakumar.cms_booking_activity,
scratch.niroshanbalakumar.cm_region_mapping,
scratch.niroshanbalakumar.count_,
scratch.niroshanbalakumar.cumulative_member_booking_history,
scratch.niroshanbalakumar.customer_data,
scratch.niroshanbalakumar.dach_session_bucketing,
scratch.niroshanbalakumar.final_table,
scratch.niroshanbalakumar.sales_booked,
scratch.niroshanbalakumar.sales_booked_full_v2,
scratch.niroshanbalakumar.spv_activity,
scratch.niroshanbalakumar.import_test_2_15072020,
scratch.niroshanbalakumar.nps,
scratch.niroshanbalakumar.sessions_table', 'collab, data_vault_mvp, se, scratch')
;

SELECT *
FROM scratch.robinpatel.table_reference_in_view
;


SELECT *
FROM snowflake.account_usage.table_storage_metrics tsm
WHERE tsm.table_catalog = 'COLLAB'
ORDER BY active_bytes DESC
;

SELECT *
FROM collab.information_schema.tables t
WHERE t.table_type = 'BASE TABLE'
  AND t.table_schema = 'DEMAND_SHARED_TABLES'
;

USE ROLE pipelinerunner
;

USE WAREHOUSE pipe_xlarge
;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage', 'collab.demand_shared_tables.first_booking_of_the_year_gross,
collab.demand_shared_tables.first_booking_of_the_year_gross_channel,
collab.demand_shared_tables.first_booking_of_the_year_gross_channelplus1,
collab.demand_shared_tables.first_booking_of_the_year_net,
collab.demand_shared_tables.first_booking_of_the_year_net_channel,
collab.demand_shared_tables.lnd_dach_channel_web_metrics,
collab.demand_shared_tables.lnd_uk_channel_booking_metrics,
collab.demand_shared_tables.lnd_uk_channel_web_metrics,
collab.demand_shared_tables.member_email_metrics19,
collab.demand_shared_tables.member_email_metrics19b,
collab.demand_shared_tables.member_email_metrics19c,
collab.demand_shared_tables.member_email_metrics20,
collab.demand_shared_tables.member_email_metrics21,
collab.demand_shared_tables.first_touch_of_the_year_channel,
collab.demand_shared_tables.monthly_active_users_segments,
collab.demand_shared_tables.lnd_dach_channel_booking_metrics')
;

SELECT *
FROM scratch.robinpatel.table_usage
;

CALL data_vault_mvp.dwh.table_reference_in_view('scratch.robinpatel.table_reference_in_view', 'collab.demand_shared_tables.first_booking_of_the_year_gross,
collab.demand_shared_tables.first_booking_of_the_year_gross_channel,
collab.demand_shared_tables.first_booking_of_the_year_gross_channelplus1,
collab.demand_shared_tables.first_booking_of_the_year_net,
collab.demand_shared_tables.first_booking_of_the_year_net_channel,
collab.demand_shared_tables.lnd_dach_channel_web_metrics,
collab.demand_shared_tables.lnd_uk_channel_booking_metrics,
collab.demand_shared_tables.lnd_uk_channel_web_metrics,
collab.demand_shared_tables.member_email_metrics19,
collab.demand_shared_tables.member_email_metrics19b,
collab.demand_shared_tables.member_email_metrics19c,
collab.demand_shared_tables.member_email_metrics20,
collab.demand_shared_tables.member_email_metrics21,
collab.demand_shared_tables.first_touch_of_the_year_channel,
collab.demand_shared_tables.monthly_active_users_segments,
collab.demand_shared_tables.lnd_dach_channel_booking_metrics', 'collab, data_vault_mvp, se, scratch')
;

SELECT *
FROM scratch.robinpatel.table_reference_in_view
;


SELECT
	ROUND(active_bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM snowflake.account_usage.table_storage_metrics tsm
WHERE tsm.table_catalog = 'SCRATCH'
ORDER BY active_bytes DESC
;


SELECT
	ROUND(active_bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM snowflake.account_usage.table_storage_metrics tsm
WHERE tsm.table_catalog = 'HYGIENE_VAULT_MVP'
ORDER BY active_bytes DESC
;

SELECT
	ROUND(active_bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM snowflake.account_usage.table_storage_metrics tsm
WHERE tsm.table_catalog = 'HYGIENE_SNAPSHOT_VAULT_MVP'
ORDER BY active_bytes DESC
;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.sales__step02__dedupe ss02d
;

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM data_vault_mvp.information_schema.tables
;

USE ROLE pipelinerunner
;

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM scratch.information_schema.tables
;


SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM data_vault_mvp.information_schema.tables
;


SELECT *
FROM se.bi.sale_date_spvs sds
;


SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM unload_vault_mvp.information_schema.tables
;

USE ROLE pipelinerunner
;

SELECT
	ROUND(SUM(bytes) / POWER(1024, 4), 3) AS storage_tb
FROM unload_vault_mvp.information_schema.tables
;


SELECT
	ROUND(SUM(bytes) / POWER(1024, 4), 3) AS storage_tb
FROM data_science.information_schema.tables
;

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM raw_vault.information_schema.tables
;

USE ROLE pipelinerunner
;

-- check out dev data_vault tables

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM data_vault_mvp_dev_robin.information_schema.tables

UNION ALL

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM data_vault_mvp_dev_kirsten.information_schema.tables

UNION ALL

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM data_vault_mvp_dev_gianni.information_schema.tables

UNION ALL

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM data_vault_mvp_dev_donald.information_schema.tables

UNION ALL

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM data_vault_mvp_dev_saur.information_schema.tables
;

-- DROP SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg;

-- check out dev hygiene_vault_mvp tables

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM hygiene_vault_mvp_dev_robin.information_schema.tables

UNION ALL

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM hygiene_vault_mvp_dev_kirsten.information_schema.tables

UNION ALL

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM hygiene_vault_mvp_dev_gianni.information_schema.tables

UNION ALL

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM hygiene_vault_mvp_dev_donald.information_schema.tables

UNION ALL

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM hygiene_vault_mvp_dev_saur.information_schema.tables
;

-- check out dev raw_vault tables

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM raw_vault_dev_robin.information_schema.tables

UNION ALL

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM raw_vault_dev_kirsten.information_schema.tables

UNION ALL

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM raw_vault_dev_gianni.information_schema.tables

UNION ALL

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM raw_vault_dev_donald.information_schema.tables

UNION ALL

SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM raw_vault_dev_saur.information_schema.tables
;


SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM data_science.information_schema.tables
;


------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM data_vault_mvp.information_schema.tables t
WHERE t.table_schema IN ('SINGLE_CUSTOMER_VIEW_STG_BAK', 'SINGLE_CUSTOMER_VIEW_STG')
;


TABLE_CATALOG	TABLE_SCHEMA	TABLE_NAME

DROP TABLE data_vault_mvp.single_customer_view_stg.page_screen_enrichment
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_extracted_params
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_unique_urls
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_url_hostname
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_url_params
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_associations
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touchifiable_events
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_time_diff_marker
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touchification
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

DROP TABLE data_vault_mvp.single_customer_view_stg.se_users_misaligned_with_travelist
;

DROP TABLE data_vault_mvp.single_customer_view_stg.travelist_unique_browser_ids
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching_idfv__merge_new_data
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching_cookie_id__merge_new_data
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching_unique_browser_id__merge_new_data
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching__step03_model_association_rank
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching__step02_create_union
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_associations_20221123
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching_20220125
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching_20221123
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_authorisation_events_20221123
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches_20221123
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_url_hostname_20221123
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer_20221017
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20220915
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20221017
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer_20210416
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer_20220915
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_transactions_20221123
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags_20221123
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.pre_ff_spv_check_20221112
;

DROP TABLE data_vault_mvp.single_customer_view_stg.pre_auth_events_check_20221114_touches
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.pre_auth_events_check_20221114_trxs
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.pre_auth_events_check_20221114_spvs
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_associations_20220113
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_associations_20220125
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching_20220113
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20220114
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20220203
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20221123
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_app_installs_20221123
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20220125
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker_20221123
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer_20221123
; --DELETE
DROP TABLE data_vault_mvp.single_customer_view_stg.page_screen_enrichment_20221123
; --DELETE

DROP SCHEMA data_vault_mvp.single_customer_view_stg_bkup_20220914
;

-- done

-- DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touch_marketing_channel__20230110t030000__daily_at_03h00;
-- DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touch_marketing_channel__20230111t030000__daily_at_03h00;
-- DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touch_utm_referrer__20230111t030000__daily_at_03h00;
-- DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touch_utm_referrer__20230110t030000__daily_at_03h00;
-- DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touched_feature_flags__20230110t030000__daily_at_03h00;
-- DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touched_feature_flags__20230111t030000__daily_at_03h00;
-- DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touch_attribution__20230111t030000__daily_at_03h00;
-- DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touch_attribution__20230110t030000__daily_at_03h00;
-- DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touch_basic_attributes__20230110t030000__daily_at_03h00;
-- DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touch_basic_attributes__20230111t030000__daily_at_03h00;


DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touched_authorisation_events__20221117t030000__daily_at_03h00
;

DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touched_authorisation_events__20221122t030000__daily_at_03h00
;

DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touched_authorisation_events__20221118t030000__daily_at_03h00
;

DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touched_authorisation_events__20221120t030000__daily_at_03h00
;

DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touched_authorisation_events__20220913t030000__daily_at_03h00
;

DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touched_authorisation_events__20221116t030000__daily_at_03h00
;

DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touched_authorisation_events__20221119t030000__daily_at_03h00
;

DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touched_authorisation_events__20220915t030000__daily_at_03h00
;

DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touched_authorisation_events__20221121t030000__daily_at_03h00
;

DROP TABLE data_vault_mvp.single_customer_view_stg_bak.module_touched_authorisation_events__20221115t030000__daily_at_03h00
;
-- above all dropped


SELECT
	ROUND(bytes / POWER(1024, 4), 3) AS storage_tb,
	*
FROM data_vault_mvp.information_schema.tables t
WHERE t.table_schema = 'SPV_REC'


SELECT
	ROUND(bytes / POWER(1024, 4), 3)                                       AS storage_tb,
	s.table_catalog,
	s.table_schema,
	s.table_name,
	LOWER(s.table_catalog || '.' || s.table_schema || '.' || s.table_name) AS table_reference,
	s.table_owner,
	s.table_type,
	s.is_transient,
	s.clustering_key,
	s.row_count,
	s.bytes,
	s.retention_time,
	s.self_referencing_column_name,
	s.reference_generation,
	s.user_defined_type_catalog,
	s.user_defined_type_schema,
	s.user_defined_type_name,
	s.is_insertable_into,
	s.is_typed,
	s.commit_action,
	s.created,
	s.last_altered,
	s.auto_clustering_on,
	s.comment
FROM dbt.information_schema.tables s
ORDER BY storage_tb DESC NULLS LAST
;


SELECT
	s.per_credit_cost,
	s.credits_per_hour,
	s.total_elapsed_time_sec,
	s.cost__query_duration,
	s.credits_used__query_duration,
	s.cost__credits_used_cloud_services,
	s.query_type,
	s.query_group,
	s.query_text,
	s.query_text_full,
	s.warehouse_group,
	s.role_group,
	s.role_name,
	s.user_name,
	s.warehouse_name,
	s.warehouse_type,
	s.warehouse_size,
	s.total_elapsed_time,
	s.tableau_incremental_refresh_date,
	s.start_time,
	s.end_time,
	s.credits_used_cloud_services,
	s.execution_status,
	s.team,
	s.position,
	s.query_tag
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.query_group = 'other'
  AND s.start_time BETWEEN CURRENT_DATE - 10 AND CURRENT_DATE
ORDER BY s.credits_used__query_duration DESC
;


------------------------------------------------------------------------------------------------------------------------


SELECT
	ROUND(bytes / POWER(1024, 4), 3)                                       AS storage_tb,
	s.table_catalog,
	s.table_schema,
	s.table_name,
	LOWER(s.table_catalog || '.' || s.table_schema || '.' || s.table_name) AS table_reference,
	s.table_owner,
	s.table_type,
	s.is_transient,
	s.clustering_key,
	s.row_count,
	s.bytes,
	s.retention_time,
	s.self_referencing_column_name,
	s.reference_generation,
	s.user_defined_type_catalog,
	s.user_defined_type_schema,
	s.user_defined_type_name,
	s.is_insertable_into,
	s.is_typed,
	s.commit_action,
	s.created,
	s.last_altered,
	s.auto_clustering_on,
	s.comment
FROM dbt.information_schema.tables s
ORDER BY storage_tb DESC NULLS LAST
;


------------------------------------------------------------------------------------------------------------------------

SELECT
	tsm.id,
	LOWER(tsm.table_catalog || '.' || tsm.table_schema || '.' || tsm.table_name) AS table_reference,
	tsm.active_bytes / POWER(1024, 4)                                            AS active_tb,
	tsm.table_name,
	tsm.table_schema,
	tsm.table_catalog,
	tsm.is_transient,
	tsm.active_bytes,
	tsm.time_travel_bytes,
	tsm.failsafe_bytes,
	tsm.retained_for_clone_bytes,
	tsm.deleted,
	tsm.table_created,
	tsm.table_dropped,
	tsm.table_entered_failsafe,
	tsm.schema_created,
	tsm.schema_dropped,
	tsm.catalog_created,
	tsm.catalog_dropped,
	tsm.comment
FROM snowflake.account_usage.table_storage_metrics tsm
WHERE tsm.deleted = FALSE
  AND tsm.schema_dropped IS NULL
  AND tsm.catalog_dropped IS NULL
  AND tsm.table_schema = 'DWH'
ORDER BY active_tb DESC NULLS LAST
;

DROP TABLE dbt.dbt_cloud_pr_289087_490_customer_insight__intermediate.ci_email_insertion_attributes
;

DROP TABLE dbt_dev.dbt_swidyatmoko_data_science__intermediate.ds_ab_hp_03_gpv_combined_lnd
;


DROP TABLE dbt_dev.dbt_swidyatmoko_customer_insight__intermediate.ci_iterable_email_04_combine_email_metrics
;

DROP TABLE dbt_dev.dbt_swidyatmoko_customer_insight__intermediate.ci_iterable_email_03a_sends_aggregated
;


WITH
	calcs AS (
		SELECT
			tsm.active_bytes / POWER(1024, 4) AS active_tb,
			active_tb * 23                    AS active_storage_cost
		FROM snowflake.account_usage.table_storage_metrics tsm
		WHERE tsm.deleted = FALSE
		  AND tsm.schema_dropped IS NULL
		  AND tsm.catalog_dropped IS NULL
	)
SELECT
	SUM(c.active_tb)           AS active_tb,
	SUM(c.active_storage_cost) AS active_storage_cost
FROM calcs c
;

-- check schemas with most storage
WITH
	calcs AS (
		SELECT
			tsm.table_catalog,
			tsm.table_schema,
			tsm.table_name,
			LOWER(tsm.table_catalog || '.' || tsm.table_schema || '.' || tsm.table_name) AS table_reference,
			tsm.active_bytes / POWER(1024, 4)                                            AS active_tb,
			active_tb * 23                                                               AS active_storage_cost
		FROM snowflake.account_usage.table_storage_metrics tsm
		WHERE tsm.deleted = FALSE
		  AND tsm.schema_dropped IS NULL
		  AND tsm.catalog_dropped IS NULL
	)
SELECT
	c.table_catalog || '.' || c.table_schema AS schema,
	SUM(c.active_tb)                         AS active_tb,
	SUM(c.active_storage_cost)               AS active_storage_cost
FROM calcs c
GROUP BY 1
ORDER BY 3 DESC NULLS LAST
;


-- need to drop objects owned by account admin
SELECT *
FROM snowflake.account_usage.tables t
WHERE t.table_owner = 'ACCOUNTADMIN'
  AND t.deleted IS NULL
;



SHOW TABLES IN DATABASE dbt
;

;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.dbt_tables AS (
	SELECT *
	FROM TABLE (RESULT_SCAN(LAST_QUERY_ID()))
)
;

SELECT *
FROM scratch.robinpatel.dbt_tables
ORDER BY 9 DESC
;


SHOW TABLES IN DATABASE dbt_dev
;

;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.dbt_dev_tables AS (
	SELECT *
	FROM TABLE (RESULT_SCAN(LAST_QUERY_ID()))
)
;

SELECT *
FROM scratch.robinpatel.dbt_dev_tables
ORDER BY 9 DESC
;



SELECT snowflake.cortex.translate('Deutschland', 'de', 'en')
;

USE ROLE cortex_user_role
;


------------------------------------------------------------------------------------------------------------------------
WITH
	storage_calcs AS (
		SELECT
			su.usage_date,
			su.storage_bytes,
			su.storage_bytes / POWER(1024, 4)       AS storage_tb,
			su.storage_bytes * 23 / POWER(1024, 4)  AS storage_cost,
			su.stage_bytes,
			su.stage_bytes / POWER(1024, 4)         AS stage_tb,
			su.stage_bytes * 23 / POWER(1024, 4)    AS stage_cost,
			su.failsafe_bytes,
			su.failsafe_bytes / POWER(1024, 4)      AS failsafe_tb,
			su.failsafe_bytes * 23 / POWER(1024, 4) AS failsafe_cost
		FROM snowflake.account_usage.storage_usage su
	)
SELECT
	DATE_TRUNC(MONTH, storage_calcs.usage_date) AS month,
	SUM(storage_tb)                             AS storage_tb,
	SUM(storage_cost)                           AS storage_cost,
	SUM(stage_tb)                               AS stage_tb,
	SUM(stage_cost)                             AS stage_cost,
	SUM(failsafe_tb)                            AS failsafe_tb,
	SUM(failsafe_cost)                          AS failsafe_cost
FROM storage_calcs
GROUP BY 1
;


SELECT
	storage_usage.failsafe_bytes             AS failsafe_bytes,
	storage_usage.hybrid_table_storage_bytes AS hybrid_table_storage_bytes,
	storage_usage.stage_bytes                AS stage_bytes,
	storage_usage.storage_bytes              AS storage_bytes,
	storage_usage.usage_date                 AS usage_date
FROM snowflake.account_usage.storage_usage storage_usage
WHERE storage_usage.usage_date <= current_date