USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.scv.04_events_of_interest.03_module_touched_searches.py' \
    --method 'run' \
    --start '2025-10-02 00:00:00' \
    --end '2025-10-02 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches__model_se_brand_data
WHERE search_context['results'] IS NOT NULL
;



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches_20251002 CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
;


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
(
	-- (lineage) metadata for the current job
	schedule_tstamp              TIMESTAMP,
	run_tstamp                   TIMESTAMP,
	operation_id                 VARCHAR,
	created_at                   TIMESTAMP,
	updated_at                   TIMESTAMP,

	event_hash                   VARCHAR PRIMARY KEY NOT NULL,
	touch_id                     VARCHAR,
	event_tstamp                 TIMESTAMP,
	event_category               VARCHAR,
	event_subcategory            VARCHAR,

	page_url                     VARCHAR,
	search_context               OBJECT,
	check_in_date                DATE,
	check_out_date               DATE,
	flexible_search              BOOLEAN,
	had_results                  BOOLEAN,
	location                     VARCHAR,
	location_search              BOOLEAN,
	months                       ARRAY,
	months_search                BOOLEAN,
	num_results                  NUMBER,
	refine_by_travel_type_search BOOLEAN,
	refine_by_trip_type_search   BOOLEAN,
	specific_dates_search        BOOLEAN,
	travel_types                 ARRAY,
	trip_types                   ARRAY,
	weekend_only_search          BOOLEAN,
	triggered_by                 VARCHAR,
	filter_context               OBJECT,
	search_results               ARRAY,
	se_brand                     VARCHAR
)
	CLUSTER BY (event_tstamp::DATE)
;

USE WAREHOUSE pipe_xlarge
;

INSERT INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	event_hash,
	touch_id,
	event_tstamp,
	event_category,
	event_subcategory,
	page_url,
	search_context,
	check_in_date,
	check_out_date,
	flexible_search,
	had_results,
	location,
	location_search,
	months,
	months_search,
	num_results,
	refine_by_travel_type_search,
	refine_by_trip_type_search,
	specific_dates_search,
	travel_types,
	trip_types,
	weekend_only_search,
	triggered_by,
	filter_context,
	COALESCE(
			search_context['results'],
			search_context['results_list']
	) AS search_results,
	se_brand
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches_20251002


SELECT
	table_catalog,
	table_schema,
	table_name,
	clustering_key,
	auto_clustering_on
FROM data_vault_mvp.information_schema.tables
WHERE table_schema = 'SINGLE_CUSTOMER_VIEW_STG'
  AND table_name = 'MODULE_TOUCHED_SEARCHES'
;


USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches_20251002 CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;


CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches
(
	-- (lineage) metadata for the current job
	schedule_tstamp              TIMESTAMP,
	run_tstamp                   TIMESTAMP,
	operation_id                 VARCHAR,
	created_at                   TIMESTAMP,
	updated_at                   TIMESTAMP,

	event_hash                   VARCHAR PRIMARY KEY NOT NULL,
	touch_id                     VARCHAR,
	event_tstamp                 TIMESTAMP,
	event_category               VARCHAR,
	event_subcategory            VARCHAR,

	page_url                     VARCHAR,
	search_context               OBJECT,
	check_in_date                DATE,
	check_out_date               DATE,
	flexible_search              BOOLEAN,
	had_results                  BOOLEAN,
	location                     VARCHAR,
	location_search              BOOLEAN,
	months                       ARRAY,
	months_search                BOOLEAN,
	num_results                  NUMBER,
	refine_by_travel_type_search BOOLEAN,
	refine_by_trip_type_search   BOOLEAN,
	specific_dates_search        BOOLEAN,
	travel_types                 ARRAY,
	trip_types                   ARRAY,
	weekend_only_search          BOOLEAN,
	triggered_by                 VARCHAR,
	filter_context               OBJECT,
	search_results               ARRAY,
	se_brand                     VARCHAR
)
	CLUSTER BY (event_tstamp::DATE)
;

USE WAREHOUSE pipe_xlarge


INSERT INTO data_vault_mvp.single_customer_view_stg.module_touched_searches
SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	event_hash,
	touch_id,
	event_tstamp,
	event_category,
	event_subcategory,
	page_url,
	search_context,
	check_in_date,
	check_out_date,
	flexible_search,
	had_results,
	location,
	location_search,
	months,
	months_search,
	num_results,
	refine_by_travel_type_search,
	refine_by_trip_type_search,
	specific_dates_search,
	travel_types,
	trip_types,
	weekend_only_search,
	triggered_by,
	filter_context,
	COALESCE(
			search_context['results'],
			search_context['results_list']
	) AS search_results,
	se_brand
FROM data_vault_mvp.single_customer_view_stg.module_touched_searches_20251002


------------------------------------------------------------------------------------------------------------------------
	use role personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
	CLONE data_vault_mvp.single_customer_view_stg.page_screen_enrichment
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;


SELECT *
FROM data_vault_mvp_dev_robin.bi.search_model__step06__model_search_results_arrays
;

SELECT *
FROM se.data.scv_touched_feature_flags stff
;


SELECT *
FROM dbt.bi_data_science__intermediate.rnr_key_events rke
;


SELECT
	sb.booking_id,
	sb.booking_completed_date,
	sb.cancellation_date,
	sb.margin_gross_of_toms_gbp
FROM se.data.se_booking sb
WHERE sb.booking_completed_date = sb.cancellation_date
  AND COALESCE(sb.margin_gross_of_toms_gbp, 0) = 0 LATERAL FLATTEN(INPUT => js[0]['sales'], OUTER => TRUE) element_sids
;


SELECT
	ci_iterable_user_profile_activity_daily.model_run_date,
	COUNT(*)
FROM dbt.bi_customer_insight.ci_iterable_user_profile_activity_daily
WHERE ci_iterable_user_profile_activity_daily.model_run_date >= CURRENT_DATE - 10
  AND ci_iterable_user_profile_activity_daily.current_affiliate_territory = 'DE'
GROUP BY ci_iterable_user_profile_activity_daily.model_run_date
;

SELECT
	ci_iterable_user_profile_activity_daily.model_run_date,
	ci_iterable_user_profile_activity_daily.segment_name,
	COUNT(*)
FROM dbt.bi_customer_insight.ci_iterable_user_profile_activity_daily
WHERE ci_iterable_user_profile_activity_daily.model_run_date >= CURRENT_DATE - 10
  AND ci_iterable_user_profile_activity_daily.current_affiliate_territory = 'DE'
GROUP BY ALL
;


SELECT
	ci_iterable_user_profile_activity_daily.model_run_date,
	ci_iterable_user_profile_activity_daily.athena_segment_name,
	COUNT(*)
FROM dbt.bi_customer_insight.ci_iterable_user_profile_activity_daily
WHERE ci_iterable_user_profile_activity_daily.model_run_date >= CURRENT_DATE - 10
  AND ci_iterable_user_profile_activity_daily.current_affiliate_territory = 'DE'
GROUP BY ALL
;


SELECT DISTINCT
	kronos_refined_deals.user_id
FROM data_science.nextoken_prod.kronos_refined_deals kronos_refined_deals
WHERE kronos_refined_deals.created_on::DATE >= CURRENT_DATE() - 1
-- 6.3M users with ranks


SELECT
	kronos_refined_deals.created_on::DATE,
	COUNT(DISTINCT kronos_refined_deals.user_id)
FROM data_science.nextoken_prod.kronos_refined_deals kronos_refined_deals
WHERE kronos_refined_deals.created_on::DATE >= CURRENT_DATE() - 10
GROUP BY 1
;



SELECT * FROM dbt.bi_data_science__intermediate.rnr_judgement_list_ncf