USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

-- optional statement to create the module target table --
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.scv.04_events_of_interest.03_module_touched_searches.py' \
    --method 'run' \
    --start '2025-10-16 00:00:00' \
    --end '2025-10-16 00:00:00'

;

SELECT
	event_stream.contexts_com_secretescapes_search_context_1,
	NULLIF(COALESCE(event_stream.contexts_com_secretescapes_search_context_1[0]['travellers_selection']::VARCHAR,
					event_stream.contexts_com_secretescapes_search_context_1[0]['travellersSelection']::VARCHAR
		   ), '') AS traveller_selection,

FROM se.data_pii.scv_event_stream event_stream
WHERE event_stream.se_brand = 'SE Brand'
  AND event_stream.event_tstamp::DATE >= CURRENT_DATE - 1
  AND event_stream.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND (
	LOWER(event_stream.device_platform) NOT LIKE 'native app%' -- everything but app has full history
		OR
	( -- for app this need to be from a specific point in time
		LOWER(event_stream.device_platform) IN ('native app ios', 'native app android')
			AND event_stream.event_tstamp::DATE >= '2024-03-01'
		)
	)
  AND event_stream.contexts_com_secretescapes_search_context_1[0]['travellersSelection']::VARCHAR IS NOT NULL


------------------------------------------------------------------------------------------------------------------------
-- post deps

-- backup table:
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches_20251016 CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
;

-- create new table
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
	travellers_selection         VARCHAR,
	se_brand                     VARCHAR
)
	CLUSTER BY (event_tstamp::DATE)
;

-- insert data

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
	search_results,
	NULL AS travellers_selection,
	se_brand
FROm data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches_20251016

-- rerun touched searches since release of search by traveller

;
USE ROLE pipelinerunner;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches_20251016 CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches

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
	travellers_selection         VARCHAR,
	se_brand                     VARCHAR
)
	CLUSTER BY (event_tstamp::DATE)
;

USE WAREHOUSE pipe_2xlarge;


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
	search_results,
	NULL AS travellers_selection,
	se_brand
FROm data_vault_mvp.single_customer_view_stg.module_touched_searches_20251016;

GRANT SELECT ON TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches TO ROLE personal_role__dbt_prod;