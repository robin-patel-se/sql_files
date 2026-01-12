WITH
	flatten_search_travel_types AS (
		SELECT
			sts.event_hash,
			travel_types_elements.value::VARCHAR AS travel_types_element,
			CASE
				WHEN travel_types_element IN ('HOTEL_ONLY', 'HOTEL') THEN 'hotel'
				WHEN travel_types_element IN ('WITH_FLIGHTS', 'PACKAGE', 'IHP') THEN 'package'
			END                                  AS travel_type_selection
		FROM se.data.scv_touched_searches sts,
			 LATERAL FLATTEN(INPUT => sts.travel_types, OUTER => TRUE) travel_types_elements
		WHERE ARRAY_SIZE(sts.travel_types) > 0
		  AND sts.se_brand = 'SE Brand'
	),
	agg_travel_selection AS (
		SELECT
			flatten_search_travel_types.event_hash,
			COUNT(DISTINCT travel_type_selection) AS num_travel_type_selection,
			CASE
				WHEN num_travel_type_selection = 1 THEN ANY_VALUE(flatten_search_travel_types.travel_type_selection)
				WHEN num_travel_type_selection = 0 THEN 'no category'
				ELSE 'mixed'
			END                                   AS travel_type_category
		FROM flatten_search_travel_types
		GROUP BY ALL
	)
SELECT *
FROM agg_travel_selection
WHERE agg_travel_selection.num_travel_type_selection = 0



-- looks like the main values are:
-- TRAVEL_TYPES_ELEMENT
-- HOTEL_ONLY
-- WITH_FLIGHTS
-- HOTEL
-- PACKAGE
-- IHP

-- however there is some dirty data with pattens like 'HOTEL_ONLYutm_source=gads-brand' and 'HOTEL_ONLYuserId=29208366'

-- there are 82M searches with a travel type array
-- the theory is that each search could only have 1 travel type
-- however it has been observed that some searches have more than one element in that array.
-- this was limited to 820 searches - 0.01%


SELECT
	COUNT(*),
	SUM(IFF(ARRAY_SIZE(sts.travel_types) > 1, 1, 0)) AS more_than_1_travel_type
FROM se.data.scv_touched_searches sts
WHERE ARRAY_SIZE(sts.travel_types) > 0
;


SELECT
	event_hash,
	sts.travel_types
FROM se.data.scv_touched_searches sts
WHERE ARRAY_SIZE(sts.travel_types) > 1
;

-- on investigation there is a lot of duplication of travel types within the array, most appear to be categorised as the
-- same thing


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
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.scv.04_events_of_interest.03_module_touched_searches.py' \
    --method 'run' \
    --start '2025-10-31 00:00:00' \
    --end '2025-10-31 00:00:00'

SELECT
	search_context,
	TRY_TO_NUMBER(search_context['budget_max']::VARCHAR)                       AS budget_max,
	TRY_TO_NUMBER(search_context['budget_min']::VARCHAR)                       AS budget_min,
	IFF(TRY_TO_NUMBER(search_context['budget_max']::VARCHAR) > 0 OR
		TRY_TO_NUMBER(search_context['budget_min']::VARCHAR) > 0, TRUE, FALSE) AS is_budget_search
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
;

SELECT
	event_hash,
	search_context['facilities'],
	facilities_elements.*
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches,
	 LATERAL FLATTEN(INPUT => search_context['facilities'], OUTER => TRUE) facilities_elements
WHERE search_context['facilities'] IS NOT NULL
  AND facilities_elements.value = TRUE


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
WHERE se_brand = 'SE Brand'
;

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
;

------------------------------------------------------------------------------------------------------------------------
--post deployment steps:
-- backup table
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches_20251031 CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
;

-- drop prod table

-- data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches

-- rerun from earliest data


USE ROLE PIPELINERUNNER;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches_20251031 CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches;
;

USE ROLE PIPELINERUNNER;
DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches;

./scripts/mwaa-cli production "dags backfill --start-date '2022-11-30 04:30:00' --end-date '2022-12-01 04:30:00' --donot-pickle single_customer_view__daily_at_02h00 --task-regex '03_module_touched_searches'"


          USE WAREHOUSE pipe_2xlarge;

        CREATE TABLE IF NOT EXISTS data_vault_mvp.single_customer_view_stg.module_touched_searches
        (
            -- (lineage) metadata for the current job
            schedule_tstamp TIMESTAMP,
            run_tstamp TIMESTAMP,
            operation_id VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,

            event_hash VARCHAR,
            touch_id VARCHAR,
            event_tstamp TIMESTAMP,
            event_category VARCHAR,
            event_subcategory VARCHAR,
            page_url VARCHAR,
            search_context OBJECT,
            check_in_date DATE,
            check_out_date DATE,
            flexible_search BOOLEAN,
            had_results BOOLEAN,
            location VARCHAR,
            location_search BOOLEAN,
            months ARRAY,
            months_search BOOLEAN,
            num_results NUMBER,
            refine_by_travel_type_search BOOLEAN,
            refine_by_trip_type_search BOOLEAN,
            specific_dates_search BOOLEAN,
            travel_types ARRAY,
            travel_type_category VARCHAR,
            trip_types ARRAY,
            weekend_only_search BOOLEAN,
            triggered_by VARCHAR,
            filter_context OBJECT,
            search_results ARRAY,
            travellers_selection VARCHAR,
            budget_max NUMBER,
            budget_min NUMBER,
            has_budget_filter BOOLEAN,
            facilities OBJECT,
            has_any_facility_filter BOOLEAN,
            se_brand VARCHAR,
            CONSTRAINT pk_module_touched_searches
                PRIMARY KEY (
                    event_hash
                )
        )
            CLUSTER BY (event_tstamp::DATE)
        ;


MERGE INTO data_vault_mvp.single_customer_view_stg.module_touched_searches AS TARGET
        USING data_vault_mvp.single_customer_view_stg.module_touched_searches__step07__union_data AS batch
            ON target.event_hash = batch.event_hash
        WHEN MATCHED AND target.touch_id != batch.touch_id
            THEN UPDATE SET
            target.schedule_tstamp = '2022-12-01 02:00:00',
            target.run_tstamp = '2025-10-31 17:57:59',
            target.operation_id = 'ScriptOperator__/usr/local/airflow/dags/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/03_module_touched_searches.py__20221201T020000__daily_at_02h00',
            target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

            target.touch_id = batch.touch_id,
            target.event_tstamp = batch.event_tstamp,
            target.event_category = batch.event_category,
            target.event_subcategory = batch.event_subcategory,
            target.page_url = batch.page_url,
            target.search_context = batch.search_context,
            target.check_in_date = batch.check_in_date,
            target.check_out_date = batch.check_out_date,
            target.flexible_search = batch.flexible_search,
            target.had_results = batch.had_results,
            target.location = batch.location,
            target.location_search = batch.location_search,
            target.months = batch.months,
            target.months_search = batch.months_search,
            target.num_results = batch.num_results,
            target.refine_by_travel_type_search = batch.refine_by_travel_type_search,
            target.refine_by_trip_type_search = batch.refine_by_trip_type_search,
            target.specific_dates_search = batch.specific_dates_search,
            target.travel_types = batch.travel_types,
            target.travel_type_category = batch.travel_type_category,
            target.trip_types = batch.trip_types,
            target.weekend_only_search = batch.weekend_only_search,
            target.triggered_by = batch.triggered_by,
            target.filter_context = batch.filter_context,
            target.search_results = batch.search_results,
            target.travellers_selection = batch.travellers_selection,
            target.budget_max = batch.budget_max,
            target.budget_min = batch.budget_min,
            target.has_budget_filter = batch.has_budget_filter,
            target.facilities = batch.facilities,
            target.has_any_facility_filter = batch.has_any_facility_filter,
            target.se_brand = batch.se_brand
        WHEN NOT MATCHED
            THEN INSERT VALUES (
                    '2022-12-01 02:00:00',
                    '2025-10-31 17:57:59',
                    'ScriptOperator__/usr/local/airflow/dags/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/03_module_touched_searches.py__20221201T020000__daily_at_02h00',
                    CURRENT_TIMESTAMP()::TIMESTAMP,
                    CURRENT_TIMESTAMP()::TIMESTAMP,

                    batch.event_hash,
                    batch.touch_id,
                    batch.event_tstamp,
                    batch.event_category,
                    batch.event_subcategory,
                    batch.page_url,
                    batch.search_context,
                    batch.check_in_date,
                    batch.check_out_date,
                    batch.flexible_search,
                    batch.had_results,
                    batch.location,
                    batch.location_search,
                    batch.months,
                    batch.months_search,
                    batch.num_results,
                    batch.refine_by_travel_type_search,
                    batch.refine_by_trip_type_search,
                    batch.specific_dates_search,
                    batch.travel_types,
                    batch.travel_type_category,
                    batch.trip_types,
                    batch.weekend_only_search,
                    batch.triggered_by,
                    batch.filter_context,
                    batch.search_results,
                    batch.travellers_selection,
                    batch.budget_max,
                    batch.budget_min,
                    batch.has_budget_filter,
                    batch.facilities,
                    batch.has_any_facility_filter,
                    batch.se_brand
            )
USE ROLE pipelinerunner;
GRANT SELECT ON TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches_20251031 TO ROLE data_team_basic;

USE ROLE personal_role__robinpatel

SELECT count(*) FROM data_vault_mvp.single_customer_view_stg.module_touched_searches;
SELECT COUNT(*) FROM data_vault_mvp.single_customer_view_stg.module_touched_searches_20251031