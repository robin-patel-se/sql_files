SELECT mts.schedule_tstamp,
       mts.run_tstamp,
       mts.operation_id,
       mts.created_at,
       mts.updated_at,
       mts.event_hash,
       mts.touch_id,
       mts.event_tstamp,
       mts.event_category,
       mts.event_subcategory,
       mts.page_url,
       mts.search_context,
       mts.check_in_date,
       mts.check_out_date,
       mts.flexible_search,
       mts.had_results,
       mts.location,
       mts.location_search,
       mts.search_context: MONTHS::ARRAY      AS months,
       mts.months_search,
       mts.num_results,
       mts.refine_by_travel_type_search,
       mts.refine_by_trip_type_search,
       mts.specific_dates_search,
       mts.search_context:travel_types::ARRAY AS travel_types,
       mts.search_context:trip_types::ARRAY   AS trip_types,
       mts.weekend_only_search
FROM data_vault_mvp.single_customer_view_stg.module_touched_searches mts;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches_20211119 CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches;

self_describing_task --include 'dv/dwh/events/07_events_of_interest/03_module_touched_searches.py'  --method 'run' --start '2021-11-18 00:00:00' --end '2021-11-18 00:00:00';

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches;
CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
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
    weekend_only_search          BOOLEAN
)
    CLUSTER BY (touch_id, event_tstamp::DATE)
;
USE WAREHOUSE pipe_2xlarge;

INSERT INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
SELECT mts.schedule_tstamp,
       mts.run_tstamp,
       mts.operation_id,
       mts.created_at,
       mts.updated_at,
       mts.event_hash,
       mts.touch_id,
       mts.event_tstamp,
       mts.event_category,
       mts.event_subcategory,
       mts.page_url,
       mts.search_context,
       mts.check_in_date,
       mts.check_out_date,
       mts.flexible_search,
       mts.had_results,
       mts.location,
       mts.location_search,
--        mts.months,
       mts.search_context:MONTHS::ARRAY      AS months,
       mts.months_search,
       mts.num_results,
       mts.refine_by_travel_type_search,
       mts.refine_by_trip_type_search,
       mts.specific_dates_search,
       mts.search_context:travel_types::ARRAY AS travel_types,
       mts.search_context:trip_types::ARRAY   AS trip_types,
--        mts.travel_types,
--        mts.trip_types,
       mts.weekend_only_search
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches_20211119 mts;


SELECT * FROm data_vault_mvp.single_customer_view_stg.module_touched_searches;