CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale AS
SELECT *
FROM data_vault_mvp.dwh.dim_sale;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.harmonised_offer_calendar_view AS
SELECT *
FROM data_vault_mvp.dwh.harmonised_offer_calendar_view;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel_sale_offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel_sale_offer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.odm_sale_allocation_and_rates CLONE data_vault_mvp.dwh.odm_sale_allocation_and_rates;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.travelbird_offer_allocation_and_rates CLONE data_vault_mvp.dwh.travelbird_offer_allocation_and_rates;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer CLONE data_vault_mvp.dwh.tb_offer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;


biapp/
task_catalogue/
dv/
dwh/
allocation_and_rates/
harmonised_sale_calendar_view.py

self_describing_task --include 'biapp/task_catalogue/dv/dwh/allocation_and_rates/harmonised_sale_calendar_view.py'  --method 'run' --start '2022-11-15 00:00:00' --end '2022-11-15 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss;

SELECT *
FROM se.data.dim_sale ds;

SELECT *
FROM data_vault_mvp.dwh.tb_offer t
WHERE t.se_sale_id IS NOT NULL
    QUALIFY COUNT(*) OVER (PARTITION BY t.se_sale_id) > 1;


------------------------------------------------------------------------------------------------------------------------
-- add to harmonised sale calendar view snapshot

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view CLONE data_vault_mvp.dwh.harmonised_sale_calendar_view;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view_snapshot CLONE data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot;


-- Post deployment steps

-- Backup production tables
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.harmonised_sale_calendar_view_20221219 CLONE data_vault_mvp.dwh.harmonised_sale_calendar_view;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot_20221121 CLONE data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot;

-- Alter snapshot to add new columns (because it is incremental)

-- Use large warehouse because lots of historic data
USE WAREHOUSE pipe_xlarge;
-- Create new production table

CREATE OR REPLACE TABLE data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot
(
    -- (lineage) metadata for the current job
    schedule_tstamp                                              TIMESTAMP,
    run_tstamp                                                   TIMESTAMP,
    operation_id                                                 VARCHAR,
    created_at                                                   TIMESTAMP,
    updated_at                                                   TIMESTAMP,

    view_date                                                    DATE,
    hotel_code                                                   VARCHAR,
    se_sale_id                                                   VARCHAR,
    sale_name                                                    VARCHAR,
    current_contractor_name                                      VARCHAR,
    concept_name                                                 VARCHAR,
    sale_active                                                  BOOLEAN,
    sale_available_in_calendar                                   BOOLEAN,

    calendar_date                                                DATE,
    day_name                                                     VARCHAR,
    available_inventory                                          NUMBER,
    reserved_inventory                                           NUMBER,
    total_inventory                                              NUMBER,

    no_available_offer_ids                                       NUMBER,
    currency                                                     VARCHAR,

    lead_rate_offer_id                                           VARCHAR,
    lead_rate_offer_los                                          NUMBER,
    lead_rate_offer_allocation_duration_days                     NUMBER,
    lead_rate_gbp                                                DECIMAL(19, 6),
    lead_rate_eur                                                DECIMAL(19, 6),
    lead_rate_rc                                                 DECIMAL(19, 6),

    lead_rate_per_night_offer_id                                 VARCHAR,
    lead_rate_per_night_offer_los                                NUMBER,
    lead_rate_per_night_offer_allocation_duration_days           NUMBER,
    lead_rate_per_night_gbp                                      DECIMAL(19, 6),
    lead_rate_per_night_eur                                      DECIMAL(19, 6),
    lead_rate_per_night_rc                                       DECIMAL(19, 6),

    available_lead_rate_offer_id                                 VARCHAR,
    available_lead_rate_offer_los                                NUMBER,
    available_lead_rate_offer_allocation_duration_days           NUMBER,
    available_lead_rate_gbp                                      DECIMAL(19, 6),
    available_lead_rate_eur                                      DECIMAL(19, 6),
    available_lead_rate_rc                                       DECIMAL(19, 6),

    available_lead_rate_per_night_offer_id                       VARCHAR,
    available_lead_rate_per_night_offer_los                      NUMBER,
    available_lead_rate_per_night_offer_allocation_duration_days NUMBER,
    available_lead_rate_per_night_gbp                            DECIMAL(19, 6),
    available_lead_rate_per_night_eur                            DECIMAL(19, 6),
    available_lead_rate_per_night_rc                             DECIMAL(19, 6),

    accommodation_source                                         VARCHAR,
    data_model                                                   VARCHAR,
    salesforce_opportunity_id                                    VARCHAR,

    CONSTRAINT pk_1 PRIMARY KEY (se_sale_id, calendar_date)
)
    CLUSTER BY (view_date);


-- insert backup data into production table
INSERT INTO data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot
SELECT
    hscvs.schedule_tstamp,
    hscvs.run_tstamp,
    hscvs.operation_id,
    hscvs.created_at,
    hscvs.updated_at,
    hscvs.view_date,
    hscvs.hotel_code,
    hscvs.se_sale_id,
    NULL AS sale_name,
    NULL AS current_contractor_name,
    NULL AS concept_name,
    hscvs.sale_active,
    hscvs.sale_available_in_calendar,
    hscvs.calendar_date,
    hscvs.day_name,
    hscvs.available_inventory,
    hscvs.reserved_inventory,
    hscvs.total_inventory,
    hscvs.no_available_offer_ids,
    hscvs.currency,
    hscvs.lead_rate_offer_id,
    hscvs.lead_rate_offer_los,
    hscvs.lead_rate_offer_allocation_duration_days,
    hscvs.lead_rate_gbp,
    hscvs.lead_rate_eur,
    hscvs.lead_rate_rc,
    hscvs.lead_rate_per_night_offer_id,
    hscvs.lead_rate_per_night_offer_los,
    hscvs.lead_rate_per_night_offer_allocation_duration_days,
    hscvs.lead_rate_per_night_gbp,
    hscvs.lead_rate_per_night_eur,
    hscvs.lead_rate_per_night_rc,
    hscvs.available_lead_rate_offer_id,
    hscvs.available_lead_rate_offer_los,
    hscvs.available_lead_rate_offer_allocation_duration_days,
    hscvs.available_lead_rate_gbp,
    hscvs.available_lead_rate_eur,
    hscvs.available_lead_rate_rc,
    hscvs.available_lead_rate_per_night_offer_id,
    hscvs.available_lead_rate_per_night_offer_los,
    hscvs.available_lead_rate_per_night_offer_allocation_duration_days,
    hscvs.available_lead_rate_per_night_gbp,
    hscvs.available_lead_rate_per_night_eur,
    hscvs.available_lead_rate_per_night_rc,
    hscvs.accommodation_source,
    hscvs.data_model,
    hscvs.salesforce_opportunity_id
FROM data_vault_mvp.dwh.harmonised_sale_calendar_view_20221219 hscvs;

-- run dag for current date


------------------------------------------------------------------------------------------------------------------------
-- developer actions

-- Alter snapshot to add new columns (because it is incremental)
-- Create new production table

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view_snapshot
(
    -- (lineage) metadata for the current job
    schedule_tstamp                                              TIMESTAMP,
    run_tstamp                                                   TIMESTAMP,
    operation_id                                                 VARCHAR,
    created_at                                                   TIMESTAMP,
    updated_at                                                   TIMESTAMP,

    view_date                                                    DATE,
    hotel_code                                                   VARCHAR,
    se_sale_id                                                   VARCHAR,
    sale_name                                                    VARCHAR,
    current_contractor_name                                      VARCHAR,
    concept_name                                                 VARCHAR,
    sale_active                                                  BOOLEAN,
    sale_available_in_calendar                                   BOOLEAN,

    calendar_date                                                DATE,
    day_name                                                     VARCHAR,
    available_inventory                                          NUMBER,
    reserved_inventory                                           NUMBER,
    total_inventory                                              NUMBER,

    no_available_offer_ids                                       NUMBER,
    currency                                                     VARCHAR,

    lead_rate_offer_id                                           VARCHAR,
    lead_rate_offer_los                                          NUMBER,
    lead_rate_offer_allocation_duration_days                     NUMBER,
    lead_rate_gbp                                                DECIMAL(19, 6),
    lead_rate_eur                                                DECIMAL(19, 6),
    lead_rate_rc                                                 DECIMAL(19, 6),

    lead_rate_per_night_offer_id                                 VARCHAR,
    lead_rate_per_night_offer_los                                NUMBER,
    lead_rate_per_night_offer_allocation_duration_days           NUMBER,
    lead_rate_per_night_gbp                                      DECIMAL(19, 6),
    lead_rate_per_night_eur                                      DECIMAL(19, 6),
    lead_rate_per_night_rc                                       DECIMAL(19, 6),

    available_lead_rate_offer_id                                 VARCHAR,
    available_lead_rate_offer_los                                NUMBER,
    available_lead_rate_offer_allocation_duration_days           NUMBER,
    available_lead_rate_gbp                                      DECIMAL(19, 6),
    available_lead_rate_eur                                      DECIMAL(19, 6),
    available_lead_rate_rc                                       DECIMAL(19, 6),

    available_lead_rate_per_night_offer_id                       VARCHAR,
    available_lead_rate_per_night_offer_los                      NUMBER,
    available_lead_rate_per_night_offer_allocation_duration_days NUMBER,
    available_lead_rate_per_night_gbp                            DECIMAL(19, 6),
    available_lead_rate_per_night_eur                            DECIMAL(19, 6),
    available_lead_rate_per_night_rc                             DECIMAL(19, 6),

    accommodation_source                                         VARCHAR,
    data_model                                                   VARCHAR,
    salesforce_opportunity_id                                    VARCHAR,

    CONSTRAINT pk_1 PRIMARY KEY (se_sale_id, calendar_date)
)
    CLUSTER BY (view_date);


-- insert backup data into production table
INSERT INTO data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view_snapshot
SELECT
    harmonised_sale_calendar_view_snapshot.schedule_tstamp,
    harmonised_sale_calendar_view_snapshot.run_tstamp,
    harmonised_sale_calendar_view_snapshot.operation_id,
    harmonised_sale_calendar_view_snapshot.created_at,
    harmonised_sale_calendar_view_snapshot.updated_at,
    harmonised_sale_calendar_view_snapshot.view_date,
    harmonised_sale_calendar_view_snapshot.hotel_code,
    harmonised_sale_calendar_view_snapshot.se_sale_id,
    NULL, -- sale_name
    NULL, -- current_contractor_name
    NULL, -- concept_name
    harmonised_sale_calendar_view_snapshot.sale_active,
    harmonised_sale_calendar_view_snapshot.sale_available_in_calendar,
    harmonised_sale_calendar_view_snapshot.calendar_date,
    harmonised_sale_calendar_view_snapshot.day_name,
    harmonised_sale_calendar_view_snapshot.available_inventory,
    harmonised_sale_calendar_view_snapshot.reserved_inventory,
    harmonised_sale_calendar_view_snapshot.total_inventory,
    harmonised_sale_calendar_view_snapshot.no_available_offer_ids,
    harmonised_sale_calendar_view_snapshot.currency,
    harmonised_sale_calendar_view_snapshot.lead_rate_offer_id,
    harmonised_sale_calendar_view_snapshot.lead_rate_offer_los,
    harmonised_sale_calendar_view_snapshot.lead_rate_offer_allocation_duration_days,
    harmonised_sale_calendar_view_snapshot.lead_rate_gbp,
    harmonised_sale_calendar_view_snapshot.lead_rate_eur,
    harmonised_sale_calendar_view_snapshot.lead_rate_rc,
    harmonised_sale_calendar_view_snapshot.lead_rate_per_night_offer_id,
    harmonised_sale_calendar_view_snapshot.lead_rate_per_night_offer_los,
    harmonised_sale_calendar_view_snapshot.lead_rate_per_night_offer_allocation_duration_days,
    harmonised_sale_calendar_view_snapshot.lead_rate_per_night_gbp,
    harmonised_sale_calendar_view_snapshot.lead_rate_per_night_eur,
    harmonised_sale_calendar_view_snapshot.lead_rate_per_night_rc,
    harmonised_sale_calendar_view_snapshot.available_lead_rate_offer_id,
    harmonised_sale_calendar_view_snapshot.available_lead_rate_offer_los,
    harmonised_sale_calendar_view_snapshot.available_lead_rate_offer_allocation_duration_days,
    harmonised_sale_calendar_view_snapshot.available_lead_rate_gbp,
    harmonised_sale_calendar_view_snapshot.available_lead_rate_eur,
    harmonised_sale_calendar_view_snapshot.available_lead_rate_rc,
    harmonised_sale_calendar_view_snapshot.available_lead_rate_per_night_offer_id,
    harmonised_sale_calendar_view_snapshot.available_lead_rate_per_night_offer_los,
    harmonised_sale_calendar_view_snapshot.available_lead_rate_per_night_offer_allocation_duration_days,
    harmonised_sale_calendar_view_snapshot.available_lead_rate_per_night_gbp,
    harmonised_sale_calendar_view_snapshot.available_lead_rate_per_night_eur,
    harmonised_sale_calendar_view_snapshot.available_lead_rate_per_night_rc,
    harmonised_sale_calendar_view_snapshot.accommodation_source,
    harmonised_sale_calendar_view_snapshot.data_model,
    harmonised_sale_calendar_view_snapshot.salesforce_opportunity_id
FROM data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot

-- run dag for current date

------------------------------------------------------------------------------------------------------------------------
-- scrapping idea above. Going to enrich the data source via the hotel global sale attributes therefore don't need to change the underlying
-- hotel sale calendar view table or snapshot table. A lot tider.

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer CLONE data_vault_mvp.dwh.tb_offer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;

WITH difference AS (
    SELECT
        hgsa.salesforce_opportunity_id,
        hgsa.hotel_id,
        hgsa.hotel_code,
        hgsa.company_name,
        hgsa.posu_city,
        hgsa.posu_country,
        hgsa.posu_division,
        hgsa.posu_cluster_sub_region,
        hgsa.posu_cluster_region,
        hgsa.posu_cluster,
        hgsa.current_contractor_name,
        hgsa.deal_category,
        hgsa.tech_platform,
        hgsa.min_sale_start_date,
        hgsa.sale_active_count,
        hgsa.contains_active_sale,
        hgsa.product_type_agg
    FROM data_vault_mvp_dev_robin.bi.hotel_global_sale_attributes hgsa
    EXCEPT
    SELECT
        hgsa.salesforce_opportunity_id,
        hgsa.hotel_id,
        hgsa.hotel_code,
        hgsa.company_name,
        hgsa.posu_city,
        hgsa.posu_country,
        hgsa.posu_division,
        hgsa.posu_cluster_sub_region,
        hgsa.posu_cluster_region,
        hgsa.posu_cluster,
        hgsa.current_contractor_name,
        hgsa.deal_category,
        hgsa.tech_platform,
        hgsa.min_sale_start_date,
        hgsa.sale_active_count,
        hgsa.contains_active_sale,
        hgsa.product_type_agg
    FROM data_vault_mvp.bi.hotel_global_sale_attributes hgsa
),
     unioned AS (
         SELECT
             hgsa.salesforce_opportunity_id,
             hgsa.hotel_id,
             hgsa.hotel_code,
             hgsa.company_name,
             hgsa.posu_city,
             hgsa.posu_country,
             hgsa.posu_division,
             hgsa.posu_cluster_sub_region,
             hgsa.posu_cluster_region,
             hgsa.posu_cluster,
             hgsa.current_contractor_name,
             hgsa.deal_category,
             hgsa.tech_platform,
             hgsa.min_sale_start_date,
             hgsa.sale_active_count,
             hgsa.contains_active_sale,
             hgsa.product_type_agg,
             'dev' AS platform
         FROM data_vault_mvp_dev_robin.bi.hotel_global_sale_attributes hgsa
         UNION ALL
         SELECT
             hgsa.salesforce_opportunity_id,
             hgsa.hotel_id,
             hgsa.hotel_code,
             hgsa.company_name,
             hgsa.posu_city,
             hgsa.posu_country,
             hgsa.posu_division,
             hgsa.posu_cluster_sub_region,
             hgsa.posu_cluster_region,
             hgsa.posu_cluster,
             hgsa.current_contractor_name,
             hgsa.deal_category,
             hgsa.tech_platform,
             hgsa.min_sale_start_date,
             hgsa.sale_active_count,
             hgsa.contains_active_sale,
             hgsa.product_type_agg,
             'prod' AS platform
         FROM data_vault_mvp.bi.hotel_global_sale_attributes hgsa
     )
SELECT
    u.*
FROM unioned u
    INNER JOIN difference d ON u.salesforce_opportunity_id = d.salesforce_opportunity_id;



SELECT
    ds.se_sale_id,
    ds.sale_name,
    ss.current_contractor_name,
    tbo.concept_name
FROM data_vault_mvp.dwh.dim_sale ds
    LEFT JOIN data_vault_mvp.dwh.se_sale ss ON ds.se_sale_id = ss.se_sale_id
    LEFT JOIN data_vault_mvp.dwh.tb_offer tbo ON ds.se_sale_id = tbo.se_sale_id;


SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
  AND ssa.se_sale_id NOT LIKE 'A%';


SELECT *
FROM latest_vault.marketing_gsheets.demand_ltvs;

SELECT *
FROM collab.fornova.price_check_update

SELECT GET_DDL('table', 'collab.fornova.price_check_update');

CREATE OR REPLACE VIEW price_check_update AS
(
WITH enrich_data AS (
    SELECT
        fpc.salesforce_opportunity_id,
        fpc.ota_check_in_date,
        fpc.ota_check_out_date,
        fpc.allocation_date,
        fpc.room_type_name,
        fpc.ota_room_name,
        fpc.offer_name,
        fpc.rate_plan_name,
        fpc.currency_local,
        fpc.ota_occupancy,
        fpc.occupancy_adults,
        fpc.rate_local_calculated,
        fpc.ota_rate,
        fpc.ota_core,
        fpc.ota_core_supplement,
        fpc.core_discount,
        fpc.core_discount_percentage,
        ROUND(fpc.core_discount_percentage)  AS core_discount_percentage_calculated,
        fpc.total_discount,
        fpc.total_discount_percentage,
        ROUND(fpc.total_discount_percentage) AS total_discount_percentage_calculated,
        fpc.record_timestamp,
--soft fail logic
--either a core discount 0-9% and/or a total discount 0-14%
--or a negative core discount with a total discount 0-14%
--Hard fail logic
--total_discount_percentage <0
        CASE
            WHEN total_discount_percentage_calculated < 0 THEN 'hard_fail'
            WHEN core_discount_percentage_calculated BETWEEN 0 AND 10
                OR total_discount_percentage_calculated BETWEEN 0 AND 15
                OR core_discount_percentage_calculated < 0 AND
                   total_discount_percentage_calculated BETWEEN 0 AND 15 THEN 'soft_fail'
            ELSE 'pass' END                  AS price_check_result
    FROM latest_vault.fornova.price_comparison fpc
    WHERE --total_discount_percentage IS NOT NULL
        allocation_date > CURRENT_DATE
      AND (record_timestamp::DATE = (
        SELECT
            MAX(record_timestamp)::DATE
        FROM latest_vault.fornova.price_comparison
    )
        OR record_timestamp::DATE IS NULL)
        QUALIFY ROW_NUMBER()
                        OVER (PARTITION BY salesforce_opportunity_id, offer_name, allocation_date ORDER BY row_loaded_at DESC) =
                1
),
     aggregated_data AS (
         SELECT
             salesforce_opportunity_id,
             offer_name,
             total_discount_percentage_calculated,
             allocation_date,
             COUNT(allocation_date)
                   OVER (PARTITION BY salesforce_opportunity_id, offer_name)                                                                        AS total_number_of_allocation_dates,
             CEIL(total_number_of_allocation_dates * 0.15)                                                                                          AS top_15_dates,
             ROW_NUMBER() OVER (PARTITION BY salesforce_opportunity_id, offer_name ORDER BY COALESCE(total_discount_percentage_calculated, 0) DESC) AS row_num
         FROM enrich_data
         WHERE price_check_result <> 'hard_fail'
     ),
     top_discount_15_percent AS (
         SELECT
             salesforce_opportunity_id,
             offer_name,
             MIN(total_discount_percentage_calculated) AS top_15_percent_discount
         FROM aggregated_data
         WHERE row_num <= top_15_dates
         GROUP BY 1, 2
     ),
     hotel_details AS
         (
             SELECT DISTINCT
                 salesforce_opportunity_id,
                 hotel_id,
                 hotel_code
             FROM se.data.se_sale_attributes
         )
SELECT
    ed.record_timestamp      AS assignment_date,
    ed.salesforce_opportunity_id,
    hd.hotel_code,
    ed.ota_check_in_date,
    ed.ota_check_out_date,
    ed.allocation_date,
    ed.room_type_name,
    ed.ota_room_name,
    ed.offer_name,
    ed.rate_plan_name,
    ed.currency_local,
    ed.ota_occupancy,
    ed.occupancy_adults,
    ed.rate_local_calculated AS se_rate,
    ed.ota_rate,
    ed.ota_core,
    ed.ota_core_supplement,
    ed.core_discount,
    ed.core_discount_percentage,
    ed.total_discount,
    ed.total_discount_percentage,
    ed.total_discount_percentage_calculated,
    ed.price_check_result,
    td.top_15_percent_discount
FROM enrich_data ed
    LEFT JOIN top_discount_15_percent td ON td.salesforce_opportunity_id = ed.salesforce_opportunity_id
    AND td.offer_name = ed.offer_name
    LEFT JOIN hotel_details hd ON hd.salesforce_opportunity_id = ed.salesforce_opportunity_id
    );