SELECT *
FROM latest_vault.mari.rate r;
SELECT *
FROM latest_vault.mari.rate_plan rp;
SELECT *
FROM latest_vault.mari.room_type rt;
SELECT *
FROM latest_vault.mari.hotel h;

WITH model_data AS (
    SELECT r.id,
           r.date_created,
           r.last_updated,
           r.rate_plan_id,
           r.date,
           r.rate,
           r.rack_rate,
           r.single_rate,
           r.child_rate,
           r.infant_rate,
           r.min_los,
           r.max_los,
           r.closed_to_arrival,
           r.closed_to_departure,
           rp.room_type_id,
           rp.code AS rate_code,
           rp.rack_code,
           rt.code AS room_type_code,
           h.code  AS hotel_code,
           h.name  AS hotel_name,
           scml.se_offer_id,
           shso.se_sale_id,
           ssa.company_id,
           ssa.company_name,
           ssa.salesforce_opportunity_id,
           ssa.sale_active
    FROM latest_vault.mari.rate r
        LEFT JOIN latest_vault.mari.rate_plan rp ON r.rate_plan_id = rp.id
        LEFT JOIN latest_vault.mari.room_type rt ON rp.room_type_id = rt.id
        LEFT JOIN latest_vault.mari.hotel h ON rt.hotel_id = h.id
        LEFT JOIN se.data.se_cms_mari_link scml ON
                h.code = scml.hotel_code AND
                rp.code = scml.rate_code AND
                rp.rack_code = scml.rack_rate_code
        LEFT JOIN se.data.se_hotel_sale_offer shso ON scml.se_offer_id = shso.se_offer_id
        LEFT JOIN se.data.se_sale_attributes ssa ON shso.se_sale_id = ssa.se_sale_id
),
     agg_data AS (
         SELECT md.se_sale_id,
                md.sale_active,
                md.hotel_code,
                md.company_id,
                md.company_name,
                md.salesforce_opportunity_id,
                MAX(IFF(md.closed_to_arrival, TRUE, FALSE)) AS use_closed_to_arrival
         FROM model_data md
         GROUP BY 1, 2, 3, 4, 5, 6
     )
-- itemised --33,631 territory sales that are connected to mari data
SELECT ad.se_sale_id,
       ad.sale_active,
       ad.hotel_code,
       ad.company_id,
       ad.company_name,
       ad.salesforce_opportunity_id,
       ad.use_closed_to_arrival
FROM agg_data ad
WHERE ad.se_sale_id IS NOT NULL

-- territory sales count
-- SELECT ad.use_closed_to_arrival,
--        COUNT(DISTINCT ad.se_sale_id) AS territory_sales
-- FROM agg_data ad
-- WHERE ad.sale_active
-- GROUP BY 1

-- USE_CLOSED_TO_ARRIVAL	TERRITORY_SALES
-- true	                    2389
-- false	                31242

-- USE_CLOSED_TO_ARRIVAL	ACTIVE_TERRITORY_SALES
-- true	                    1755
-- false	                23047


-- company id count
-- SELECT ad.use_closed_to_arrival,
--        COUNT(DISTINCT ad.company_id) AS companies
-- FROM agg_data ad
-- WHERE ad.sale_active
-- GROUP BY 1

-- USE_CLOSED_TO_ARRIVAL	COMPANIES
-- true	                    336
-- false	                4285

-- USE_CLOSED_TO_ARRIVAL	ACTIVE_COMPANIES
-- true	                    261
-- false	                3111


-- global id count
-- SELECT ad.use_closed_to_arrival,
--        COUNT(DISTINCT ad.salesforce_opportunity_id) AS global_sales
-- FROM agg_data ad
-- WHERE ad.sale_active
-- GROUP BY 1

-- USE_CLOSED_TO_ARRIVAL	GLOBAL_SALES
-- true	                    358
-- false	                4432

-- USE_CLOSED_TO_ARRIVAL	ACTIVE_GLOBAL_SALES
-- true	                    262
-- false	                3115


;
-- added data to gsheet
-- https://docs.google.com/spreadsheets/d/13j9GCzE2FekVXibXvhUy8eARtlKaFBDd1570JDLEdgc/edit#gid=0


SELECT *
FROM se.data.se_cms_mari_link scml;


------------------------------------------------------------------------------------------------------------------------
-- surface closed_to_arrival

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.mari.hotel CLONE latest_vault.mari.hotel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.hotel_room_availability CLONE data_vault_mvp.dwh.hotel_room_availability;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel_sale_offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel_sale_offer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.cms_mari_link CLONE data_vault_mvp.dwh.cms_mari_link;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_offer CLONE data_vault_mvp.dwh.se_offer;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.mari.rate_plan CLONE latest_vault.mari.rate_plan;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.room_rates CLONE data_vault_mvp.dwh.room_rates;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.mari.room_type CLONE latest_vault.mari.room_type;

self_describing_task --include 'dv/dwh/mari/room_type_rooms_and_rates.py'  --method 'run' --start '2022-02-22 00:00:00' --end '2022-02-22 00:00:00'

SELECT rtrr.room_type_id,
       rtrr.room_type_name,
       rtrr.hotel_code,
       rtrr.hotel_name,
       rtrr.rate_date,
       rtrr.rate_currency,
       rtrr.lead_rate_plan_name,
       rtrr.lead_rate_plan_code,
       rtrr.rt_lead_rate_gbp,
       rtrr.rt_lead_rate_eur,
       rtrr.rt_lead_rate_rc,
       rtrr.rt_lead_single_rate_gbp,
       rtrr.rt_lead_single_rate_eur,
       rtrr.rt_lead_single_rate_rc,
       rtrr.rt_lead_child_rate_gbp,
       rtrr.rt_lead_child_rate_eur,
       rtrr.rt_lead_child_rate_rc,
       rtrr.rt_lead_infant_rate_gbp,
       rtrr.rt_lead_infant_rate_eur,
       rtrr.rt_lead_infant_rate_rc,
       rtrr.rt_avg_rack_rate_gbp,
       rtrr.rt_avg_rack_rate_eur,
       rtrr.rt_avg_rack_rate_rc,
       rtrr.rt_avg_single_rate_gbp,
       rtrr.rt_avg_single_rate_eur,
       rtrr.rt_avg_single_rate_rc,
       rtrr.rt_avg_child_rate_gbp,
       rtrr.rt_avg_child_rate_eur,
       rtrr.rt_avg_child_rate_rc,
       rtrr.rt_avg_infant_rate_gbp,
       rtrr.rt_avg_infant_rate_eur,
       rtrr.rt_avg_infant_rate_rc,
       rtrr.rt_avg_discount_percentage,
       rtrr.rt_top_discount_percentage,
       rtrr.rt_no_rates,
       rtrr.rt_no_total_rooms,
       rtrr.rt_no_available_rooms,
       rtrr.rt_no_booked_rooms,
       rtrr.rt_no_closedout_rooms,
       rtrr.rt_available_lead_rate_gbp,
       rtrr.rt_available_lead_rate_eur,
       rtrr.rt_available_lead_rate_rc,
       rtrr.rt_available_lead_single_rate_gbp,
       rtrr.rt_available_lead_single_rate_eur,
       rtrr.rt_available_lead_single_rate_rc,
       rtrr.rt_available_lead_child_rate_gbp,
       rtrr.rt_available_lead_child_rate_eur,
       rtrr.rt_available_lead_child_rate_rc,
       rtrr.rt_available_lead_infant_rate_gbp,
       rtrr.rt_available_lead_infant_rate_eur,
       rtrr.rt_available_lead_infant_rate_rc,
       rtrr.rt_available_lead_rate_rooms,
       rtrr.offer_active_and_connected
FROM data_vault_mvp.dwh.room_type_rooms_and_rates rtrr
    EXCEPT
SELECT rtrr.room_type_id,
       rtrr.room_type_name,
       rtrr.hotel_code,
       rtrr.hotel_name,
       rtrr.rate_date,
       rtrr.rate_currency,
       rtrr.lead_rate_plan_name,
       rtrr.lead_rate_plan_code,
       rtrr.rt_lead_rate_gbp,
       rtrr.rt_lead_rate_eur,
       rtrr.rt_lead_rate_rc,
       rtrr.rt_lead_single_rate_gbp,
       rtrr.rt_lead_single_rate_eur,
       rtrr.rt_lead_single_rate_rc,
       rtrr.rt_lead_child_rate_gbp,
       rtrr.rt_lead_child_rate_eur,
       rtrr.rt_lead_child_rate_rc,
       rtrr.rt_lead_infant_rate_gbp,
       rtrr.rt_lead_infant_rate_eur,
       rtrr.rt_lead_infant_rate_rc,
       rtrr.rt_avg_rack_rate_gbp,
       rtrr.rt_avg_rack_rate_eur,
       rtrr.rt_avg_rack_rate_rc,
       rtrr.rt_avg_single_rate_gbp,
       rtrr.rt_avg_single_rate_eur,
       rtrr.rt_avg_single_rate_rc,
       rtrr.rt_avg_child_rate_gbp,
       rtrr.rt_avg_child_rate_eur,
       rtrr.rt_avg_child_rate_rc,
       rtrr.rt_avg_infant_rate_gbp,
       rtrr.rt_avg_infant_rate_eur,
       rtrr.rt_avg_infant_rate_rc,
       rtrr.rt_avg_discount_percentage,
       rtrr.rt_top_discount_percentage,
       rtrr.rt_no_rates,
       rtrr.rt_no_total_rooms,
       rtrr.rt_no_available_rooms,
       rtrr.rt_no_booked_rooms,
       rtrr.rt_no_closedout_rooms,
       rtrr.rt_available_lead_rate_gbp,
       rtrr.rt_available_lead_rate_eur,
       rtrr.rt_available_lead_rate_rc,
       rtrr.rt_available_lead_single_rate_gbp,
       rtrr.rt_available_lead_single_rate_eur,
       rtrr.rt_available_lead_single_rate_rc,
       rtrr.rt_available_lead_child_rate_gbp,
       rtrr.rt_available_lead_child_rate_eur,
       rtrr.rt_available_lead_child_rate_rc,
       rtrr.rt_available_lead_infant_rate_gbp,
       rtrr.rt_available_lead_infant_rate_eur,
       rtrr.rt_available_lead_infant_rate_rc,
       rtrr.rt_available_lead_rate_rooms,
       rtrr.offer_active_and_connected
FROM data_vault_mvp_dev_robin.dwh.room_type_rooms_and_rates rtrr;

SELECT *
FROM data_vault_mvp.dwh.room_type_rooms_and_rates rtrar
WHERE rtrar.room_type_id = 8163
  AND rtrar.hotel_code = '0011r00002NeLae'
  AND rtrar.rate_date = '2022-08-01'
SELECT *
FROM data_vault_mvp_dev_robin.dwh.room_type_rooms_and_rates rtrar
WHERE rtrar.room_type_id = 8163
  AND rtrar.hotel_code = '0011r00002NeLae'
  AND rtrar.rate_date = '2022-08-01';


SELECT *
FROM data_vault_mvp.dwh.room_rates rr
WHERE rr.room_type_id = 8163
  AND rr.date = '2022-08-01';


SELECT *
FROM latest_vault.mari.rate r
    self_describing_task --include 'dv/dwh/mari/room_rates.py'  --method 'run' --start '2022-02-22 00:00:00' --end '2022-02-22 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.mari.cash_to_settle_rate CLONE latest_vault.mari.cash_to_settle_rate;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.rates CLONE data_vault_mvp.fx.rates;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.mari.hotel CLONE latest_vault.mari.hotel;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.mari.rate CLONE latest_vault.mari.rate;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.mari.rate_plan CLONE latest_vault.mari.rate_plan;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.mari.room_type CLONE latest_vault.mari.room_type;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.room_rates rr;

SELECT *
FROM latest_vault.mari.inventory_item ii;
SELECT *
FROM latest_vault.mari.inventory i;
SELECT *
FROM latest_vault.mari.room_type rt;

SELECT *
FROM se.data.se_room_rates srr
WHERE srr.hotel_code = '0016900002eXRXf';


SELECT *
FROM latest_vault.mari.rate r
WHERE r.rate_plan_id = 10094
  AND r.date >= CURRENT_DATE
  AND r.closed_to_arrival;





------------------------------------------------------------------------------------------------------------------------

WITH model_data AS (
    SELECT r.id,
           r.date_created,
           r.last_updated,
           r.rate_plan_id,
           r.date,
           r.rate,
           r.rack_rate,
           r.single_rate,
           r.child_rate,
           r.infant_rate,
           r.min_los,
           r.max_los,
           r.closed_to_arrival,
           r.closed_to_departure,
           rp.room_type_id,
           rp.code AS rate_code,
           rp.rack_code,
           rt.code AS room_type_code,
           h.code  AS hotel_code,
           h.name  AS hotel_name,
           scml.se_offer_id,
           shso.se_sale_id,
           ssa.company_id,
           ssa.company_name,
           ssa.salesforce_opportunity_id,
           ssa.sale_active
    FROM latest_vault.mari.rate r
        LEFT JOIN latest_vault.mari.rate_plan rp ON r.rate_plan_id = rp.id
        LEFT JOIN latest_vault.mari.room_type rt ON rp.room_type_id = rt.id
        LEFT JOIN latest_vault.mari.hotel h ON rt.hotel_id = h.id
        LEFT JOIN se.data.se_cms_mari_link scml ON
                h.code = scml.hotel_code AND
                rp.code = scml.rate_code AND
                rp.rack_code = scml.rack_rate_code
        LEFT JOIN se.data.se_hotel_sale_offer shso ON scml.se_offer_id = shso.se_offer_id
        LEFT JOIN se.data.se_sale_attributes ssa ON shso.se_sale_id = ssa.se_sale_id
),
     agg_data AS (
         SELECT md.se_sale_id,
                md.sale_active,
                md.hotel_code,
                md.company_id,
                md.company_name,
                md.salesforce_opportunity_id,
                MAX(IFF(md.closed_to_arrival, TRUE, FALSE)) AS use_closed_to_arrival
         FROM model_data md
         GROUP BY 1, 2, 3, 4, 5, 6
     )
-- itemised --33,631 territory sales that are connected to mari data
-- SELECT ad.se_sale_id,
--        ad.hotel_code,
--        ad.company_id,
--        ad.company_name,
--        ad.salesforce_opportunity_id,
--        ad.use_closed_to_arrival
-- FROM agg_data ad
-- WHERE ad.se_sale_id IS NOT NULL

-- territory sales count
-- SELECT ad.use_closed_to_arrival,
--        COUNT(DISTINCT ad.se_sale_id) AS territory_sales
-- FROM agg_data ad
-- WHERE ad.sale_active
-- GROUP BY 1

-- USE_CLOSED_TO_ARRIVAL        TERRITORY_SALES
-- true                            2389
-- false                        31242

-- USE_CLOSED_TO_ARRIVAL        ACTIVE_TERRITORY_SALES
-- true                            1755
-- false                        23047


-- company id count
-- SELECT ad.use_closed_to_arrival,
--        COUNT(DISTINCT ad.company_id) AS companies
-- FROM agg_data ad
-- WHERE ad.sale_active
-- GROUP BY 1

-- USE_CLOSED_TO_ARRIVAL        COMPANIES
-- true                            336
-- false                        4285

-- USE_CLOSED_TO_ARRIVAL        ACTIVE_COMPANIES
-- true                            261
-- false                        3111


-- global id count
-- SELECT ad.use_closed_to_arrival,
--        COUNT(DISTINCT ad.salesforce_opportunity_id) AS global_sales
-- FROM agg_data ad
-- WHERE ad.sale_active
-- GROUP BY 1

-- USE_CLOSED_TO_ARRIVAL        GLOBAL_SALES
-- true                            358
-- false                        4432

-- USE_CLOSED_TO_ARRIVAL        ACTIVE_GLOBAL_SALES
-- true                            262
-- false                        3115

"