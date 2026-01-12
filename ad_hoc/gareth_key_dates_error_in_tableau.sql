-- sql from hscv snapshot datasource

/*
SELECT
	view_date,
	DATE_TRUNC('month', view_date) AS view_month,
	MONTH(view_date)               AS view_date_all_years,
	hotel_code,
	se_sale_id,
	posu_cluster,
	posu_cluster_region,
	posu_cluster_sub_region,
	sale_active,
	sale_available_in_calendar,
	calendar_date,
	available_inventory,
	reserved_inventory,
	total_inventory,
	no_available_offer_ids,
	lead_rate_gbp,
	lead_rate_eur,
	lead_rate_per_night_gbp,
	lead_rate_per_night_eur,
	min_discount_percentage,
	avg_discount_percentage,
	max_discount_percentage,
	available_lead_rate_gbp,
	available_lead_rate_eur,
	available_lead_rate_per_night_gbp,
	available_lead_rate_per_night_eur,
	available_min_discount_percentage,
	available_avg_discount_percentage,
	available_max_discount_percentage,
	accommodation_source,
	salesforce_opportunity_id,
	hard_fail,
	lead_release_period,
	lead_is_outside_release_period,
	'TRUE'                         AS key_seasonal_travel_date,
	reference_month                AS reference_month_all_years,
	start_date_key_seasonal_travel_period,
	end_date_key_seasonal_travel_period
FROM se.bi.harmonised_sale_calendar_view_snapshot*/


SELECT
	hscvs.view_date,
	DATE_TRUNC('month', hscvs.view_date)             AS view_month,
	MONTH(hscvs.view_date)                           AS view_date_all_years,
	hscvs.hotel_code,
	hscvs.se_sale_id,
	hscvs.posu_cluster,
	hscvs.posu_cluster_region,
	hscvs.posu_cluster_sub_region,
	hscvs.sale_active,
	hscvs.sale_available_in_calendar,
	hscvs.calendar_date,
	hscvs.available_inventory,
	hscvs.reserved_inventory,
	hscvs.total_inventory,
	hscvs.no_available_offer_ids,
	hscvs.lead_rate_gbp,
	hscvs.lead_rate_eur,
	hscvs.lead_rate_per_night_gbp,
	hscvs.lead_rate_per_night_eur,
	hscvs.min_discount_percentage,
	hscvs.avg_discount_percentage,
	hscvs.max_discount_percentage,
	hscvs.available_lead_rate_gbp,
	hscvs.available_lead_rate_eur,
	hscvs.available_lead_rate_per_night_gbp,
	hscvs.available_lead_rate_per_night_eur,
	hscvs.available_min_discount_percentage,
	hscvs.available_avg_discount_percentage,
	hscvs.available_max_discount_percentage,
	hscvs.accommodation_source,
	hscvs.salesforce_opportunity_id,
	hscvs.hard_fail,
	hscvs.lead_release_period,
	hscvs.lead_is_outside_release_period,
	'TRUE'                                           AS key_seasonal_travel_date,
	hscvs.reference_month                            AS reference_month_all_years,
	hscvs.start_date_key_seasonal_travel_period,
	hscvs.end_date_key_seasonal_travel_period,
	IFF(
				hscvs.calendar_date BETWEEN hscvs.start_date_key_seasonal_travel_period AND hscvs.end_date_key_seasonal_travel_period AND
				hscvs.available_inventory > 0, 1, 0) AS available_key_date,
	IFF(hscvs.available_inventory > 0, 1, 0)         AS available_date
FROM se.bi.harmonised_sale_calendar_view_snapshot hscvs
WHERE hscvs.salesforce_opportunity_id = '006w000000kOFH4' -- TODO REMOVE
  AND hscvs.view_date = CURRENT_DATE
;

SELECT GET_DDL('table', 'se.bi.harmonised_sale_calendar_view_snapshot')
;

/*
create or replace view se.bi.HARMONISED_SALE_CALENDAR_VIEW_SNAPSHOT(
	VIEW_DATE,
	HOTEL_CODE,
	SE_SALE_ID,
	POSU_CLUSTER,
	POSU_CLUSTER_REGION,
	POSU_CLUSTER_SUB_REGION,
	SALE_ACTIVE,
	SALE_AVAILABLE_IN_CALENDAR,
	CALENDAR_DATE,
	AVAILABLE_INVENTORY,
	RESERVED_INVENTORY,
	TOTAL_INVENTORY,
	NO_AVAILABLE_OFFER_IDS,
	LEAD_RATE_GBP,
	LEAD_RATE_EUR,
	LEAD_RATE_PER_NIGHT_GBP,
	LEAD_RATE_PER_NIGHT_EUR,
	MIN_DISCOUNT_PERCENTAGE,
	AVG_DISCOUNT_PERCENTAGE,
	MAX_DISCOUNT_PERCENTAGE,
	AVAILABLE_LEAD_RATE_GBP,
	AVAILABLE_LEAD_RATE_EUR,
	AVAILABLE_LEAD_RATE_PER_NIGHT_GBP,
	AVAILABLE_LEAD_RATE_PER_NIGHT_EUR,
	AVAILABLE_MIN_DISCOUNT_PERCENTAGE,
	AVAILABLE_AVG_DISCOUNT_PERCENTAGE,
	AVAILABLE_MAX_DISCOUNT_PERCENTAGE,
	ACCOMMODATION_SOURCE,
	SALESFORCE_OPPORTUNITY_ID,
	LEAD_RELEASE_PERIOD,
	LEAD_IS_OUTSIDE_RELEASE_PERIOD,
	HARD_FAIL,
	START_DATE,
	END_DATE,
	KEY_SEASONAL_TRAVEL_DATE,
	REFERENCE_MONTH,
	START_DATE_KEY_SEASONAL_TRAVEL_PERIOD,
	END_DATE_KEY_SEASONAL_TRAVEL_PERIOD
) as
        SELECT
            sale_calendar.view_date,
            sale_calendar.hotel_code,
            sale_calendar.se_sale_id,
            dim_sale.posu_cluster,
            dim_sale.posu_cluster_region,
            dim_sale.posu_cluster_sub_region,
            sale_calendar.sale_active,
            sale_calendar.sale_available_in_calendar,
            sale_calendar.calendar_date,
            sale_calendar.available_inventory,
            sale_calendar.reserved_inventory,
            sale_calendar.total_inventory,
            sale_calendar.no_available_offer_ids,
            sale_calendar.lead_rate_gbp,
            sale_calendar.lead_rate_eur,
            sale_calendar.lead_rate_per_night_gbp,
            sale_calendar.lead_rate_per_night_eur,
            sale_calendar.min_discount_percentage,
            sale_calendar.avg_discount_percentage,
            sale_calendar.max_discount_percentage,
            sale_calendar.available_lead_rate_gbp,
            sale_calendar.available_lead_rate_eur,
            sale_calendar.available_lead_rate_per_night_gbp,
            sale_calendar.available_lead_rate_per_night_eur,
            sale_calendar.available_min_discount_percentage,
            sale_calendar.available_avg_discount_percentage,
            sale_calendar.available_max_discount_percentage,
            sale_calendar.accommodation_source,
            sale_calendar.salesforce_opportunity_id,
            sale_calendar.lead_release_period,
            sale_calendar.lead_is_outside_release_period,
            sale_calendar.hard_fail,
            key_dates.start_date,
            key_dates.end_date,
            'TRUE' AS key_seasonal_travel_date,
            key_dates.ref_date AS reference_month,
            DATEADD('day', key_dates.days_start_range, DATE_TRUNC('month',sale_calendar.view_date)) AS start_date_key_seasonal_travel_period,
            DATEADD('day', key_dates.days_end_range, DATE_TRUNC('month',sale_calendar.view_date)) AS end_date_key_seasonal_travel_period

        FROM data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot sale_calendar
        LEFT JOIN data_vault_mvp.dwh.dim_sale dim_sale ON dim_sale.se_sale_id = sale_calendar.se_sale_id
        LEFT JOIN latest_vault.cro_gsheets.key_dates_definition key_dates
            ON CONCAT(key_dates.cluster, key_dates.cluster_region, key_dates.cluster_sub_region, MONTH(key_dates.ref_date)) =
            CONCAT(dim_sale.posu_cluster, dim_sale.posu_cluster_region, dim_sale.posu_cluster_sub_region, MONTH(sale_calendar.view_date))

        WHERE
        (
            sale_calendar.view_date BETWEEN CURRENT_DATE - 790 AND CURRENT_DATE - 31
            AND DAYOFWEEK(sale_calendar.view_date) = 0
        )
        OR
            sale_calendar.view_date >= CURRENT_DATE - 30
;*/


USE ROLE pipelinerunner
;

GRANT SELECT ON TABLE se.bi.harmonised_sale_calendar_view_snapshot TO ROLE tableau
;

CREATE OR REPLACE VIEW se.bi.harmonised_sale_calendar_view_snapshot
			(
			 view_date,
			 hotel_code,
			 se_sale_id,
			 posu_cluster,
			 posu_cluster_region,
			 posu_cluster_sub_region,
			 sale_active,
			 sale_available_in_calendar,
			 calendar_date,
			 available_inventory,
			 reserved_inventory,
			 total_inventory,
			 no_available_offer_ids,
			 lead_rate_gbp,
			 lead_rate_eur,
			 lead_rate_per_night_gbp,
			 lead_rate_per_night_eur,
			 min_discount_percentage,
			 avg_discount_percentage,
			 max_discount_percentage,
			 available_lead_rate_gbp,
			 available_lead_rate_eur,
			 available_lead_rate_per_night_gbp,
			 available_lead_rate_per_night_eur,
			 available_min_discount_percentage,
			 available_avg_discount_percentage,
			 available_max_discount_percentage,
			 accommodation_source,
			 salesforce_opportunity_id,
			 lead_release_period,
			 lead_is_outside_release_period,
			 hard_fail,
			 start_date,
			 end_date,
			 key_seasonal_travel_date,
			 reference_month,
			 start_date_key_seasonal_travel_period,
			 end_date_key_seasonal_travel_period
				)
AS
SELECT
	sale_calendar.view_date,
	sale_calendar.hotel_code,
	sale_calendar.se_sale_id,
	dim_sale.posu_cluster,
	dim_sale.posu_cluster_region,
	dim_sale.posu_cluster_sub_region,
	sale_calendar.sale_active,
	sale_calendar.sale_available_in_calendar,
	sale_calendar.calendar_date,
	sale_calendar.available_inventory,
	sale_calendar.reserved_inventory,
	sale_calendar.total_inventory,
	sale_calendar.no_available_offer_ids,
	sale_calendar.lead_rate_gbp,
	sale_calendar.lead_rate_eur,
	sale_calendar.lead_rate_per_night_gbp,
	sale_calendar.lead_rate_per_night_eur,
	sale_calendar.min_discount_percentage,
	sale_calendar.avg_discount_percentage,
	sale_calendar.max_discount_percentage,
	sale_calendar.available_lead_rate_gbp,
	sale_calendar.available_lead_rate_eur,
	sale_calendar.available_lead_rate_per_night_gbp,
	sale_calendar.available_lead_rate_per_night_eur,
	sale_calendar.available_min_discount_percentage,
	sale_calendar.available_avg_discount_percentage,
	sale_calendar.available_max_discount_percentage,
	sale_calendar.accommodation_source,
	sale_calendar.salesforce_opportunity_id,
	sale_calendar.lead_release_period,
	sale_calendar.lead_is_outside_release_period,
	sale_calendar.hard_fail,
	key_dates.start_date,
	key_dates.end_date,
	'TRUE'                                                AS key_seasonal_travel_date,
	key_dates.ref_date                                    AS reference_month,
	DATEADD('day', key_dates.days_start_range,
			DATE_TRUNC('month', sale_calendar.view_date)) AS start_date_key_seasonal_travel_period,
	DATEADD('day', key_dates.days_end_range,
			DATE_TRUNC('month', sale_calendar.view_date)) AS end_date_key_seasonal_travel_period

FROM data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot sale_calendar
	LEFT JOIN data_vault_mvp.dwh.dim_sale dim_sale ON dim_sale.se_sale_id = sale_calendar.se_sale_id
	LEFT JOIN latest_vault.cro_gsheets.key_dates_definition key_dates
			  ON CONCAT(key_dates.cluster, key_dates.cluster_region, key_dates.cluster_sub_region,
						MONTH(key_dates.ref_date)) =
				 CONCAT(dim_sale.posu_cluster, dim_sale.posu_cluster_region, dim_sale.posu_cluster_sub_region,
						MONTH(sale_calendar.view_date))

WHERE sale_calendar.view_date >= CURRENT_DATE
;


CREATE OR REPLACE VIEW se.bi.harmonised_sale_calendar_view_snapshot
			COPY GRANTS (
						 view_date,
						 hotel_code,
						 se_sale_id,
						 posu_cluster,
						 posu_cluster_region,
						 posu_cluster_sub_region,
						 sale_active,
						 sale_available_in_calendar,
						 calendar_date,
						 available_inventory,
						 reserved_inventory,
						 total_inventory,
						 no_available_offer_ids,
						 lead_rate_gbp,
						 lead_rate_eur,
						 lead_rate_per_night_gbp,
						 lead_rate_per_night_eur,
						 min_discount_percentage,
						 avg_discount_percentage,
						 max_discount_percentage,
						 available_lead_rate_gbp,
						 available_lead_rate_eur,
						 available_lead_rate_per_night_gbp,
						 available_lead_rate_per_night_eur,
						 available_min_discount_percentage,
						 available_avg_discount_percentage,
						 available_max_discount_percentage,
						 accommodation_source,
						 salesforce_opportunity_id,
						 lead_release_period,
						 lead_is_outside_release_period,
						 hard_fail,
						 start_date,
						 end_date,
						 key_seasonal_travel_date,
						 reference_month,
						 start_date_key_seasonal_travel_period,
						 end_date_key_seasonal_travel_period
		)
AS
SELECT
	sale_calendar.view_date,
	sale_calendar.hotel_code,
	sale_calendar.se_sale_id,
	dim_sale.posu_cluster,
	dim_sale.posu_cluster_region,
	dim_sale.posu_cluster_sub_region,
	sale_calendar.sale_active,
	sale_calendar.sale_available_in_calendar,
	sale_calendar.calendar_date,
	sale_calendar.available_inventory,
	sale_calendar.reserved_inventory,
	sale_calendar.total_inventory,
	sale_calendar.no_available_offer_ids,
	sale_calendar.lead_rate_gbp,
	sale_calendar.lead_rate_eur,
	sale_calendar.lead_rate_per_night_gbp,
	sale_calendar.lead_rate_per_night_eur,
	sale_calendar.min_discount_percentage,
	sale_calendar.avg_discount_percentage,
	sale_calendar.max_discount_percentage,
	sale_calendar.available_lead_rate_gbp,
	sale_calendar.available_lead_rate_eur,
	sale_calendar.available_lead_rate_per_night_gbp,
	sale_calendar.available_lead_rate_per_night_eur,
	sale_calendar.available_min_discount_percentage,
	sale_calendar.available_avg_discount_percentage,
	sale_calendar.available_max_discount_percentage,
	sale_calendar.accommodation_source,
	sale_calendar.salesforce_opportunity_id,
	sale_calendar.lead_release_period,
	sale_calendar.lead_is_outside_release_period,
	sale_calendar.hard_fail,
	key_dates.start_date,
	key_dates.end_date,
	'TRUE'                                                AS key_seasonal_travel_date,
	key_dates.ref_date                                    AS reference_month,
	DATEADD('day', key_dates.days_start_range,
			DATE_TRUNC('month', sale_calendar.view_date)) AS start_date_key_seasonal_travel_period,
	DATEADD('day', key_dates.days_end_range,
			DATE_TRUNC('month', sale_calendar.view_date)) AS end_date_key_seasonal_travel_period

FROM data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot sale_calendar
	LEFT JOIN data_vault_mvp.dwh.dim_sale dim_sale ON dim_sale.se_sale_id = sale_calendar.se_sale_id
	LEFT JOIN latest_vault.cro_gsheets.key_dates_definition key_dates
			  ON CONCAT(key_dates.cluster, key_dates.cluster_region, key_dates.cluster_sub_region,
						MONTH(key_dates.ref_date)) =
				 CONCAT(dim_sale.posu_cluster, dim_sale.posu_cluster_region, dim_sale.posu_cluster_sub_region,
						MONTH(sale_calendar.view_date))

WHERE (
			  sale_calendar.view_date BETWEEN CURRENT_DATE - 790 AND CURRENT_DATE - 31
			  AND DAYOFWEEK(sale_calendar.view_date) = 0
		  ) OR
	  sale_calendar.view_date >= CURRENT_DATE - 30
;


SELECT DISTINCT
	se_sale_id
FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales dr02tyms
;

SELECT *
FROM se.data_pii.tvl_user_attributes
;

USE WAREHOUSE pipe_xlarge
;

SELECT
-- 	stba.touch_id,
-- 	stba.touch_start_tstamp,
-- stba.touch_experience,
stmc.touch_mkt_channel,
COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_experience LIKE 'native app%'
GROUP BY 1;