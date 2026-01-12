SELECT
	MIN(dss.view_date)
FROM se.data.dim_sale_snapshot dss

-- snapshots go back to 4th Feb 2022

WITH
	sale_active_days AS
		(
			SELECT
				dss.se_sale_id,
				COUNT(*) AS days_active
			FROM se.data.dim_sale_snapshot dss
			WHERE dss.sale_active
			GROUP BY 1
		),
	sale_booking_metrics AS (
			SELECT
				fcb.se_sale_id,
				COUNT(DISTINCT fcb.booking_id)                      AS total_bookings,
				SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS total_margin_gbp,
				SUM(fcb.gross_revenue_gbp_constant_currency)        AS total_gross_revenue
			FROM se.data.fact_complete_booking fcb
-- snapshots go back to 4th Feb 2022 so filtering
			WHERE fcb.booking_completed_timestamp >= '2022-02-04'
			GROUP BY 1
		),
	active_daily_metrics AS (
			SELECT
				sbm.se_sale_id,
				sbm.total_bookings,
				sbm.total_margin_gbp,
				sbm.total_gross_revenue,
				sad.days_active,
				sbm.total_bookings / sad.days_active      AS avg_active_daily_bookings,
				sbm.total_margin_gbp / sad.days_active    AS avg_active_daily_margin_gbp,
				sbm.total_gross_revenue / sad.days_active AS avg_active_daily_gross_revenue_gbp
			FROM sale_booking_metrics sbm
				INNER JOIN sale_active_days sad ON sbm.se_sale_id = sad.se_sale_id
		)
SELECT
	d.se_sale_id,
	d.view_date,
	d.posu_cluster_region,
	d.posu_city,
	d.posu_country,
	d.posa_territory,
	d.data_model,
	d.tech_platform,
	adm.total_bookings,
	adm.total_margin_gbp,
	adm.total_gross_revenue,
	adm.days_active,
	adm.avg_active_daily_bookings,
	adm.avg_active_daily_margin_gbp,
	adm.avg_active_daily_gross_revenue_gbp
FROM se.data.dim_sale_snapshot d
	LEFT JOIN active_daily_metrics adm ON d.se_sale_id = adm.se_sale_id
WHERE d.sale_active

;
/*
20% > 0.005
40% > 0.013
60% > 0.035
80% > 0.142
100% < 0.142
 */

USE WAREHOUSE pipe_xlarge
;


/*
Notes:
 - Data from 4th February 2022
 - Complete bookings only
 */


-- sale level dataset that gives each sale a value of avg daily booking count and which percentile that is in.

WITH
	sale_active_days AS
		(
			SELECT
				dss.se_sale_id,
				COUNT(*) AS days_active
			FROM se.data.dim_sale_snapshot dss
			WHERE dss.sale_active
			GROUP BY 1
		),
	sale_booking_metrics AS (
			SELECT
				fcb.se_sale_id,
				COUNT(DISTINCT fcb.booking_id)                      AS total_bookings,
				SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS total_margin_gbp,
				SUM(fcb.gross_revenue_gbp_constant_currency)        AS total_gross_revenue
			FROM se.data.fact_complete_booking fcb
-- snapshots go back to 4th Feb 2022 so filtering
			WHERE fcb.booking_completed_timestamp >= '2022-02-04'
			GROUP BY 1
		),
	active_daily_metrics AS (
			SELECT
				sbm.se_sale_id,
				sbm.total_bookings,
				sbm.total_margin_gbp,
				sbm.total_gross_revenue,
				sad.days_active,
				sbm.total_bookings / sad.days_active      AS avg_active_daily_bookings,
				sbm.total_margin_gbp / sad.days_active    AS avg_active_daily_margin_gbp,
				sbm.total_gross_revenue / sad.days_active AS avg_active_daily_gross_revenue_gbp
			FROM sale_booking_metrics sbm
				INNER JOIN sale_active_days sad ON sbm.se_sale_id = sad.se_sale_id
		),
	avg_active_daily_booking_sale_count AS (
			SELECT
				adm.avg_active_daily_bookings,
				COUNT(DISTINCT adm.se_sale_id) AS sales
			FROM active_daily_metrics adm
			GROUP BY 1
		),
	avg_daily_bookings_percentile AS (
			SELECT
				avg_active_daily_bookings,
				sales,
				SUM(sales) OVER (ORDER BY aadbsc.avg_active_daily_bookings) AS cumulative_sales,
				SUM(sales) OVER ()                                          AS total_sales,
				cumulative_sales / total_sales                              AS percentile
			FROM avg_active_daily_booking_sale_count aadbsc
		)
SELECT
	adm.se_sale_id,
	adm.total_bookings,
	adm.total_margin_gbp,
	adm.total_gross_revenue,
	adm.days_active,
	adm.avg_active_daily_bookings,
	adm.avg_active_daily_margin_gbp,
	adm.avg_active_daily_gross_revenue_gbp,
	adbp.percentile AS avg_active_daily_bookings_percentile,
	CASE
		WHEN adbp.percentile < 0.2 THEN 'V Low'
		WHEN adbp.percentile < 0.4 THEN 'Low'
		WHEN adbp.percentile < 0.6 THEN 'Med'
		WHEN adbp.percentile < 0.8 THEN 'High'
		WHEN adbp.percentile < 1 THEN 'V High'
	END             AS avg_active_daily_bookings_group
FROM active_daily_metrics adm
	INNER JOIN avg_daily_bookings_percentile adbp ON adm.avg_active_daily_bookings = adbp.avg_active_daily_bookings
;


------------------------------------------------------------------------------------------------------------------------


SELECT
	dss.view_date,
	dss.se_sale_id,
	dss.tech_platform,
	dss.posa_territory,
	dss.salesforce_opportunity_id,
	dss.posu_country,
	dss.posu_city,
	dss.posu_division,
FROM se.data.dim_sale_snapshot dss
WHERE DAYNAME(dss.view_date) = 'Sun'
  AND dss.sale_active
  AND dss.se_brand = 'SE Brand'



WITH
	calendar AS (
		SELECT
			sc.date_value,
			sc.day_name
		FROM se.data.se_calendar sc
		WHERE sc.day_name = 'Sun'
		  AND sc.date_value >= '2022-02-04'
		  AND sc.date_value < CURRENT_DATE
	),
	sale_snapshot_data AS (
		SELECT
			dss.view_date,
			dss.se_sale_id,
			dss.tech_platform,
			dss.posa_territory,
			dss.salesforce_opportunity_id,
			dss.posu_country,
			dss.posu_city,
			dss.posu_division,
		FROM se.data.dim_sale_snapshot dss
		WHERE DAYNAME(dss.view_date) = 'Sun'
		  AND dss.sale_active
		  AND dss.se_brand = 'SE Brand'
	),
	grain AS (
		SELECT
			st.name AS posa_territory,
			c.*
		FROM se.data.se_territory st
			CROSS JOIN calendar c
	)
SELECT
	g.posa_territory,
	g.date_value                                                               AS view_date,
	g.day_name,
	ssd.se_sale_id,
	ssd.tech_platform,
	ssd.posa_territory,
	ssd.salesforce_opportunity_id,
	ssd.posu_country,
	ssd.posu_city,
	ssd.posu_division,
	LAG(g.date_value) OVER (PARTITION BY ssd.se_sale_id ORDER BY g.date_value) AS last_view_date,
	DATEDIFF(DAY, last_view_date, g.date_value)                                AS diff_from_last_view_date
FROM grain g
	LEFT JOIN sale_snapshot_data ssd ON g.posa_territory = ssd.posa_territory AND g.date_value = ssd.view_date


;

------------------------------------------------------------------------------------------------------------------------


WITH
	sale_active_days AS
		(
			SELECT
				dss.se_sale_id,
				COUNT(*) AS days_active
			FROM se.data.dim_sale_snapshot dss
			WHERE dss.sale_active
			GROUP BY 1
		),
	sale_booking_metrics AS (
			SELECT
				fcb.se_sale_id,
				COUNT(DISTINCT fcb.booking_id)                      AS total_bookings,
				SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS total_margin_gbp,
				SUM(fcb.gross_revenue_gbp_constant_currency)        AS total_gross_revenue
			FROM se.data.fact_complete_booking fcb
-- snapshots go back to 4th Feb 2022 so filtering
			WHERE fcb.booking_completed_timestamp >= '2022-02-04'
			GROUP BY 1
		),
	active_daily_metrics AS (
			SELECT
				sbm.se_sale_id,
				sbm.total_bookings,
				sbm.total_margin_gbp,
				sbm.total_gross_revenue,
				sad.days_active,
				sbm.total_bookings / sad.days_active      AS avg_active_daily_bookings,
				sbm.total_margin_gbp / sad.days_active    AS avg_active_daily_margin_gbp,
				sbm.total_gross_revenue / sad.days_active AS avg_active_daily_gross_revenue_gbp
			FROM sale_booking_metrics sbm
				INNER JOIN sale_active_days sad ON sbm.se_sale_id = sad.se_sale_id
		),
	avg_active_daily_booking_sale_count AS (
			SELECT
				adm.avg_active_daily_bookings,
				SUM(adm.total_bookings)        AS total_bookings,
				COUNT(DISTINCT adm.se_sale_id) AS sales
			FROM active_daily_metrics adm
			GROUP BY 1
		),
	avg_daily_bookings_percentile AS (
			SELECT
				avg_active_daily_bookings,
				sales,
				SUM(sales) OVER (ORDER BY aadbsc.avg_active_daily_bookings)          AS cumulative_sales,
				SUM(sales) OVER ()                                                   AS total_sales,
				cumulative_sales / total_sales                                       AS sale_percentile,
				SUM(total_bookings) OVER (ORDER BY aadbsc.avg_active_daily_bookings) AS cumulative_bookings,
				SUM(total_bookings) OVER ()                                          AS total_bookings,
				cumulative_bookings / total_bookings                                 AS booking_percentile
			FROM avg_active_daily_booking_sale_count aadbsc
		),
	percentile_groups AS
		(
			SELECT
				adm.se_sale_id,
				adm.total_bookings,
				adm.total_margin_gbp,
				adm.total_gross_revenue,
				adm.days_active,
				adm.avg_active_daily_bookings,
				adm.avg_active_daily_margin_gbp,
				adm.avg_active_daily_gross_revenue_gbp,
				adbp.sale_percentile    AS avg_active_daily_bookings_percentile,
				CASE
					WHEN adbp.sale_percentile < 0.2 THEN 'V Low'
					WHEN adbp.sale_percentile < 0.4 THEN 'Low'
					WHEN adbp.sale_percentile < 0.6 THEN 'Med'
					WHEN adbp.sale_percentile < 0.8 THEN 'High'
					WHEN adbp.sale_percentile < 1 THEN 'V High'
				END                     AS avg_active_daily_bookings_sale_group,
				adbp.booking_percentile AS avg_active_daily_bookings_percentile,
				CASE
					WHEN adbp.booking_percentile < 0.2 THEN 'V Low'
					WHEN adbp.booking_percentile < 0.4 THEN 'Low'
					WHEN adbp.booking_percentile < 0.6 THEN 'Med'
					WHEN adbp.booking_percentile < 0.8 THEN 'High'
					WHEN adbp.booking_percentile < 1 THEN 'V High'
				END                     AS avg_active_daily_bookings_booking_group
			FROM active_daily_metrics adm
				INNER JOIN avg_daily_bookings_percentile adbp
						   ON adm.avg_active_daily_bookings = adbp.avg_active_daily_bookings
		)
SELECT *
FROM percentile_groups gp
;

------------------------------------------------------------------------------------------------------------------------
/*
Proposed next steps:
Plot this categorisation against trading numbers - to prove or disprove the theory
Add lead rate into the mix
Create a sale churn dataset - I think this will be quite powerful in us understanding sale diversity
Plot this attribute against churn
Ideate and create a similar attributes of this nature eg. margin
 */


WITH
	sale_active_days AS
		(
			SELECT
				dss.se_sale_id,
				COUNT(*) AS days_active
			FROM se.data.dim_sale_snapshot dss
			WHERE dss.sale_active
			GROUP BY 1
		),
	sale_booking_metrics AS (
			SELECT
				fcb.se_sale_id,
				COUNT(DISTINCT fcb.booking_id)                      AS total_bookings,
				SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS total_margin_gbp,
				SUM(fcb.gross_revenue_gbp_constant_currency)        AS total_gross_revenue
			FROM se.data.fact_complete_booking fcb
-- snapshots go back to 4th Feb 2022 so filtering
			WHERE fcb.booking_completed_timestamp >= '2022-02-04'
			GROUP BY 1
		),
	active_daily_metrics AS (
			SELECT
				sbm.se_sale_id,
				sbm.total_bookings,
				sbm.total_margin_gbp,
				sbm.total_gross_revenue,
				sad.days_active,
				sbm.total_bookings / sad.days_active      AS avg_active_daily_bookings,
				sbm.total_margin_gbp / sad.days_active    AS avg_active_daily_margin_gbp,
				sbm.total_gross_revenue / sad.days_active AS avg_active_daily_gross_revenue_gbp
			FROM sale_booking_metrics sbm
				INNER JOIN sale_active_days sad ON sbm.se_sale_id = sad.se_sale_id
		),
	avg_active_daily_gross_revenue_sale_count AS (
			SELECT
				adm.avg_active_daily_gross_revenue_gbp,
-- 				SUM(adm.total_bookings)        AS total_bookings,
				COUNT(DISTINCT adm.se_sale_id) AS sales
			FROM active_daily_metrics adm
			GROUP BY 1
		),
	avg_daily_gross_revenue_percentile AS (
			SELECT
				avg_active_daily_gross_revenue_gbp,
				sales,
				SUM(sales) OVER (ORDER BY aadbsc.avg_active_daily_gross_revenue_gbp) AS cumulative_sales,
				SUM(sales) OVER ()                                                   AS total_sales,
				cumulative_sales / total_sales                                       AS sale_percentile
			-- 				SUM(total_bookings) OVER (ORDER BY aadbsc.avg_active_daily_gross_revenue_gbp) AS cumulative_bookings,
-- 				SUM(total_bookings) OVER ()                                                   AS total_bookings,
-- 				cumulative_bookings / total_bookings                                          AS booking_percentile
			FROM avg_active_daily_gross_revenue_sale_count aadbsc
		),
	percentile_groups AS
		(
			SELECT
				adm.se_sale_id,
				adm.total_bookings,
				adm.total_margin_gbp,
				adm.total_gross_revenue,
				adm.days_active,
				adm.avg_active_daily_bookings,
				adm.avg_active_daily_margin_gbp,
				adm.avg_active_daily_gross_revenue_gbp,
				adbp.sale_percentile AS avg_active_daily_gross_revenue_percentile,
				CASE
					WHEN adbp.sale_percentile < 0.2 THEN 'V Low'
					WHEN adbp.sale_percentile < 0.4 THEN 'Low'
					WHEN adbp.sale_percentile < 0.6 THEN 'Med'
					WHEN adbp.sale_percentile < 0.8 THEN 'High'
					WHEN adbp.sale_percentile < 1 THEN 'V High'
				END                  AS avg_active_daily_gross_revenue_sale_group
			-- 				adbp.booking_percentile AS avg_active_daily_gross_revenue_percentile,
-- 				CASE
-- 					WHEN adbp.booking_percentile < 0.2 THEN 'V Low'
-- 					WHEN adbp.booking_percentile < 0.4 THEN 'Low'
-- 					WHEN adbp.booking_percentile < 0.6 THEN 'Med'
-- 					WHEN adbp.booking_percentile < 0.8 THEN 'High'
-- 					WHEN adbp.booking_percentile < 1 THEN 'V High'
-- 				END                     AS avg_active_daily_gross_revenue_booking_group
			FROM active_daily_metrics adm
				INNER JOIN avg_daily_gross_revenue_percentile adbp
						   ON adm.avg_active_daily_gross_revenue_gbp = adbp.avg_active_daily_gross_revenue_gbp
		)
SELECT
-- 	*
avg_active_daily_gross_revenue_sale_group,
COUNT(DISTINCT gp.se_sale_id)                  AS sales,
MIN(gp.avg_active_daily_gross_revenue_gbp) AS min_avg_active_gross_revenue_gbp,
MAX(gp.avg_active_daily_gross_revenue_gbp) AS max_avg_active_gross_revenue_gbp
FROM percentile_groups gp
GROUP BY 1

-- SELECT
-- 	*
-- FROM percentile_groups gp
;


SELECT
	MIN(hscvs.view_date)
FROM se.data.harmonised_sale_calendar_view_snapshot hscvs
;

-- 2022-08-12

WITH
	lead_rate AS (
		SELECT
			hscvs.view_date,
			hscvs.se_sale_id,
			AVG(hscvs.available_lead_rate_gbp)  AS avg_available_lead_rate,
			COUNT(DISTINCT hscvs.calendar_date) AS dates,

		FROM se.data.harmonised_sale_calendar_view_snapshot hscvs
		GROUP BY 1, 2
	)

SELECT
	dss.view_date,
	dss.se_sale_id,
	dss.sale_name,
	dss.sale_product,
	dss.sale_type,
	dss.product_type,
	dss.product_configuration,
	dss.product_line,
	dss.supplier_id,
	dss.supplier_name,
	dss.partner_id,
	dss.partner_title,
	dss.data_model,
	dss.sale_start_date,
	dss.sale_end_date,
	dss.sale_active,
	dss.posa_territory,
	dss.posa_country,
	dss.posu_country,
	dss.posu_division,
	dss.posu_city,
	dss.travel_type,
	dss.target_account_list,
	dss.posu_sub_region,
	dss.posu_region,
	dss.posu_cluster,
	dss.posu_cluster_region,
	dss.posu_cluster_sub_region,
	dss.cm_region,
	dss.salesforce_opportunity_id,
	dss.se_brand,
	dss.array_sale_translation,
	dss.posa_territory_id,
	dss.tech_platform,
	avg_available_lead_rate
FROM se.data.dim_sale_snapshot dss
	INNER JOIN lead_rate lr
			   ON dss.se_sale_id = lr.se_sale_id AND dss.view_date = lr.view_date
WHERE dss.sale_active
;



SELECT
	sa.view_date,
	sa.se_sale_id,
	sa.sale_active,
	sa.sale_id,
	sa.base_sale_id,
	sa.tb_offer_id,
	sa.sale_start_date,
	sa.sale_end_date,
	sa.active,
	sa.tech_platform,
	sa.is_flashsale,
	ssa.base_sale_id,
	ssa.salesforce_opportunity_id,
	ssa.exclusive_sale,
	ssa.smart_stay_sale,
	ssa.sale_name,
	ssa.destination_name,
	ssa.sale_name_object,
	ssa.sale_active,
	ssa.class,
	ssa.has_flights_available,
	ssa.default_preferred_airport_code,
	ssa.type,
	ssa.hotel_chain_link,
	ssa.closest_airport_code,
	ssa.is_team20package,
	ssa.sale_able_to_sell_flights,
	ssa.sale_product,
	ssa.sale_type,
	ssa.product_type,
	ssa.product_configuration,
	ssa.product_line,
	ssa.data_model,
	ssa.hotel_location_info_id,
	ssa.active,
	ssa.default_hotel_offer_id,
	ssa.commission,
	ssa.commission_type,
	ssa.original_contractor_id,
	ssa.original_contractor_name,
	ssa.original_joint_contractor_id,
	ssa.original_joint_contractor_name,
	ssa.current_contractor_id,
	ssa.current_contractor_name,
	ssa.current_joint_contractor_id,
	ssa.current_joint_contractor_name,
	ssa.salesforce_current_contractor_name,
	ssa.hotel_contractor_name,
	ssa.date_created,
	ssa.destination_type,
	ssa.start_date,
	ssa.end_date,
	ssa.hotel_id,
	ssa.base_currency,
	ssa.city_district_id,
	ssa.company_id,
	ssa.company_name,
	ssa.hotel_code,
	ssa.latitude,
	ssa.longitude,
	ssa.location_info_id,
	ssa.redirect_url,
	ssa.posa_territory,
	ssa.posa_country,
	ssa.posa_currency,
	ssa.posu_division,
	ssa.posu_country,
	ssa.posu_city,
	ssa.supplier_id,
	ssa.supplier_name,
	ssa.travel_type,
	ssa.is_flashsale,
	ssa.deal_category,
	ssa.pulled_type,
	ssa.pulled_reason,
	ssa.channel_manager,
	ssa.salesforce_opportunity_id_full,
	ssa.salesforce_account_id,
	ssa.deal_profile,
	ssa.salesforce_proposed_start_date,
	ssa.salesforce_deal_label_multi,
	ssa.salesforce_stage_name,
	ssa.salesforce_repeat,
	ssa.salesforce_currency_hotel_sales,
	ssa.salesforce_currencyisocode,
	ssa.salesforce_opted_in_for_always_on,
	ssa.salesforce_parentid,
	ssa.salesforce_opted_in_for_refundable_deals,
	ssa.salesforce_opted_in_for_suvc,
	ssa.salesforce_red_flag,
	ssa.salesforce_red_flag_reason,
	ssa.target_account_list,
	ssa.star_rating,
	ssa.rating_booking_com,
	ssa.promotion_label,
	ssa.promotion_description,
	ssa.se_api_lead_rate,
	ssa.se_api_lead_rate_per_person,
	ssa.se_api_currency,
	ssa.se_api_show_discount,
	ssa.se_api_show_prices,
	ssa.se_api_discount,
	ssa.se_api_url,
	ssa.cancellation_policy_id,
	ssa.is_cancellable,
	ssa.cancellation_policy_number_of_days,
	ssa.cancellation_policy_percentage,
	ssa.number_of_reviews,
	ssa.promoter_reviews,
	ssa.passive_reviews,
	ssa.detractor_reviews,
	ssa.avg_review_score,
	ssa.nps_score,
	ssa.array_sale_translation,
	ssa.posa_territory_id,
	ssa.posu_sub_region,
	ssa.posu_region,
	ssa.posu_cluster,
	ssa.posu_cluster_region,
	ssa.posu_cluster_sub_region,
	ssa.se_brand,
	ssa.type_of_third_party,
	ssa.is_connected_to_se,
	ssa.pre_qualification_status,
	ssa.forecasted_segment,
	ssa.cms_channel_manager,
	ssa.salesforce_channel_manager,
	ssa.posa_coordinates,
	posu_coordinates,
	distance_between_posa_posu_km,
	flight_time_between_posa_posu_hours,
	haul_travel_type

FROM se.data.sale_active sa
	INNER JOIN se.data.se_sale_attributes ssa ON sa.se_sale_id = ssa.se_sale_id