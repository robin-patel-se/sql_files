WITH

-- Finds all available dates with inventory available on a deal level.

available_dates AS (

	SELECT DISTINCT
		hscv.view_date,
		hscv.salesforce_opportunity_id,
		hscv.calendar_date,
		CASE
			WHEN hscv.available_inventory > 0
				THEN 1
			ELSE 0
		END AS date_available
	FROM se.data.harmonised_sale_calendar_view_snapshot hscv
	WHERE sale_available_in_calendar = TRUE
	  AND date_available = 1
	  AND view_date > '2023-01-01'
),

-- Returns the average discount from the latest scrape

average_discount AS (

	SELECT DISTINCT
		fpc.record_timestamp                     AS record_timestamp,
		fpc.salesforce_opportunity_id,
		fpc.ota_check_in_date                    AS check_in_date,
		AVG(fpc.total_discount_percentage) / 100 AS avg_discount,
		AVG(fpc.core_discount_percentage) / 100  AS core_discount
	FROM latest_vault.fornova.price_comparison fpc
	WHERE fpc.total_discount_percentage BETWEEN -100 AND 100
	GROUP BY 1, 2, 3
),

-- filtering scrapes for most recent scrape that occurred before the availability snapshot

filtering_scrapes AS (
	SELECT
		a.view_date,
		a.salesforce_opportunity_id,
		a.calendar_date,
		a.date_available,
		ad.record_timestamp,
		ad.avg_discount,
		ad.core_discount,
		ROW_NUMBER() OVER (PARTITION BY a.salesforce_opportunity_id, a.calendar_date, a.view_date ORDER BY ad.record_timestamp DESC) AS rn
	FROM available_dates a
		INNER JOIN average_discount ad
				   ON a.salesforce_opportunity_id = ad.salesforce_opportunity_id
					   AND a.calendar_date = ad.check_in_date
					   -- only return scrapes that are before a view date
					   AND ad.record_timestamp <= a.view_date
-- limit to the most recent scrape
	QUALIFY rn = 1
),

-- Provide further sale information

sale_attributes AS (


	SELECT DISTINCT
		sa.salesforce_opportunity_id,
		sa.company_name,
		sa.current_contractor_name,
		sa.posu_cluster,
		sa.posu_cluster_region,
		sa.posu_cluster_sub_region
	FROM se.data.se_sale_attributes sa
),
modelling_data AS (
	SELECT
		fs.view_date,
		fs.record_timestamp,
		fs.salesforce_opportunity_id,
		sa.company_name,
		sa.current_contractor_name,
		sa.posu_cluster                                                   AS cluster,
		sa.posu_cluster_region                                            AS cluster_region,
		sa.posu_cluster_sub_region                                        AS cluster_sub_region,
		fs.calendar_date,
		fs.avg_discount,
		fs.core_discount,
		IFF(fs.avg_discount >= 0.3, 1, 0)                                 AS date_has_over_30_avg_discount,
		IFF(fs.core_discount >= 0.2, 1, 0)                                AS date_has_over_20_avg_core_discount,
		AVG(fs.avg_discount)
			OVER (PARTITION BY fs.salesforce_opportunity_id,fs.view_date) AS deal_average_discount,
		AVG(fs.core_discount)
			OVER (PARTITION BY fs.salesforce_opportunity_id,fs.view_date) AS deal_average_core_discount,
		IFF(deal_average_discount >= 0.3, 1, 0)                           AS deal_average_over_30,
		IFF(deal_average_core_discount >= 0.2, 1, 0)                      AS deal_core_average_over_20
	FROM filtering_scrapes fs
		LEFT JOIN sale_attributes sa
				  ON fs.salesforce_opportunity_id = sa.salesforce_opportunity_id
)
SELECT
	md.view_date,
	md.salesforce_opportunity_id,
	COUNT(DISTINCT IFF(md.date_has_over_30_avg_discount = 1, calendar_date, NULL)) AS dates_over_30_avg_discount,
	COUNT(DISTINCT md.calendar_date)                                               AS total_dates
FROM modelling_data md
GROUP BY 1, 2


-- sum of dates that are over a certain % of discount / total dates


------------------------------------------------------------------------------------------------------------------------
-- adding in key dates


WITH

-- input cte to limit to only necessary view dates

calendar_base AS (
	SELECT
		DATE(date_value) AS calendar_date
	FROM se.data.se_calendar
	WHERE date_value >= '2023-08-01' AND date_value <= '2025-12-31'
),


-- Finds all available dates with inventory available on a deal level.

available_dates AS (

	SELECT DISTINCT
		hscv.view_date,
		hscv.salesforce_opportunity_id,
		hscv.calendar_date,
		ssa.company_name,
		ssa.current_contractor_name,
		ssa.posu_cluster,
		ssa.posu_cluster_region,
		ssa.posu_cluster_sub_region,
		CASE
			WHEN hscv.available_inventory > 0
				THEN 1
			ELSE 0
		END                                                                                             AS date_available,
		IFF((kdd.start_date <= hscv.calendar_date AND kdd.end_date >= hscv.calendar_date), TRUE, FALSE) AS key_date_flag
	FROM se.data.harmonised_sale_calendar_view_snapshot hscv
		INNER JOIN calendar_base cb ON hscv.calendar_date = cb.calendar_date
		LEFT JOIN  se.data.se_sale_attributes ssa ON hscv.se_sale_id = ssa.se_sale_id
		LEFT JOIN  latest_vault.cro_gsheets.key_dates_definition kdd
				   ON CONCAT(kdd.cluster, kdd.cluster_region, kdd.cluster_sub_region, MONTH(kdd.ref_date)) =
					  CONCAT(ssa.posu_cluster, ssa.posu_cluster_region, ssa.posu_cluster_sub_region,
							 MONTH(hscv.view_date))

	WHERE hscv.sale_available_in_calendar = TRUE
	  AND date_available = 1
),

-- Returns the average discount from the latest scrape

average_discount AS (

	SELECT DISTINCT
		fpc.record_timestamp                     AS record_timestamp,
		fpc.salesforce_opportunity_id,
		fpc.ota_check_in_date                    AS check_in_date,
		AVG(fpc.total_discount_percentage) / 100 AS avg_discount,
		AVG(fpc.core_discount_percentage) / 100  AS core_discount
	FROM latest_vault.fornova.price_comparison fpc
	WHERE fpc.total_discount_percentage BETWEEN -100 AND 100
	GROUP BY 1, 2, 3
),

-- filtering scrapes for most recent scrape that occurred before the availability snapshot

filtering_scrapes AS (

	SELECT
		a.view_date,
		a.salesforce_opportunity_id,
		a.calendar_date,
		a.date_available,
		a.key_date_flag,
		a.company_name,
		a.current_contractor_name,
		a.posu_cluster,
		a.posu_cluster_region,
		a.posu_cluster_sub_region,
		ad.record_timestamp,
		ad.avg_discount,
		ad.core_discount,
		ROW_NUMBER() OVER (PARTITION BY a.salesforce_opportunity_id, a.calendar_date, a.view_date ORDER BY ad.record_timestamp DESC) AS rn
	FROM available_dates a
		INNER JOIN average_discount ad
				   ON a.salesforce_opportunity_id = ad.salesforce_opportunity_id
					   AND a.calendar_date = ad.check_in_date
					   -- only return scrapes that are before a view date
					   AND ad.record_timestamp <= a.view_date
-- limit to the most recent scrape
	QUALIFY rn = 1
)


SELECT
	fs.view_date,
	fs.record_timestamp,
	fs.salesforce_opportunity_id,
	fs.company_name,
	fs.current_contractor_name,
	fs.posu_cluster                                                   AS cluster,
	fs.posu_cluster_region                                            AS cluster_region,
	fs.posu_cluster_sub_region                                        AS cluster_sub_region,
	fs.calendar_date,
	fs.key_date_flag,
	fs.avg_discount,
	fs.core_discount,
	IFF(fs.avg_discount >= 0.3, 1, 0)                                 AS date_has_over_30_avg_discount,
	IFF(fs.core_discount >= 0.2, 1, 0)                                AS date_has_over_20_avg_core_discount,
	IFF(date_has_over_30_avg_discount + date_has_over_20_avg_core_discount = 2, 1,
		0)                                                            AS date_meets_avg_and_core_thresholds,
	AVG(fs.avg_discount)
		OVER (PARTITION BY fs.salesforce_opportunity_id,fs.view_date) AS deal_average_discount,
	AVG(fs.core_discount)
		OVER (PARTITION BY fs.salesforce_opportunity_id,fs.view_date) AS deal_average_core_discount,
	IFF(deal_average_discount >= 0.3, 1, 0)                           AS deal_average_over_30,
	IFF(deal_average_core_discount >= 0.2, 1, 0)                      AS deal_core_average_over_20,
	IFF(deal_average_over_30 + deal_core_average_over_20 = 2, 1, 0)   AS deal_meets_avg_and_core_thresholds
FROM filtering_scrapes fs
-- WHERE fs.company_name = 'Tortworth Court'
