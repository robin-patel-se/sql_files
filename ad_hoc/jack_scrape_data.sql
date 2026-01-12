WITH
	latest_shop AS (
		SELECT
			salesforce_opportunity_id,
			ota_check_in_date,
			room_type_name,
			MAX(record_timestamp) AS latest_record_timestamp
		FROM latest_vault.fornova.price_comparison
		WHERE ota_check_in_date IS NOT NULL
		GROUP BY salesforce_opportunity_id, ota_check_in_date, room_type_name
	),


	sale_active AS (
		SELECT
			salesforce_opportunity_id,
			MAX(sale_active) AS sale_active,
			is_connected_to_se,
			channel_manager
		FROM se.data.se_sale_attributes_snapshot
		WHERE view_date = '2024-11-04' AND sale_active = TRUE
		GROUP BY 1, 3, 4
	),


	available_dates AS (
		SELECT
			salesforce_opportunity_id,
			view_date,
			available_inventory,
			calendar_date,
			CASE WHEN available_inventory > 0 THEN 1 ELSE 0 END AS date_available
		FROM se.data.harmonised_sale_calendar_view_snapshot
		WHERE view_date = '2024-11-04' AND date_available = 1
	),


	latest_discount_data AS (
		SELECT
			fpc.salesforce_opportunity_id,
			fpc.ota_check_in_date              AS check_in_date,
			fpc.room_type_name,
			fpc.record_timestamp,
			AVG(fpc.total_discount_percentage) AS total_discount
		FROM latest_vault.fornova.price_comparison fpc
			INNER JOIN latest_shop ls ON
			ls.salesforce_opportunity_id = fpc.salesforce_opportunity_id
				AND ls.room_type_name = fpc.room_type_name
				AND ls.latest_record_timestamp = fpc.record_timestamp
				AND ls.ota_check_in_date = fpc.ota_check_in_date
		WHERE fpc.availability = 'available'
		  AND fpc.total_discount_percentage BETWEEN -100 AND 100
		GROUP BY fpc.salesforce_opportunity_id, fpc.ota_check_in_date, fpc.room_type_name, fpc.record_timestamp
	),


	filter_data AS (
		SELECT DISTINCT
			fpc.salesforce_opportunity_id,
			fpc.record_timestamp,
			fpc.hotel_name,
			sa.sale_active,
			ds.posu_cluster_region,
			ds.posu_cluster_sub_region,
			ad.view_date,
			fpc.room_type_name,
			latest_discount_data.check_in_date,
			latest_discount_data.total_discount
		FROM latest_vault.fornova.price_comparison fpc
			INNER JOIN latest_discount_data ON
			latest_discount_data.salesforce_opportunity_id = fpc.salesforce_opportunity_id
				AND latest_discount_data.check_in_date = fpc.ota_check_in_date
				AND latest_discount_data.record_timestamp = fpc.record_timestamp
			LEFT JOIN  se.data.dim_sale ds ON ds.salesforce_opportunity_id = fpc.salesforce_opportunity_id
			INNER JOIN sale_active sa ON sa.salesforce_opportunity_id = fpc.salesforce_opportunity_id
			LEFT JOIN  available_dates ad ON ad.salesforce_opportunity_id = fpc.salesforce_opportunity_id
			AND ad.calendar_date = fpc.ota_check_in_date
		WHERE fpc.record_timestamp::date BETWEEN ad.view_date - 30 AND ad.view_date
		  AND fpc.availability = 'available'
		  AND fpc.total_discount_percentage BETWEEN -100 AND 100
		  AND fpc.salesforce_opportunity_id = '0066900001NlKRF'
		  AND ad.view_date = '2024-11-04'
		  AND fpc.ota_check_in_date = '2025-08-03'
	),


	final_data AS (
		SELECT
			fd.view_date,
			fd.posu_cluster_region                                  AS region,
			fd.posu_cluster_sub_region                              AS sub_region,
			fd.salesforce_opportunity_id                            AS salesforce_opp_id,
			fd.hotel_name                                           AS hotel_name,
			fd.record_timestamp                                     AS latest_price_check,
			fd.check_in_date,
			fd.room_type_name,
			ROUND(AVG(fd.total_discount), 2)                        AS avg_total_discount,
			SUM(IFF(fd.total_discount BETWEEN 0 AND 15, 1, 0))      AS number_of_soft_failing_dates,
			SUM(IFF(fd.total_discount NOT BETWEEN 0 AND 15, 1, 0))  AS number_of_non_soft_failing_dates,
			COUNT(*)                                                AS total_available_dates,
			COUNT(DISTINCT fd.salesforce_opportunity_id)            AS deals_scraped,
			COUNT_IF(number_of_soft_failing_dates > 0)
					 OVER (PARTITION BY fd.posu_cluster_sub_region) AS sub_region_total_deals_soft_failing,
			ROUND((sub_region_total_deals_soft_failing /
				   COUNT(deals_scraped) OVER (PARTITION BY fd.posu_cluster_sub_region)) * 100,
				  2)                                                AS sub_region_pct_deals_soft_failing
		FROM filter_data fd
		GROUP BY fd.view_date, fd.posu_cluster_region, fd.posu_cluster_sub_region,
				 fd.salesforce_opportunity_id, fd.hotel_name, fd.record_timestamp,
				 fd.check_in_date, fd.room_type_name
	)


SELECT *
FROM final_data
ORDER BY view_date, region, salesforce_opp_id, hotel_name, latest_price_check, check_in_date, room_type_name ASC
;

WITH
	filter_fornova_scrapes AS (
		-- filter out multiple searches to only show most recent search for each salesforce opportunity id / room / check in date
		SELECT
			pc.salesforce_opportunity_id,
			pc.hotel_name,
			pc.offer_name,
			pc.room_type_name,
			pc.ota_check_in_date,
			pc.total_discount_percentage,
			pc.record_timestamp
		FROM latest_vault.fornova.price_comparison pc
		WHERE pc.record_timestamp::DATE <= pc.ota_check_in_date -- filter out scrapes that occurred for after the check in date
		  AND pc.salesforce_opportunity_id = '0066900001NlKRF'  -- TODO REMOVE
		  AND pc.ota_check_in_date = '2025-08-03'               -- TODO REMOVE
		QUALIFY
			ROW_NUMBER() OVER (PARTITION BY pc.salesforce_opportunity_id, pc.room_type_name, pc.ota_check_in_date ORDER BY pc.record_timestamp DESC) =
			1 -- remove all but the most recent scrape
	),
	avg_daily_discount AS (
		-- average the total discount to salesforce opportunity id and check in (i.e. remove room grain)
		SELECT
			ffs.salesforce_opportunity_id,
			ffs.ota_check_in_date,
			ROUND(AVG(ffs.total_discount_percentage), 2) AS avg_total_discount_percentage
		FROM filter_fornova_scrapes ffs -- ;) see what I did there?
		GROUP BY ffs.salesforce_opportunity_id, ffs.ota_check_in_date
	),
	sale_active AS (
		SELECT
			salesforce_opportunity_id,
			MAX(sale_active) AS sale_active,
			is_connected_to_se,
			channel_manager
		FROM se.data.se_sale_attributes_snapshot
		WHERE view_date = '2024-11-04' AND sale_active = TRUE
		GROUP BY 1, 3, 4
	),
	available_dates AS (
		SELECT
			salesforce_opportunity_id,
			view_date,
			available_inventory,
			calendar_date,
			CASE WHEN available_inventory > 0 THEN 1 ELSE 0 END AS date_available
		FROM se.data.harmonised_sale_calendar_view_snapshot
		WHERE view_date = '2024-11-04' AND date_available = 1
	)

SELECT *
FROM avg_daily_discount
;

------------------------------------------------------------------------------------------------------------------------
-- 		SELECT
-- 			fd.view_date,
-- 			fd.posu_cluster_region                                  AS region,
-- 			fd.posu_cluster_sub_region                              AS sub_region,
-- 			fd.salesforce_opportunity_id                            AS salesforce_opp_id,
-- 			fd.hotel_name                                           AS hotel_name,
-- 			fd.record_timestamp                                     AS latest_price_check,
-- 			fd.check_in_date,
-- 			fd.room_type_name,
-- 			ROUND(AVG(fd.total_discount), 2)                        AS avg_total_discount,
-- 			SUM(IFF(fd.total_discount BETWEEN 0 AND 15, 1, 0))      AS number_of_soft_failing_dates,
-- 			SUM(IFF(fd.total_discount NOT BETWEEN 0 AND 15, 1, 0))  AS number_of_non_soft_failing_dates,
-- 			COUNT(*)                                                AS total_available_dates,
-- 			COUNT(DISTINCT fd.salesforce_opportunity_id)            AS deals_scraped,
-- 			COUNT_IF(number_of_soft_failing_dates > 0)
-- 					 OVER (PARTITION BY fd.posu_cluster_sub_region) AS sub_region_total_deals_soft_failing,
-- 			ROUND((sub_region_total_deals_soft_failing /
-- 				   COUNT(deals_scraped) OVER (PARTITION BY fd.posu_cluster_sub_region)) * 100,
-- 				  2)                                                AS sub_region_pct_deals_soft_failing
-- 		FROM filter_data fd
-- 		GROUP BY fd.view_date, fd.posu_cluster_region, fd.posu_cluster_sub_region,
-- 				 fd.salesforce_opportunity_id, fd.hotel_name, fd.record_timestamp,
-- 				 fd.check_in_date, fd.room_type_name
------------------------------------------------------------------------------------------------------------------------

-- understand the common grain, this is normally dictacted by expected output, at this point it would appear to be sf op id, view date, check in date
-- model independent data feeds up to that grain, however sale active is not at calendar view grain so will need to use as enrichment
-- compute any isolated data source specific metrics independently before joining


WITH
	sale_active AS (
		SELECT
			ssas.view_date,
			ssas.salesforce_opportunity_id,
			ssas.is_connected_to_se,
			ssas.channel_manager,
			MAX(ssas.sale_active) AS sale_active
		FROM se.data.se_sale_attributes_snapshot ssas
		WHERE view_date = '2024-11-04' AND sale_active = TRUE
		GROUP BY 1, 2, 3, 4
	),
	available_dates AS (
		SELECT DISTINCT
			hscvs.view_date,
			hscvs.salesforce_opportunity_id,
			hscvs.calendar_date,
			TRUE AS is_available_date
		FROM se.data.harmonised_sale_calendar_view_snapshot hscvs
		WHERE view_date = '2024-11-04'
		  AND hscvs.available_inventory > 0
	),
	filter_fornova_scrapes AS (
		-- filter nonsense scrapes
		SELECT
			pc.salesforce_opportunity_id,
			pc.ota_check_in_date,
			pc.record_timestamp,
			pc.total_discount_percentage
		-- 			pc.hotel_name,
-- 			pc.offer_name,
-- 			pc.room_type_name,
		FROM latest_vault.fornova.price_comparison pc
		WHERE pc.record_timestamp::DATE <= pc.ota_check_in_date -- filter out scrapes that occurred for after the check in date
		  AND pc.total_discount_percentage IS NOT NULL -- there are entries without ota rate and therefore no discount percentage
	),
	scrapes AS (
		-- average the discount percentage up to salesforce opportunity level by check in date
		-- i.e. remove the room level grain
		SELECT
			ffs.salesforce_opportunity_id,
			ffs.ota_check_in_date,
			ffs.record_timestamp::DATE         AS scrape_date,
			AVG(ffs.total_discount_percentage) AS avg_total_discount_percentage
		FROM filter_fornova_scrapes ffs -- ;) see what I did there?
		GROUP BY 1, 2, 3
	),
	model_scrape_data AS (
		SELECT
			ad.view_date,
			ad.salesforce_opportunity_id,
			ad.calendar_date,
			ad.is_available_date,
			s.scrape_date AS latest_scare_date,
			s.avg_total_discount_percentage -- will be the latest avg total discount
		FROM available_dates ad
			LEFT JOIN scrapes s
					  ON ad.salesforce_opportunity_id = s.salesforce_opportunity_id
						  AND ad.calendar_date = s.ota_check_in_date
						  AND s.scrape_date <= ad.view_date -- only limit to scrapes that are before or on the view date
		-- limit to the latest scrape at the time of view date
		QUALIFY
			ROW_NUMBER() OVER (PARTITION BY ad.view_date, ad.salesforce_opportunity_id, ad.calendar_date ORDER BY s.scrape_date DESC NULLS LAST) =
			1
	)
SELECT
	msd.view_date,
	msd.salesforce_opportunity_id,
	msd.calendar_date,
	msd.is_available_date,
	msd.latest_scare_date,
	msd.avg_total_discount_percentage,
	sa.is_connected_to_se,
	sa.channel_manager,
	sa.sale_active
FROM model_scrape_data msd
	-- enrich with sale active data
	LEFT JOIN sale_active sa
			  ON msd.view_date = sa.view_date
				  AND msd.salesforce_opportunity_id = sa.salesforce_opportunity_id
;


SELECT *
FROM latest_vault.fornova.price_comparison pc
WHERE salesforce_opportunity_id = '0066900001ae2Z0'
  AND pc.ota_check_in_date = '2025-10-22';
