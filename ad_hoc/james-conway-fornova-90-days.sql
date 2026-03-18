/*
Currently, we price check every single check-in date. For the scheduled price checks (not on demand) if we moved this to
every date for the first 90 check in dates, and every other date thereafter, what would the impact be on our
'top discount' message on site? For reference, the top discount has to be found on at least 10% of available dates for
us to claim it.


*/

-- baseline data
WITH
	input_data AS (
		SELECT *
		FROM se.data.fornova_price_comparison fpc
		WHERE fpc.allocation_date >= CURRENT_DATE
		  AND fpc.ota_rate IS NOT NULL
	)
		,
	aggregations AS (
		SELECT
			input_data.salesforce_opportunity_id,
			MIN(input_data.allocation_date)            AS first_allocation_date,
			MAX(input_data.allocation_date)            AS last_allocation_date,
			COUNT(DISTINCT input_data.allocation_date) AS total_dates,
			COUNT(DISTINCT
				  IFF(input_data.allocation_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 90, input_data.allocation_date,
					  NULL))                           AS dates_within_90_days,
			MIN(input_data.rate_local_calculated)      AS min_se_rate,
			MIN(input_data.ota_core_supplement)        AS min_ota_rate, -- with core and inclusions
			MIN(input_data.total_discount_percentage)  AS min_total_dicount,
		FROM input_data
		GROUP BY 1
	)
SELECT
	AVG(aggregations.total_dates),
	AVG(aggregations.dates_within_90_days)
FROM aggregations
;




SELECT
	fpc.salesforce_opportunity_id,
	fpc.allocation_date,
	fpc.rate_local_calculated,
	fpc.ota_core_supplement,
	fpc.total_discount_percentage
FROM se.data.fornova_price_comparison fpc
WHERE fpc.allocation_date >= CURRENT_DATE
  AND fpc.ota_rate IS NOT NULL
  AND fpc.salesforce_opportunity_id = '0061r00001DYydF' --TODO remove

