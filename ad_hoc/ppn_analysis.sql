-- ppn
-- -- transactions?
-- ppn
-- avg no nights
-- lead time


-- booking category based on previous booking

WITH
	booking_date_diff AS
		(
			SELECT
				fcb.booking_id,
				fcb.booking_completed_date,
				fcb.margin_gross_of_toms_gbp_constant_currency,
				LAG(fcb.booking_completed_date)
					OVER (PARTITION BY fcb.shiro_user_id ORDER BY fcb.booking_completed_date) AS previous_booking_date,
				DATEDIFF(DAY, previous_booking_date, fcb.booking_completed_date)              AS difference_since_last_booking
			FROM se.data.fact_complete_booking fcb
		)

SELECT
	booking_date_diff.difference_since_last_booking,
	COUNT(*)
FROM booking_date_diff
GROUP BY 1
;

-- https://docs.google.com/spreadsheets/d/1Aibk2sQma5jooNmbyZqloG4FYOii2XyjuYjrKdITEFI/edit#gid=1662977108

-- Identified that sweet spot looks around 401 days, eg ~14 months


------------------------------------------------------------------------------------------------------------------------
-- 2022 ppn to determine buckets, look at hscv lead rates in that time by those

WITH
	booking_data AS (
		SELECT
			fb.price_per_night,
			COUNT(DISTINCT fb.booking_id) AS bookings
		FROM se.data.fact_booking fb
			INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
		WHERE fb.se_brand = 'SE Brand'
		  AND ds.travel_type = 'Domestic'
		  AND fb.territory = 'UK'
		  AND fb.price_per_night > 0
		  AND YEAR(fb.booking_completed_date) = 2022 -- adjust as necessary
		  AND DAYOFYEAR(fb.booking_completed_date) < DAYOFYEAR(CURRENT_DATE)
		  AND (fb.booking_status_type = 'live'
			OR (
				   fb.booking_status_type = 'cancelled'
					   AND DAYOFYEAR(fb.cancellation_date) > DAYOFYEAR(CURRENT_DATE)
				   ))
		GROUP BY 1
	),
	ppn_buckets AS (

		SELECT
			apa.price_per_night,
			bookings,
			SUM(apa.bookings) OVER (ORDER BY apa.price_per_night) AS cumulative_total_bookings,
			SUM(apa.bookings) OVER ()                             AS total_bookings,
			cumulative_total_bookings / total_bookings            AS booking_percentile,
			CASE
				WHEN booking_percentile < 0.33 THEN 'LOW'
				WHEN booking_percentile < 0.66 THEN 'MED'
				WHEN booking_percentile < 1 THEN 'HIGH'
			END                                                   AS percentile_group
		FROM booking_data apa
	)
SELECT
	pb.percentile_group,
	SUM(pb.bookings),
	MIN(pb.price_per_night) AS min_ppn,
	MAX(pb.price_per_night) AS max_ppn
FROM ppn_buckets pb
GROUP BY 1;

-- Domestic Thresholds
-- PERCENTILE_GROUP	SUM(PB.BOOKINGS)			MIN_PPN	MAX_PPN
-- LOW				13987	35.000000000000		118.750000000000
-- MED				15442	119.000000000000	159.800000000000
-- HIGH				15455	160.000000000000	1434.000000000000


-- International Thresholds
-- PERCENTILE_GROUP	SUM(PB.BOOKINGS)	MIN_PPN				MAX_PPN
-- LOW				2882				29.250000000000		131.770000000000
-- MED				2882				131.795000000000	207.380000000000
-- HIGH				2969				207.422900000000	1724.166700000000


------------------------------------------------------------------------------------------------------------------------
--2022 thresholds
SELECT
	YEAR(hscvs.view_date)                        AS _year,
	ds.travel_type,
	CASE
		WHEN ds.travel_type = 'Domestic' AND hscvs.available_lead_rate_gbp <= 119 THEN 'LOW'
		WHEN ds.travel_type = 'Domestic' AND hscvs.available_lead_rate_gbp <= 160 THEN 'MED'
		WHEN ds.travel_type = 'Domestic' AND hscvs.available_lead_rate_gbp > 160 THEN 'MIGH'
		WHEN ds.travel_type = 'International' AND hscvs.available_lead_rate_gbp <= 132 THEN 'LOW'
		WHEN ds.travel_type = 'International' AND hscvs.available_lead_rate_gbp <= 207 THEN 'MED'
		WHEN ds.travel_type = 'International' AND hscvs.available_lead_rate_gbp > 207 THEN 'MIGH'
	END                                          AS available_lead_rate_group,
	COUNT(DISTINCT hscvs.se_sale_id)             AS num_sale_ids,
	COUNT(*)                                     AS num_dates,
	AVG(hscvs.available_lead_rate_per_night_gbp) AS average_available_lead_rate_per_night
FROM data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot hscvs
	LEFT JOIN se.data.dim_sale ds
			  ON hscvs.se_sale_id = ds.se_sale_id
WHERE hscvs.sale_available_in_calendar -- only looking at dates that customers can book
  AND MONTH(hscvs.view_date) IN ('1', '2', '3')
  AND ds.posa_territory = 'UK'
GROUP BY 1, 2, 3
;

SELECT
	YEAR(fb.booking_completed_date) AS year,
	ds.travel_type,
	CASE
		WHEN fb.travel_type = 'Domestic' AND fb.price_per_night <= 119 THEN 'LOW'
		WHEN fb.travel_type = 'Domestic' AND fb.price_per_night <= 160 THEN 'MED'
		WHEN fb.travel_type = 'Domestic' AND fb.price_per_night > 160 THEN 'MIGH'
		WHEN fb.travel_type = 'International' AND fb.price_per_night <= 132 THEN 'LOW'
		WHEN fb.travel_type = 'International' AND fb.price_per_night <= 207 THEN 'MED'
		WHEN fb.travel_type = 'International' AND fb.price_per_night > 207 THEN 'MIGH'
	END                             AS booking_price_per_night_group,
	COUNT(DISTINCT fb.booking_id)   AS bookings
FROM se.data.fact_booking fb
	INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.se_brand = 'SE Brand'
  AND fb.territory = 'UK'
  AND fb.price_per_night > 0
  AND fb.booking_completed_date >= '2023-01-01'
  AND DAYOFYEAR(fb.booking_completed_date) < DAYOFYEAR(CURRENT_DATE)
  AND (fb.booking_status_type = 'live'
	OR (
		   fb.booking_status_type = 'cancelled'
			   AND DAYOFYEAR(fb.cancellation_date) > DAYOFYEAR(CURRENT_DATE)
		   ))
GROUP BY 1, 2, 3


------------------------------------------------------------------------------------------------------------------------

--2023 thresholds
SELECT
	YEAR(hscvs.view_date)                         AS _year,
	ds.travel_type,
	CASE
		WHEN ds.travel_type = 'Domestic' AND hscvs.available_lead_rate_gbp <= 139 THEN 'LOW'
		WHEN ds.travel_type = 'Domestic' AND hscvs.available_lead_rate_gbp <= 184 THEN 'MED'
		WHEN ds.travel_type = 'Domestic' AND hscvs.available_lead_rate_gbp > 184 THEN 'MIGH'
		WHEN ds.travel_type = 'International' AND hscvs.available_lead_rate_gbp <= 166 THEN 'LOW'
		WHEN ds.travel_type = 'International' AND hscvs.available_lead_rate_gbp <= 274 THEN 'MED'
		WHEN ds.travel_type = 'International' AND hscvs.available_lead_rate_gbp > 274 THEN 'MIGH'
	END                                           AS available_lead_rate_group,
	COUNT(DISTINCT hscvs.se_sale_id)              AS num_sale_ids,
	COUNT(*)                                      AS num_dates,
	AVG(hscvs.available_lead_rate_per_night_gbp)  AS average_available_lead_rate_per_night
FROM data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot hscvs
	LEFT JOIN se.data.dim_sale ds
			  ON hscvs.se_sale_id = ds.se_sale_id
WHERE hscvs.sale_available_in_calendar -- only looking at dates that customers can book
  AND MONTH(hscvs.view_date) IN ('1', '2', '3')
  AND ds.posa_territory = 'UK'
GROUP BY 1, 2, 3
;

SELECT
	YEAR(fb.booking_completed_date) AS year,
	ds.travel_type,
	CASE
		WHEN fb.travel_type = 'Domestic' AND fb.price_per_night <= 139 THEN 'LOW'
		WHEN fb.travel_type = 'Domestic' AND fb.price_per_night <= 184 THEN 'MED'
		WHEN fb.travel_type = 'Domestic' AND fb.price_per_night > 184 THEN 'MIGH'
		WHEN fb.travel_type = 'International' AND fb.price_per_night <= 166 THEN 'LOW'
		WHEN fb.travel_type = 'International' AND fb.price_per_night <= 274 THEN 'MED'
		WHEN fb.travel_type = 'International' AND fb.price_per_night > 274 THEN 'MIGH'
	END                             AS booking_price_per_night_group,
	COUNT(DISTINCT fb.booking_id)   AS bookings
FROM se.data.fact_booking fb
	INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.se_brand = 'SE Brand'
  AND fb.territory = 'UK'
  AND fb.price_per_night > 0
  AND fb.booking_completed_date >= '2023-01-01'
  AND DAYOFYEAR(fb.booking_completed_date) < DAYOFYEAR(CURRENT_DATE)
  AND (fb.booking_status_type = 'live'
	OR (
		   fb.booking_status_type = 'cancelled'
			   AND DAYOFYEAR(fb.cancellation_date) > DAYOFYEAR(CURRENT_DATE)
		   ))
GROUP BY 1, 2, 3;




