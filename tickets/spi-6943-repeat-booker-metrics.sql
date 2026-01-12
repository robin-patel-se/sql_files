WITH
	input_data AS (
		SELECT
			YEAR(fcb.booking_completed_timestamp) AS year,
			fcb.days_since_previous_live_booking,
			COUNT(*)                              AS bookings
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.se_brand = 'SE Brand'
		  AND fcb.live_booking_index = 2
-- 		  AND YEAR(fcb.booking_completed_timestamp) = 2024
		GROUP BY 1, 2
	),
	percentage_calc AS (
		SELECT
			year,
			ind.days_since_previous_live_booking,
			ind.bookings,
			SUM(ind.bookings)
				OVER (PARTITION BY ind.year ORDER BY ind.days_since_previous_live_booking) AS rolling_bookings,
			SUM(ind.bookings) OVER (PARTITION BY ind.year)                                 AS total_bookings,
			rolling_bookings / total_bookings                                              AS rolling_percentage
		FROM input_data ind
		ORDER BY 1
	)
SELECT *
FROM percentage_calc
WHERE percentage_calc.days_since_previous_live_booking = 395


WITH
	input_data AS (
		SELECT
			YEAR(fcb.booking_completed_timestamp) AS year,
			fcb.days_since_previous_live_booking,
			COUNT(*)                              AS bookings
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.se_brand = 'SE Brand'
		  AND fcb.live_booking_index = 3
		  AND YEAR(fcb.booking_completed_timestamp) = 2024
		GROUP BY 1, 2
	),
	percentage_calc AS (
		SELECT
			year,
			ind.days_since_previous_live_booking,
			ind.bookings,
			SUM(ind.bookings)
				OVER (PARTITION BY ind.year ORDER BY ind.days_since_previous_live_booking) AS rolling_bookings,
			SUM(ind.bookings) OVER (PARTITION BY ind.year)                                 AS total_bookings,
			rolling_bookings / total_bookings                                              AS rolling_percentage
		FROM input_data ind
		ORDER BY 1
	)
SELECT *
FROM percentage_calc



WITH
	input_data AS (
		SELECT
			fcb.days_since_previous_live_booking,
			COUNT(*) AS bookings
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.se_brand = 'SE Brand'
		  AND fcb.live_booking_index = 3
-- 		  AND YEAR(fcb.booking_completed_timestamp) = 2024
		GROUP BY 1
	),
	percentage_calc AS (
		SELECT
			ind.days_since_previous_live_booking,
			ind.bookings,
			SUM(ind.bookings)
				OVER ( ORDER BY ind.days_since_previous_live_booking) AS rolling_bookings,
			SUM(ind.bookings) OVER ()                                 AS total_bookings,
			rolling_bookings / total_bookings                         AS rolling_percentage
		FROM input_data ind
		ORDER BY 1
	)
SELECT *
FROM percentage_calc
WHERE percentage_calc.days_since_previous_live_booking = 395
;


USE WAREHOUSE pipe_xlarge
;

WITH
	input_data AS (
		SELECT
			fcb.days_since_previous_live_booking,
			COUNT(*) AS bookings
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.se_brand = 'SE Brand'
		  AND fcb.live_booking_index > 1
		  AND YEAR(fcb.booking_completed_timestamp) >= 2019
		GROUP BY 1
	),
	percentage_calc AS (
		SELECT
			ind.days_since_previous_live_booking,
			ind.bookings,
			SUM(ind.bookings)
				OVER ( ORDER BY ind.days_since_previous_live_booking) AS rolling_bookings,
			SUM(ind.bookings) OVER ()                                 AS total_bookings,
			rolling_bookings / total_bookings                         AS rolling_percentage
		FROM input_data ind
		ORDER BY 1
	)
SELECT *
FROM percentage_calc



WITH
	input_data AS (
		SELECT
			YEAR(fcb.booking_completed_timestamp) AS year,
			fcb.days_since_previous_live_booking,
			COUNT(*)                              AS bookings
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.se_brand = 'SE Brand'
		  AND fcb.live_booking_index > 1
-- 		  AND YEAR(fcb.booking_completed_timestamp) = 2024
		GROUP BY 1, 2
	),
	percentage_calc AS (
		SELECT
			year,
			ind.days_since_previous_live_booking,
			ind.bookings,
			SUM(ind.bookings)
				OVER (PARTITION BY ind.year ORDER BY ind.days_since_previous_live_booking) AS rolling_bookings,
			SUM(ind.bookings) OVER (PARTITION BY ind.year)                                 AS total_bookings,
			rolling_bookings / total_bookings                                              AS rolling_percentage
		FROM input_data ind
		ORDER BY 1
	)
SELECT *
FROM percentage_calc
WHERE days_since_previous_live_booking = 401
;

------------------------------------------------------------------------------------------------------------------------

/*On part 1 I was after the % of 1st time bookers who a) repeat ever, b) repeat in the first [13] months. Maybe the best way to select the [13] months is to see a histogram as to when the second bookings happen.

I think what we have above is take the total population of 1st bookers who DO repeat and then split them between within 13 months and after 13 months.

For ref I would expect it is something like 20% who do make a 2nd booking in 13-15 months. Interesting to understanding this in UK and DACH separately.*/

WITH
	users_first_booking AS (
		SELECT
			fcb.shiro_user_id,
			YEAR(fcb.booking_completed_timestamp) AS first_booking_year,
			fcb.territory                         AS first_booking_territory
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.se_brand = 'SE Brand'
		  AND fcb.live_booking_index = 1
	),
	repeat_bookings AS (
		SELECT
			fcb.shiro_user_id,
			IFF(fcb.days_since_previous_live_booking < 395, 'within 13 months',
				'outside 13 months') AS second_booking_category
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.se_brand = 'SE Brand'
		  AND fcb.live_booking_index = 2
	),
	modelling AS (
		SELECT
			ufb.shiro_user_id,
			first_booking_year,
			first_booking_territory,
			IFF(rb.shiro_user_id IS NULL, 'no repeat booking', second_booking_category) AS repeat_booking_status
		FROM users_first_booking ufb
			LEFT JOIN repeat_bookings rb ON ufb.shiro_user_id = rb.shiro_user_id
	)
SELECT
	first_booking_year,
	first_booking_territory,
	repeat_booking_status,
	COUNT(*) AS users
FROM modelling m
WHERE m.first_booking_territory IN ('UK', 'DE')
GROUP BY ALL
;


------------------------------------------------------------------------------------------------------------------------

-- booking type for second booking
WITH
	booking_data AS (
		SELECT
			b.booking_id,
			b.gross_revenue_gbp_constant_currency,
			b.margin_gross_of_toms_gbp_constant_currency,
			b.travel_type,
			b.last_live_booking_id,
			b.live_booking_index,
			b.booking_completed_date,
			b.territory,
			ds.product_configuration,
			ds.posu_country
		FROM se.data.fact_complete_booking b
			INNER JOIN se.data.dim_sale ds ON b.se_sale_id = ds.se_sale_id
		WHERE b.se_brand = 'SE Brand'
	),
	modelling_data AS (
		SELECT
			fcb.booking_id,
			YEAR(fcb.booking_completed_date)             AS year,
			fcb.territory,
			fcb.gross_revenue_gbp_constant_currency,
			fcb.margin_gross_of_toms_gbp_constant_currency,
			fcb.product_configuration,
			fcb.travel_type,
			f.booking_id                                 AS previous_booking_id,
			f.gross_revenue_gbp_constant_currency        AS previous_gross_revenue_gbp_constant_currency,
			f.margin_gross_of_toms_gbp_constant_currency AS previous_margin_gross_of_toms_gbp_constant_currency,
			f.product_configuration                      AS previous_product_configuration,
			f.travel_type                                AS previous_travel_type
		FROM booking_data fcb
			INNER JOIN booking_data f ON fcb.last_live_booking_id = f.booking_id
-- 		WHERE fcb.live_booking_index > 2 -- second booking
		WHERE fcb.live_booking_index > 1 -- repeat booking
	)

SELECT
	md.year,
	md.travel_type,
	md.previous_travel_type,
	COUNT(*)
FROM modelling_data md
GROUP BY ALL
;

SELECT
	stba.*,
	stmc.touch_mkt_channel,
	stmc2.touch_affiliate_territory
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_attribution sa ON stba.touch_id = sa.touch_id AND sa.attribution_model = 'last non direct'
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON sa.attributed_touch_id = stmc.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc2 ON stba.touch_id = stmc2.touch_id