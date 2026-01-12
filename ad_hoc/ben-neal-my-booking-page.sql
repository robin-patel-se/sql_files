WITH
	my_bookings_events AS (
		SELECT DISTINCT
			event_tstamp::DATE                                                     AS event_date,
			contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR AS screen_name,
			TRY_TO_NUMBER(user_id)                                                 AS shiro_user_id
		FROM se.data_pii.scv_event_stream
		WHERE collector_tstamp::DATE >= CURRENT_DATE - 190
		  AND screen_name = 'my bookings page'
		  AND se_brand = 'SE Brand'
	),
	modelling AS (
		SELECT
			fcb.booking_id,
			fcb.booking_completed_date::DATE AS booking_completed_date,
			fcb.shiro_user_id,
			fcb.check_in_date,
			fcb.margin_gross_of_toms_gbp_constant_currency,
			mbe.event_date
		-- 	COUNT(*)                                               AS bookings,
-- 	SUM(IFF(fcb.device_platform LIKE 'native app%', 1, 0)) AS app_bookings,
-- 	bookings - app_bookings                                AS non_app_bookings
		FROM se.data.fact_complete_booking fcb
			LEFT JOIN my_bookings_events mbe
					  ON fcb.shiro_user_id = mbe.shiro_user_id
						  AND fcb.booking_completed_date <= mbe.event_date
						  AND fcb.check_in_date >= mbe.event_date
		WHERE fcb.se_brand = 'SE Brand'
		  AND fcb.booking_completed_date >= CURRENT_DATE - 190
		  AND fcb.territory IN ('UK', 'DE', 'IT')
	)
SELECT
	COUNT(DISTINCT m.booking_id)                                      AS bookings,
	COUNT(DISTINCT m.shiro_user_id)                                   AS bookers,
	COUNT(DISTINCT IFF(m.event_date IS NOT NULL, m.shiro_user_id, 0)) AS bookings_page_bookers,
FROM modelling m
;



WITH
	my_bookings_events AS (
		SELECT DISTINCT
			event_tstamp::DATE                                                     AS event_date,
			contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR AS screen_name,
			TRY_TO_NUMBER(user_id)                                                 AS shiro_user_id
		FROM se.data_pii.scv_event_stream
		WHERE collector_tstamp::DATE >= CURRENT_DATE - 190
		  AND screen_name = 'my bookings page'
		  AND se_brand = 'SE Brand'
	),
	modelling AS (
		SELECT
			fcb.booking_id,
			fcb.booking_completed_date::DATE AS booking_completed_date,
			fcb.shiro_user_id,
			fcb.check_in_date,
			fcb.margin_gross_of_toms_gbp_constant_currency,
			mbe.event_date
		-- 	COUNT(*)                                               AS bookings,
-- 	SUM(IFF(fcb.device_platform LIKE 'native app%', 1, 0)) AS app_bookings,
-- 	bookings - app_bookings                                AS non_app_bookings
		FROM se.data.fact_complete_booking fcb
			LEFT JOIN my_bookings_events mbe
					  ON fcb.shiro_user_id = mbe.shiro_user_id
						  AND fcb.booking_completed_date <= mbe.event_date
						  AND fcb.check_in_date >= mbe.event_date
		WHERE fcb.se_brand = 'SE Brand'
		  AND fcb.booking_completed_date >= CURRENT_DATE - 190
		  AND fcb.territory IN ('UK', 'DE', 'IT')
	),
	aggregation AS (
		SELECT
			m.booking_id,
			m.booking_completed_date,
			m.shiro_user_id,
			m.check_in_date,
			m.margin_gross_of_toms_gbp_constant_currency,
			m.event_date IS NOT NULL                                 AS has_viewed_my_bookings_page,
			COUNT(IFF(m.event_date IS NOT NULL, m.event_date, NULL)) AS my_bookings_page_views
		FROM modelling m
		GROUP BY 1, 2, 3, 4, 5, 6
	)
SELECT
	a.has_viewed_my_bookings_page,
	COUNT(*)                        AS bookings,
	COUNT(DISTINCT a.shiro_user_id) AS bookers
FROM aggregation a
GROUP BY 1