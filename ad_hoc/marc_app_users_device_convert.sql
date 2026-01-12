WITH
	app_active_users AS (
-- list of users that were active in the app on a paricular date
		SELECT
			stba.attributed_user_id,
			stba.touch_start_tstamp::DATE AS app_active_date,
			COUNT(*)                      AS sessions
		FROM se.data_pii.scv_touch_basic_attributes stba
		WHERE stba.touch_se_brand = 'SE Brand'
		  AND stba.stitched_identity_type = 'se_user_id'
		  AND stba.touch_experience LIKE 'native app%'
		  AND stba.touch_start_tstamp >= CURRENT_DATE - 30 -- TODO Adjust date
		GROUP BY 1, 2
	)

SELECT
	fcb.shiro_user_id,
	fcb.booking_completed_date,
	fcb.device_platform,
	COUNT(*)                                            AS bookings,
	SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
FROM se.data.fact_complete_booking fcb
	INNER JOIN app_active_users ON fcb.shiro_user_id = app_active_users.attributed_user_id
	AND app_active_users.app_active_date BETWEEN fcb.booking_completed_date - 7 AND fcb.booking_completed_date
WHERE fcb.se_brand = 'SE Brand'
GROUP BY 1, 2, 3
;


-- user that had a booking on web on the 27th May 2024 but had an app session before it 50927025


SELECT *
FROM se.data.fact_complete_booking fcb
WHERE fcb.shiro_user_id = '50927025'
; -- booking id: A18705697


SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '50927025' -- had several app sessions before booking, 25h may, 19th may, 17th may


WITH
	app_active_users AS (
-- list of users that were active in the app on a paricular date
		SELECT
			stba.attributed_user_id,
			stba.touch_start_tstamp::DATE AS app_active_date,
			COUNT(*)                      AS sessions
		FROM se.data_pii.scv_touch_basic_attributes stba
		WHERE stba.touch_se_brand = 'SE Brand'
		  AND stba.stitched_identity_type = 'se_user_id'
		  AND stba.touch_experience LIKE 'native app%'
		  AND stba.touch_start_tstamp >= CURRENT_DATE - 40 -- TODO Adjust date
		GROUP BY 1, 2
	),
	bookers AS (
		SELECT
			fcb.shiro_user_id,
			fcb.booking_completed_date,
			fcb.device_platform                                 AS booking_device_platform,
			fcb.territory,
			COUNT(*)                                            AS bookings,
			SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.se_brand = 'SE Brand'
		  AND fcb.booking_completed_date >= CURRENT_DATE - 30
		GROUP BY 1, 2, 3, 4
	),
	model_data AS (
		-- attach app users to bookers information
		SELECT *
		FROM bookers b
			LEFT JOIN app_active_users aau ON b.shiro_user_id = aau.attributed_user_id
			AND aau.app_active_date BETWEEN b.booking_completed_date - 7 AND b.booking_completed_date

	),
	aggregate_bookers AS (
		-- reduce granularity of app active users for users who had multiple days of activity before a booking
		SELECT
			md.shiro_user_id,
			md.booking_completed_date,
			md.booking_device_platform,
			SUM(md.bookings)                       AS bookings,
			SUM(md.margin_gbp)                     AS margin_gbp,
			ARRAY_AGG(DISTINCT md.app_active_date) AS active_app_dates,
			SUM(md.sessions)                       AS app_sessions
		FROM model_data md
		GROUP BY 1, 2, 3
	)
SELECT *
FROM aggregate_bookers
;


------------------------------------------------------------------------------------------------------------------------

-- investigate spvs IN the app AS a proxy

-- user with a web booking but has had previous app spvs


WITH
	sale_bookings AS (
		SELECT
			fcb.shiro_user_id,
			fcb.booking_completed_date,
			fcb.booking_completed_timestamp,
			fcb.device_platform,
			IFF(fcb.device_platform LIKE 'native app%', 'App', 'Non App') AS booking_device_platform,
			fcb.territory,
			fcb.se_sale_id,
			fcb.booking_id,
			fcb.margin_gross_of_toms_gbp_constant_currency                AS margin_gbp
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.se_brand = 'SE Brand'
		  AND fcb.booking_completed_date >= CURRENT_DATE - 30 -- TODO adjust
		  AND fcb.territory IN ('UK', 'DE', 'IT')
	),
	spvs AS (

		SELECT
			stba.attributed_user_id,
			sts.se_sale_id,
			sts.event_tstamp                                                AS spv_event_tstamp,
			IFF(stba.touch_experience LIKE 'native app%', 'App', 'Non App') AS spv_app_status
		FROM se.data.scv_touched_spvs sts
			INNER JOIN se.data_pii.scv_touch_basic_attributes stba
					   ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id' AND
						  stba.touch_se_brand = 'SE Brand'
		WHERE sts.event_tstamp >= CURRENT_DATE - 40
	)
SELECT
	sb.shiro_user_id,
	sb.booking_completed_date,
	sb.booking_completed_timestamp,
	sb.device_platform,
	sb.booking_device_platform,
	sb.territory,
	sb.se_sale_id,
	sb.booking_id,
	sb.margin_gbp,
	LISTAGG(DISTINCT s.spv_app_status, ', ') WITHIN GROUP ( ORDER BY s.spv_app_status ) AS spv_platform,
	COUNT(DISTINCT s.spv_app_status)                                                    AS no_of_spv_platform,
	MIN(s.spv_event_tstamp)                                                             AS first_spv_tstamp,
	MIN(IFF(spv_app_status = 'App', s.spv_event_tstamp, NULL))                          AS first_app_spv_tstamp,
	MIN(IFF(spv_app_status = 'Non App', s.spv_event_tstamp, NULL))                      AS first_non_app_spv_tstamp,
	COUNT(*)                                                                            AS spvs,
	SUM(IFF(spv_app_status = 'App', 1, 0))                                              AS app_spvs,
	SUM(IFF(spv_app_status = 'Non App', 1, 0))                                          AS non_app_spvs,
	CASE
		WHEN first_app_spv_tstamp < first_non_app_spv_tstamp AND no_of_spv_platform > 1 THEN 'App'
		WHEN first_app_spv_tstamp > first_non_app_spv_tstamp AND no_of_spv_platform > 1 THEN 'Non App'
		ELSE sb.booking_device_platform
	END                                                                                 AS first_spv_platform
FROM sale_bookings sb
	LEFT JOIN spvs s ON sb.se_sale_id = s.se_sale_id
	AND sb.shiro_user_id = s.attributed_user_id
	AND s.spv_event_tstamp BETWEEN DATEADD(DAY, -7, sb.booking_completed_timestamp) AND sb.booking_completed_timestamp
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

