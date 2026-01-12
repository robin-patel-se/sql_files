WITH
	spv_metrics AS (
		SELECT
			sts.touch_id,
			COUNT(*) AS spvs
		FROM se.data.scv_touched_spvs sts
		WHERE sts.event_tstamp >= '2022-01-01'
		GROUP BY 1
	),
	booking_metrics AS (
		SELECT
			stt.touch_id,
			COUNT(DISTINCT fcb.booking_id)                      AS bookings,
			SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.scv_touched_transactions stt
			LEFT JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
		GROUP BY 1
	)
SELECT
	stba.touch_id,
	stba.touch_start_tstamp::DATE AS session_date,
	stba.touch_experience,
	stmc.touch_affiliate_territory,
	stba.touch_logged_in,
	stba.stitched_identity_type,
	stmc.touch_mkt_channel,
	stmc.channel_category,
	stmc.affiliate,
	s.touch_id IS NOT NULL        AS is_session_with_spv,
	s.spvs,
	b.bookings,
	b.margin_gbp,
	stba.attributed_user_id_hash
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
	LEFT JOIN  spv_metrics s ON stba.touch_id = s.touch_id
	LEFT JOIN  booking_metrics b ON stba.touch_id = b.touch_id
WHERE stba.touch_start_tstamp >= '2022-01-01'
;



WITH
	session_with_spv AS (
		SELECT
			sts.touch_id,
			COUNT(*) AS spvs
		FROM se.data.scv_touched_spvs sts
		WHERE sts.event_tstamp >= '2022-01-01'
		GROUP BY 1
	),
	booking_metrics AS (
		SELECT
			stt.touch_id,
			COUNT(DISTINCT fcb.booking_id)                      AS bookings,
			SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.scv_touched_transactions stt
			LEFT JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
		GROUP BY 1
	)
SELECT
	stba.touch_start_tstamp::DATE                AS session_date,
	stba.touch_experience,
	stba.touch_logged_in,
	stba.stitched_identity_type,
	stmc.touch_mkt_channel,
	stmc.channel_category,
	stmc.touch_affiliate_territory,
	stmc.affiliate,
	s.touch_id IS NOT NULL                       AS is_session_with_spv,
	SUM(s.spvs)                                  AS spvs,
	SUM(b.bookings)                              AS bookings,
	SUM(b.margin_gbp)                            AS margin_gbp,
	COUNT(DISTINCT stba.touch_id)                AS sessions,
	COUNT(DISTINCT stba.attributed_user_id_hash) AS users
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
	LEFT JOIN  session_with_spv s ON stba.touch_id = s.touch_id
	LEFT JOIN  booking_metrics b ON stba.touch_id = b.touch_id
WHERE stba.touch_start_tstamp >= '2022-01-01'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
;

USE WAREHOUSE pipe_xlarge
;

SELECT
	DATE_TRUNC(WEEK, stba.touch_start_tstamp)    AS week,
	CASE
		WHEN stmc.touch_affiliate_territory IN ('UK') THEN 'UK'
		WHEN stmc.touch_affiliate_territory IN ('DE', 'AT', 'CH') THEN 'DACH'
		WHEN stmc.touch_affiliate_territory IN ('ANOMALOUS', 'NON_VERIFIED', 'SE TECH', 'SE_TEMP') THEN 'SE TECH'
		ELSE 'ROW'
	END                                          AS territory,
	COUNT(DISTINCT stba.attributed_user_id_hash) AS territory_wau
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stba.touch_start_tstamp >= '2022-01-01'
GROUP BY 1, 2
;


SELECT
	sc.se_week,
	sc.se_year,
	CASE
		WHEN stmc.touch_affiliate_territory IN ('UK') THEN 'UK'
		WHEN stmc.touch_affiliate_territory IN ('DE', 'AT', 'CH') THEN 'DACH'
		WHEN stmc.touch_affiliate_territory IN ('ANOMALOUS', 'NON_VERIFIED', 'SE TECH', 'SE_TEMP') THEN 'SE TECH'
		ELSE 'ROW'
	END                                                     AS territory,
	MIN(date_value)                                         AS date,
	COUNT(DISTINCT stba.attributed_user_id_hash)            AS wau,
	COUNT(DISTINCT IFF(stba.stitched_identity_type = 'se_user_id', stba.attributed_user_id_hash,
					   NULL))                               AS member_wau,
	COUNT(DISTINCT
		  IFF(stba.stitched_identity_type = 'se_user_id' AND stba.touch_logged_in, stba.attributed_user_id_hash,
			  NULL))                                        AS member_logged_in_wau,
	COUNT(DISTINCT
		  IFF(stba.touch_logged_in = FALSE AND stba.stitched_identity_type = 'se_user_id', stba.attributed_user_id_hash,
			  NULL))                                        AS member_logged_out_wau,
	COUNT(DISTINCT
		  IFF(stba.touch_logged_in = FALSE AND stba.stitched_identity_type IS DISTINCT FROM 'se_user_id',
			  stba.attributed_user_id_hash,
			  NULL))                                        AS non_member_logged_out_wau,


	COUNT(DISTINCT IFF(stba.touch_experience IN (
												 'native app android',
												 'native app ios',
												 'mobile wrap android',
												 'mobile wrap ios'),
					   stba.attributed_user_id_hash, NULL)) AS app_wau,

	COUNT(DISTINCT IFF(stba.touch_experience IN (
												 'native app android',
												 'native app ios',
												 'mobile wrap android',
												 'mobile wrap ios')
						   AND stba.stitched_identity_type = 'se_user_id',
					   stba.attributed_user_id_hash,
					   NULL))                               AS app_member_wau,

	COUNT(DISTINCT IFF(stba.touch_experience IN (
												 'native app android',
												 'native app ios',
												 'mobile wrap android',
												 'mobile wrap ios')
						   AND stba.stitched_identity_type = 'se_user_id' AND stba.touch_logged_in,
					   stba.attributed_user_id_hash,
					   NULL))                               AS app_member_logged_in_wau,

	COUNT(DISTINCT IFF(stba.touch_experience IN (
												 'native app android',
												 'native app ios',
												 'mobile wrap android',
												 'mobile wrap ios')
						   AND stba.stitched_identity_type = 'se_user_id' AND stba.touch_logged_in = FALSE,
					   stba.attributed_user_id_hash,
					   NULL))                               AS app_member_logged_out_wau,

	COUNT(DISTINCT IFF(stba.touch_experience IN (
												 'native app android',
												 'native app ios',
												 'mobile wrap android',
												 'mobile wrap ios')
						   AND stba.stitched_identity_type IS DISTINCT FROM 'se_user_id' AND
					   stba.touch_logged_in = FALSE,
					   stba.attributed_user_id_hash,
					   NULL))                               AS app_non_member_logged_out_wau,


	COUNT(DISTINCT IFF(stba.touch_experience IN (
												 'tablet web',
												 'web',
												 'mobile web'),
					   stba.attributed_user_id_hash, NULL)) AS web_wau,


	COUNT(DISTINCT IFF(stba.touch_experience IN (
												 'tablet web',
												 'web',
												 'mobile web')
						   AND stba.stitched_identity_type = 'se_user_id' AND stba.touch_logged_in,
					   stba.attributed_user_id_hash,
					   NULL))                               AS web_member_logged_in_wau,

	COUNT(DISTINCT IFF(stba.touch_experience IN (
												 'tablet web',
												 'web',
												 'mobile web')
						   AND stba.stitched_identity_type = 'se_user_id' AND stba.touch_logged_in = FALSE,
					   stba.attributed_user_id_hash,
					   NULL))                               AS web_member_logged_out_wau,

	COUNT(DISTINCT IFF(stba.touch_experience IN (
												 'tablet web',
												 'web',
												 'mobile web')
						   AND stba.stitched_identity_type IS DISTINCT FROM 'se_user_id' AND
					   stba.touch_logged_in = FALSE,
					   stba.attributed_user_id_hash,
					   NULL))                               AS web_non_member_logged_out_wau,
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
	INNER JOIN se.data.se_calendar sc
			   ON stba.touch_start_tstamp::DATE = sc.date_value AND sc.date_value BETWEEN '2022-01-01' AND CURRENT_DATE
WHERE stba.touch_se_brand = 'SE Brand'
  AND stba.touch_start_tstamp >= '2022-01-01'
GROUP BY 1, 2, 3
;