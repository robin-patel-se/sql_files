USE WAREHOUSE pipe_xlarge
;


WITH
	spvs AS (
		SELECT
			sts.touch_id,
			COUNT(*)                                         AS spvs,
			SUM(IFF(ds.travel_type = 'International', 1, 0)) AS international_spvs,
			SUM(IFF(ds.travel_type = 'Domestic', 1, 0))      AS domestic_spvs
		FROM se.data.scv_touched_spvs sts
			INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
		WHERE sts.event_tstamp >= CURRENT_DATE - 30
		GROUP BY 1
	),
	bookings AS (
		SELECT
			stt.touch_id,
			COUNT(*)                                            AS bookings,
			SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.scv_touched_transactions stt
			-- only currently complete bookings
			INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
		WHERE stt.event_tstamp >= CURRENT_DATE - 30
		GROUP BY 1
	),
	model_data AS (
		SELECT
			stba.touch_start_tstamp::DATE     AS session_date,
			stba.touch_id,
			stmc.channel_category,
			stmc.touch_mkt_channel,
			stmc.touch_affiliate_territory,
			stba.touch_experience,
			stba.touch_duration_seconds,
			stba.touch_event_count,
			COALESCE(s.spvs, 0)               AS spvs,
			COALESCE(s.international_spvs, 0) AS international_spvs,
			COALESCE(s.domestic_spvs, 0)      AS domestic_spvs,
			COALESCE(b.bookings, 0)           AS bookings,
			COALESCE(b.margin_gbp, 0)         AS margin_gbp
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
			LEFT JOIN  spvs s ON stba.touch_id = s.touch_id
			LEFT JOIN  bookings b ON stba.touch_id = b.touch_id
		WHERE stba.touch_se_brand = 'SE Brand'
		  AND stba.touch_start_tstamp >= CURRENT_DATE - 30
	)
SELECT
	md.session_date,
	md.touch_mkt_channel,
	md.channel_category,
	md.touch_affiliate_territory,
	md.touch_experience,
	COUNT(DISTINCT md.touch_id) AS sessions,
	AVG(md.touch_duration_seconds) AS avg_duration,
	SUM(md.spvs)                AS spvs,
	SUM(md.international_spvs)  AS international_spvs,
	SUM(md.domestic_spvs)       AS domestic_spvs,
	SUM(md.bookings)            AS bookings,
	SUM(md.margin_gbp)          AS margin_gbp
FROM model_data AS md
GROUP BY 1, 2, 3, 4, 5
;