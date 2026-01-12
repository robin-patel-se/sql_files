SET (from_date, to_date)= ('2018-01-01', '2023-08-27')
;


WITH
	sess_bookings AS (
		SELECT
			stt.touch_id,
			COUNT(DISTINCT fcb.booking_id)                      AS bookings,
			SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touched_transactions stt ON stba.touch_id = stt.touch_id
			INNER JOIN se.data.fact_booking fcb ON stt.booking_id = fcb.booking_id
		WHERE stba.touch_start_tstamp::DATE >= $from_date
		  AND stba.touch_start_tstamp::DATE <= $to_date
		  AND booking_status IN ('COMPLETE', 'REFUNDED', 'AUTHORISED', 'CANCELLED', 'PARTIAL_PAID', 'BOOKED')
		GROUP BY 1
	)
		,
	sess_spvs AS (
		SELECT
			stba.touch_id,
			COUNT(*) AS spvs
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
		WHERE stba.touch_start_tstamp::DATE >= $from_date
		  AND stba.touch_start_tstamp::DATE <= $to_date
		GROUP BY 1
	),


	base_data AS
		(
			SELECT
				stba.touch_id,
				DATE_TRUNC('year', stba.touch_start_tstamp) AS session_year,
				back_up_tmc.touch_mkt_channel               AS old_touch_mkt_channel,
				live_tmc.touch_mkt_channel                  AS new_touch_mkt_channel


			FROM se.data.scv_touch_basic_attributes stba
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution_20230829 back_up_att
						   ON stba.touch_id = back_up_att.touch_id AND back_up_att.attribution_model = 'last paid'
				INNER JOIN  data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_20230829 back_up_tmc
						   ON back_up_att.attributed_touch_id = back_up_tmc.touch_id

				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution live_att
						   ON stba.touch_id = live_att.touch_id AND live_att.attribution_model = 'last paid'
				INNER JOIN  data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel live_tmc
						   ON live_att.attributed_touch_id = live_tmc.touch_id
			WHERE stba.touch_start_tstamp::DATE >= $from_date
			  AND stba.touch_start_tstamp::DATE <= $to_date
		),
	model_data AS (
		SELECT
			bs.*,
			ss.spvs,
			sb.bookings,
			sb.margin
		FROM base_data bs
			LEFT JOIN sess_bookings sb ON sb.touch_id = bs.touch_id
			LEFT JOIN sess_spvs ss ON ss.touch_id = bs.touch_id
	)
SELECT
	old_touch_mkt_channel,
	new_touch_mkt_channel,
	session_year,
	IFF(old_touch_mkt_channel != new_touch_mkt_channel, TRUE, FALSE) AS channel_change,
	COUNT(*)                                                         AS num_sessions,
	SUM(margin),
	SUM(spvs),
	SUM(bookings)
FROM model_data
GROUP BY 1, 2, 3, 4