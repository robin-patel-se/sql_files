SELECT *
FROM dbt.bi_data_science__intermediate.rnr_ab_session_metrics rasm
;


WITH
	booking_data AS (
		SELECT
			fb.shiro_user_id,
			stt.booking_id,
			fb.booking_completed_timestamp,
			fb.booking_completed_date,
		FROM se.data.scv_touched_transactions stt
			INNER JOIN se.data.fact_booking fb ON stt.booking_id = fb.booking_id
	),
	model_data AS (
		SELECT
			bd.booking_id,
			bd.shiro_user_id,
			bd.booking_completed_timestamp,
			bd.booking_completed_date,
			rasm.*
		FROM booking_data bd
			INNER JOIN dbt.bi_data_science__intermediate.rnr_ab_session_metrics rasm
					   ON bd.shiro_user_id::VARCHAR = rasm.attributed_user_id
						   AND rasm.has_search
						   AND
						  rasm.touch_start_tstamp BETWEEN DATEADD('day', -10, bd.booking_completed_timestamp) AND bd.booking_completed_timestamp
						   AND rasm.ab_test_name = 'RNR V3'
	),
	aggregate_data AS (
		SELECT
			md.booking_id,
			md.shiro_user_id,
			md.booking_completed_timestamp,
			md.booking_completed_date,
			COUNT(DISTINCT md.touch_id)                                          AS search_sessions_7_days_prior,
			COUNT(DISTINCT IFF(md.ab_test_group = 'Control', md.touch_id, NULL)) AS search_control_sessions_7_days_prior,
			COUNT(DISTINCT
				  IFF(md.ab_test_group = 'Treatment', md.touch_id, NULL))        AS search_treatment_sessions_7_days_prior
		FROM model_data md
		GROUP BY 1, 2, 3, 4
	),
	filtering AS (
		SELECT
			booking_id,
			shiro_user_id,
			booking_completed_timestamp,
			booking_completed_date,
			search_sessions_7_days_prior,
			search_control_sessions_7_days_prior,
			search_treatment_sessions_7_days_prior
		FROM aggregate_data ad
-- filter to people who exclusively only have control or treatment
		WHERE NOT (ad.search_control_sessions_7_days_prior > 0 AND ad.search_treatment_sessions_7_days_prior > 0)
	)
SELECT
	f.booking_completed_date,
	IFF(f.search_treatment_sessions_7_days_prior > 0, 'Treatment', 'Control') AS test_group,
	COUNT(DISTINCT f.booking_id)                                              AS bookings
FROM filtering f
GROUP BY 1, 2
;

SELECT
	rasm.touch_start_tstamp::DATE AS session_date,
	rasm.ab_test_group,
	COUNT(DISTINCT rasm.touch_id) AS sessions
FROM dbt.bi_data_science__intermediate.rnr_ab_session_metrics rasm
WHERE rasm.ab_test_name = 'RNR V3'
  AND rasm.has_search
  AND rasm.had_multiple_assignments = FALSE
GROUP BY 1, 2
;