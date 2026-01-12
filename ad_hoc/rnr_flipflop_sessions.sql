SELECT *
FROM snowflake.account_usage.query_history qh
WHERE qh.user_name = 'JINGYU'
  AND qh.start_time::DATE = '2024-06-14'
;

USE ROLE personal_role__robinpatel
;

-- create transient table infinite_lambda_db.staging.rnr_ab_session_metrics_june14 clone dbt.bi_data_science__intermediate.rnr_ab_session_metrics;
-- create transient table infinite_lambda_db.staging.rnr_ab_user_assignment_june14 clone dbt.bi_data_science__intermediate.rnr_ab_user_assignment;
-- create transient table infinite_lambda_db.staging.rnr_ab_session_metrics_june14 clone dbt.bi_data_science__intermediate.rnr_ab_session_metrics;
-- create transient table infinite_lambda_db.staging.rnr_ab_user_assignment_june14 clone dbt.bi_data_science__intermediate.rnr_ab_user_assignment;

-- backup on 14th june
SELECT *
FROM infinite_lambda_db.staging.rnr_ab_session_metrics_june14
WHERE touch_start_tstamp::DATE = '2024-06-13'
  AND has_booking
;

--current data
SELECT *
FROM dbt.bi_data_science__intermediate.rnr_ab_session_metrics
WHERE touch_start_tstamp::DATE = '2024-06-13'

-- session numbers on the 13th June 2024:
-- snapshot as of 14th June:
-- 131,923 rows
-- current day:
-- 132,141 rows


-- backup on 14th june by ab test group
SELECT
	ab_test_group,
	COUNT(*)
FROM infinite_lambda_db.staging.rnr_ab_session_metrics_june14
WHERE touch_start_tstamp::DATE = '2024-06-13'
  AND had_multiple_assignments = FALSE
GROUP BY 1
;

-- current data by ab test group
SELECT
	ab_test_group,
	COUNT(*)
FROM dbt.bi_data_science__intermediate.rnr_ab_session_metrics
WHERE touch_start_tstamp::DATE = '2024-06-13'
  AND had_multiple_assignments = FALSE
GROUP BY 1
;

-- backup on 14th june by multiple assignments
SELECT
	ab_test_group,
	had_multiple_assignments,
	COUNT(*)
FROM infinite_lambda_db.staging.rnr_ab_session_metrics_june14
WHERE touch_start_tstamp::DATE = '2024-06-13'
GROUP BY 1, 2
;

-- current data by multiple assignments
SELECT
	ab_test_group,
	had_multiple_assignments,
	COUNT(*)
FROM dbt.bi_data_science__intermediate.rnr_ab_session_metrics
WHERE touch_start_tstamp::DATE = '2024-06-13'
GROUP BY 1, 2
;


-- backup on 14th june bookings by ab test group
SELECT
	ab_test_group,
	SUM(cnt_completed_bookings)                AS bookings,
	SUM(margin_gbp)                            AS margin,
	SUM(IFF(has_booking, 1, 0))                AS sessions_with_booking,
	SUM(IFF(has_booking AND has_search, 1, 0)) AS sessions_with_search_and_booking
FROM infinite_lambda_db.staging.rnr_ab_session_metrics_june14
WHERE touch_start_tstamp::DATE = '2024-06-13'
  AND had_multiple_assignments = FALSE
GROUP BY 1
;

-- current data bookings by ab test group
SELECT
	ab_test_group,
	SUM(cnt_completed_bookings)                AS bookings,
	SUM(margin_gbp)                            AS margin,
	SUM(IFF(has_booking, 1, 0))                AS sessions_with_booking,
	SUM(IFF(has_booking AND has_search, 1, 0)) AS sessions_with_search_and_booking
FROM dbt.bi_data_science__intermediate.rnr_ab_session_metrics
WHERE touch_start_tstamp::DATE = '2024-06-13'
  AND had_multiple_assignments = FALSE
GROUP BY 1
;


-- backup on 14th june session with search by ab test group
SELECT
	ab_test_group,
	SUM(IFF(has_search, 1, 0)) AS sessions_with_search
FROM infinite_lambda_db.staging.rnr_ab_session_metrics_june14
WHERE touch_start_tstamp::DATE = '2024-06-13'
  AND had_multiple_assignments = FALSE
GROUP BY 1
;

-- current data session with search by ab test group
SELECT
	ab_test_group,
	SUM(IFF(has_search, 1, 0)) AS sessions_with_search
FROM dbt.bi_data_science__intermediate.rnr_ab_session_metrics
WHERE touch_start_tstamp::DATE = '2024-06-13'
  AND had_multiple_assignments = FALSE
GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------
WITH
	input AS (
		SELECT
			rasm.touch_id,
			rasm.touch_start_tstamp,
			rasm.touch_duration_seconds,
			rasm.touch_affiliate_territory,
			rasm.touch_device_type,
			rasm.touch_landing_page,
			rasm.stitched_identity_type,
			rasm.attributed_user_id,
			rasm.channel_category,
			rasm.ab_test_name,
			rasm.ab_test_group,
			rasm.had_multiple_assignments,
			rasm.user_tenure_days,
			rasm.user_tenure_months,
			rasm.has_search,
			rasm.has_search_clickthrough,
			rasm.has_page_view,
			rasm.has_booking_form_view,
			rasm.has_booking,
			rasm.cnt_searches,
			rasm.cnt_searches_with_clickthrough,
			rasm.cnt_page_views,
			rasm.cnt_unique_sale_views,
			rasm.cnt_bfvs,
			rasm.cnt_completed_bookings,
			rasm.margin_gbp,
			rasm.gross_revenue_gbp,
			rasm.cnt_completed_bookings_overall,
			rasm.margin_gbp_overall,
			rasm.gross_revenue_gbp_overall,
			stmc.touch_mkt_channel
		FROM dbt.bi_data_science__intermediate.rnr_ab_session_metrics rasm
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON rasm.touch_id = stmc.touch_id
		WHERE rasm.ab_test_name = 'RNR V3'
	)
SELECT
	touch_start_tstamp::DATE                       AS date,
	i.ab_test_group,
	i.touch_mkt_channel,
	SUM(IFF(i.has_search, 1, 0))                   AS search_sessions,
	SUM(IFF(i.has_search AND i.has_booking, 1, 0)) AS search_booked_sessions,
FROM input i
GROUP BY 1, 2, 3
;

-- Top Level Channel Sessions look good, between treatment and control

SELECT
	sts.touch_id,
	stba.touch_landing_page,
	stmc.touch_mkt_channel,
	sts.page_url,
	PARSE_URL(sts.page_url)['parameters'],
	PARSE_URL(sts.page_url)['parameters']['sortBy']::VARCHAR IS NOT NULL AS is_sort_by_search,
	PARSE_URL(sts.page_url)['parameters']['sortBy']::VARCHAR             AS sort_by_value
FROM se.data.scv_touched_searches sts
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
	INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= CURRENT_DATE - 1
  AND sts.se_brand = 'SE Brand'
;



SELECT *
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON stba.touch_id = ssel.touch_id AND ssel.event_tstamp >= CURRENT_DATE - 1
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON ssel.event_hash = ses.event_hash AND ses.event_tstamp >= CURRENT_DATE - 1
WHERE stba.touch_id = '8c294c46bf2d1378e900a29221731e43f02ca02dd05fc8f69ede5251eb66740e'
  AND stba.touch_start_tstamp >= CURRENT_DATE - 1
;


SELECT *
FROM dbt.bi_data_science__intermediate.rnr_ab_session_metrics rasm
WHERE rasm.ab_test_name = 'RNR V3'
  AND rasm.ab_test_group = 'Treatment'
  AND rasm.has_search
  AND rasm.has_booking
  AND rasm.touch_start_tstamp::DATE = '2024-07-08'
;

SELECT
	ssel.touch_id,
	ses.event_tstamp,
	ses.event_hash,
	ses.page_url,
	ses.event_name,
	ses.se_sale_id,
	ses.booking_id,
	ses.contexts_com_secretescapes_search_context_1,
	PARSE_URL(ses.page_url)['parameters']['sortBy']::VARCHAR AS sort_by_value
FROM se.data_pii.scv_session_events_link ssel
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON ssel.event_hash = ses.event_hash AND ses.event_tstamp::DATE = '2024-07-08'
WHERE ssel.event_tstamp::DATE = '2024-07-08'
  AND ssel.touch_id IN ('869316845812a7c945649bdbcf2fd20e8a7f0dd126b632981818e9dc7e10cbaf',
						'cd4136746e723eac0f4b72c67266aecca7933fd7ad44af95a788cccfa54cbd12',
						'686ee2abb81c68e1b13a58868f3b0cbc65e8507564ee55ca95e0c2e64cff0ff9',
						'ff8335db44f7607507fa77e742cf8252787679978ebfa269ba7fe0f32b3e13eb',
						'154993ad5a954fb6a09a56801dfea7000a9d1b55821fbeaa1efd26c232f2f590',
						'7f6d3a0b11fa466891cba0bcfb74d979f2635b45f699b0fd8fa0103d75e68d57',
						'9eff894de9dc349b84c71928fc0ba744d6a63ee9f2f14a5fb072c217d9cba24e',
						'6c0e7d837808490f9044cb019deb21c0b235f3f0516b12b437c5d73ebca567c6',
						'4ad4ef2451acd3ec8728c0dca243b7195f363efcb98fcae9b9f92f5fada69486',
						'c97ac7acb8c7297c3ed052a3850a6bd0dc3e44d637898a0df55f0831917b17b2')

;

WITH
	bookings AS (
		SELECT
			stt.touch_id,
			LISTAGG(DISTINCT fb.se_sale_id, ', ')              AS sale_id,
			COUNT(*)                                           AS bookings,
			SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.scv_touched_transactions stt
			INNER JOIN se.data.fact_booking fb ON stt.booking_id = fb.booking_id
		WHERE fb.booking_status_type IN ('live', 'cancelled')
		GROUP BY 1
	),
	model_data AS (
		SELECT *
		FROM dbt.bi_data_science__intermediate.rnr_ab_session_metrics rasm
			LEFT JOIN bookings b ON rasm.touch_id = b.touch_id
		WHERE rasm.ab_test_name = 'RNR V3'
	)
SELECT
	md.touch_start_tstamp::DATE AS date,
	md.ab_test_group,
	md.touch_affiliate_territory,
	SUM(bookings)               AS bookings,
	COUNT(DISTINCT md.sale_id)  AS distinct_sales
FROM model_data md
WHERE md.had_multiple_assignments = FALSE
GROUP BY 1, 2, 3