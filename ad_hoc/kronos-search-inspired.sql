-- kronos search results
-- dashboard to visualise


-- number of results
-- search session to spv conversion by day
-- recommended for you spv clicks
-- search to book
-- searches where we don't have recommended for you vs those where we do - how are they performing for those metrics
-- data is in scv already
-- triggered by with have kronos

SELECT
	sts.triggered_by,
	COUNT(*)
FROM se.data.scv_touched_searches sts
WHERE sts.event_tstamp >= '2025-07-02'
  AND sts.se_brand = 'SE Brand'
GROUP BY 1
;

SELECT
	COUNT(DISTINCT sts.touch_id)
FROM se.data.scv_touched_searches sts
WHERE sts.event_tstamp >= '2025-07-02'
  AND sts.se_brand = 'SE Brand'
;

SELECT
	sts.touch_id,
	sts.triggered_by,
	sts.search_context['results'],
	search_result.value['saleId']::VARCHAR AS sale_id_in_search_result
FROM se.data.scv_touched_searches sts,
	 LATERAL FLATTEN(INPUT => sts.search_context['results'], OUTER => TRUE) search_result
WHERE sts.event_tstamp >= '2025-07-02'
  AND sts.triggered_by = 'kronosRecommendations' -- TODO REMOVE
;

WITH
	searches AS (
		SELECT
			sts.touch_id,
			COUNT_IF(sts.triggered_by = 'user')                                         AS user_searches,
			COUNT_IF(sts.triggered_by = 'pageLoad')                                     AS page_load_searches,
			COUNT_IF(sts.triggered_by = 'kronosRecommendations')                        AS kronos_searches,
			AVG(IFF(sts.triggered_by = 'user', sts.num_results, NULL))                  AS avg_num_results_user,
			AVG(IFF(sts.triggered_by = 'pageLoad', sts.num_results, NULL))              AS avg_num_results_page_load,
			AVG(IFF(sts.triggered_by = 'kronosRecommendations', sts.num_results, NULL)) AS avg_num_results_kronos
		FROM se.data.scv_touched_searches sts
		WHERE sts.event_tstamp >= '2025-07-02'
		  AND sts.se_brand = 'SE Brand'
		GROUP BY 1
	),
	spvs AS (
		SELECT
			sts.touch_id,
			COUNT(*) AS spvs
		FROM se.data.scv_touched_spvs sts
		WHERE sts.event_tstamp >= '2025-07-02'
		GROUP BY 1
	),
	transactions AS (
		SELECT
			stt.touch_id,
			COUNT(*)                                           AS bookings,
			SUM(fb.gross_revenue_gbp_constant_currency)        AS gross_revenue_gbp,
			SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.scv_touched_transactions stt
			INNER JOIN se.data.fact_booking fb ON stt.booking_id = fb.booking_id
			AND fb.booking_status_type IN ('live', 'cancelled')
		WHERE stt.event_tstamp >= '2025-07-02'
		GROUP BY 1
	)
SELECT
	stba.touch_id,
	stba.touch_start_tstamp,
	stba.touch_experience,
	stmc.touch_mkt_channel,
	stmc.touch_affiliate_territory,
	IFF(searches.user_searches >= 1, TRUE, FALSE)             AS had_user_search,
	IFF(searches.page_load_searches >= 1, TRUE, FALSE)        AS had_page_load_search,
	IFF(had_user_search OR had_page_load_search, TRUE, FALSE) AS had_search,
	IFF(searches.kronos_searches >= 1, TRUE, FALSE)           AS saw_kronos,
	IFF(spvs.spvs >= 1, TRUE, FALSE)                          AS had_spv,
	IFF(transactions.bookings >= 1, TRUE, FALSE)              AS converted,
	COALESCE(searches.user_searches, 0)                       AS user_searches,
	COALESCE(searches.kronos_searches, 0)                     AS kronos_searches,
	COALESCE(searches.page_load_searches, 0)                  AS page_load_searches,
	COALESCE(spvs.spvs, 0)                                    AS spvs,
	COALESCE(transactions.bookings, 0)                        AS bookings,
	COALESCE(transactions.gross_revenue_gbp, 0)               AS gross_revenue_gbp,
	COALESCE(transactions.margin_gbp, 0)                      AS margin_gbp
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
	LEFT JOIN  searches ON stba.touch_id = searches.touch_id
	LEFT JOIN  spvs ON stba.touch_id = spvs.touch_id
	LEFT JOIN  transactions ON stba.touch_id = transactions.touch_id
WHERE stba.touch_start_tstamp BETWEEN '2025-07-02' AND CURRENT_TIMESTAMP -- date kronos sec to item went live
  AND stba.touch_se_brand = 'SE Brand'
;

SELECT 10 BETWEEN 1 AND 10
;

SELECT
	bookings.touch_id,
	bookings.bookings,
	bookings.booking_1_adult,
	bookings.booking_2_adults,
	bookings.bookings_more_than_2_people,
	bookings.total_margin,
	bookings.margin_booking_1_adult,
	bookings.margin_booking_2_adults,
	bookings.margin_more_than_2_people
FROM data_vault_mvp_dev_robin.bi.search_model__step01__aggregate_bookings_to_touch_id AS bookings
;

SELECT
	spvs.touch_id,
	spvs.spvs
FROM data_vault_mvp_dev_robin.bi.search_model__step02__aggregate_spvs_to_touch_id AS spvs
;

SELECT
	bfvs.touch_id,
	bfvs.bfvs
FROM data_vault_mvp_dev_robin.bi.search_model__step03__aggregate_bfvs_to_touch_id AS bfvs
;

SELECT
	session_metrics.touch_id,
	session_metrics.event_date,
	session_metrics.touch_experience,
	session_metrics.touch_mkt_channel,
	session_metrics.posa_category,
	session_metrics.touch_affiliate_territory,

	COUNT(DISTINCT session_metrics.touch_id)                                                         AS sessions,
	COUNT(DISTINCT session_metrics.attributed_user_id)                                               AS users,

	COUNT(DISTINCT IFF(session_metrics.session_has_booking, session_metrics.touch_id,
					   NULL))                                                                        AS sessions_with_booking,
	COUNT(DISTINCT
		  IFF(session_metrics.session_has_spv, session_metrics.touch_id, NULL))                      AS sessions_with_spv,
	COUNT(DISTINCT
		  IFF(session_metrics.session_has_bfv, session_metrics.touch_id, NULL))                      AS sessions_with_bfv,
	COUNT(DISTINCT IFF(session_metrics.session_has_search, session_metrics.touch_id,
					   NULL))                                                                        AS sessions_with_search,
	COUNT(DISTINCT IFF(session_metrics.session_has_user_search, session_metrics.touch_id,
					   NULL))                                                                        AS sessions_with_user_search,
	COUNT(DISTINCT IFF(session_metrics.session_has_pageload_search, session_metrics.touch_id,
					   NULL))                                                                        AS sessions_with_pageload_search,
	COUNT(DISTINCT IFF(session_metrics.session_has_kronos_search, session_metrics.touch_id,
					   NULL))                                                                        AS sessions_with_kronos_search,

	SUM(session_metrics.bookings)                                                                    AS bookings,
	SUM(session_metrics.booking_1_adult)                                                             AS booking_1_adult,
	SUM(session_metrics.booking_2_adults)                                                            AS booking_2_adults,
	SUM(session_metrics.bookings_more_than_2_people)                                                 AS bookings_more_than_2_people,
	SUM(session_metrics.total_margin)                                                                AS total_margin,
	SUM(session_metrics.margin_booking_1_adult)                                                      AS margin_booking_1_adult,
	SUM(session_metrics.margin_booking_2_adults)                                                     AS margin_booking_2_adults,
	SUM(session_metrics.margin_more_than_2_people)                                                   AS margin_more_than_2_people,
	SUM(session_metrics.spvs)                                                                        AS spvs,
	SUM(session_metrics.bfvs)                                                                        AS bfvs,
--             SUM(session_metrics.first_triggered_by) AS first_triggered_by,
	SUM(session_metrics.searches)                                                                    AS searches,
	SUM(session_metrics.user_searches)                                                               AS user_searches,
	SUM(session_metrics.user_searches_zero_results)                                                  AS user_searches_zero_results,
	SUM(session_metrics.user_searches_one_five_results)                                              AS user_searches_one_five_results,
	SUM(session_metrics.user_searches_six_ten_results)                                               AS user_searches_six_ten_results,
	SUM(session_metrics.user_searches_one_ten_results)                                               AS user_searches_one_ten_results,
	SUM(session_metrics.user_searches_greater_than_ten_results)                                      AS user_searches_greater_than_ten_results,
	SUM(session_metrics.pageload_searches)                                                           AS pageload_searches,
	SUM(session_metrics.pageload_searches_zero_results)                                              AS pageload_searches_zero_results,
	SUM(session_metrics.pageload_searches_one_five_results)                                          AS pageload_searches_onqe_five_results,
	SUM(session_metrics.pageload_searches_six_ten_results)                                           AS pageload_searches_six_ten_results,
	SUM(session_metrics.pageload_searches_one_ten_results)                                           AS pageload_searches_one_ten_results,
	SUM(session_metrics.pageload_searches_greater_than_ten_results)                                  AS pageload_searches_greater_than_ten_results,
	SUM(session_metrics.kronos_searches)                                                             AS kronos_searches,
	SUM(session_metrics.kronos_searches_zero_results)                                                AS kronos_searches_zero_results,
	SUM(session_metrics.kronos_searches_one_five_results)                                            AS kronos_searches_one_five_results,
	SUM(session_metrics.kronos_searches_six_ten_results)                                             AS kronos_searches_six_ten_results,
	SUM(session_metrics.kronos_searches_one_ten_results)                                             AS kronos_searches_one_ten_results,
	SUM(session_metrics.kronos_searches_greater_than_ten_results)                                    AS kronos_searches_greater_than_ten_results

FROM data_vault_mvp_dev_robin.bi.search_model__step05__model_data_at_session_level session_metrics
GROUP BY session_metrics.touch_id,
		 session_metrics.event_date,
		 session_metrics.touch_experience,
		 session_metrics.touch_mkt_channel,
		 session_metrics.posa_category,
		 session_metrics.touch_affiliate_territory


SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.bi.search_model__step05__model_data_at_session_level session_metrics 467,194,223


SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.bi.search_model__step06__aggregate_to_output_model_grain')
;


CREATE OR REPLACE TRANSIENT TABLE search_model__step06__aggregate_to_output_model_grain
(
	event_date DATE,
	touch_experience VARCHAR,
	touch_mkt_channel VARCHAR,
	posa_category VARCHAR,
	touch_affiliate_territory VARCHAR,
	sessions INTEGER,
	users INTEGER,
	sessions_with_booking INTEGER,
	sessions_with_spv INTEGER,
	sessions_with_bfv INTEGER,
	sessions_with_search INTEGER,
	sessions_with_user_search INTEGER,
	sessions_with_pageload_search INTEGER,
	sessions_with_kronos_search INTEGER,
	bookings INTEGER,
	booking_1_adult INTEGER,
	booking_2_adults INTEGER,
	bookings_more_than_2_people INTEGER,
	total_margin INTEGER,
	margin_booking_1_adult INTEGER,
	margin_booking_2_adults INTEGER,
	margin_more_than_2_people INTEGER,
	spvs INTEGER,
	bfvs INTEGER,
	searches INTEGER,
	user_searches INTEGER,
	user_searches_zero_results INTEGER,
	user_searches_one_five_results INTEGER,
	user_searches_six_ten_results INTEGER,
	user_searches_one_ten_results INTEGER,
	user_searches_greater_than_ten_results INTEGER,
	pageload_searches INTEGER,
	pageload_searches_zero_results INTEGER,
	pageload_searches_one_five_results INTEGER,
	pageload_searches_six_ten_results INTEGER,
	pageload_searches_one_ten_results INTEGER,
	pageload_searches_greater_than_ten_results INTEGER,
	kronos_searches INTEGER,
	kronos_searches_zero_results INTEGER,
	kronos_searches_one_five_results INTEGER,
	kronos_searches_six_ten_results INTEGER,
	kronos_searches_one_ten_results INTEGER,
	kronos_searches_greater_than_ten_results INTEGER,
	first_triggered_user_bookings INTEGER,
	first_triggered_pageload_bookings INTEGER,
	first_triggered_user_margin_gbp INTEGER,
	first_triggered_pageload_margin_gbp INTEGER,
	first_triggered_user_sessions INTEGER,
	first_triggered_pageload_sessions INTEGER
)
;

USE ROLE PIPELINERUNNER
DROP VIEW se.data.search_model;
