-- has kronos done anything to search conversion?
-- went live on 3rd of July
-- member session search to book
-- Split seen kronos not kronos

-- serach results array,
-- spv sale id within array
-- booking sale id within array

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.search_model
	CLONE data_vault_mvp.bi.search_model
;

/*self_describing_task \
    --include 'biapp.task_catalogue.dv.bi.search.search_model.py' \
    --method 'run' \
    --start '2025-07-21 00:00:00' \
    --end '2025-07-21 00:00:00'*/

------------------------------------------------------------------------------------------------------------------------

WITH
	first_value_search AS (
		SELECT
			touched_searches.touch_id,
			touched_searches.num_results,
			touched_searches.triggered_by,
			-- first triggered
			FIRST_VALUE(touched_searches.triggered_by) IGNORE NULLS OVER (
				PARTITION BY
					touched_searches.touch_id
				ORDER BY
					touched_searches.event_tstamp
					ASC
				) AS first_triggered_by,
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches touched_searches
		WHERE touched_searches.event_tstamp::DATE >= '2024-01-01'
	)
SELECT
	search_events.touch_id,
	ANY_VALUE(search_events.first_triggered_by)                               AS first_triggered_by,
	COUNT(*)                                                                  AS searches,

	COUNT(IFF(search_events.triggered_by = 'user', 1, NULL))                  AS user_searches,
	COUNT(IFF(search_events.num_results = 0 AND search_events.triggered_by = 'user', 1,
			  NULL))                                                          AS user_searches_zero_results,
	COUNT(IFF(search_events.num_results BETWEEN 1 AND 5 AND search_events.triggered_by = 'user', 1,
			  NULL))                                                          AS user_searches_one_five_results,
	COUNT(IFF(search_events.num_results BETWEEN 6 AND 10 AND search_events.triggered_by = 'user', 1,
			  NULL))                                                          AS user_searches_six_ten_results,
	COUNT(IFF(search_events.num_results BETWEEN 1 AND 10 AND search_events.triggered_by = 'user', 1,
			  NULL))                                                          AS user_searches_one_ten_results,
	COUNT(IFF(search_events.num_results > 10 AND search_events.triggered_by = 'user', 1,
			  NULL))                                                          AS user_searches_greater_than_ten_results,

	COUNT(IFF(search_events.triggered_by = 'pageLoad', 1, NULL))              AS pageload_searches,
	COUNT(IFF(search_events.num_results = 0 AND search_events.triggered_by = 'pageLoad', 1,
			  NULL))                                                          AS pageload_searches_zero_results,
	COUNT(IFF(search_events.num_results BETWEEN 1 AND 5 AND search_events.triggered_by = 'pageLoad', 1,
			  NULL))                                                          AS pageload_searches_one_five_results,
	COUNT(IFF(search_events.num_results BETWEEN 6 AND 10 AND search_events.triggered_by = 'pageLoad', 1,
			  NULL))                                                          AS pageload_searches_six_ten_results,
	COUNT(IFF(search_events.num_results BETWEEN 1 AND 10 AND search_events.triggered_by = 'pageLoad', 1,
			  NULL))                                                          AS pageload_searches_one_ten_results,
	COUNT(IFF(search_events.num_results > 10 AND search_events.triggered_by = 'pageLoad', 1,
			  NULL))                                                          AS pageload_searches_greater_than_ten_results,

	COUNT(IFF(search_events.triggered_by = 'kronosRecommendations', 1, NULL)) AS kronos_searches,
	COUNT(IFF(search_events.num_results = 0 AND search_events.triggered_by = 'kronosRecommendations', 1,
			  NULL))                                                          AS kronos_searches_zero_results,
	COUNT(IFF(search_events.num_results BETWEEN 1 AND 5 AND search_events.triggered_by = 'kronosRecommendations', 1,
			  NULL))                                                          AS kronos_searches_one_five_results,
	COUNT(IFF(search_events.num_results BETWEEN 6 AND 10 AND search_events.triggered_by = 'kronosRecommendations', 1,
			  NULL))                                                          AS kronos_searches_six_ten_results,
	COUNT(IFF(search_events.num_results BETWEEN 1 AND 10 AND search_events.triggered_by = 'kronosRecommendations', 1,
			  NULL))                                                          AS kronos_searches_one_ten_results,
	COUNT(IFF(search_events.num_results > 10 AND search_events.triggered_by = 'kronosRecommendations', 1,
			  NULL))                                                          AS kronos_searches_greater_than_ten_results,

	COUNT(DISTINCT IFF(search_events.triggered_by = 'user', search_events.touch_id,
					   NULL))                                                 AS sessions_user_search,
	COUNT(DISTINCT IFF(search_events.num_results = 0 AND search_events.triggered_by = 'user', search_events.touch_id,
					   NULL))                                                 AS sessions_user_search_zero_results,
	COUNT(DISTINCT
		  IFF(search_events.num_results BETWEEN 1 AND 5 AND search_events.triggered_by = 'user', search_events.touch_id,
			  NULL))                                                          AS sessions_user_search_one_five_results,
	COUNT(DISTINCT IFF(search_events.num_results BETWEEN 6 AND 10 AND search_events.triggered_by = 'user',
					   search_events.touch_id,
					   NULL))                                                 AS sessions_user_search_six_ten_results,
	COUNT(DISTINCT IFF(search_events.num_results BETWEEN 1 AND 10 AND search_events.triggered_by = 'user',
					   search_events.touch_id,
					   NULL))                                                 AS sessions_user_search_one_ten_results,
	COUNT(DISTINCT IFF(search_events.num_results > 10 AND search_events.triggered_by = 'user', search_events.touch_id,
					   NULL))                                                 AS sessions_user_search_greater_than_ten_results,

	COUNT(DISTINCT IFF(search_events.triggered_by = 'pageLoad', search_events.touch_id,
					   NULL))                                                 AS sessions_page_load_search,
	COUNT(DISTINCT
		  IFF(search_events.num_results = 0 AND search_events.triggered_by = 'pageLoad', search_events.touch_id,
			  NULL))                                                          AS sessions_page_load_search_zero_results,
	COUNT(DISTINCT IFF(search_events.num_results BETWEEN 1 AND 5 AND search_events.triggered_by = 'pageLoad',
					   search_events.touch_id,
					   NULL))                                                 AS sessions_pageload_search_one_five_results,
	COUNT(DISTINCT IFF(search_events.num_results BETWEEN 6 AND 10 AND search_events.triggered_by = 'pageLoad',
					   search_events.touch_id,
					   NULL))                                                 AS sessions_pageload_search_six_ten_results,
	COUNT(DISTINCT IFF(search_events.num_results BETWEEN 1 AND 10 AND search_events.triggered_by = 'pageLoad',
					   search_events.touch_id,
					   NULL))                                                 AS sessions_pageload_search_one_ten_results,
	COUNT(DISTINCT
		  IFF(search_events.num_results > 10 AND search_events.triggered_by = 'pageLoad', search_events.touch_id,
			  NULL))                                                          AS sessions_pageload_search_greater_than_ten_results

FROM first_value_search AS search_events
GROUP BY search_events.touch_id


USE WAREHOUSE pipe_xlarge
;

WITH
	explode_search_results AS (
		SELECT
			touched_searches.touch_id,
			touched_searches.num_results,
			touched_searches.triggered_by,
			touched_searches.search_context,
			results.value['saleId']::VARCHAR AS se_sale_id
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches touched_searches,
			 LATERAL FLATTEN(INPUT => touched_searches.search_context['results'], OUTER => TRUE) results
-- 		WHERE touched_searches.event_tstamp::DATE >= '2024-01-01' -- TODO UNCOMMENT
		WHERE touched_searches.event_tstamp::DATE >= CURRENT_DATE() - 1 -- TODO REMOVE
	),
	results_arrays AS (
		SELECT
			search_results.touch_id,
			ARRAY_AGG(DISTINCT search_results.se_sale_id) AS results_array,
			ARRAY_AGG(DISTINCT
					  IFF(search_results.triggered_by = 'user', search_results.se_sale_id,
						  NULL))                          AS results_user_array,
			-- pageload events don't currently have the results array populated, leaving code in place
			-- for posterity and incase sometime in the future this changes.
			ARRAY_AGG(DISTINCT
					  IFF(search_results.triggered_by = 'pageLoad', search_results.se_sale_id,
						  NULL))                          AS results_pageload_array,
			ARRAY_AGG(DISTINCT
					  IFF(search_results.triggered_by = 'kronosRecommendations', search_results.se_sale_id,
						  NULL))                          AS results_kronos_array
		FROM explode_search_results search_results
		GROUP BY search_results.touch_id
	)
SELECT
	touch_id,
	results_array,
	ARRAY_SIZE(results_user_array),
	ARRAY_SIZE(results_pageload_array),
	ARRAY_SIZE(results_kronos_array)
FROM results_arrays
WHERE ARRAY_SIZE(results_arrays.results_pageload_array) > 0
;

SELECT *
FROM se.data.scv_touched_booking_form_views stbfv
;


SELECT *
FROM data_vault_mvp_dev_robin.bi.search_model__step06__model_data_at_session_level
-- WHERE session_has_spv_from_user_search AND session_has_spv_from_kronos_search

SELECT *
FROM se.data.scv_touched_searches sts
WHERE sts.event_tstamp >= CURRENT_DATE - 1
;

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 1 AND
	  ses.event_hash IN (
						 '483efc6d0938a79aa51e2edb3909be07588e3e295fef87e347d875c62a6406ec',
						 '7d7c85f7efb99ddc76d252152c93a7f02458a9d169a5632ac528a07489b1d893',
						 'f7bf093a47f3c1f0fac05fbf1743f02d0ad8d36a04d90b5d85d55778edee6ce4',
						 'ccd92604ad12160582174ddc84e44dfe196900a1efd8a0dc3366697cd8b61a25',
						 '1d70641c043525db8863dee9e7a4484621420014775704fa0636f95a42c8d723',
						 'f783797a7e86595ad66b94beca8d37f4656ff69b9a17d851e5d5fd0bfc28bfcf',
						 '1ca4debe3f95dbc7f98fe1dc00525a8dcd19229185b585c791ca703816292039',
						 '57cdc9dbd0b409ef2095434edf0833ed420563604e5ff1e62002197641e24b6b'
		  )
;
-- find search results pages with content interactions
SELECT *
FROM se.data_pii.scv_page_screen_enrichment spse
WHERE spse.event_tstamp >= CURRENT_DATE - 1
  AND spse.page_url LIKE '%search/search%'
  AND spse.content_interaction_array IS NOT NULL
;

-- we want to check if for every user search there's a kronos search

-- 47c89c44fb50c040285d6d0b68f8c9183d5a0ded52011fc8a0b48cbaa25bd4ca event hash where in page enrichement the content interation looks like they have clicked both user search results and kronos search results 2025-07-20 09:52:46.364000000

SELECT *
FROM se.data_pii.scv_page_screen_enrichment spse
WHERE spse.event_tstamp::DATE = '2025-07-20'
  AND spse.page_url LIKE '%search/search%'
  AND spse.event_hash = '47c89c44fb50c040285d6d0b68f8c9183d5a0ded52011fc8a0b48cbaa25bd4ca'

-- find the touch id for the pageview event
SELECT *
FROM se.data_pii.scv_session_events_link ssel
WHERE ssel.event_tstamp::DATE = '2025-07-20'
  AND ssel.event_hash = '47c89c44fb50c040285d6d0b68f8c9183d5a0ded52011fc8a0b48cbaa25bd4ca'

-- associated touch id: 47c89c44fb50c040285d6d0b68f8c9183d5a0ded52011fc8a0b48cbaa25bd4ca;

SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_id = '47c89c44fb50c040285d6d0b68f8c9183d5a0ded52011fc8a0b48cbaa25bd4ca'

SELECT *
FROM data_vault_mvp_dev_robin.bi.search_model__step06__model_data_at_session_level
WHERE search_model__step06__model_data_at_session_level.touch_id =
	  '47c89c44fb50c040285d6d0b68f8c9183d5a0ded52011fc8a0b48cbaa25bd4ca'


-- 58e445322d3edba80db63394aab63ea2d09b17a188506953103274f6eabf83e1 event hash where in page enrichement the content interation looks like they have clicked both user search results and kronos search results 2025-07-20 09:52:46.364000000
-- clicked A47267 from kronos (absense of category)
-- clicked A43890 from user search results (category set)

SELECT *
FROM se.data_pii.scv_page_screen_enrichment spse
WHERE spse.event_tstamp::DATE = '2025-07-20'
  AND spse.page_url LIKE '%search/search%'
  AND spse.event_hash = '58e445322d3edba80db63394aab63ea2d09b17a188506953103274f6eabf83e1'

-- find the touch id for the pageview event
SELECT *
FROM se.data_pii.scv_session_events_link ssel
WHERE ssel.event_tstamp::DATE = '2025-07-20'
  AND ssel.event_hash = '58e445322d3edba80db63394aab63ea2d09b17a188506953103274f6eabf83e1'

-- associated touch id: 966ea1369177412a8242472557be7556e7d16b9e7b49610ffec0736b84471c15;

SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_id = '966ea1369177412a8242472557be7556e7d16b9e7b49610ffec0736b84471c15'

SELECT *
FROM data_vault_mvp_dev_robin.bi.search_model__step06__model_data_at_session_level
WHERE search_model__step06__model_data_at_session_level.touch_id =
	  '966ea1369177412a8242472557be7556e7d16b9e7b49610ffec0736b84471c15' 22fac69d029c183fa0e6d6b1a10fab983b98e2345d7a2eab8cacdd8c64e34a6d5

-- fac69d029c183fa0e6d6b1a10fab983b98e2345d7a2eab8cacdd8c64e34a6d5 event hash where in page enrichement the content interation looks like they have clicked both user search results and kronos search results 2025-07-20 09:52:46.364000000
-- clicked A61146 from kronos (absense of category)
-- clicked A72748 from user search results (category set)

SELECT *
FROM se.data_pii.scv_page_screen_enrichment spse
WHERE spse.event_tstamp::DATE = '2025-07-20'
  AND spse.page_url LIKE '%search/search%'
  AND spse.event_hash = '2fac69d029c183fa0e6d6b1a10fab983b98e2345d7a2eab8cacdd8c64e34a6d5'
;

-- find the touch id for the pageview event
SELECT *
FROM se.data_pii.scv_session_events_link ssel
WHERE ssel.event_tstamp::DATE = '2025-07-20'
  AND ssel.event_hash = '2fac69d029c183fa0e6d6b1a10fab983b98e2345d7a2eab8cacdd8c64e34a6d5'
;

SELECT *
FROM data_vault_mvp_dev_robin.bi.search_model__step06__model_data_at_session_level
WHERE search_model__step06__model_data_at_session_level.touch_id =
	  'ce24b1c40d221b9574152da9b599d574d8196c2315b05735b77b144c55d2f113'


-- 64dee5de926f2321f562c62559c4f5ebb36d810769a751174f73c61c09aed218 event hash where in page enrichement the content interation looks like they have clicked both user search results and kronos search results 2025-07-20 09:52:46.364000000
-- clicked A27338 from kronos (absense of category)
-- clicked A75657 from user search results (category set)
-- clicked A73796 from user search results (category set)
-- clicked A73397 from user search results (category set)
-- clicked A76391 from user search results (category set)
-- clicked A69196 from user search results (category set)
-- clicked A76816 from user search results (category set)

SELECT *
FROM se.data_pii.scv_page_screen_enrichment spse
WHERE spse.event_tstamp::DATE = '2025-07-20'
  AND spse.page_url LIKE '%search/search%'
  AND spse.event_hash = '64dee5de926f2321f562c62559c4f5ebb36d810769a751174f73c61c09aed218'
;

-- find the touch id for the pageview event
SELECT *
FROM se.data_pii.scv_session_events_link ssel
WHERE ssel.event_tstamp::DATE = '2025-07-20'
  AND ssel.event_hash = '64dee5de926f2321f562c62559c4f5ebb36d810769a751174f73c61c09aed218'
;

SELECT *
FROM data_vault_mvp_dev_robin.bi.search_model__step06__model_data_at_session_level
WHERE search_model__step06__model_data_at_session_level.touch_id =
	  'f2b6df2153c498423437337ea9044466c2fd82e8d88f5f25493a2a92c5cf0c2e'



SELECT *
FROM se.bi.search_model
;

SELECT IFNULL(NULL + 1, 0)


SELECT
	touched_transactions.touch_id,
	COUNT(DISTINCT touched_transactions.booking_id)                          AS bookings,
	COUNT(DISTINCT
		  IFF(fact_booking.adult_guests = 1, fact_booking.booking_id, NULL)) AS booking_1_adult,
	COUNT(DISTINCT
		  IFF(fact_booking.adult_guests = 2, fact_booking.booking_id, NULL)) AS booking_2_adults,
	COUNT(DISTINCT IFF(fact_booking.adult_guests + fact_booking.child_guests > 2, fact_booking.booking_id,
					   NULL))                                                AS bookings_more_than_2_people,

	SUM(fact_booking.margin_gross_of_toms_gbp_constant_currency)             AS margin_gbp,
	SUM(IFF(fact_booking.adult_guests = 1, fact_booking.margin_gross_of_toms_gbp_constant_currency,
			NULL))                                                           AS margin_gbp_booking_1_adult,
	SUM(IFF(fact_booking.adult_guests = 2, fact_booking.margin_gross_of_toms_gbp_constant_currency,
			NULL))                                                           AS margin_gbp_booking_2_adults,
	SUM(IFF(COALESCE(fact_booking.adult_guests, 0) + COALESCE(fact_booking.child_guests, 0) > 2,
			fact_booking.margin_gross_of_toms_gbp_constant_currency, NULL))  AS margin_gbp_more_than_2_people,
	ARRAY_AGG(DISTINCT fact_booking.se_sale_id)                              AS bookings_sale_id_array,
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions touched_transactions
	INNER JOIN data_vault_mvp.dwh.fact_booking AS fact_booking
			   ON touched_transactions.booking_id = fact_booking.booking_id
				   AND fact_booking.booking_completed_date > '2024-01-01'
	INNER JOIN data_vault_mvp.dwh.dim_sale AS dim_sale
			   ON fact_booking.se_sale_id = dim_sale.se_sale_id
WHERE touched_transactions.event_tstamp::DATE >= '2024-01-01'
GROUP BY touched_transactions.touch_id
;

SELECT
	sm.session_date,
	SUM(sm.sessions_with_user_search_one_ten_results)                 AS sessions_with_user_search_one_ten_results,
	SUM(sm.sessions_with_user_search_one_ten_results_spv)             AS sessions_with_user_search_one_ten_results_spv,
	SUM(sm.sessions_with_user_search_one_ten_results_spv_bfv)         AS sessions_with_user_search_one_ten_results_spv_bfv,
	SUM(sm.sessions_with_user_search_one_ten_results_spv_bfv_booking) AS sessions_with_user_search_one_ten_results_spv_bfv_booking,
FROM se.bi.search_model sm
GROUP BY 1
;

SELECT
	sm.session_date,
	SUM(sm.sessions_with_user_search_one_ten_results)                 AS sessions_with_user_search_one_ten_results,
	SUM(sm.sessions_with_user_search_one_ten_results_spv)             AS sessions_with_user_search_one_ten_results_spv,
	SUM(sm.sessions_with_user_search_one_ten_results_spv_bfv)         AS sessions_with_user_search_one_ten_results_spv_bfv,
	SUM(sm.sessions_with_user_search_one_ten_results_spv_bfv_booking) AS sessions_with_user_search_one_ten_results_spv_bfv_booking,
FROM se_dev_robin.bi.search_model sm
GROUP BY 1
;



SELECT
	sm.session_date,
	SUM(sm.sessions_with_user_search_one_five_results)                 AS sessions_with_user_search_one_five_results,
	SUM(sm.sessions_with_user_search_one_five_results_spv)             AS sessions_with_user_search_one_five_results_spv,
	SUM(sm.sessions_with_user_search_one_five_results_spv_bfv)         AS sessions_with_user_search_one_five_results_spv_bfv,
	SUM(sm.sessions_with_user_search_one_five_results_spv_bfv_booking) AS sessions_with_user_search_one_five_results_spv_bfv_booking,
FROM se.bi.search_model sm
GROUP BY 1
;


SELECT
	sm.session_date,
	SUM(sm.sessions_with_user_search)                 AS sessions_with_user_search,
	SUM(sm.sessions_with_user_search_spv)             AS sessions_with_user_search_spv,
	SUM(sm.sessions_with_user_search_spv_bfv)         AS sessions_with_user_search_spv_bfv,
	SUM(sm.sessions_with_user_search_spv_bfv_booking) AS sessions_with_user_search_spv_bfv_booking,
FROM se.bi.search_model sm
GROUP BY 1



SELECT
	sm.session_date,
	SUM(sm.sessions),
	SUM(sm.sessions_with_user_search),
	SUM(sm.sessions_with_spv),
	SUM(sm.sessions_with_booking),
	SUM(sm.sessions_with_user_search_one_ten_results_spv_bfv_booking) AS sessions_with_user_search_one_ten_results_spv_bfv_booking,
FROM se.bi.search_model sm
GROUP BY 1
;

WITH
	step07 AS
		(
			SELECT * FROM data_vault_mvp_dev_robin.bi.search_model__step07__model_data_at_session_level sms07mdasl
		)
		,
	step08 AS (
			SELECT
				session_date,
				touch_experience,
				touch_mkt_channel,
				posa_category,
				touch_affiliate_territory,
				COUNT(DISTINCT IFF(session_has_atleast_one_user_search_one_ten_results
									   AND session_has_spv
									   AND session_has_bfv
									   AND session_has_booking,
								   touch_id, NULL)) AS sessions_with_user_search_one_ten_results_spv_bfv_booking,
			FROM step07
			GROUP BY session_date,
					 touch_experience,
					 touch_mkt_channel,
					 posa_category,
					 touch_affiliate_territory
		)
SELECT
	session_date,
	SUM(sessions_with_user_search_one_ten_results_spv_bfv_booking)
-- FROM step08
FROM data_vault_mvp_dev_robin.bi.search_model__step09__enrich_model_with_se_calendar
WHERE session_date >= '2025-07-01'
GROUP BY session_date

;


SELECT
	batch.session_date,
	batch.last_year_ytd,
	batch.last_year,
	batch.this_year,
	batch.this_year_ytd,
	batch.touch_experience,
	batch.touch_mkt_channel,
	batch.posa_category,
	batch.touch_affiliate_territory,
	batch.sessions,
	batch.users,
	batch.sessions_with_booking,
	batch.sessions_with_spv,
	batch.sessions_with_bfv,
	batch.sessions_with_search,
	batch.sessions_with_user_search,
	batch.sessions_with_pageload_search,
	batch.sessions_with_kronos_search,
	batch.bookings,
	batch.bookings_1_adult,
	batch.bookings_2_adults,
	batch.bookings_more_than_2_people,
	batch.margin_gbp,
	batch.margin_gbp_1_adult,
	batch.margin_gbp_2_adults,
	batch.margin_gbp_more_than_2_people,
	batch.spvs,
	batch.bfvs,
	batch.searches,
	batch.user_searches,
	batch.user_searches_zero_results,
	batch.user_searches_one_five_results,
	batch.user_searches_six_ten_results,
	batch.user_searches_one_ten_results,
	batch.user_searches_greater_than_ten_results,
	batch.pageload_searches,
	batch.pageload_searches_zero_results,
	batch.pageload_searches_one_five_results,
	batch.pageload_searches_six_ten_results,
	batch.pageload_searches_one_ten_results,
	batch.pageload_searches_greater_than_ten_results,
	batch.kronos_searches,
	batch.kronos_searches_zero_results,
	batch.kronos_searches_one_five_results,
	batch.kronos_searches_six_ten_results,
	batch.kronos_searches_one_ten_results,
	batch.kronos_searches_greater_than_ten_results,
	batch.sessions_with_user_search_zero_results,
	batch.sessions_with_user_search_one_five_results,
	batch.sessions_with_user_search_six_ten_results,
	batch.sessions_with_user_search_one_ten_results,
	batch.sessions_with_user_search_greater_than_ten_results,
	batch.sessions_with_pageload_search_zero_results,
	batch.sessions_with_pageload_search_one_five_results,
	batch.sessions_with_pageload_search_six_ten_results,
	batch.sessions_with_pageload_search_one_ten_results,
	batch.sessions_with_pageload_search_greater_than_ten_results,
	batch.sessions_with_user_search_spv,
	batch.sessions_with_user_search_spv_bfv,
	batch.sessions_with_user_search_spv_bfv_booking,
	batch.sessions_with_user_search_zero_results_spv,
	batch.sessions_with_user_search_zero_results_spv_bfv,
	batch.search_spv_bfv_book_sessions_with_user_search_zero_results,
	batch.sessions_with_user_search_one_five_results_spv,
	batch.sessions_with_user_search_one_five_results_spv_bfv,
	batch.sessions_with_user_search_one_five_results_spv_bfv_booking,
	batch.sessions_with_user_search_six_ten_results_spv,
	batch.search_spv_bfv_sessions_with_user_search_six_ten_results,
	batch.sessions_with_user_search_six_ten_results_spv_bfv_booking,
	batch.sessions_with_user_search_one_ten_results_spv,
	batch.sessions_with_user_search_one_ten_results_spv_bfv,
	batch.sessions_with_user_search_one_ten_results_spv_bfv_booking,
	batch.sessions_with_user_search_greater_ten_results_spv,
	batch.sessions_with_user_search_greater_ten_results_spv_bfv,
	batch.sessions_with_user_search_greater_ten_results_spv_bfv_booking,
	batch.sessions_with_user_search_booking,
	batch.margin_gbp_user_search,
	batch.booking_user_search,
	batch.first_triggered_user_search_bookings,
	batch.first_triggered_user_search_margin_gbp,
	batch.first_triggered_user_search_sessions,
	batch.first_triggered_pageload_search_bookings,
	batch.first_triggered_pageload_search_pageload_margin_gbp,
	batch.first_triggered_pageload_search_sessions,
	batch.sessions_with_spv_from_user_search,
	batch.sessions_with_spv_from_kronos_search,
	batch.sessions_with_bfv_from_user_search,
	batch.sessions_with_bfv_from_kronos_search,
	batch.sessions_with_booking_from_user_search,
	batch.sessions_with_booking_from_kronos_search
FROM data_vault_mvp_dev_robin.bi.search_model__step09__enrich_model_with_se_calendar

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.bi.search_model__step09__enrich_model_with_se_calendar')
;

CREATE TABLE scratch.robinpatel.search_model AS
SELECT *
FROM data_vault_mvp_dev_robin.bi.search_model__step09__enrich_model_with_se_calendar

SELECT GET_DDL('table', 'scratch.robinpatel.search_model')
;


CREATE OR REPLACE TABLE search_model
(
	session_date                                                  DATE,
	last_year_ytd                                                 BOOLEAN,
	last_year                                                     BOOLEAN,
	this_year                                                     BOOLEAN,
	this_year_ytd                                                 BOOLEAN,
	touch_experience                                              VARCHAR,
	touch_mkt_channel                                             VARCHAR,
	posa_category                                                 VARCHAR,
	touch_affiliate_territory                                     VARCHAR,
	sessions                                                      NUMBER,
	users                                                         NUMBER,
	sessions_with_booking                                         NUMBER,
	sessions_with_spv                                             NUMBER,
	sessions_with_bfv                                             NUMBER,
	sessions_with_search                                          NUMBER,
	sessions_with_user_search                                     NUMBER,
	sessions_with_pageload_search                                 NUMBER,
	sessions_with_kronos_search                                   NUMBER,
	bookings                                                      NUMBER,
	bookings_1_adult                                              NUMBER,
	bookings_2_adults                                             NUMBER,
	bookings_more_than_2_people                                   NUMBER,
	margin_gbp                                                    NUMBER,
	margin_gbp_1_adult                                            NUMBER,
	margin_gbp_2_adults                                           NUMBER,
	margin_gbp_more_than_2_people                                 NUMBER,
	spvs                                                          NUMBER,
	bfvs                                                          NUMBER,
	searches                                                      NUMBER,
	user_searches                                                 NUMBER,
	user_searches_zero_results                                    NUMBER,
	user_searches_one_five_results                                NUMBER,
	user_searches_six_ten_results                                 NUMBER,
	user_searches_one_ten_results                                 NUMBER,
	user_searches_greater_than_ten_results                        NUMBER,
	pageload_searches                                             NUMBER,
	pageload_searches_zero_results                                NUMBER,
	pageload_searches_one_five_results                            NUMBER,
	pageload_searches_six_ten_results                             NUMBER,
	pageload_searches_one_ten_results                             NUMBER,
	pageload_searches_greater_than_ten_results                    NUMBER,
	kronos_searches                                               NUMBER,
	kronos_searches_zero_results                                  NUMBER,
	kronos_searches_one_five_results                              NUMBER,
	kronos_searches_six_ten_results                               NUMBER,
	kronos_searches_one_ten_results                               NUMBER,
	kronos_searches_greater_than_ten_results                      NUMBER,
	sessions_with_user_search_zero_results                        NUMBER,
	sessions_with_user_search_one_five_results                    NUMBER,
	sessions_with_user_search_six_ten_results                     NUMBER,
	sessions_with_user_search_one_ten_results                     NUMBER,
	sessions_with_user_search_greater_than_ten_results            NUMBER,
	sessions_with_pageload_search_zero_results                    NUMBER,
	sessions_with_pageload_search_one_five_results                NUMBER,
	sessions_with_pageload_search_six_ten_results                 NUMBER,
	sessions_with_pageload_search_one_ten_results                 NUMBER,
	sessions_with_pageload_search_greater_than_ten_results        NUMBER,
	sessions_with_user_search_spv                                 NUMBER,
	sessions_with_user_search_spv_bfv                             NUMBER,
	sessions_with_user_search_spv_bfv_booking                     NUMBER,
	sessions_with_user_search_zero_results_spv                    NUMBER,
	sessions_with_user_search_zero_results_spv_bfv                NUMBER,
	search_spv_bfv_book_sessions_with_user_search_zero_results    NUMBER,
	sessions_with_user_search_one_five_results_spv                NUMBER,
	sessions_with_user_search_one_five_results_spv_bfv            NUMBER,
	sessions_with_user_search_one_five_results_spv_bfv_booking    NUMBER,
	sessions_with_user_search_six_ten_results_spv                 NUMBER,
	search_spv_bfv_sessions_with_user_search_six_ten_results      NUMBER,
	sessions_with_user_search_six_ten_results_spv_bfv_booking     NUMBER,
	sessions_with_user_search_one_ten_results_spv                 NUMBER,
	sessions_with_user_search_one_ten_results_spv_bfv             NUMBER,
	sessions_with_user_search_one_ten_results_spv_bfv_booking     NUMBER,
	sessions_with_user_search_greater_ten_results_spv             NUMBER,
	sessions_with_user_search_greater_ten_results_spv_bfv         NUMBER,
	sessions_with_user_search_greater_ten_results_spv_bfv_booking NUMBER,
	sessions_with_user_search_booking                             NUMBER,
	margin_gbp_user_search                                        NUMBER,
	booking_user_search                                           NUMBER,
	first_triggered_user_search_bookings                          NUMBER,
	first_triggered_user_search_margin_gbp                        NUMBER,
	first_triggered_user_search_sessions                          NUMBER,
	first_triggered_pageload_search_bookings                      NUMBER,
	first_triggered_pageload_search_pageload_margin_gbp           NUMBER,
	first_triggered_pageload_search_sessions                      NUMBER,
	sessions_with_spv_from_user_search                            NUMBER,
	sessions_with_spv_from_kronos_search                          NUMBER,
	sessions_with_bfv_from_user_search                            NUMBER,
	sessions_with_bfv_from_kronos_search                          NUMBER,
	sessions_with_booking_from_user_search                        NUMBER,
	sessions_with_booking_from_kronos_search                      NUMBER,
)
;

-- check search result arrays for app

USE WAREHOUSE pipe_xlarge
-- calculating clicks
WITH
	exploding_clicks AS (
		SELECT
			ssel.touch_id,
			spse.event_tstamp,
			spse.event_hash,
			spse.page_url,
			spse.content_interaction_array,
			clicks.value,
			clicks.value['element_category']::VARCHAR     AS element_category,
			clicks.value['element_sub_category']::VARCHAR AS element_sub_category,
			clicks.value['interaction_type']::VARCHAR     AS interaction_type,
			clicks.value['sale_id']::VARCHAR              AS se_sale_id,
		FROM se.data_pii.scv_page_screen_enrichment spse
				 INNER JOIN se.data_pii.scv_session_events_link ssel
							ON spse.event_hash = ssel.event_hash
								AND ssel.event_tstamp >= '2024-01-01',
			 LATERAL FLATTEN(INPUT => spse.content_interaction_array, OUTER => TRUE) clicks
		WHERE spse.event_tstamp >= '2024-01-01'
		  AND spse.page_url LIKE '%search/search%'
		  AND spse.content_interaction_array IS NOT NULL
		  AND (
			clicks.value['element_category']::VARCHAR IS NULL
				OR clicks.value['element_category']::VARCHAR IN ('search results', 'kronos_recommended_for_you')
			)
	)
SELECT
	exploding_clicks.touch_id,
	COUNT(*)                                                                                AS search_clicks,
	SUM(IFF(exploding_clicks.element_category IS NOT DISTINCT FROM 'search results', 1, 0)) AS search_results_clicks,
	SUM(IFF(exploding_clicks.element_category IS NULL OR
			exploding_clicks.element_category = 'kronos_recommended_for_you', 1,
			0))                                                                             AS search_results_kronos_clicks,
	ARRAY_AGG(DISTINCT exploding_clicks.se_sale_id)                                         AS search_clicks_array,
	ARRAY_AGG(DISTINCT
			  IFF(exploding_clicks.element_category IS NOT DISTINCT FROM 'search results', exploding_clicks.se_sale_id,
				  NULL))                                                                    AS search_results_clicks_array,
	ARRAY_AGG(DISTINCT IFF(exploding_clicks.element_category IS NULL OR
						   exploding_clicks.element_category = 'kronos_recommended_for_you',
						   exploding_clicks.se_sale_id,
						   NULL))                                                           AS search_results_kronos_clicks_array,
FROM exploding_clicks
GROUP BY 1
;



SELECT
	event_tstamp::DATE                   AS click_date,
	-- kronos clicks currently don't have a category, we are assuming clicks without a category on
	-- the search page are kronos clicks.
	COALESCE(element_category, 'kronos') AS element_category,
	COUNT(*)                             AS clicks
FROM exploding_clicks
WHERE element_category IS NULL OR element_category = 'search results'
GROUP BY 1, 2
;


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND ses.event_tstamp >= CURRENT_DATE - 1


SELECT *
FROM se.data_pii.scv_page_screen_enrichment spse
;


SELECT
	contexts_com_secretescapes_content_element_interaction_context_1,
	contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR     AS element_category,
	contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category
FROM snowplow.atomic.events
WHERE collector_tstamp >= CURRENT_DATE()
  AND contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
