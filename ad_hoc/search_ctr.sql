USE WAREHOUSE pipe_xlarge
;



-- web spvs
-- SE brand
-- UK


SELECT
	rasm.touch_start_tstamp::DATE AS session_date,
	SUM(rasm.cnt_searches),
	SUM(rasm.cnt_searches_with_clickthrough)
FROM dbt.bi_data_science__intermediate.rnr_ab_session_metrics rasm
WHERE rasm.cnt_searches > 0
GROUP BY 1
;



SELECT
	DATE_TRUNC(HOUR, fcb.booking_completed_timestamp)   AS hour,
	SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin,
	COUNT(DISTINCT fcb.booking_id)                      AS bookings
FROM se.data.fact_complete_booking fcb
WHERE fcb.se_brand = 'SE Brand'
  AND fcb.booking_completed_date > '2024-04-15'
  AND fcb.territory = 'UK'
GROUP BY 1
;



WITH
	clicks_from_searches
		SELECT
			sts.event_hash,
			sts.touch_id,
			sts.event_tstamp,
			sts.se_sale_id,
			sts.tb_offer_id,
			sts.event_category,
			sts.event_subcategory,
			sts.page_url,
			ses.page_referrer
		FROM se.data.scv_touched_spvs sts
			INNER JOIN se.data.scv_touch_basic_attributes stba
					   ON sts.touch_id = stba.touch_id AND stba.touch_se_brand = 'SE Brand'
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON sts.touch_id = stmc.touch_id AND stmc.touch_affiliate_territory = 'UK'
			INNER JOIN se.data_pii.scv_event_stream ses
					   ON sts.event_hash = ses.event_hash AND ses.event_tstamp >= '2024-04-25'
		WHERE sts.event_tstamp >= '2024-04-25'
		  AND sts.event_category = 'page views'
		  AND sts.page_url LIKE 'search/search'
		)
;

SELECT DISTINCT
	stmeoi.event_category
FROM se.data.scv_touched_module_events_of_interest stmeoi
;

-- EVENT_CATEGORY
-- screen views
-- transaction
-- app notification event
-- page views
-- web redirect
-- app install event
-- search event

SELECT
	stmeoi.touch_id,
	SUM(IFF(stmeoi.event_category = 'search event', 1, 0)) AS search_events,
	SUM(IFF(stmeoi.event_subcategory = 'SPV', 1, 0))       AS spvs
FROM se.data.scv_touched_module_events_of_interest stmeoi
WHERE stmeoi.event_tstamp >= ' 2024-01-01'
GROUP BY 1
;

-- Find a session with search and spv '67c8213bd9a7a53c3cff48d298d6ebdfe61a66db7f1c0438c8d83460a1fe0043'
SELECT
	stmeoi.event_hash,
	stmeoi.touch_id,
	stmeoi.event_category,
	stmeoi.event_subcategory,
	stmeoi.se_sale_id,
	LEAD(stmeoi.event_category)
		 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC) AS next_event_category,
	LEAD(stmeoi.event_subcategory)
		 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC) AS next_event_sub_category,
	LEAD(stmeoi.se_sale_id)
		 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC) AS next_se_sale_id,
	stmeoi.search_context['results'],
	stmeoi.search_context['results']::VARCHAR LIKE '%' || next_se_sale_id || '%',
	stmeoi.event_tstamp,
	stmeoi.page_url,
	stmeoi.page_urlpath
FROM se.data.scv_touched_module_events_of_interest stmeoi
WHERE stmeoi.event_tstamp >= ' 2024-01-01'
  AND stmeoi.touch_id = '67c8213bd9a7a53c3cff48d298d6ebdfe61a66db7f1c0438c8d83460a1fe0043'
;

-- model this data to aggregate

WITH
	events_of_interest_modelling AS (
		SELECT
			stmeoi.event_hash,
			stmeoi.touch_id,
			stba.touch_se_brand,
			stmc.touch_affiliate_territory,
			stmeoi.event_category,
			stmeoi.event_subcategory,
			stmeoi.se_sale_id,
			LEAD(stmeoi.event_category)
				 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_event_category,
			LEAD(stmeoi.event_subcategory)
				 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_event_sub_category,
			LEAD(stmeoi.se_sale_id)
				 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_se_sale_id,
			stmeoi.search_context['results'],
			stmeoi.search_context['results']::VARCHAR LIKE '%' || next_se_sale_id || '%' AS next_se_sale_id_in_results,
			stmeoi.event_tstamp,
			stmeoi.page_url,
			stmeoi.page_urlpath
		FROM se.data.scv_touched_module_events_of_interest stmeoi
			INNER JOIN se.data.scv_touch_basic_attributes stba ON stmeoi.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stmeoi.touch_id = stmc.touch_id
		WHERE stmeoi.event_tstamp >= ' 2024-01-01'
		  AND stba.touch_se_brand = 'SE Brand'
		  AND stmc.touch_affiliate_territory = 'UK'
		  AND stmeoi.touch_id = '67c8213bd9a7a53c3cff48d298d6ebdfe61a66db7f1c0438c8d83460a1fe0043'
	)
SELECT
	eoim.event_tstamp::DATE                                                             AS date,
	COUNT(*)                                                                            AS events,
	SUM(IFF(eoim.event_category = 'search event', 1, 0))                                AS searches,
	SUM(IFF(eoim.event_subcategory = 'SPV', 1, 0))                                      AS spvs,
	SUM(IFF(eoim.event_category = 'search event' AND next_se_sale_id_in_results, 1, 0)) AS search_click_through_spv
FROM events_of_interest_modelling eoim
GROUP BY 1
;

-- find session in rnr data that has click through
SELECT *
FROM dbt.bi_data_science__intermediate.rnr_ab_session_metrics rasm
WHERE rasm.has_search_clickthrough

-- 5dc7ae92890eadee55d46cc9fb70038f1cf62c73d1104dc0d80ab8868f0b983d


SELECT
	stmeoi.event_hash,
	stmeoi.touch_id,
	stba.touch_se_brand,
	stmc.touch_affiliate_territory,
	stmeoi.event_category,
	stmeoi.event_subcategory,
	stmeoi.se_sale_id,
	LEAD(stmeoi.event_category)
		 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_event_category,
	LEAD(stmeoi.event_subcategory)
		 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_event_sub_category,
	LEAD(stmeoi.se_sale_id)
		 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_se_sale_id,
	stmeoi.search_context['results']                                             AS search_results,
	-- check if next sale id is included in results array
	stmeoi.search_context,
	stmeoi.search_context['results']::VARCHAR LIKE '%' || next_se_sale_id || '%' AS next_se_sale_id_in_results,
	stmeoi.event_tstamp,
	stmeoi.page_url,
	stmeoi.page_urlpath,
	stmeoi.triggered_by
FROM se.data.scv_touched_module_events_of_interest stmeoi
	INNER JOIN se.data.scv_touch_basic_attributes stba ON stmeoi.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stmeoi.touch_id = stmc.touch_id
WHERE stmeoi.event_tstamp >= ' 2024-03-20'
  AND stba.touch_se_brand = 'SE Brand'
  AND stmeoi.touch_id = 'bd09ff84db72c824d57f5d18ba439f42ee266d73918167070e487089fd1986a6'
;


-- session with search and spv but no click through: 7d37495b172365b838c508f1a09ce5a0d9f5c828ea201217db6915855423f7ed
-- session with search and spv with click through: bd09ff84db72c824d57f5d18ba439f42ee266d73918167070e487089fd1986a6


WITH
	events_of_interest_modelling AS (
		SELECT
			stmeoi.event_hash,
			stmeoi.touch_id,
			stba.touch_se_brand,
			stmc.touch_affiliate_territory,
			stmeoi.event_category,
			stmeoi.event_subcategory,
			stmeoi.se_sale_id,
			LEAD(stmeoi.event_category)
				 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_event_category,
			LEAD(stmeoi.event_subcategory)
				 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_event_sub_category,
			LEAD(stmeoi.se_sale_id)
				 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_se_sale_id,
			stmeoi.search_context['results'],
			stmeoi.search_context['results']::VARCHAR LIKE '%' || next_se_sale_id || '%' AS next_se_sale_id_in_results,
			stmeoi.event_tstamp,
			stmeoi.page_url,
			stmeoi.page_urlpath,
			stmeoi.triggered_by
		FROM se.data.scv_touched_module_events_of_interest stmeoi
			INNER JOIN se.data.scv_touch_basic_attributes stba ON stmeoi.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stmeoi.touch_id = stmc.touch_id
		WHERE stmeoi.event_tstamp >= ' 2024-01-01'
		  AND stba.touch_se_brand = 'SE Brand'
-- 		  AND stmc.touch_affiliate_territory = 'UK'
-- 		  AND stmeoi.touch_id = '67c8213bd9a7a53c3cff48d298d6ebdfe61a66db7f1c0438c8d83460a1fe0043'
	)
SELECT
	eoim.event_tstamp::DATE                                                             AS date,
	touch_affiliate_territory,
	COUNT(*)                                                                            AS events,
	SUM(IFF(eoim.event_category = 'search event' AND eoim.triggered_by = 'user', 1, 0)) AS user_searches,
	SUM(IFF(eoim.event_subcategory = 'SPV', 1, 0))                                      AS spvs,
	SUM(IFF(eoim.event_category = 'search event' AND eoim.triggered_by = 'user' AND next_se_sale_id_in_results, 1,
			0))                                                                         AS user_search_click_through_spv
FROM events_of_interest_modelling eoim
GROUP BY 1
;


WITH
	events_of_interest_modelling AS (
		SELECT
			stmeoi.event_hash,
			stmeoi.touch_id,
			stba.touch_se_brand,
			stba.touch_experience,
			stmc.touch_affiliate_territory,
			stmeoi.event_category,
			stmeoi.event_subcategory,
			stmeoi.se_sale_id,
			LEAD(stmeoi.event_category)
				 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_event_category,
			LEAD(stmeoi.event_subcategory)
				 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_event_sub_category,
			LEAD(stmeoi.se_sale_id)
				 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_se_sale_id,
			stmeoi.search_context['results'],
			stmeoi.search_context['results']::VARCHAR LIKE '%' || next_se_sale_id || '%' AS next_se_sale_id_in_results,
			stmeoi.event_tstamp,
			stmeoi.page_url,
			stmeoi.page_urlpath,
			stmeoi.triggered_by
		FROM se.data.scv_touched_module_events_of_interest stmeoi
			INNER JOIN se.data.scv_touch_basic_attributes stba ON stmeoi.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stmeoi.touch_id = stmc.touch_id
		WHERE stmeoi.event_tstamp >= ' 2023-01-01'
		  AND stba.touch_se_brand = 'SE Brand'
-- 		  AND stmc.touch_affiliate_territory = 'UK'
-- 		  AND stmeoi.touch_id = '67c8213bd9a7a53c3cff48d298d6ebdfe61a66db7f1c0438c8d83460a1fe0043'
	)
SELECT
	eoim.event_tstamp::DATE                                                             AS date,
	touch_affiliate_territory,
	eoim.touch_experience,
	COUNT(*)                                                                            AS events,
	SUM(IFF(eoim.event_category = 'search event' AND eoim.triggered_by = 'user', 1, 0)) AS user_searches,
	SUM(IFF(eoim.event_subcategory = 'SPV', 1, 0))                                      AS spvs,
	SUM(IFF(eoim.event_category = 'search event' AND eoim.triggered_by = 'user' AND next_se_sale_id_in_results, 1,
			0))                                                                         AS user_search_click_through_spv
FROM events_of_interest_modelling eoim
GROUP BY 1, 2, 3
;

------------------------------------------------------------------------------------------------------------------------
-- creating a session level dataset


WITH
	events_of_interest_modelling AS (
		SELECT
			stmeoi.event_hash,
			stmeoi.touch_id,
			stba.touch_start_tstamp,
			stba.touch_se_brand,
			stba.touch_experience,
			stmc.touch_affiliate_territory,
			stmeoi.event_category,
			stmeoi.event_subcategory,
			stmeoi.se_sale_id,
			LEAD(stmeoi.event_category)
				 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_event_category,
			LEAD(stmeoi.event_subcategory)
				 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_event_sub_category,
			LEAD(stmeoi.se_sale_id)
				 OVER (PARTITION BY stmeoi.touch_id ORDER BY stmeoi.event_tstamp ASC)    AS next_se_sale_id,
			stmeoi.search_context['results'],
			stmeoi.search_context['results']::VARCHAR LIKE '%' || next_se_sale_id || '%' AS next_se_sale_id_in_results,
			stmeoi.event_tstamp,
			stmeoi.page_url,
			stmeoi.page_urlpath,
			stmeoi.triggered_by
		FROM se.data.scv_touched_module_events_of_interest stmeoi
			INNER JOIN se.data.scv_touch_basic_attributes stba ON stmeoi.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stmeoi.touch_id = stmc.touch_id
		WHERE stba.touch_start_tstamp::DATE >= ' 2024-01-01'
		  AND stba.touch_se_brand = 'SE Brand'
-- 		  AND stmc.touch_affiliate_territory = 'UK'
-- 		  AND stmeoi.touch_id = '67c8213bd9a7a53c3cff48d298d6ebdfe61a66db7f1c0438c8d83460a1fe0043'
	)
SELECT
	eoim.touch_start_tstamp::DATE                                                       AS date,
	eoim.touch_id,
	touch_affiliate_territory,
	eoim.touch_experience,
	COUNT(*)                                                                            AS events,
	SUM(IFF(eoim.event_category = 'search event' AND eoim.triggered_by = 'user', 1, 0)) AS user_searches,
	SUM(IFF(eoim.event_subcategory = 'SPV', 1, 0))                                      AS spvs,
	SUM(IFF(eoim.event_category = 'search event' AND eoim.triggered_by = 'user' AND next_se_sale_id_in_results, 1,
			0))                                                                         AS user_search_click_through_spv,
	IFF(user_searches > 0, TRUE, FALSE)                                                 AS session_has_user_search,
	IFF(spvs > 0, TRUE, FALSE)                                                          AS session_has_spv,
	IFF(user_search_click_through_spv > 0, TRUE, FALSE)                                 AS session_has_user_search_click_through_spv,
FROM events_of_interest_modelling eoim
GROUP BY 1, 2, 3, 4
;


SELECT
	sts.event_tstamp::DATE                                        AS date,
	COUNT(*)                                                      AS searches,
	SUM(IFF(sts.triggered_by = 'user', 1, 0))                     AS user_searches,
	SUM(IFF(sts.triggered_by = 'pageLoad', 1, 0))                 AS page_load_searches,
	AVG(ARRAY_SIZE(sts.search_context['results']))                AS avg_results_array_size,
	SUM(IFF(ARRAY_SIZE(sts.search_context['results']) = 0, 1, 0)) AS zero_result_array_size_searches
FROM se.data.scv_touched_searches sts
	INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp >= '2024-01-01'
  AND sts.se_brand = 'SE Brand'
GROUP BY 1
;


SELECT *
FROM se.data.scv_touched_searches sts
WHERE sts.event_tstamp >= CURRENT_DATE - 1
;


SELECT
	sb.booking_completed_date,
	SUM(sb.margin_gross_of_toms_gbp_constant_currency)
FROM se.data.se_booking sb
WHERE sb.booking_completed_date >= CURRENT_DATE - 30
  AND sb.booking_status = 'COMPLETE'
GROUP BY 1
;

