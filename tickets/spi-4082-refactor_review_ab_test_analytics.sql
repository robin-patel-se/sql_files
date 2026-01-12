-- https://docs.google.com/spreadsheets/d/1pdlGRsFG-6vKNp8PpGdkhe1x47xJGKzMfqiE_D2i4QA/edit?pli=1#gid=1156090138


WITH
	session_ab_group AS (
		-- identify review ab test, categorise sessions based on flags
		SELECT
			stff.touch_id,
			CASE
				WHEN SUM(IFF(stff.feature_flag = 'sale.reviews.enabled', 1, 0)) > 0
					AND SUM(IFF(stff.feature_flag = 'sale.reviews.control', 1, 0)) > 0 THEN 'both_review_groups'
				WHEN SUM(IFF(stff.feature_flag = 'sale.reviews.control', 1, 0)) > 0 THEN 'review_control_group'
				WHEN SUM(IFF(stff.feature_flag = 'sale.reviews.enabled', 1, 0)) > 0 THEN 'review_test_group'
			END AS touch_ab_group
		FROM se.data.scv_touched_feature_flags stff
		WHERE feature_flag IN ('sale.reviews.enabled', 'sale.reviews.control')
		GROUP BY 1
	),
	trust_you_sales AS (
		-- model trust you mapped sales based on when the mapping was in place
		SELECT
			ty.client_id AS hotel_code,
			'2023-07-12' AS start_date,
			'2023-07-21' AS end_date
		FROM collab.marketing.trustyou__hotel_mapping ty
		UNION
		SELECT
			ty.target_id AS hotel_code,
			'2023-07-22' AS start_date,
			CURRENT_DATE AS end_date
		FROM collab.marketing.trustyou__hotel_mapping_updated ty
	),
	mapped_spvs AS (
		-- categorise spvs based on their mapping to trust you, note join is dependent on when the sale was in mapping
		SELECT
			tys.hotel_code IS NOT NULL AS trust_you_mapped_sale,
			tys.*,
			sts.*,
			ssa.travel_type
		FROM se.data.scv_touched_spvs sts
			INNER JOIN se.data.se_sale_attributes ssa ON sts.se_sale_id = ssa.se_sale_id
			LEFT JOIN  trust_you_sales tys ON ssa.hotel_code = tys.hotel_code
			AND sts.event_tstamp::DATE BETWEEN tys.start_date AND tys.end_date
		WHERE sts.event_tstamp >= '2023-07-12'
	),
	agg_spvs_to_session AS (
		-- aggregate spvs up to session level based on their trust you mapping and categorise the spv activity on a session
		SELECT
			ms.touch_id,
			COUNT(DISTINCT ms.event_hash)                                                AS spvs,
			COUNT(DISTINCT IFF(ms.trust_you_mapped_sale, ms.event_tstamp, NULL))         AS trust_you_mapped_spvs,
			COUNT(DISTINCT IFF(ms.trust_you_mapped_sale = FALSE, ms.event_tstamp, NULL)) AS non_trust_you_mapped_spvs,
			CASE
				WHEN trust_you_mapped_spvs > 0 AND non_trust_you_mapped_spvs = 0 THEN 'trust_you_only_spvs'
				WHEN trust_you_mapped_spvs = 0 AND non_trust_you_mapped_spvs > 0 THEN 'non_trust_you_only_spvs'
				ELSE 'mixed'
			END                                                                          AS session_spv_type
		FROM mapped_spvs ms
		GROUP BY 1
	),
	mapped_bookings AS (
		-- categorise bookings based on their mapping to trust you, note join is dependent on when the sale was in mapping
		SELECT
			tys.hotel_code IS NOT NULL                    AS trust_you_mapped_sale,
			tys.*,
			stt.*,
			fb.margin_gross_of_toms_gbp_constant_currency AS margin_gbp
		FROM se.data.scv_touched_transactions stt
			INNER JOIN se.data.fact_booking fb ON stt.booking_id = fb.booking_id
			LEFT JOIN  se.data.se_sale_attributes ssa ON fb.se_sale_id = ssa.se_sale_id
			LEFT JOIN  trust_you_sales tys ON ssa.hotel_code = tys.hotel_code
			AND stt.event_tstamp::DATE BETWEEN tys.start_date AND tys.end_date
		WHERE stt.event_tstamp >= '2023-07-12'
	),
	agg_booking_metrics_to_session AS (
		SELECT
			mb.touch_id,
			COUNT(DISTINCT mb.booking_id)                                              AS bookings,
			COUNT(DISTINCT IFF(mb.trust_you_mapped_sale, mb.booking_id, NULL))         AS trust_you_mapped_bookings,
			COUNT(DISTINCT IFF(mb.trust_you_mapped_sale = FALSE, mb.booking_id, NULL)) AS non_trust_you_mapped_bookings,
			SUM(mb.margin_gbp)                                                         AS margin_gbp,
			SUM(DISTINCT IFF(mb.trust_you_mapped_sale, mb.margin_gbp, NULL))           AS trust_you_mapped_margin_gbp,
			SUM(DISTINCT
				IFF(mb.trust_you_mapped_sale = FALSE, mb.margin_gbp, NULL))            AS non_trust_you_mapped_margin_gbp
		FROM mapped_bookings mb
		GROUP BY 1
	),
	modelling AS (
		SELECT
			ab.touch_id,
			ab.touch_ab_group,
			ab.
				stba.touch_start_tstamp,
			stba.touch_end_tstamp,
			stba.touch_experience,
			stmc.touch_affiliate_territory,
			stba.attributed_user_id,
			COALESCE(aspv.spvs, 0)                          AS spvs,
			COALESCE(aspv.trust_you_mapped_spvs, 0)         AS trust_you_mapped_spvs,
			COALESCE(aspv.non_trust_you_mapped_spvs, 0)     AS non_trust_you_mapped_spvs,
			COALESCE(aspv.session_spv_type, 'no_spvs')      AS session_spv_type,
			COALESCE(bm.bookings, 0)                        AS bookings,
			COALESCE(bm.trust_you_mapped_bookings, 0)       AS trust_you_mapped_bookings,
			COALESCE(bm.non_trust_you_mapped_bookings, 0)   AS non_trust_you_mapped_bookings,
			COALESCE(bm.margin_gbp, 0)                      AS margin_gbp,
			COALESCE(bm.trust_you_mapped_margin_gbp, 0)     AS trust_you_mapped_margin_gbp,
			COALESCE(bm.non_trust_you_mapped_margin_gbp, 0) AS non_trust_you_mapped_margin_gbp

		FROM session_ab_group ab
			INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON ab.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON ab.touch_id = stmc.touch_id
			LEFT JOIN  agg_spvs_to_session aspv ON ab.touch_id = aspv.touch_id
			LEFT JOIN  agg_booking_metrics_to_session bm ON ab.touch_id = bm.touch_id
	)
SELECT
	m.touch_ab_group,
	m.touch_experience,
	COUNT(*)                         AS sessions,
	SUM(m.bookings)                  AS bookings,
	SUM(m.trust_you_mapped_spvs)     AS trust_you_mapped_spvs,
	SUM(m.non_trust_you_mapped_spvs) AS non_trust_you_mapped_spvs
FROM modelling m
WHERE m.touch_affiliate_territory = 'DE'
  AND m.touch_start_tstamp >= '2023-07-12'
GROUP BY 1, 2
;

SELECT *
FROM se.data_pii.scv_session_events_link ssel


------------------------------------------------------------------------------------------------------------------------
SELECT DISTINCT
	feature_flag
FROM se.data.scv_touched_feature_flags stff
;

SELECT
	stff.touch_id,
	stff.feature_flag,
	stff.touch_start_tstamp,
	stff.affiliate_type,
	stff.control_key,
	stff.description,
	stff.platform,
	stff.traffic_split,
	stff.type,
	stff.url_param,
	stff.ff_in_feature_toggle_table,
	stff.ff_is_control
FROM se.data.scv_touched_feature_flags stff
;

------------------------------------------------------------------------------------------------------------------------

WITH
	session_ab_group AS (
		-- identify review ab test, categorise sessions based on flags
		SELECT
			stff.touch_id,
			CASE
				WHEN SUM(IFF(stff.feature_flag = 'sale.reviews.enabled', 1, 0)) > 0
					AND SUM(IFF(stff.feature_flag = 'sale.reviews.control', 1, 0)) > 0 THEN 'both_review_groups'
				WHEN SUM(IFF(stff.feature_flag = 'sale.reviews.control', 1, 0)) > 0 THEN 'review_control_group'
				WHEN SUM(IFF(stff.feature_flag = 'sale.reviews.enabled', 1, 0)) > 0 THEN 'review_test_group'
			END AS touch_ab_group
		FROM se.data.scv_touched_feature_flags stff
		WHERE feature_flag IN ('sale.reviews.enabled', 'sale.reviews.control')
		GROUP BY 1
	),
	trust_you_sales AS (
		-- model trust you mapped sales based on when the mapping was in place
		SELECT
			ty.client_id AS hotel_code,
			'2023-07-12' AS start_date,
			'2023-07-21' AS end_date
		FROM collab.marketing.trustyou__hotel_mapping ty
		UNION
		SELECT
			ty.target_id AS hotel_code,
			'2023-07-22' AS start_date,
			CURRENT_DATE AS end_date
		FROM collab.marketing.trustyou__hotel_mapping_updated ty
	),
	mapped_spvs AS (
		-- categorise spvs based on their mapping to trust you, note join is dependent on when the sale was in mapping
		SELECT
			tys.hotel_code IS NOT NULL AS trust_you_mapped_sale,
			tys.*,
			sts.*
		FROM se.data.scv_touched_spvs sts
			INNER JOIN se.data.se_sale_attributes ssa ON sts.se_sale_id = ssa.se_sale_id
			LEFT JOIN  trust_you_sales tys ON ssa.hotel_code = tys.hotel_code
			AND sts.event_tstamp::DATE BETWEEN tys.start_date AND tys.end_date
		WHERE sts.event_tstamp >= '2023-07-12'
	),
	agg_spvs_to_session AS (
		-- aggregate spvs up to session level based on their trust you mapping and categorise the spv activity on a session
		SELECT
			ms.touch_id,
			COUNT(DISTINCT ms.event_hash)                                                AS spvs,
			COUNT(DISTINCT IFF(ms.trust_you_mapped_sale, ms.event_tstamp, NULL))         AS trust_you_mapped_spvs,
			COUNT(DISTINCT IFF(ms.trust_you_mapped_sale = FALSE, ms.event_tstamp, NULL)) AS non_trust_you_mapped_spvs,
			CASE
				WHEN trust_you_mapped_spvs > 0 AND non_trust_you_mapped_spvs = 0 THEN 'trust_you_only_spvs'
				WHEN trust_you_mapped_spvs = 0 AND non_trust_you_mapped_spvs > 0 THEN 'non_trust_you_only_spvs'
				ELSE 'mixed'
			END                                                                          AS session_spv_type
		FROM mapped_spvs ms
		GROUP BY 1
	),
	agg_booking_metrics_to_session AS (
		SELECT
			stt.touch_id,
			COUNT(DISTINCT fb.booking_id)                      AS bookings,
			SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.scv_touched_transactions stt
			INNER JOIN se.data.fact_booking fb ON stt.booking_id = fb.booking_id
		WHERE stt.event_tstamp >= '2023-07-12'
		GROUP BY 1
	)
SELECT
	ab.touch_id,
	ab.touch_ab_group,
	stba.touch_start_tstamp,
	stba.touch_end_tstamp,
	stba.touch_experience,
	stmc.touch_affiliate_territory,
	stba.attributed_user_id,
	COALESCE(aspv.spvs, 0)                      AS spvs,
	COALESCE(aspv.trust_you_mapped_spvs, 0)     AS trust_you_mapped_spvs,
	COALESCE(aspv.non_trust_you_mapped_spvs, 0) AS non_trust_you_mapped_spvs,
	COALESCE(aspv.session_spv_type, 'no_spvs')  AS session_spv_type,
	COALESCE(bm.bookings, 0)                    AS bookings,
	COALESCE(bm.margin_gbp, 0)                  AS margin_gbp
FROM session_ab_group ab
	INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON ab.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON ab.touch_id = stmc.touch_id
	LEFT JOIN  agg_spvs_to_session aspv ON ab.touch_id = aspv.touch_id
	LEFT JOIN  agg_booking_metrics_to_session bm ON ab.touch_id = bm.touch_id
;


SELECT
	stff.touch_id,
	CASE
		WHEN SUM(IFF(stff.feature_flag = 'sale.reviews.enabled', 1, 0)) > 0
			AND SUM(IFF(stff.feature_flag = 'sale.reviews.control', 1, 0)) > 0 THEN 'both_review_groups'
		WHEN SUM(IFF(stff.feature_flag = 'sale.reviews.control', 1, 0)) > 0 THEN 'review_control_group'
		WHEN SUM(IFF(stff.feature_flag = 'sale.reviews.enabled', 1, 0)) > 0 THEN 'review_test_group'
	END AS touch_ab_group,
	CASE
		WHEN SUM(IFF(stff.feature_flag = 'iapage.ab.test', 1, 0)) > 0
			AND SUM(IFF(stff.feature_flag = 'iapage.ab.test.control', 1, 0)) > 0 THEN 'both_ia_groups'
		WHEN SUM(IFF(stff.feature_flag = 'iapage.ab.test', 1, 0)) > 0 THEN 'ia_test_group'
		WHEN SUM(IFF(stff.feature_flag = 'iapage.ab.test.control', 1, 0)) > 0 THEN 'ia_control_group'
	END AS touch_ia_group
FROM dbt_dev.dbt_robinpatel_staging.base_scv__module_touched_feature_flags stff
WHERE feature_flag IN ('sale.reviews.enabled', 'sale.reviews.control', 'iapage.ab.test', 'iapage.ab.test.control')
GROUP BY 1
;


SELECT
	m.touch_ab_group,
	m.touch_experience,
	COUNT(*)                             AS sessions,
	SUM(m.bookings)                      AS bookings,
	SUM(m.trust_you_mapped_spvs)         AS trust_you_mapped_spvs,
	SUM(m.non_trust_you_mapped_spvs)     AS non_trust_you_mapped_spvs,
	SUM(m.trust_you_mapped_bookings)     AS trust_you_mapped_bookings,
	SUM(m.non_trust_you_mapped_bookings) AS non_trust_you_mapped_bookings
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_session_modelling m
WHERE m.touch_affiliate_territory = 'DE'
  AND m.touch_start_tstamp >= '2023-07-12'
GROUP BY 1, 2
;


SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_session_modelling m
;

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp::DATE = CURRENT_DATE
  AND ses.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
;


SELECT *
FROM se.data_pii.scv_page_screen_enrichment spse
;

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_reviews_ab_session_modelling
;

SELECT DISTINCT
	stba.stitched_identity_type
FROM se.data.scv_touch_basic_attributes stba
;


SELECT
	rab.touch_ab_group,
	rab.session_spv_type,
	rab.touch_ia_group,
	rab.touch_affiliate_territory,
	rab.touch_experience,
	rab.touch_start_tstamp::DATE             AS date,
	COUNT(DISTINCT rab.touch_id)             AS sessions,
	AVG(rab.session_duration)                AS avg_session_duration,
	SUM(rab.page_screen_views)               AS page_screen_views,
	SUM(IFF(rab.engaged_session, 1, 0))      AS engaged_session,
	SUM(rab.spvs)                            AS spvs,
	SUM(rab.trust_you_mapped_spvs)           AS trust_you_mapped_spvs,
	SUM(rab.non_trust_you_mapped_spvs)       AS non_trust_you_mapped_spvs,
	SUM(rab.bookings)                        AS bookings,
	SUM(rab.trust_you_mapped_bookings)       AS trust_you_mapped_bookings,
	SUM(rab.non_trust_you_mapped_bookings)   AS non_trust_you_mapped_bookings,
	SUM(rab.margin_gbp)                      AS margin_gbp,
	SUM(rab.trust_you_mapped_margin_gbp)     AS trust_you_mapped_margin_gbp,
	SUM(rab.non_trust_you_mapped_margin_gbp) AS non_trust_you_mapped_margin_gbp

FROM dbt.bi_data_platform.dp_reviews_ab_sessions rab
GROUP BY 1, 2, 3, 4, 5, 6


;

SELECT
	rab.touch_id,
	rab.touch_ab_group,
	rab.touch_ia_group,
	rab.touch_start_tstamp,
	rab.touch_end_tstamp,
	rab.session_duration,
	rab.touch_experience,
	rab.touch_affiliate_territory,
	rab.stitched_identity_type,
	rab.attributed_user_id,
	rab.page_screen_views,
	rab.engaged_session,
	rab.spvs,
	rab.trust_you_mapped_spvs,
	rab.non_trust_you_mapped_spvs,
	rab.session_spv_type,
	rab.bookings,
	rab.trust_you_mapped_bookings,
	rab.non_trust_you_mapped_bookings,
	rab.margin_gbp,
	rab.trust_you_mapped_margin_gbp,
	rab.non_trust_you_mapped_margin_gbp
FROM dbt.bi_data_platform.dp_reviews_ab_sessions rab
;


SELECT
	mr.ty_id,
	mr.summary,
	mr.summary[0]['score']::VARCHAR                                   AS score,
	mr.summary[0]['reviews_distribution'][0]['reviews_count']::NUMBER AS one_star_review,
	mr.summary[0]['reviews_distribution'][1]['reviews_count']::NUMBER AS two_star_review,
	mr.summary[0]['reviews_distribution'][2]['reviews_count']::NUMBER AS three_star_review,
	mr.summary[0]['reviews_distribution'][3]['reviews_count']::NUMBER AS four_star_review,
	mr.summary[0]['reviews_distribution'][4]['reviews_count']::NUMBER AS five_star_review,
	mr.version,
	mr.lang,
	mr.reviews_count,
	mr.summary,
	mr.relevant_now,
	mr.category_list,
	mr.hotel_type_list,
	mr.good_to_know_list,
	mr.trip_type_meta_review_list,
	mr.language_meta_review_list,
	mr.badge_list,
	mr.record
FROM latest_vault.trustyou.meta_review mr
;

SELECT
	mr.summary[0]['score']::NUMBER AS score,
	COUNT(*)
FROM latest_vault.trustyou.meta_review mr
GROUP BY 1
;

SELECT
	CASE
		WHEN mr.summary[0]['score']::NUMBER < 69 THEN '<69'
		WHEN mr.summary[0]['score']::NUMBER < 79 THEN '70-79'
		WHEN mr.summary[0]['score']::NUMBER < 89 THEN '80-89'
		WHEN mr.summary[0]['score']::NUMBER < 99 THEN '90-99'
		WHEN mr.summary[0]['score']::NUMBER = 100 THEN '100'
	END AS score_buckets,
	COUNT(*)
FROM latest_vault.trustyou.meta_review mr
GROUP BY 1
ORDER BY CASE
			 WHEN score_buckets = '<69' THEN 5
			 WHEN score_buckets = '70-79' THEN 4
			 WHEN score_buckets = '80-89' THEN 3
			 WHEN score_buckets = '90-99' THEN 2
			 WHEN score_buckets = '100' THEN 1
		 END
;


SELECT
	meta_review.ty_id,
	meta_review.old_ty_ids,
	meta_review.version,
	meta_review.lang,
	meta_review.reviews_count,
	meta_review.summary,
	meta_review.relevant_now,
	meta_review.category_list,
	meta_review.hotel_type_list,
	meta_review.good_to_know_list,
	meta_review.trip_type_meta_review_list,
	meta_review.language_meta_review_list,
	meta_review.badge_list,
	meta_review.record
FROM latest_vault.trustyou.meta_review
;


SELECT

	h.ty_id,
	h.giata_id,
	h.expedia_id,
	h.bookingcom_id,
	h.accommodation_type,
	h.hotel_name,
	h.street,
	h.zip,
	h.city,
	h.country_code,
	h.country,
	h.phone,
	h.lat_lng,
	h.record
FROM latest_vault.trustyou.hotels h
;

------------------------------------------------------------------------------------------------------------------------
-- https://www.secretescapes.com/playfully-hip-east-london-hotel-with-garden-bar-refundable-hotel-mama-shelter-london-shoreditch/sale-hotel?source=swp
-- A14786
SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.se_sale_id = 'A14786'

-- hotel code 0011r00002L8WCY


SELECT *
FROM collab.marketing.trustyou__hotel_mapping_updated
WHERE target_id = '0011r00002L8WCY'
;

SELECT *
FROM latest_vault.trustyou.meta_review mr
WHERE mr.ty_id = '17150e15-de9e-4856-b06f-38a3d2a0919a'
;

-- https://miro.com/app/board/uXjVMwMLIjw=/

SELECT
	mdh.service_type,
	mdh.usage_date,
	mdh.credits_used_compute,
	mdh.credits_used_cloud_services,
	mdh.credits_used, -- comparative to Snowflake
	mdh.credits_adjustment_cloud_services,
	mdh.credits_billed,
	mdh.credits_billed * 2.08                                                                       AS compute_cost,
	AVG(mdh.credits_billed) OVER (ORDER BY mdh.usage_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7_day
FROM snowflake.account_usage.metering_daily_history mdh
WHERE mdh.usage_date BETWEEN '2023-02-28' AND CURRENT_DATE - 1
ORDER BY mdh.usage_date
;


SELECT *
FROM dbt.bi_data_platform.dp_reviews_ab_sessions dras
;

SELECT
	m.touch_id,
	m.touch_ab_group,
	m.touch_ia_group,
	m.touch_start_tstamp,
	m.touch_end_tstamp,
	m.session_duration,
	m.touch_experience,
	m.touch_affiliate_territory,
	m.stitched_identity_type,
	m.attributed_user_id,
	m.page_screen_views,
	m.engaged_session,
	m.spvs,
	m.trust_you_mapped_spvs,
	m.non_trust_you_mapped_spvs,
	m.session_spv_type,
	m.domestic_spvs,
	m.international_spvs,
	m.travel_spv_type,
	m.average_review_score_spvs,
	m.avg_review_score_rating,
	m.poor_review_score_spvs,
	m.fair_review_score_spvs,
	m.good_review_score_spvs,
	m.very_good_review_score_spvs,
	m.excellent_review_score_spvs,
	m.bookings,
	m.trust_you_mapped_bookings,
	m.non_trust_you_mapped_bookings,
	m.domestic_bookings,
	m.international_bookings,
	m.travel_booking_type,
	m.average_review_score_bookings,
	m.poor_review_score_bookings,
	m.fair_review_score_bookings,
	m.good_review_score_bookings,
	m.very_good_review_score_bookings,
	m.excellent_review_score_bookings,
	m.margin_gbp,
	m.trust_you_mapped_margin_gbp,
	m.non_trust_you_mapped_margin_gbp
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_session_modelling m
;


SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_session_modelling
;


SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_05_booking_modelling
;

SELECT
	tys.hotel_code IS NOT NULL                    AS trust_you_mapped_sale,
	tys.*,
	stt.*,
	fb.se_sale_id,
	fb.margin_gross_of_toms_gbp_constant_currency AS margin_gbp,
	ssa.travel_type,
	mr.score                                      AS review_score
FROM se.data.scv_touched_transactions stt
	INNER JOIN se.data.fact_booking fb ON stt.booking_id = fb.booking_id
	LEFT JOIN  se.data.se_sale_attributes ssa ON fb.se_sale_id = ssa.se_sale_id
	LEFT JOIN  dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales tys
			   ON ssa.se_sale_id = tys.se_sale_id
				   AND stt.event_tstamp::DATE BETWEEN tys.start_date AND tys.end_date
	LEFT JOIN  dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling mr
			   ON fb.se_sale_id = mr.se_sale_id
WHERE stt.event_tstamp >= '2023-07-12'
  AND stt.booking_id = 'A14920989'
;



SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales
WHERE dp_reviews_02_trust_you_mapped_sales.se_sale_id = 'A53616'
;


SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_session_modelling
;


SELECT *
FROM data_vault_mvp.dwh.se_sale_tags_snapshot ssts
;


SELECT
	rab.touch_ab_group,
	rab.session_spv_type,
	rab.travel_spv_type,
	rab.avg_spvs_review_score_rating,
	rab.avg_booking_review_score_rating,
	rab.touch_ia_group,
	rab.touch_affiliate_territory,
	rab.touch_experience,
	rab.touch_start_tstamp::DATE             AS date,
	COUNT(DISTINCT rab.touch_id)             AS sessions,
	AVG(rab.session_duration)                AS avg_session_duration,
	SUM(rab.page_screen_views)               AS page_screen_views,
	SUM(IFF(rab.engaged_session, 1, 0))      AS engaged_sessions,
	SUM(rab.spvs)                            AS spvs,
	SUM(rab.trust_you_mapped_spvs)           AS trust_you_mapped_spvs,
	SUM(rab.non_trust_you_mapped_spvs)       AS non_trust_you_mapped_spvs,
	SUM(rab.domestic_spvs)                   AS domestic_spvs,
	SUM(rab.international_spvs)              AS international_spvs,
	SUM(rab.poor_review_score_spvs)          AS poor_review_score_spvs,
	SUM(rab.fair_review_score_spvs)          AS fair_review_score_spvs,
	SUM(rab.good_review_score_spvs)          AS good_review_score_spvs,
	SUM(rab.very_good_review_score_spvs)     AS very_good_review_score_spvs,
	SUM(rab.excellent_review_score_spvs)     AS excellent_review_score_spvs,
	SUM(rab.bookings)                        AS bookings,
	SUM(rab.trust_you_mapped_bookings)       AS trust_you_mapped_bookings,
	SUM(rab.non_trust_you_mapped_bookings)   AS non_trust_you_mapped_bookings,
	SUM(rab.poor_review_score_bookings)      AS poor_review_score_bookings,
	SUM(rab.fair_review_score_bookings)      AS fair_review_score_bookings,
	SUM(rab.good_review_score_bookings)      AS good_review_score_bookings,
	SUM(rab.very_good_review_score_bookings) AS very_good_review_score_bookings,
	SUM(rab.excellent_review_score_bookings) AS excellent_review_score_bookings,
	SUM(rab.margin_gbp)                      AS margin_gbp,
	SUM(rab.trust_you_mapped_margin_gbp)     AS trust_you_mapped_margin_gbp,
	SUM(rab.non_trust_you_mapped_margin_gbp) AS non_trust_you_mapped_margin_gbp,
	AVG(rab.avg_spvs_review_score)           AS avg_spvs_review_score,
	AVG(rab.avg_booking_review_score)        AS avg_booking_review_score

FROM dbt.bi_data_platform.dp_reviews_ab_sessions rab
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
;


SELECT
	rab.touch_id,
	rab.touch_ab_group,
	rab.touch_ia_group,
	rab.touch_start_tstamp,
	rab.touch_end_tstamp,
	rab.session_duration,
	rab.touch_experience,
	rab.touch_affiliate_territory,
	rab.stitched_identity_type,
	rab.attributed_user_id,
	rab.page_screen_views,
	rab.engaged_session,
	rab.spvs,
	rab.trust_you_mapped_spvs,
	rab.non_trust_you_mapped_spvs,
	rab.session_spv_type,
	rab.domestic_spvs,
	rab.international_spvs,
	rab.travel_spv_type,
	rab.avg_spvs_review_score,
	rab.avg_spvs_review_score_rating,
	rab.poor_review_score_spvs,
	rab.fair_review_score_spvs,
	rab.good_review_score_spvs,
	rab.very_good_review_score_spvs,
	rab.excellent_review_score_spvs,
	rab.bookings,
	rab.trust_you_mapped_bookings,
	rab.non_trust_you_mapped_bookings,
	rab.domestic_bookings,
	rab.international_bookings,
	rab.travel_booking_type,
	rab.avg_booking_review_score,
	rab.avg_booking_review_score_rating,
	rab.poor_review_score_bookings,
	rab.fair_review_score_bookings,
	rab.good_review_score_bookings,
	rab.very_good_review_score_bookings,
	rab.excellent_review_score_bookings,
	rab.margin_gbp,
	rab.trust_you_mapped_margin_gbp,
	rab.non_trust_you_mapped_margin_gbp
FROM dbt.bi_data_platform.dp_reviews_ab_sessions rab
;

WITH
	unpack_good_to_know_list AS (
		SELECT
			mr.se_sale_id,
			mr.good_to_know_list,
			gtkl.index,
			gtkl.value['short_text']::VARCHAR AS sentiment,
			gtkl.value['sentiment']::VARCHAR  AS sentiment_inclination
		FROM dbt.bi_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling mr,
			 LATERAL FLATTEN(INPUT => mr.good_to_know_list, OUTER => TRUE) gtkl
		WHERE mr.se_sale_id = 'A19025' -- todo remove
	)
SELECT
	u.se_sale_id,
	LISTAGG('- ' || u.sentiment || ' (' || u.sentiment_inclination || ')', '\n')
			WITHIN GROUP ( ORDER BY u.index)               AS good_to_know_list,
	COUNT(IFF(u.sentiment_inclination = 'pos', 1, NULL))   AS good_to_know_pos_sentiments,
	COUNT(IFF(u.sentiment_inclination = 'neu', 1, NULL))   AS good_to_know_neutral_sentiments,
	COUNT(IFF(u.sentiment_inclination = 'mixed', 1, NULL)) AS good_to_know_mixed_sentiments,
	COUNT(IFF(u.sentiment_inclination = 'neg', 1, NULL))   AS good_to_know_neg_sentiments
FROM unpack_good_to_know_list u
GROUP BY 1
;


WITH
	distinct_ty_ids AS (
		SELECT DISTINCT
			trust_you_id,
			se_sale_id
		FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales
	),
	structure_reviews AS (

		SELECT
			dti.se_sale_id,
			mr.summary[0]['score']::VARCHAR                                   AS score,
			mr.summary[0]['reviews_distribution'][0]['reviews_count']::NUMBER AS one_star_review,
			mr.summary[0]['reviews_distribution'][1]['reviews_count']::NUMBER AS two_star_review,
			mr.summary[0]['reviews_distribution'][2]['reviews_count']::NUMBER AS three_star_review,
			mr.summary[0]['reviews_distribution'][3]['reviews_count']::NUMBER AS four_star_review,
			mr.summary[0]['reviews_distribution'][4]['reviews_count']::NUMBER AS five_star_review,
			mr.version,
			mr.lang,
			mr.reviews_count,
			mr.summary,
			mr.relevant_now,
			mr.category_list,
			mr.hotel_type_list,
			mr.good_to_know_list,
			mr.trip_type_meta_review_list,
			mr.language_meta_review_list,
			mr.badge_list,
			mr.record
		FROM dbt_dev.dbt_robinpatel_staging.base_trust_you__meta_review mr
			INNER JOIN distinct_ty_ids dti ON mr.ty_id = dti.trust_you_id
	),
	unpack_good_to_know_list AS (
		SELECT
			mr.se_sale_id,
			mr.good_to_know_list,
			gtkl.index,
			gtkl.value['short_text']::VARCHAR AS good_to_know_list_short
		FROM dbt.bi_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling mr,
			 LATERAL FLATTEN(INPUT => mr.good_to_know_list, OUTER => TRUE) gtkl
	),
	agg_good_to_know_list AS (
		SELECT
			u.se_sale_id,
			LISTAGG('- ' || u.good_to_know_list_short, '\n') WITHIN GROUP ( ORDER BY u.index) AS good_to_know_list
		FROM unpack_good_to_know_list u
		GROUP BY 1
	)
SELECT *
FROM structure_reviews sr
	LEFT JOIN agg_good_to_know_list al ON sr.se_sale_id = al.se_sale_id
WHERE al.se_sale_id = 'A19025'



SELECT
	mr.category_list,
	cl.value['category_name']::VARCHAR AS category,
	cl.value['score']::VARCHAR         AS score,
	cl.value['count']::VARCHAR         AS reviews,
	cl.*
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling mr,
	 LATERAL FLATTEN(INPUT => mr.category_list, OUTER => TRUE) cl
WHERE mr.se_sale_id = 'A19025'
;


SELECT
	mr.se_sale_id,
	cl.index                           AS category_index,
	cl.value['category_name']::VARCHAR AS category_name,
	ssl.index                          AS sentiment_index,
	ssl.value['sentiment']::VARCHAR    AS sentiment_inclination,
	ssl.value['text']::VARCHAR         AS sentiment_text
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling mr,
	 LATERAL FLATTEN(INPUT => mr.category_list, OUTER => TRUE) cl,
	 LATERAL FLATTEN(INPUT => cl.value['summary_sentence_list'], OUTER => TRUE) ssl
WHERE mr.se_sale_id = 'A19025'
;

WITH
	structure_categories AS (
		SELECT
			mr.se_sale_id,
			cl.index                           AS category_index,
			cl.value['category_name']::VARCHAR AS category_name,
			cl.value['score']::VARCHAR         AS score,
			cl.value['count']::VARCHAR         AS reviews,
			ssl.index                          AS sentiment_index,
			ssl.value['sentiment']::VARCHAR    AS sentiment_inclination,
			ssl.value['text']::VARCHAR         AS sentiment_text
		FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling mr,
			 LATERAL FLATTEN(INPUT => mr.category_list, OUTER => TRUE) cl,
			 LATERAL FLATTEN(INPUT => cl.value['summary_sentence_list'], OUTER => TRUE) ssl
		WHERE mr.se_sale_id = 'A14786' -- todo remove
	),
	agg_sentiments AS (
		SELECT
			ss.se_sale_id,
			COUNT(*)                                                               AS sentiments,
			LISTAGG(ss.category_name || ' (' || ss.sentiment_inclination || ') - ' || ss.sentiment_text, '\n')
					WITHIN GROUP ( ORDER BY ss.category_index, ss.sentiment_index) AS sentiment_list,
			COUNT(IFF(ss.sentiment_inclination = 'pos', 1, NULL))                  AS pos_sentiments,
			COUNT(IFF(ss.sentiment_inclination = 'neu', 1, NULL))                  AS neutral_sentiments,
			COUNT(IFF(ss.sentiment_inclination = 'mixed', 1, NULL))                AS mixed_sentiments,
			COUNT(IFF(ss.sentiment_inclination = 'neg', 1, NULL))                  AS neg_sentiments
		FROM structure_categories ss
		GROUP BY 1
	),
	distinct_categories AS (
		SELECT DISTINCT
			sc.se_sale_id,
			sc.category_index,
			sc.category_name,
			sc.score,
			sc.reviews
		FROM structure_categories sc
	),
	agg_categories AS (
		SELECT
			dc.se_sale_id,
			LISTAGG(dc.category_name || ' (' || dc.score || ') - ' || dc.reviews || ' reviews', '\n')
					WITHIN GROUP ( ORDER BY dc.category_index) AS category_list
		FROM distinct_categories dc
		GROUP BY 1
	)
SELECT
	ac.se_sale_id,
	ac.category_list,
	ags.se_sale_id,
	ags.sentiments,
	ags.sentiment_list,
	ags.pos_sentiments,
	ags.neutral_sentiments,
	ags.mixed_sentiments,
	ags.neg_sentiments
FROM agg_categories ac
	LEFT JOIN agg_sentiments ags ON ac.se_sale_id = ags.se_sale_id
;


-- https://www.secretescapes.com/playfully-hip-east-london-hotel-with-garden-bar-refundable-hotel-mama-shelter-london-shoreditch/sale-hotel -- A14786
-- https://www.secretescapes.com/elegant-country-retreat-near-historic-stratford-upon-avon-fully-refundable-the-charlecote-pheasant-hotel-warwickshire/sale-hotel -- A19025


SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_reviews_03_meta_review_score_modelling mr
WHERE mr.se_sale_id = 'A19025'
;

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_reviews_sale_meta_reviews
;

USE ROLE pipelinerunner
;

GRANT SELECT ON TABLE dbt.bi_data_platform.dp_reviews_sale_meta_reviews TO ROLE se_basic
;

SELECT *
FROM dbt.bi_data_platform.dp_reviews_sale_meta_reviews drsmr
;


lies
LAST reply today AT 4:28 PMView thread


Robin
  4:54 PM
LAST MINUTE vs SE (edited)
2 FILES

image.png


image.png




Robin
  5:02 PM
This IS the sexy dataset:

SELECT *
FROM dbt.bi_data_platform.dp_reviews_sale_meta_reviews drsmr
;

:gianni-strawb:
1



Geri Pancheva
:afp:  5:07 PM
https://it.secretescapes.com/the-welcombe-hotel-bw-premier-collection-100-rimborsabile-stratford-upon-avon-regno-unito/sale-hotel
it.secretescapes.comit.secretescapes.com
The Welcombe Hotel
– BW Premier Collection
Ripercorri la vita di Shakespeare soggiornando IN un edificio IN stile giacobino a breve distanza dalla sua citt
à natale Stratford-upon-Avon (31 kB)
https://it.secretescapes.com/the-welcombe-hotel-bw-premier-collection-100-rimborsabile-stratford-upon-avon-regno-unito/sale-hotel












Message reviews









Shift + Return TO ADD a new line
Thread
reviews




Alex Henshaw
  1 HOUR ago

CREATE OR REPLACE TABLE customer_insight.sandbox.ah_reviews_ab_test_deal_level AS
WITH
	sales_80 AS
		(
			SELECT
				ssa.se_sale_id
					,
				hm.client_id
					,
				hm.trust_score
			FROM se.data.se_sale_attributes ssa -- this is distinct on se_sale_id, not distinct on hotel_code
				INNER JOIN collab.marketing.trustyou__hotel_mapping hm
						   ON ssa.hotel_code = hm.client_id
		)
-- distinct on se_sale_id; clean
-- won't be distinct on client_id because this is equivalent to hotel_code, and hotels can have multiple sale_ids.
--2: Identify sessions that have had at least 1 spv
		,
	spvs_level AS
		(
			SELECT
				spvs.event_hash
					,
				spvs.se_sale_id
					,
				spvs.touch_id
					,
				trust_score
					,
				spvs.event_tstamp
					,
				DATE(spvs.event_tstamp)                               AS spv_date
					,
				CASE WHEN s8.se_sale_id IS NOT NULL THEN 1 ELSE 0 END AS is_trustyou_mapped
			FROM se.data.scv_touched_spvs spvs
				LEFT JOIN sales_80 s8
						  ON spvs.se_sale_id = s8.se_sale_id
			WHERE spvs.event_tstamp::date > '2023-07-12'
		)
-- distinct on event_hash; clean
--3: Identify bookings
		,
	transactions_cte AS
		(
			SELECT
				stt.booking_id
					,
				stt.touch_id
					,
				fcb.se_sale_id
					,
				fcb.booking_status_type, -- added as a way to check; 100% of rows should say live
				fcb.booking_completed_date
					,
				margin_gross_of_toms_gbp_constant_currency
			FROM se.data.scv_touched_transactions stt -- stt contains live bookings as well as other booking types
				INNER JOIN se.data.fact_booking fcb -- inner joining means we will only return live bookings
						   ON stt.booking_id = fcb.booking_id
				LEFT JOIN  se.data.dim_sale ds
						   ON fcb.se_sale_id = ds.se_sale_id
			WHERE fcb.booking_status_type IN ('live', 'cancelled') -- include cancelled bookings because people still booked these; use cancelled bookings as well as live unless we think the change will have an impact on canx rates
			  AND fcb.booking_completed_date > '2023-07-12'
		)
-- distinct on booking_id; clean
--4: Identify Feature Flags
		,
	feature_flag_selector AS (
		SELECT
			touch_id
				,
			feature_flag
				,
			COUNT(touch_id) OVER (PARTITION BY touch_id) AS rows_
		FROM se.data.scv_touched_feature_flags stff
		WHERE DATE(touch_start_tstamp) >= '2023-07-12' -- go live date
		  AND feature_flag IN ('sale.reviews.enabled', 'sale.reviews.control')
	)
-- identifies duplicates in feature_flags_selector
		,
	duplicate_identifier AS (
		SELECT DISTINCT
			touch_id
		FROM feature_flag_selector
		WHERE rows_ > 1
	)
		,
	spvs_agg AS
		(
			SELECT
				sa.se_sale_id
					,
				ffs.feature_flag
					,
				spv_date          AS date_
					,
				trust_score
					,
				CASE
					WHEN trust_score >= 87 THEN 'excellent'
					WHEN trust_score BETWEEN 81 AND 87 THEN 'very good'
					WHEN trust_score BETWEEN 75 AND 81 THEN 'good'
					WHEN trust_score BETWEEN 70 AND 75 THEN 'fair'
					WHEN trust_score < 70 THEN 'poor'
					WHEN is_trustyou_mapped = 0 THEN 'not mapped'
				END               AS trust_score_banded
					,
				COUNT(event_hash) AS spvs
			FROM spvs_level sa
				LEFT JOIN duplicate_identifier di
						  USING (touch_id)
				LEFT JOIN feature_flag_selector ffs
						  USING (touch_id)
			WHERE di.touch_id IS NULL -- exclude dupes
			GROUP BY 1, 2, 3, 4, 5
		)
		,
	bookings_agg AS
		(
			SELECT
				tc.se_sale_id
					,
				feature_flag
					,
				booking_completed_date                          AS date_
					,
				COUNT(booking_id)                               AS bookings
					,
				SUM(margin_gross_of_toms_gbp_constant_currency) AS margin
			FROM transactions_cte tc
				LEFT JOIN duplicate_identifier di
						  USING (touch_id)
				LEFT JOIN feature_flag_selector ffs
						  USING (touch_id)
			WHERE di.touch_id IS NULL -- exclude dupes
			GROUP BY 1, 2, 3
		)
SELECT
	sa.*
		,
	ba.bookings
		,
	ba.margin
FROM spvs_agg sa
	LEFT JOIN bookings_agg ba
			  USING (se_sale_id, feature_flag, date_)

