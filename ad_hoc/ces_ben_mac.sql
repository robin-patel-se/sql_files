WITH
	customer_score_avg AS (
		SELECT
			csaesr.shiro_user_id,
			COUNT(*)                  AS ces_reviews,
			ROUND(AVG(csaesr.answer)) AS avg_customer_score
		FROM se.data_pii.customer_satisfaction_and_effort_survey_responses csaesr
		WHERE csaesr.response_type IN ('CES')
		GROUP BY 1
	),
	user_lifetime_margin AS (

		SELECT
			fcb.shiro_user_id,
			COUNT(DISTINCT fcb.booking_id)                      AS bookings,
			SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.fact_complete_booking fcb
		GROUP BY 1
	),
	user_level_modelling AS (
		SELECT
			csa.shiro_user_id,
			se.data.member_recency_status(sua.signup_tstamp, CURRENT_DATE) AS member_recency,
			csa.avg_customer_score,
			csa.ces_reviews,
			ulm.bookings,
			ulm.margin_gbp
		FROM customer_score_avg csa
			LEFT JOIN se.data.se_user_attributes sua ON csa.shiro_user_id = sua.shiro_user_id
			LEFT JOIN user_lifetime_margin ulm ON csa.shiro_user_id = ulm.shiro_user_id
	)
SELECT
	ulm.avg_customer_score,
-- 	ulm.member_recency,
	COUNT(DISTINCT ulm.shiro_user_id) AS users,
	SUM(ulm.ces_reviews)              AS total_reviews,
	SUM(ulm.bookings)                 AS total_bookings,
	users / total_bookings            AS average_user_bookings,
	SUM(ulm.margin_gbp)               AS total_margin_gbp,
	total_margin_gbp / total_bookings AS average_booking_value,
	SUM(ulm.bookings) / SUM(total_bookings) OVER ()
FROM user_level_modelling ulm
GROUP BY 1
;


/*

SELECT
	csaesr.shiro_user_id,
	csaesr.completed_date,
	csaesr.answer
FROM se.data_pii.customer_satisfaction_and_effort_survey_responses csaesr
WHERE csaesr.response_type IN ('CES')


SELECT
	SUM(fcb.margin_gross_of_toms_gbp_constant_currency)
FROM se.data.fact_complete_booking fcb
WHERE fcb.shiro_user_id = 20476436

SELECT
	s.id,
	s.archived,
	s.created_at_timestamp,
	s.survey_name,
	s.survey_type,
	s.record['survey_folder_name']::VARCHAR AS folder_name,
	s.record
FROM latest_vault.survey_sparrow.surveys s
;


SELECT
	srb.row_loaded_at::DATE,
	COUNT(*),
	COUNT(srb.shiro_user_id)
FROM latest_vault.hotjar.survey_responses_browse srb
GROUP BY 1*/


------------------------------------------------------------------------------------------------------------------------

SELECT
	srb.row_extract_metadata['remote_filename']::VARCHAR,
	COUNT(*)
FROM latest_vault.hotjar.survey_responses_browse srb
WHERE row_loaded_at::DATE = '2023-11-17'
GROUP BY 1
;

SELECT *
FROM latest_vault.hotjar.survey_responses_browse srb
;

SELECT *
FROM latest_vault.hotjar.survey_responses_post_book srb
;

SELECT
	srb.row_dataset_name,
	COUNT(*)
FROM latest_vault.hotjar.survey_responses_browse srb
GROUP BY 1
;



SELECT
	srb.row_loaded_at::DATE,
	COUNT(*)
FROM latest_vault.hotjar.survey_responses_post_book srb
GROUP BY 1
;


WITH
	stack AS (
		SELECT
			SPLIT_PART(srb.row_extract_metadata['remote_filename']::VARCHAR, '__', -2) AS territory,
			srb.response_number,
			srb.user,
			srb.date_submitted,
			srb.country,
			srb.source_url,
			srb.device,
			srb.browser,
			srb.os,
			srb.hotjar_user_id,
			srb.ces_response,
			srb.main_reason_for_ces_score,
			srb.shiro_user_id,
			'browse'                                                                   AS hotjar_source
		FROM latest_vault.hotjar.survey_responses_browse srb
		-- full dump of reviews by ben mac on this date
		WHERE srb.row_loaded_at::DATE >= '2023-11-17'
		UNION ALL
		SELECT
			SPLIT_PART(srb.row_extract_metadata['remote_filename']::VARCHAR, '__', -2) AS territory,
			srb.response_number,
			srb.user,
			srb.date_submitted,
			srb.country,
			srb.source_url,
			srb.device,
			srb.browser,
			srb.os,
			srb.hotjar_user_id,
			srb.ces_response,
			srb.main_reason_for_ces_score,
			srb.shiro_user_id,
			'post_book'                                                                AS hotjar_source
		FROM latest_vault.hotjar.survey_responses_post_book srb
		-- full dump of reviews by ben mac on this date
		WHERE srb.row_loaded_at::DATE >= '2023-11-17'
	)
SELECT
	s.hotjar_source,
	COUNT(*)                                    AS total_reviews,
	SUM(IFF(s.shiro_user_id IS NOT NULL, 1, 0)) AS has_shiro_user_id,
	has_shiro_user_id / total_reviews           AS perc_reviews_with_user_id

FROM stack s
GROUP BY 1
;

-- https://secretescapes.atlassian.net/wiki/spaces/DW/pages/2292908033/User+Review+calculations#Calculation%3A


WITH
	stack_hotjar_reviews AS (
		SELECT
			SPLIT_PART(srb.row_extract_metadata['remote_filename']::VARCHAR, '__', -2) AS territory,
			srb.response_number,
			srb.user,
			srb.date_submitted,
			srb.country,
			srb.source_url,
			srb.device,
			srb.browser,
			srb.os,
			srb.hotjar_user_id,
			srb.ces_response,
			srb.main_reason_for_ces_score,
			srb.shiro_user_id,
			'browse'                                                                   AS hotjar_source
		FROM latest_vault.hotjar.survey_responses_browse srb
		WHERE srb.row_loaded_at::DATE >= '2023-11-17' -- full dump export added by Ben
		UNION ALL
		SELECT
			SPLIT_PART(srb.row_extract_metadata['remote_filename']::VARCHAR, '__', -2) AS territory,
			srb.response_number,
			srb.user,
			srb.date_submitted,
			srb.country,
			srb.source_url,
			srb.device,
			srb.browser,
			srb.os,
			srb.hotjar_user_id,
			srb.ces_response,
			srb.main_reason_for_ces_score,
			srb.shiro_user_id,
			'post_book'                                                                AS hotjar_source
		FROM latest_vault.hotjar.survey_responses_post_book srb
		WHERE srb.row_loaded_at::DATE >= '2023-11-17' -- full dump export added by Ben
	),

	customer_score_avg AS (
		SELECT
			shr.shiro_user_id,
			COUNT(*)                     AS ces_reviews,
			ROUND(AVG(shr.ces_response)) AS avg_customer_score
		FROM stack_hotjar_reviews shr
		WHERE shr.shiro_user_id IS NOT NULL
		GROUP BY 1
	),
	user_lifetime_margin AS (
		SELECT
			fcb.shiro_user_id,
			COUNT(DISTINCT fcb.booking_id)                      AS bookings,
			SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.fact_complete_booking fcb
		GROUP BY 1
	),
	user_level_modelling AS (
		SELECT
			csa.shiro_user_id,
			se.data.member_recency_status(sua.signup_tstamp, CURRENT_DATE) AS member_recency,
			csa.avg_customer_score,
			csa.ces_reviews,
			ulm.bookings,
			ulm.margin_gbp
		FROM customer_score_avg csa
			LEFT JOIN se.data.se_user_attributes sua ON csa.shiro_user_id = sua.shiro_user_id
			LEFT JOIN user_lifetime_margin ulm ON csa.shiro_user_id = ulm.shiro_user_id
	)
SELECT
	ulm.avg_customer_score,
-- 	ulm.member_recency,
	COUNT(DISTINCT ulm.shiro_user_id) AS users,
	SUM(ulm.ces_reviews)              AS total_reviews,
	SUM(ulm.bookings)                 AS total_bookings,
	users / total_bookings            AS average_user_bookings,
	SUM(ulm.margin_gbp)               AS total_margin_gbp,
	total_margin_gbp / total_bookings AS average_booking_value,
	SUM(ulm.bookings) / SUM(total_bookings) OVER ()
FROM user_level_modelling ulm
GROUP BY 1
;

SELECT *
FROM raw_vault.hotjar.survey_responses_post_book
WHERE loaded_at::DATE = '2023-11-17'
;


WITH
	stack_hotjar_reviews AS (
		SELECT
			SPLIT_PART(srb.row_extract_metadata['remote_filename']::VARCHAR, '__', -2) AS territory,
			srb.response_number,
			srb.user,
			srb.date_submitted,
			srb.country,
			srb.source_url,
			srb.device,
			srb.browser,
			srb.os,
			srb.hotjar_user_id,
			srb.ces_response,
			srb.main_reason_for_ces_score,
			srb.shiro_user_id,
			srb.ces_response IN ('5', '6', '7')                                        AS agree_response,
			'browse'                                                                   AS hotjar_source
		FROM latest_vault.hotjar.survey_responses_browse srb
		WHERE srb.row_loaded_at::DATE >= '2023-11-17' -- full dump export added by Ben M
		UNION ALL
		SELECT
			SPLIT_PART(srb.row_extract_metadata['remote_filename']::VARCHAR, '__', -2) AS territory,
			srb.response_number,
			srb.user,
			srb.date_submitted,
			srb.country,
			srb.source_url,
			srb.device,
			srb.browser,
			srb.os,
			srb.hotjar_user_id,
			srb.ces_response,
			srb.main_reason_for_ces_score,
			srb.shiro_user_id,
			srb.ces_response IN ('5', '6', '7')                                        AS agree_response,
			'post_book'                                                                AS hotjar_source
		FROM latest_vault.hotjar.survey_responses_post_book srb
		WHERE srb.row_loaded_at::DATE >= '2023-11-17' -- full dump export added by Ben M
	)

SELECT
	stack_hotjar_reviews.date_submitted,
	SUM(IFF(agree_response = TRUE, 1, 0))       AS num_agree_responses,
	COUNT(*)                                    AS total_responses,
	num_agree_responses / total_responses * 100 AS ces_score
FROM stack_hotjar_reviews
GROUP BY 1