USE WAREHOUSE pipe_xlarge
;

WITH
	input_sessions AS (
		SELECT
			psm.feature_flag_test_array,
			test_feature_flags.value['feature_flag']::VARCHAR  AS feature_flag,
			test_feature_flags.value['min_tstamp']::TIMESTAMP  AS feature_flag_min_tstamp,
			test_feature_flags.value['num_occurences']::NUMBER AS num_occurences,
			test_feature_flags.value['test_group']::VARCHAR    AS test_group,
			COUNT(DISTINCT test_feature_flags.value['test_group']::VARCHAR)
				  OVER (PARTITION BY psm.touch_id)             AS num_of_test_groups_assigned,
			psm.*
		FROM dbt.bi_product_analytics__intermediate.pda_session_metrics psm,
			 LATERAL FLATTEN(INPUT => psm.feature_flag_test_array, OUTER => TRUE) test_feature_flags
		WHERE psm.touch_start_tstamp::DATE BETWEEN '2024-07-29' AND CURRENT_DATE - 1
		  AND LOWER(psm.feature_flag_test_array::VARCHAR) LIKE '%abtest.opensite%'
		  AND LOWER(test_feature_flags.value['feature_flag']::VARCHAR) LIKE 'abtest.opensite%'
		QUALIFY ROW_NUMBER() OVER (PARTITION BY psm.touch_id ORDER BY feature_flag_min_tstamp, test_group) = 1
	)
		,
	user_first_login_type AS (
		SELECT DISTINCT
			ins.attributed_user_id,
			FIRST_VALUE(ins.first_login_type)
						OVER (PARTITION BY ins.attributed_user_id ORDER BY ins.touch_start_tstamp ASC) AS user_first_login_type,
			FIRST_VALUE(ins.test_group)
						OVER (PARTITION BY ins.attributed_user_id ORDER BY ins.touch_start_tstamp ASC) AS user_first_test_group
		FROM input_sessions ins
	),
	user_modelling AS (
		SELECT
			ins.attributed_user_id,
			IFF(ins.stitched_identity_type = 'se_user_id', 'se user', 'other')              AS user_state,
			sua.email_opt_in_status,
			uflt.user_first_login_type,
			uflt.user_first_test_group,
			sua.signup_tstamp,
			CASE
				WHEN sua.signup_tstamp BETWEEN '2024-07-29' AND CURRENT_DATE - 1 THEN 'new sign up'
				WHEN sua.signup_tstamp IS NOT NULL THEN 'existing user'
				WHEN sua.signup_tstamp IS NULL THEN 'non user'
			END                                                                             AS sign_up_state,
			LISTAGG(DISTINCT ins.test_group, ', ') WITHIN GROUP ( ORDER BY ins.test_group ) AS test_group,
			COUNT(DISTINCT ins.test_group)                                                  AS number_of_test_groups,
			COUNT(DISTINCT ins.touch_id)                                                    AS sessions,
			COUNT(DISTINCT IFF(ins.spvs > 0, ins.touch_id, NULL))                           AS spv_sessions,
			COUNT(DISTINCT IFF(ins.booking_form_views > 0, ins.touch_id, NULL))             AS bfv_sessions,
			COUNT(DISTINCT IFF(ins.bookings > 0, ins.touch_id, NULL))                       AS booking_sessions,
			COUNT(DISTINCT
				  IFF(ins.first_login_type = 'logged_out', ins.touch_id, NULL))             AS logged_out_first_sessions,
			COUNT(DISTINCT ins.touch_start_tstamp::DATE)                                    AS days_with_session,
			SUM(ins.spvs)                                                                   AS total_spvs,
			COUNT(DISTINCT
				  IFF(ins.spvs > 0, ins.touch_start_tstamp::DATE, NULL))                    AS days_with_spv_session,
			total_spvs / sessions                                                           AS spv_to_session_rate,
			SUM(ins.bookings)                                                               AS bookings,
			SUM(ins.booking_form_views)                                                     AS booking_form_views,
			SUM(ins.margin_gbp)                                                             AS margin_gbp
		FROM input_sessions ins
			LEFT JOIN user_first_login_type uflt ON ins.attributed_user_id = uflt.attributed_user_id
			LEFT JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(ins.attributed_user_id) = sua.shiro_user_id

		GROUP BY 1, 2, 3, 4, 5, 6, 7
	)
SELECT
	um.user_first_test_group,
	um.user_state,
	COUNT(DISTINCT um.attributed_user_id)                                                AS users,
	SUM(um.sessions)                                                                     AS total_sessions,
	COUNT(DISTINCT
		  IFF(um.sign_up_state = 'new sign up', um.attributed_user_id, NULL))            AS new_sign_up_users,
	COUNT(DISTINCT IFF(um.sign_up_state = 'existing user', um.attributed_user_id, NULL)) AS existing_users,
	COUNT(DISTINCT IFF(um.sign_up_state = 'new sign up' AND um.email_opt_in_status = 'weekly', um.attributed_user_id,
					   NULL))                                                            AS sign_ups_with_weekly_opt_in,
	COUNT(DISTINCT IFF(um.sign_up_state = 'new sign up' AND um.email_opt_in_status = 'daily', um.attributed_user_id,
					   NULL))                                                            AS sign_ups_with_daily_opt_in,
	COUNT(DISTINCT IFF(um.sign_up_state = 'new sign up' AND um.email_opt_in_status = 'opted out', um.attributed_user_id,
					   NULL))                                                            AS sign_ups_with_opted_out,
	total_sessions / users                                                               AS sessions_per_user,
	SUM(um.margin_gbp)                                                                   AS margin,
	COUNT(DISTINCT IFF(um.bookings > 0, um.attributed_user_id, NULL))                    AS users_with_booking,
	AVG(um.total_spvs)                                                                   AS avg_user_spvs,
	SUM(um.total_spvs)                                                                   AS spvs,
	SUM(um.spv_sessions)                                                                 AS spv_sessions,
	AVG(um.days_with_session)                                                            AS avg_days_with_session,
	AVG(um.days_with_spv_session)                                                        AS avg_days_with_spv_session
FROM user_modelling um
-- filter for users first log in type
WHERE user_first_login_type = 'logged_out'
GROUP BY 1, 2
ORDER BY 2, 1
;

------------------------------------------------------------------------------------------------------------------------


USE WAREHOUSE pipe_xlarge
;

WITH
	input_sessions AS (
		SELECT
			psm.feature_flag_test_array,
			test_feature_flags.value['feature_flag']::VARCHAR  AS feature_flag,
			test_feature_flags.value['min_tstamp']::TIMESTAMP  AS feature_flag_min_tstamp,
			test_feature_flags.value['num_occurences']::NUMBER AS num_occurences,
			test_feature_flags.value['test_group']::VARCHAR    AS test_group,
			COUNT(DISTINCT test_feature_flags.value['test_group']::VARCHAR)
				  OVER (PARTITION BY psm.touch_id)             AS num_of_test_groups_assigned,
			psm.*
		FROM dbt.bi_product_analytics__intermediate.pda_session_metrics psm,
			 LATERAL FLATTEN(INPUT => psm.feature_flag_test_array, OUTER => TRUE) test_feature_flags
		WHERE psm.touch_start_tstamp::DATE BETWEEN '2024-07-29' AND CURRENT_DATE - 1
		  AND LOWER(psm.feature_flag_test_array::VARCHAR) LIKE '%abtest.opensite%'
		  AND LOWER(test_feature_flags.value['feature_flag']::VARCHAR) LIKE 'abtest.opensite%'
		  AND psm.first_login_type = 'logged_out'
		QUALIFY ROW_NUMBER() OVER (PARTITION BY psm.touch_id ORDER BY feature_flag_min_tstamp, test_group) = 1
			AND psm.touch_affiliate_territory IN ('DE', 'UK')
	)
SELECT
	ins.feature_flag,
	COUNT(DISTINCT ins.touch_id)           AS sessions,
	COUNT(DISTINCT ins.attributed_user_id) AS users,
	SUM(ins.bookings)                      AS bookings
FROM input_sessions ins
GROUP BY 1


-- 1.4M


SELECT DATEADD(MONTH, -4, '2025-03-11')


;


SELECT
	stbfv.event_tstamp::DATE AS date,
	stbfv.se_sale_id IS NOT NULL,
	stbfv.event_subcategory,
	stbfv.tech_platform,
	COUNT(*)
FROM se.data.scv_touched_booking_form_views stbfv
WHERE stbfv.event_tstamp >= CURRENT_DATE - 20
GROUP BY 1, 2, 3, 4
;


SELECT
	stmeoi.event_tstamp::DATE AS date,
	IFF(stmeoi.tech_platform = 'TRAVELBIRD' AND stmeoi.event_subcategory = 'booking_form_view', TRUE,
		FALSE)                AS is_tracy,
	stmeoi.se_sale_id IS NULL,
	COUNT(*)                  AS bfvs
FROM se.data.scv_touched_module_events_of_interest stmeoi
	INNER JOIN se.data.dim_sale ds ON stmeoi.se_sale_id = ds.se_sale_id AND ds.sale_type = 'Hotel Plus'
WHERE stmeoi.event_tstamp >= CURRENT_DATE - 10
  AND stmeoi.event_subcategory = 'booking_form_view'
AND ds.tech_platform = 'SECRET_ESCAPES'
GROUP BY ALL;



SELECT
	stmeoi.event_tstamp::DATE AS date,
	IFF(stmeoi.tech_platform = 'TRAVELBIRD' AND stmeoi.event_subcategory = 'booking_form_view', TRUE,
		FALSE)                AS is_tracy,
	stmeoi.se_sale_id IS NULL,
	*
FROM se.data.scv_touched_module_events_of_interest stmeoi
	LEFT JOIN se.data.dim_sale ds ON stmeoi.se_sale_id = ds.se_sale_id AND ds.sale_type = 'Hotel Plus'
WHERE stmeoi.event_tstamp >= CURRENT_DATE - 10
  AND stmeoi.event_subcategory = 'booking_form_view'
  AND ds.se_sale_id IS NULL;


SELECT * FROM se.data.dim_sale ds WHERE ds.se_sale_id = 'A64993'

