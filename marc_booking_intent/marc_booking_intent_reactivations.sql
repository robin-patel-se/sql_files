------------------------------------------------------------------------------------------------------------------------
-- alex reactivation logic

WITH
	reactivations AS (
		SELECT
			ci_react_cust.reactivation_date,
			ci_react_cust.shiro_user_id,
			ci_react_cust.territory_grouped,
			ci_react_cust.uuid
		FROM dbt.bi_customer_insight.ci_react_cust
		WHERE reactivated_from IN ('Long-Term Lapsed', 'Dormant', 'Sunset') --  filter for reactivated users
		  AND ci_react_cust.reactivation_date BETWEEN '2023-01-01' AND CURRENT_DATE - 30
	),
	intent_data AS (
		-- limit intent data to only show intent for 30 days since sign up
		SELECT
			reactivations.shiro_user_id,
			booking_intent.inference_ts,
			booking_intent.booking_probability,
			booking_intent.booking_prob_percentile,
			booking_intent.booking_probability_bucket,
			reactivations.reactivation_date
		FROM data_science.operational_output.booking_intent_prediction_prod booking_intent
		INNER JOIN reactivations
			ON booking_intent.user_id = reactivations.shiro_user_id
			AND booking_intent.inference_ts::DATE BETWEEN reactivations.reactivation_date::DATE
				   AND DATEADD(DAY, 30, reactivations.reactivation_date)
	),
	user_intent_30_day_from_reactivation AS (
		SELECT
			intent_data.shiro_user_id,
			intent_data.reactivation_date,
			MAX(intent_data.booking_prob_percentile)    AS max_booking_probability_percentile_30days_post_signup,
			MAX(intent_data.booking_probability_bucket) AS max_booking_probability_bucket_30days_post_signup,
		FROM intent_data
		GROUP BY intent_data.shiro_user_id,
				 intent_data.reactivation_date
	),
	model_data AS (
		SELECT
			reactivations.reactivation_date,
			reactivations.shiro_user_id,
			reactivations.territory_grouped,
			reactivations.uuid,
			max_booking_probability_percentile_30days_post_signup,
			max_booking_probability_bucket_30days_post_signup
		FROM reactivations
		LEFT JOIN user_intent_30_day_from_reactivation AS intent
			ON reactivations.shiro_user_id = intent.shiro_user_id
			AND reactivations.reactivation_date = intent.reactivation_date
	)

SELECT
	DATE_TRUNC(MONTH, model_data.reactivation_date) AS reactivation_month,
	model_data.territory_grouped,
	COUNT(DISTINCT model_data.shiro_user_id)        AS reactivations,
	COUNT(DISTINCT
		  IFF(model_data.max_booking_probability_bucket_30days_post_signup BETWEEN 1 AND 3, model_data.shiro_user_id,
			  NULL))                                AS reactivations_with_booking_intent_1_to_3,

	COUNT(DISTINCT
		  IFF(model_data.max_booking_probability_bucket_30days_post_signup BETWEEN 4 AND 6, model_data.shiro_user_id,
			  NULL))                                AS reactivations_with_booking_intent_4_to_6,

	COUNT(DISTINCT
		  IFF(model_data.max_booking_probability_bucket_30days_post_signup BETWEEN 7 AND 10, model_data.shiro_user_id,
			  NULL))                                AS reactivations_with_booking_intent_7_to_10,

	COUNT(DISTINCT IFF(model_data.max_booking_probability_bucket_30days_post_signup IS NULL, model_data.shiro_user_id,
					   NULL))                       AS reactivations_with_booking_intent_null
FROM model_data
GROUP BY model_data.reactivation_date,
		 model_data.territory_grouped



SELECT *
FROM dbt.bi_customer_insight.ci_react_cust;
WHERE reactivated_from IN ('Long-Term Lapsed', 'Dormant', 'Sunset')
;
