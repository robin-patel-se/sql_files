-- check the greatest intent of users in the first 30 days since they signed up yoy
USE WAREHOUSE pipe_xlarge
;

WITH
	session_channels AS (
		-- list of sessions will be used to determine sign up channel
		SELECT
			stba.attributed_user_id AS shiro_user_id,
			stba.touch_start_tstamp,
			stmc.touch_mkt_channel,
			stmc.channel_category,
		FROM se.data_pii.scv_touch_basic_attributes stba
		INNER JOIN se.data.scv_touch_attribution sta
			ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
		INNER JOIN se.data.scv_touch_marketing_channel stmc
			ON sta.attributed_touch_id = stmc.touch_id
			AND stmc.touch_start_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE - 30
		WHERE stba.touch_start_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE - 30
		  AND stba.stitched_identity_type = 'se_user_id'
	),
	users AS (
		-- will be used to filter intent data but also defines grain of output
		SELECT
			sua.shiro_user_id,
			sua.signup_tstamp,
			sua.main_affiliate_name,
			sua.original_affiliate_territory,
			sua.current_affiliate_territory
		FROM se.data.se_user_attributes sua
		WHERE sua.membership_account_status IS DISTINCT FROM 'DELETED'
		  -- restricting sign up to give 30 day window for intent for fair comps
		  AND sua.signup_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE - 30
-- 		  AND sua.original_affiliate_territory = 'UK'
		  AND sua.original_affiliate_territory IN ('DE', 'UK')
	),
	acquisition_costs AS (
		-- cost of acquiring session - logic owned by KJ
		SELECT
			attributed_user_id AS shiro_user_id,
			cost_per_session   AS user_acquistion_cost
		FROM scratch.krystynajohnson.cost_per_signup_session_user_level_robin_lnd
	),
	signup_channel AS (
		SELECT
			users.shiro_user_id,
			users.signup_tstamp,
			users.main_affiliate_name,
			users.original_affiliate_territory,
			users.current_affiliate_territory,
			session_channels.touch_start_tstamp,
			session_channels.touch_mkt_channel,
			session_channels.channel_category,
			COALESCE(session_channels.touch_mkt_channel, 'Unknown') AS signup_channel,
			COALESCE(session_channels.channel_category, 'Unknown')  AS signup_channel_category
		FROM users
		ASOF JOIN session_channels
		MATCH_CONDITION (users.signup_tstamp >= session_channels.touch_start_tstamp)
			ON users.shiro_user_id = session_channels.shiro_user_id::VARCHAR
	),
	intent_data AS (
		-- limit intent data to only show intent for 30 days since sign up
		SELECT
			users.shiro_user_id,
			booking_intent.inference_ts,
			booking_intent.booking_probability,
			booking_intent.booking_prob_percentile,
			booking_intent.booking_probability_bucket,
			users.signup_tstamp
		FROM data_science.operational_output.booking_intent_prediction_prod booking_intent
		INNER JOIN users
			ON booking_intent.user_id = users.shiro_user_id
			AND
			   booking_intent.inference_ts::DATE BETWEEN users.signup_tstamp::DATE AND DATEADD(DAY, 30, users.signup_tstamp)
	),
	user_intent_30_day_from_signup AS (
		SELECT
			intent_data.shiro_user_id,
			intent_data.signup_tstamp,
			MAX(intent_data.booking_prob_percentile)    AS max_booking_probability_percentile_30days_post_signup,
			MAX(intent_data.booking_probability_bucket) AS max_booking_probability_bucket_30days_post_signup,
		FROM intent_data
		GROUP BY intent_data.shiro_user_id,
				 intent_data.signup_tstamp
	),
	model_data AS (
		SELECT
			users.shiro_user_id,
			users.signup_tstamp,
			users.main_affiliate_name,
			users.original_affiliate_territory,
			users.current_affiliate_territory,
			users.signup_channel,
			users.signup_channel_category,
			acquisition_costs.user_acquistion_cost,
			intent.max_booking_probability_bucket_30days_post_signup
		FROM signup_channel users
		LEFT JOIN acquisition_costs
			ON
			users.shiro_user_id = acquisition_costs.shiro_user_id
		LEFT JOIN user_intent_30_day_from_signup intent
			ON users.shiro_user_id = intent.shiro_user_id
	)

SELECT
	DATE_TRUNC(MONTH, model_data.signup_tstamp) AS month,
	model_data.signup_channel,
	model_data.signup_channel_category,
	model_data.original_affiliate_territory,
	COUNT(DISTINCT model_data.shiro_user_id)    AS sign_ups,
	SUM(model_data.user_acquistion_cost)        AS user_acquisition_costs,

	COUNT(DISTINCT
		  IFF(model_data.max_booking_probability_bucket_30days_post_signup BETWEEN 1 AND 3, model_data.shiro_user_id,
			  NULL))                            AS signups_with_booking_intent_1_to_3,
	SUM(IFF(model_data.max_booking_probability_bucket_30days_post_signup BETWEEN 1 AND 3,
			model_data.user_acquistion_cost,
			NULL))                              AS user_acquisition_costs_with_booking_intent_1_to_3,
	COUNT(DISTINCT
		  IFF(model_data.max_booking_probability_bucket_30days_post_signup BETWEEN 4 AND 6, model_data.shiro_user_id,
			  NULL))                            AS signups_with_booking_intent_4_to_6,
	SUM(IFF(model_data.max_booking_probability_bucket_30days_post_signup BETWEEN 4 AND 6,
			model_data.user_acquistion_cost,
			NULL))                              AS user_acquisition_costs_with_booking_intent_4_to_6,
	COUNT(DISTINCT
		  IFF(model_data.max_booking_probability_bucket_30days_post_signup BETWEEN 7 AND 10, model_data.shiro_user_id,
			  NULL))                            AS signups_with_booking_intent_7_to_10,
	SUM(IFF(model_data.max_booking_probability_bucket_30days_post_signup BETWEEN 7 AND 10,
			model_data.user_acquistion_cost,
			NULL))                              AS user_acquisition_costs_with_booking_intent_7_to_10,
	COUNT(DISTINCT IFF(model_data.max_booking_probability_bucket_30days_post_signup IS NULL, model_data.shiro_user_id,
					   NULL))                   AS signups_with_booking_intent_null,
	SUM(IFF(model_data.max_booking_probability_bucket_30days_post_signup IS NULL, model_data.user_acquistion_cost,
			NULL))                              AS user_acquisition_costs_with_booking_intent_null
FROM model_data
GROUP BY DATE_TRUNC(MONTH, model_data.signup_tstamp),
		 model_data.signup_channel,
		 model_data.original_affiliate_territory,
		 signup_channel_category
;

SELECT
	inference_ts::DATE,
	COUNT(*)
FROM data_science.operational_output.booking_intent_prediction_prod booking_intent
WHERE booking_intent.inference_ts >= '2024-01-01'
GROUP BY 1
;


SELECT
	sua.shiro_user_id,
	sua.original_affiliate_name,
	sua.original_affiliate_territory,
	sua.signup_tstamp
FROM se.data.se_user_attributes sua
;


SELECT
	stba.attributed_user_id AS shiro_user_id,
	stba.touch_start_tstamp,
	stmc.touch_mkt_channel
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
	AND stmc.touch_start_tstamp BETWEEN '2024-01-01' AND CURRENT_DATE - 30
WHERE stba.touch_start_tstamp BETWEEN '2024-01-01' AND CURRENT_DATE - 30
  AND stba.stitched_identity_type = 'se_user_id'
;



SELECT *
FROM scratch.krystynajohnson.cost_per_signup_session_user_level_robin
;



SELECT
	events.event_tstamp::DATE,
	COUNT(*)
FROM se.data_pii.scv_event_stream events
WHERE events.unstruct_event_com_branch_secretescapes_purchase_1 IS NOT NULL
  AND events.event_tstamp >= CURRENT_DATE - 100
GROUP BY 1


SELECT
	events.unstruct_event_com_branch_secretescapes_purchase_1
FROM se.data_pii.scv_event_stream events
WHERE events.unstruct_event_com_branch_secretescapes_purchase_1 IS NOT NULL
  AND events.event_tstamp >= CURRENT_DATE - 100
  AND events.event_tstamp >= CURRENT_DATE - 50


