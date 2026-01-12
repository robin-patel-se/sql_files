-- will wants some numbers
-- avg member bookings in the last 3 years is 1.7 bookings
-- how many times did they try (a period of sustained engagement)
-- 3 days with spv in 14 day period

-- apollo data is snapshotted
-- (2 years of data)
-- 30 days counts as a new engagement

-- the engagement for members that have booked


-- from will: "
-- the right answer is
-- our demographic go on hols 5 times a year
-- our bookers try to book 4 times a year with us
-- and they only book once
-- ie its implausible they try 15 times a year
-- "


SELECT *
FROM data_science.operational_output.booking_intent_prediction_prod bipp
WHERE user_id = '80752440'
;

SELECT
	MIN(bipp.inference_ts)
FROM data_science.operational_output.booking_intent_prediction_prod bipp
;

-- 2023-09-14 -- booking intent data goes back to this date


-- in a 2 year period

USE WAREHOUSE pipe_xlarge
;
-- remove dupes
WITH
	remove_dupes AS (
		SELECT
			booking_intent_prediction_prod.inference_ts::DATE AS event_date,
			booking_intent_prediction_prod.user_id,
-- 			booking_intent_prediction_prod.booking_percentile_bucket,
			booking_intent_prediction_prod.recommended_model
		FROM data_science.operational_output.booking_intent_prediction_prod
		WHERE booking_intent_prediction_prod.booking_percentile_bucket >= 5 -- are in the top 50th percentile are going to book
-- 			AND booking_intent_prediction_prod.recommended_model = 'Artemis' -- apollo has deemed them high booking intent
-- 		  AND booking_intent_prediction_prod.user_id = '79174087'          -- TODO REMOVE test user
		  AND booking_intent_prediction_prod.inference_ts >= '2023-12-01'
		QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, inference_ts::DATE ORDER BY inference_ts DESC) = 1
	),
	booking_intent_flag AS (
		SELECT *,
			   LAG(remove_dupes.event_date)
				   OVER (PARTITION BY remove_dupes.user_id ORDER BY remove_dupes.event_date) AS previous_intent_event_date,
			   -- decided the window of engagement inactivity to constitute a new engagement is 30/14 days
			   -- coalesce to TRUE to handle first engagement
			   COALESCE(DATEDIFF(DAY, previous_intent_event_date, remove_dupes.event_date) > 14,
						TRUE)                                                                AS new_intent_flag
		FROM remove_dupes
	),
	user_engagements AS (
		SELECT
			booking_intent_flag.user_id,
			COUNT_IF(booking_intent_flag.new_intent_flag) AS engagements
		FROM booking_intent_flag
		GROUP BY 1
	),
	bookers AS (
		SELECT
			fact_booking.shiro_user_id,
			COUNT(*) AS bookings
		FROM se.data.fact_booking
		WHERE fact_booking.se_brand = 'SE Brand'
		  AND fact_booking.booking_completed_date BETWEEN '2023-12-01' AND '2025-06-30' -- apollo earliest intent date
		  AND fact_booking.booking_status_type IN ('live', 'cancelled')                 -- anyone who's made a successful or cancelled booking
		  AND fact_booking.territory IN ('UK', 'DE', 'IT')                              -- apollo only produces intent for users in UK, DE, IT from September 2023
		GROUP BY 1
	),
	first_6m_bookers AS (
		-- theory is that people who booked after this window included in output dataset will dilute figures
		SELECT DISTINCT
			fact_booking.shiro_user_id
		FROM se.data.fact_booking
		WHERE fact_booking.se_brand = 'SE Brand'
		  AND fact_booking.booking_completed_date BETWEEN '2023-12-01' AND '2024-06-01' -- limit to people who booked in the first 6m
		  AND fact_booking.booking_status_type IN ('live', 'cancelled')                 -- anyone who's made a successful or cancelled booking
		  AND fact_booking.territory IN ('UK', 'DE', 'IT') -- apollo only produces intent for users in UK, DE, IT from September 2023
	),
	model_data AS (
		SELECT
			bookers.shiro_user_id,
			se_user_attributes.current_affiliate_territory,
			bookers.bookings,
			user_engagements.engagements,
			bookers.bookings / user_engagements.engagements AS some_cool_new_acronym
		FROM bookers
			INNER JOIN se.data.se_user_attributes ON bookers.shiro_user_id = se_user_attributes.shiro_user_id
			AND se_user_attributes.current_affiliate_territory IN ('UK', 'DE', 'IT')
						   -- filter to 6m bookers
			INNER JOIN first_6m_bookers ON bookers.shiro_user_id = first_6m_bookers.shiro_user_id
			LEFT JOIN  user_engagements ON bookers.shiro_user_id = user_engagements.user_id
	)

SELECT
	model_data.current_affiliate_territory,
	COUNT(*)                    AS bookers,
	AVG(model_data.bookings)    AS avg_bookings,
	AVG(model_data.engagements) AS avg_engagements
FROM model_data
GROUP BY ALL
;


SELECT
	bipp.territory_id,
	se.data.territory_name_from_territory_id(bipp.territory_id),
	MIN(bipp.inference_ts),
	COUNT(*)
FROM data_science.operational_output.booking_intent_prediction_prod bipp
GROUP BY 1


SELECT
	bipp.booking_percentile_bucket,
	bipp.recommended_model,
	COUNT(*)
FROM data_science.operational_output.booking_intent_prediction_prod bipp
GROUP BY ALL
;


SELECT *
FROM data_science.operational_output.booking_intent_prediction_prod bipp
WHERE bipp.booking_probability_bucket = 3 AND bipp.recommended_model = 'Artemis'



SELECT *
FROM se.data_pii.se_user_attributes sua
WHERE sua.email = 'gianni.raftis@gmail.com'

-- 72868430 g's user

SELECT *
FROM data_science.operational_output.booking_intent_prediction_prod bipp
WHERE bipp.user_id = '38898959'

-- Use the booking probability