-- we need to do the following (for the last ‘n’ = 5 or 6 deals) we need to find the max availability, min availability,
-- average availability over the next 3 months, and the same for weekends of the next 3 months. It should follow the
-- below given template

WITH
	samples AS (
		SELECT
			user_id
		FROM data_science.tmp.booking_intent_samples
		WHERE dataset_id = 'development'
		  AND territory_id = 11
		  AND day_of_run = '2023-05-29'
	),
	availability AS (
		SELECT <YOUR QUERY GOES HERE>

WHERE territory = 'IT' AND event_tstamp > '2023-05-21' AND event_tstamp < '2023-05-29'
	)

SELECT
	sa.user_id,
	availability.feat_window,
	availability.feature1 AS feature1,
	availability.feature2 AS feature2
FROM samples sa

	INNER JOIN availabixlity ON sa.user_id = TRY_CAST(availability.user_id AS INTEGER)


-- give a day and some user ids
-- look at the last 5 deals they looked at up to that date
-- then compute the availablity aggregations on those sales

SET day_of_run = '2023-05-28'
;

SET feature_start = '2023-05-21'
;

USE WAREHOUSE pipe_xlarge
;

WITH
	input_users AS (
		-- you replace this with your list of users and the date
		SELECT
			user_id
		FROM data_science.tmp.booking_intent_samples
		WHERE dataset_id = 'UK_TRAINING_2023-05-01_2023-05-31'
		  AND territory_id = 1
		  AND day_of_run = $day_of_run
	),
	temp_list_of_users AS (
		-- TODO for testing only, replace with input users afterwards
		SELECT
			column1 AS user_id
		FROM
		VALUES ('34422807')
	),
	spvs AS (
		-- get unique list of sale spvs within date parameters
		SELECT
			stba.attributed_user_id::INT AS user_id,
			sts.se_sale_id,
			MAX(sts.event_tstamp)        AS max_tstamp
		FROM se.data.scv_touched_spvs sts
			INNER JOIN se.data_pii.scv_touch_basic_attributes stba
					   ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
						   -- TODO swap this with input_users
			INNER JOIN temp_list_of_users u ON TRY_TO_NUMBER(stba.attributed_user_id) = u.user_id
		WHERE sts.event_tstamp BETWEEN $feature_start AND $day_of_run
		GROUP BY 1, 2
	),
	list_of_user_last_x_sales AS (
		-- limit to the most recent 5
		SELECT
			s.user_id,
			s.se_sale_id,
			s.max_tstamp,
			ROW_NUMBER() OVER (PARTITION BY s.user_id ORDER BY s.max_tstamp DESC) AS feat_window
		FROM spvs s
		QUALIFY feat_window < 6
	),
	list_of_sales AS (
		--distinct list of sale ids we need availability for filtering the a
		SELECT DISTINCT
			us.se_sale_id
		FROM list_of_user_last_x_sales us
	),
	aggregate_availability AS (
		-- aggregate availability for sales that were observed
		SELECT
			hscv.se_sale_id,
			MIN(hscv.available_inventory) AS min_available_inventory,
			MAX(hscv.available_inventory) AS max_available_inventory,
			AVG(hscv.available_inventory) AS avg_available_inventory,
			SUM(IFF(hscv.available_inventory = 0, 1, 0)) /
			COUNT(hscv.calendar_date)     AS days_with_no_available_inventory,
			SUM(IFF(hscv.available_inventory = 0 AND hscv.day_name IN ('Fri', 'Sat', 'Sun'), 1, 0)) /
			COUNT(IFF(hscv.day_name IN ('Fri', 'Sat', 'Sun'), hscv.calendar_date,
					  NULL))              AS weekend_days_with_no_available_inventory
		FROM se.data.harmonised_sale_calendar_view_snapshot hscv
			INNER JOIN list_of_sales ls ON hscv.se_sale_id = ls.se_sale_id
		WHERE hscv.calendar_date BETWEEN $day_of_run AND DATEADD(MONTH, 2, $day_of_run)
		  AND hscv.view_date = $day_of_run
		GROUP BY 1
	)
-- join the aggregated availability back to the user sale id grain
SELECT
	u.user_id,
	u.se_sale_id,
	u.max_tstamp,
	u.feat_window,
	aa.min_available_inventory,
	aa.max_available_inventory,
	aa.avg_available_inventory,
	aa.days_with_no_available_inventory,
	aa.weekend_days_with_no_available_inventory
FROM list_of_user_last_x_sales u
	LEFT JOIN aggregate_availability aa ON u.se_sale_id = aa.se_sale_id
;


SELECT
    months,
	months[0]::VARCHAR AS months
FROM se.data.scv_touched_searches sts
;