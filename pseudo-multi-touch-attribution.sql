-- write a query to identify sessions that have a attribute that you want to understand multi touch attribution to. eg.
-- sessions with bookings, you want to know what previous sessions contribute to this conversion.


WITH
	sessions_of_interest AS (
		-- list of sessions with the attribute you care about, in this case, a booking
		SELECT *
		FROM se.data_pii.scv_touch_basic_attributes stba
		WHERE stba.touch_has_booking -- attribute I want
		  AND stba.stitched_identity_type = 'se_user_id'
	),
	users_of_interest AS (
		-- get a unique list of users
		SELECT DISTINCT
			attributed_user_id
		FROM sessions_of_interest
	),
	sessions_for_those_users AS (
		-- return all sessions for that user
		SELECT
			stba.*
		FROM se.data_pii.scv_touch_basic_attributes stba
			INNER JOIN users_of_interest uoi ON stba.attributed_user_id = uoi.attributed_user_id
	),
	model_sessions AS (
		-- limit to sessions that are within 7 days of a session of interest
		SELECT
			sftu.*,
			soi.touch_id AS session_of_interest_touch_id
		FROM sessions_for_those_users sftu -- almost ;)
			INNER JOIN sessions_of_interest soi ON sftu.attributed_user_id = soi.attributed_user_id
			AND sftu.touch_start_tstamp <= soi.touch_start_tstamp
			AND DATEDIFF(DAY, sftu.touch_start_tstamp, soi.touch_start_tstamp) <=
				7 -- attributing sessions within 7 days
	),
	calculate_weight AS (

		SELECT
			ms.*,
			stmc.touch_mkt_channel,
			COUNT(*) OVER (PARTITION BY session_of_interest_touch_id)                                         AS count_of_attributed_sessions,
			ROW_NUMBER() OVER (PARTITION BY session_of_interest_touch_id ORDER BY ms.touch_start_tstamp ASC)  AS index_of_attributed_sessions,
			ROW_NUMBER() OVER (PARTITION BY session_of_interest_touch_id ORDER BY ms.touch_start_tstamp DESC) AS reverse_index_of_attributed_sessions,
			1 / count_of_attributed_sessions                                                                  AS linear_weight,
			CASE
				-- when its only 1 or 2 sessions apply linear weight
				WHEN count_of_attributed_sessions <= 2 THEN linear_weight
				-- else 40% goes to first, 40% to last and 20% remaining is split
				ELSE
					CASE
						WHEN index_of_attributed_sessions = 1 OR reverse_index_of_attributed_sessions = 1
							THEN 0.4
						ELSE -- spliting the 20%
							0.2 / (count_of_attributed_sessions - 2) -- remove the first and last sessions from count
					END
			END                                                                                               AS u_shape_weight,
			IFF(index_of_attributed_sessions = 1, 1, 0)                                                       AS first_attributed_session_weight,
			IFF(reverse_index_of_attributed_sessions = 1, 1, 0)                                               AS last_attributed_session_weight,

-- 			https://docs.google.com/spreadsheets/d/1NBwtLfOe7a-mq0NGndOJrOzhMlHWzkn-cnDmE6870D4/edit?gid=796675574#gid=796675574
			index_of_attributed_sessions /
			((count_of_attributed_sessions * (count_of_attributed_sessions + 1)) / 2)                         AS position_decay_weight,
			CASE
				WHEN count_of_attributed_sessions = 1 THEN 1
				ELSE
					POWER(2, index_of_attributed_sessions) / (POWER(2, (count_of_attributed_sessions + 1)) - 2)
			END                                                                                               AS half_life_decay_weight
		FROM model_sessions ms
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON ms.touch_id = stmc.touch_id
	)
SELECT *
FROM calculate_weight