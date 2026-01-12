-- WAU
SELECT
	date_value
FROM se.data.se_calendar sc
WHERE yesterday OR yesterday_ly
;


------------------------------------------------------------------------------------------------------------------------

-- find comparative date
SELECT
	sc.date_value,
	sc.se_week,
	sc.se_year
FROM se.data.se_calendar sc
WHERE sc.se_year IN ('2022', '2023')
  AND sc.se_week = 48
  AND sc.day_name = 'Sun'
;

------------------------------------------------------------------------------------------------------------------------
-- check demand model vs user activity
SELECT
	g.event_date,
	SUM(g.wau)
FROM data_vault_mvp.bi.grain g
WHERE g.event_date IN (
					   '2022-12-04',
					   '2023-12-03'
	)
GROUP BY 1
;

SELECT
	ua.date,
	COUNT(DISTINCT ua.shiro_user_id)
FROM se.data.user_activity ua
WHERE ua.date IN (
				  '2022-12-04',
				  '2023-12-03'
	) AND
	  (ua.web_sessions_7d > 0
		  OR ua.app_sessions_7d > 0
		  )
GROUP BY 1

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.wau_comparison_sessions AS (
	WITH
		input_dates AS (
			SELECT
				sc.date_value,
				sc.se_week,
				sc.se_year
			FROM se.data.se_calendar sc
			WHERE sc.se_year IN ('2022', '2023')
			  AND sc.se_week = 48
			  AND sc.day_name = 'Sun'
		),
		user_level_activity AS (
			SELECT
				ua.date,
				sd.se_week,
				sd.se_year,
				ua.shiro_user_id,
				ua.web_sessions_7d,
				ua.app_sessions_7d
			FROM se.data.user_activity ua
				INNER JOIN input_dates sd ON ua.date = sd.date_value
			WHERE ua.date >= '2019-01-01' AND
				  (ua.web_sessions_7d > 0
					  OR ua.app_sessions_7d > 0
					  )
		)
	SELECT
		ula.date               AS wau_date,
		ula.se_week,
		ula.se_year,
		ula.shiro_user_id,
		ula.web_sessions_7d,
		ula.app_sessions_7d,
		stba.touch_id,
		stba.touch_start_tstamp,
		stba.touch_duration_seconds,
		stba.touch_event_count,
		stba.touch_logged_in,
		stba.touch_experience,
		stmc.touch_affiliate_territory,
		stmc.touch_mkt_channel AS last_non_direct_channel
	FROM user_level_activity ula
		INNER JOIN se.data_pii.scv_touch_basic_attributes stba
				   ON ula.shiro_user_id = TRY_TO_NUMBER(stba.attributed_user_id)
					   AND stba.touch_start_tstamp BETWEEN ula.date - 7 AND ula.date
		INNER JOIN se.data.scv_touch_attribution sta
				   ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
		INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
-- 		INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
	WHERE stba.stitched_identity_type = 'se_user_id'

)
;

SELECT
	wau_date,
	COUNT(DISTINCT shiro_user_id)
FROM scratch.robinpatel.wau_comparison_sessions
GROUP BY 1
;

SELECT
	wau_date,
	COUNT(DISTINCT shiro_user_id)
FROM scratch.robinpatel.wau_comparison_sessions_v2
GROUP BY 1
;



------------------------------------------------------------------------------------------------------------------------
-- understanding cannibalisation from one channel to another
USE WAREHOUSE pipe_xlarge
;

SET start_date = '2023-01-02' -- first monday was 2nd
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.wau_user_classification AS (
	-- create a table that puts every WAU user in a channel bucket
	WITH
		user_sessions AS (
			-- get session data for each user
			SELECT
				stba.touch_id,
				stba.attributed_user_id AS shiro_user_id,
				stba.touch_start_tstamp,
				stba.touch_experience,
				stmc.touch_affiliate_territory,
				stmc.touch_mkt_channel  AS last_non_direct_channel
			FROM se.data_pii.scv_touch_basic_attributes stba
				INNER JOIN se.data.scv_touch_attribution sta
						   ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
				INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
			WHERE stba.stitched_identity_type = 'se_user_id'
			  AND stba.touch_start_tstamp BETWEEN $start_date AND CURRENT_DATE
		),
		session_aggregation AS (
-- aggregate session counts for users up to channel and wau week to be filtered in next step
			SELECT
				DATE_TRUNC(WEEK, us.touch_start_tstamp)::DATE + 6 AS wau_week, -- -1 to choose sunday of the week for sessions that occurred in the week prior
				us.shiro_user_id,
				us.last_non_direct_channel,
				COUNT(*)                                          AS sessions
			FROM user_sessions us
			GROUP BY 1, 2, 3
		)
	-- filter for most common channel in that week for each user
-- note top channel is non deterministic - can improve in future
	SELECT
		wau_week,
		shiro_user_id,
		last_non_direct_channel AS user_channel
	FROM session_aggregation sa
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sa.wau_week, sa.shiro_user_id ORDER BY sa.sessions DESC) = 1
)
;

SELECT COUNT(DISTINCT shiro_user_id) FROM scratch.robinpatel.wau_user_classification
WHERE wau_week = '2023-12-03'

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.wau_session_modelling AS (
	WITH
		week_grain AS (
			SELECT
				sc.date_value
			FROM se.data.se_calendar sc
			WHERE sc.date_value BETWEEN $start_date AND CURRENT_DATE
			  AND sc.day_name = 'Sun'
		),
		distinct_users AS (
			SELECT DISTINCT
				shiro_user_id
			FROM scratch.robinpatel.wau_user_classification
		)
			,
		user_grain AS (
			SELECT
				date_value,
				shiro_user_id
			FROM week_grain
				CROSS JOIN distinct_users
		)
	SELECT
		date_value,
		g.shiro_user_id,
		cw.user_channel,
		lw.user_channel AS last_week_user_channel,
		CASE
			WHEN cw.user_channel IS NOT NULL AND lw.user_channel IS NULL THEN 'New'
			WHEN cw.user_channel IS NULL AND lw.user_channel IS NOT NULL THEN 'Lost'
			WHEN cw.user_channel = lw.user_channel THEN 'Retained'
			WHEN cw.user_channel IS DISTINCT FROM lw.user_channel THEN 'Shift'
		END             AS wau_category
	FROM user_grain g
		LEFT JOIN scratch.robinpatel.wau_user_classification cw
				  ON g.date_value = cw.wau_week AND g.shiro_user_id = cw.shiro_user_id
		LEFT JOIN scratch.robinpatel.wau_user_classification lw
				  ON g.date_value - 7 = lw.wau_week AND g.shiro_user_id = lw.shiro_user_id
	WHERE COALESCE(cw.user_channel, lw.user_channel) IS NOT NULL -- to remove weeks where no activity for the user
)
;

SELECT
	m.date_value,
	m.user_channel,
	m.last_week_user_channel,
	m.wau_category,
	COUNT(DISTINCT m.shiro_user_id) AS users
FROM scratch.robinpatel.wau_session_modelling m
GROUP BY 1, 2, 3, 4
;




-- model the data further for tableau
WITH
	aggregate_channels AS (
		SELECT
			m.date_value,
			m.user_channel,
			m.last_week_user_channel,
			m.wau_category,
			COUNT(DISTINCT m.shiro_user_id) AS users
		FROM scratch.robinpatel.wau_session_modelling m
		GROUP BY 1, 2, 3, 4
	)

SELECT
	date_value,
	user_channel,
	last_week_user_channel,
	wau_category,
	users
FROM aggregate_channels AS wa
WHERE wa.wau_category IS DISTINCT FROM 'Lost'

UNION

SELECT
	date_value,
	last_week_user_channel,
	NULL,
	wau_category,
	users * -1 AS users
FROM aggregate_channels AS wa
WHERE wa.wau_category = 'Lost'
;

------------------------------------------------------------------------------------------------------------------------

SELECT
	wau_category,
	COUNT(*)
FROM scratch.robinpatel.wau_session_modelling wsm
WHERE date_value = '2023-12-03'
GROUP BY 1