USE WAREHOUSE pipe_xlarge
;


-- Monthly
SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp)                                                     AS month,
	COUNT(*)                                                                                       AS total_sessions,
	COUNT(DISTINCT stba.attributed_user_id)                                                        AS users,
	COUNT(DISTINCT IFF(stba.stitched_identity_type = 'se_user_id', stba.attributed_user_id, NULL)) AS members,
	COUNT(DISTINCT
		  IFF(stba.stitched_identity_type = 'se_user_id' AND stba.touch_hostname_territory IS DISTINCT FROM 'SE TECH',
			  stba.attributed_user_id,
			  NULL))                                                                               AS members_wihout_se_tech,
	SUM(IFF(stba.touch_hostname_territory IN ('UK', 'DE', 'AT', 'CH'), 1, 0))                      AS real_total_sessions,
	SUM(IFF(stba.touch_duration_seconds = 0, 1, 0))                                                AS zero_second_sessions,
	zero_second_sessions / total_sessions                                                          AS zero_second_session_percentage
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_experience = 'native app ios'
--   AND stba.touch_start_tstamp >= '2020-01-01'

GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------
-- Weekly
SELECT
	DATE_TRUNC(WEEK, stba.touch_start_tstamp)                                                      AS week,
	sc.se_week,
	sc.se_year,
	COUNT(*)                                                                                       AS total_sessions,
	COUNT(DISTINCT stba.attributed_user_id)                                                        AS users,
	COUNT(DISTINCT IFF(stba.stitched_identity_type = 'se_user_id', stba.attributed_user_id, NULL)) AS members,
	COUNT(DISTINCT
		  IFF(stba.stitched_identity_type = 'se_user_id' AND stba.touch_hostname_territory IS DISTINCT FROM 'SE TECH',
			  stba.attributed_user_id,
			  NULL))                                                                               AS members_wihout_se_tech,

	SUM(IFF(stba.touch_duration_seconds = 0, 1, 0))                                                AS zero_second_sessions,
	zero_second_sessions / total_sessions                                                          AS zero_second_session_percentage
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data.se_calendar sc ON stba.touch_start_tstamp::DATE = sc.date_value
WHERE stba.touch_experience = 'native app ios'
--   AND stba	SUM(IFF(stba.touch_hostname_territory IN ('UK', 'DE', 'AT', 'CH'), 1, 0))                      AS real_total_sessions,.touch_start_tstamp >= '2020-01-01'

GROUP BY 1, 2, 3
;

-- Weekly
SELECT
	DATE_TRUNC(WEEK, stba.touch_start_tstamp)                    AS week,
	sc.se_week,
	sc.se_year,
	COUNT(*)                                                     AS total_sessions,
	SUM(IFF(stba.touch_experience = 'native app ios', 1, 0))     AS native_app_ios_sessions,
	SUM(IFF(stba.touch_experience = 'native app android', 1, 0)) AS native_app_android_sessions
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data.se_calendar sc ON stba.touch_start_tstamp::DATE = sc.date_value
WHERE stba.touch_experience IN ('native app ios', 'native app android')
  AND stba.touch_hostname_territory IN ('UK', 'DE', 'AT', 'CH') -- does not include se tech
  AND stba.touch_start_tstamp >= '2020-01-01'
  AND stba.stitched_identity_type = 'se_user_id'                -- member sessions
GROUP BY 1, 2, 3
;


------------------------------------------------------------------------------------------------------------------------
-- looking at search
WITH
	user_searches AS (
		SELECT DISTINCT
			sts.touch_id
		FROM se.data.scv_touched_searches sts
		WHERE sts.triggered_by = 'user'
		  AND sts.event_tstamp >= CURRENT_DATE - 60
	)

SELECT
	stba.*,
	IFF(us.touch_id IS NOT NULL, TRUE, FALSE) AS has_user_search
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
	LEFT JOIN user_searches us ON stba.touch_id = us.touch_id
WHERE stba.touch_experience LIKE 'native app%'
  AND stba.touch_start_tstamp >= CURRENT_DATE - 60
;

USE WAREHOUSE pipe_xlarge
;

WITH
	model_data AS (
		SELECT
			stba.touch_id,
			stba.touch_start_tstamp,
			stba.num_user_searches,
			stba.touch_has_booking,
			stba.touch_experience
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
		WHERE stba.touch_experience LIKE 'native app%'
		  AND stba.touch_start_tstamp >= '2024-01-01'
	)
SELECT
	DATE_TRUNC(MONTH, md.touch_start_tstamp)                          AS month,
	md.touch_experience,
	COUNT(*)                                                          AS sessions,
	SUM(IFF(md.num_user_searches > 0, 1, 0))                          AS sessions_with_user_search,
	COUNT(IFF(md.touch_has_booking, md.touch_id, NULL))               AS sessions_with_booking,
	SUM(IFF(md.num_user_searches > 0 AND md.touch_has_booking, 1, 0)) AS sessions_with_user_search_and_booking
FROM model_data md
GROUP BY 1, 2
;


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge
;

-- production
SELECT
-- 	stba.touch_hostname_territory,
DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
sc.se_year,
COUNT(DISTINCT stba.attributed_user_id)    AS total_users,
COUNT(DISTINCT IFF(stba.touch_experience = 'native app ios', stba.attributed_user_id,
				   NULL))                  AS native_app_ios_users,
COUNT(DISTINCT IFF(stba.touch_experience = 'native app android', stba.attributed_user_id,
				   NULL))                  AS native_app_android_users
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data.se_calendar sc ON stba.touch_start_tstamp::DATE = sc.date_value
WHERE stba.touch_experience IN ('native app ios', 'native app android')
  AND stba.touch_hostname_territory IS DISTINCT FROM 'SE TECH'
  AND stba.touch_start_tstamp >= '2024-10-01'
  AND stba.touch_start_tstamp < '2024-10-23'
  AND stba.stitched_identity_type = 'se_user_id' -- member sessions
GROUP BY 1, 2
;

-- backup
SELECT
-- 	stba.touch_hostname_territory,
DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
sc.se_year,
COUNT(DISTINCT stba.attributed_user_id)    AS total_users,
COUNT(DISTINCT IFF(stba.touch_experience = 'native app ios', stba.attributed_user_id,
				   NULL))                  AS native_app_ios_users,
COUNT(DISTINCT IFF(stba.touch_experience = 'native app android', stba.attributed_user_id,
				   NULL))                  AS native_app_android_users
FROM data_vault_mvp.single_customer_view_stg_20241023.module_touch_basic_attributes stba
	INNER JOIN se.data.se_calendar sc ON stba.touch_start_tstamp::DATE = sc.date_value
WHERE stba.touch_experience IN ('native app ios', 'native app android')
--   AND stba.touch_hostname_territory IN ('UK', 'DE', 'AT', 'CH') -- does not include se tech
  AND stba.touch_start_tstamp >= '2024-10-01'
  AND stba.touch_start_tstamp < '2024-10-23'
  AND stba.stitched_identity_type = 'se_user_id' -- member sessions
GROUP BY 1, 2
;


SELECT
	sss.view_date,
	class,
	COUNT(*)
FROM data_vault_mvp.dwh.se_sale_snapshot sss
WHERE sss.view_date >= CURRENT_DATE - 2
GROUP BY 1, 2