SELECT
	g.event_date,
	g.day_name,
	g.se_week,
	SUM(g.wau_count_of_unique_users) AS wau_count_of_unique_users
FROM data_vault_mvp.bi.grain g
WHERE g.se_week >= 20
  AND g.year = '2024'
GROUP BY 1, 2, 3


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_activity CLONE data_vault_mvp.dwh.user_activity
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes
;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.weekly_active_users CLONE data_vault_mvp.bi.weekly_active_users
;


self_describing_task --include 'biapp/task_catalogue/dv/bi/tableau/demand_model/weekly_active_users.py'  --method 'run' --start '2024-07-14 00:00:00' --end '2024-07-14 00:00:00'


-- production

SELECT
	date,
	SUM(wau_count_of_unique_users)
FROM data_vault_mvp.bi.weekly_active_users
WHERE date >= CURRENT_DATE - 30
GROUP BY 1

-- observe that in dev new field will be populated
SELECT
	date,
	SUM(wau_count_of_unique_users)
FROM data_vault_mvp_dev_robin.bi.weekly_active_users
WHERE date >= CURRENT_DATE - 30
GROUP BY 1
;

-- sense check the figure with underlying data
WITH
	wau_sessions AS (
		SELECT
			ua.shiro_user_id,
			ua.date - 1 AS view_date,
			ua.web_sessions_1d,
			ua.app_sessions_1d,
			ua.emails_1d
		FROM data_vault_mvp_dev_robin.dwh.user_activity ua
		WHERE ua.date - 1 BETWEEN '2024-07-08' AND '2024-07-14'
		  AND (ua.web_sessions_1d > 0
			OR ua.app_sessions_1d > 0
			)
	)
SELECT
	COUNT(DISTINCT ws.shiro_user_id) AS users
FROM wau_sessions ws


-- checking 'grain' dataset with new wau table
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.sends CLONE data_vault_mvp.bi.sends
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.opens CLONE data_vault_mvp.bi.opens
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.clicks CLONE data_vault_mvp.bi.clicks
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.monthly_active_users CLONE data_vault_mvp.bi.monthly_active_users
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.sign_ups CLONE data_vault_mvp.bi.sign_ups
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_grain CLONE data_vault_mvp.bi.session_grain
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.grain CLONE data_vault_mvp.bi.grain
;

self_describing_task --include 'biapp/task_catalogue/dv/bi/tableau/demand_model/grain.py'  --method 'run' --start '2024-07-14 00:00:00' --end '2024-07-14 00:00:00'

-- production

SELECT
	event_date,
	SUM(wau_count_of_unique_users)
FROM data_vault_mvp.bi.grain
WHERE event_date BETWEEN '2024-01-01' AND CURRENT_DATE
GROUP BY 1
;

-- development
SELECT
	event_date,
	SUM(wau_count_of_unique_users)
FROM data_vault_mvp_dev_robin.bi.grain
WHERE event_date BETWEEN '2024-01-01' AND CURRENT_DATE
GROUP BY 1
;


USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data_20240716 CLONE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data
;

ALTER TABLE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data
	RENAME COLUMN three_most_recent_stayed_booking_details TO five_most_recent_stayed_booking_details
;


SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_transaction_base_data


SELECT
	sc.date_value,
	sc.se_week
FROM se.data.se_calendar sc
WHERE sc.year = 2024
;


SELECT
	ses.contexts_com_secretescapes_search_context_1
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE
  AND ses.se_brand = 'SE Brand'
  AND ses.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND ARRAY_SIZE(ses.contexts_com_secretescapes_search_context_1[0]['results_list']::ARRAY) > 0
;

WITH
	user_bookings AS (
		SELECT
			fb.shiro_user_id,
			COUNT(DISTINCT fb.booking_id) AS bookings
		FROM se.data.fact_booking fb
		WHERE fb.booking_status_type IN ('live', 'cancelled')
		GROUP BY 1
	)
SELECT * FROM user_bookings ub WHERE ub.bookings = 502

-- 945 - 78431846
-- 502 - 9523266
/*
SELECT ub.bookings,
       count(DISTINCT ub.shiro_user_id) AS bookers
FROM user_bookings ub
GROUP BY 1
ORDER BY 1 DESC
 */

SELECT * FROM se.data_pii.se_user_attributes sua;

SELECT * FROm se.data_pii.scv_touch_basic_attributes stba WHERE stba.is_se_internal_touch = TRUE