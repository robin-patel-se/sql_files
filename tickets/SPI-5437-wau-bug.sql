CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_activity CLONE data_vault_mvp.dwh.user_activity
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_emails CLONE data_vault_mvp.dwh.user_emails
;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.weekly_active_users CLONE data_vault_mvp.bi.weekly_active_users
;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.weekly_active_users_20240729 CLONE data_vault_mvp.bi.weekly_active_users
;

ALTER TABLE data_vault_mvp_dev_robin.bi.weekly_active_users
	DROP COLUMN weekly_view_date
;

self_describing_task --include 'biapp/task_catalogue/dv/bi/tableau/demand_model/weekly_active_users.py'  --method 'run' --start '2024-07-22 00:00:00' --end '2024-07-22 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.grain CLONE data_vault_mvp.bi.grain
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.grain_20240729 CLONE data_vault_mvp.bi.grain
;

ALTER TABLE data_vault_mvp_dev_robin.bi.grain
	DROP COLUMN weekly_view_date
;

self_describing_task --include 'biapp/task_catalogue/dv/bi/tableau/demand_model/grain.py'  --method 'run' --start '2024-07-28 00:00:00' --end '2024-07-28 00:00:00'


SELECT *
FROM data_vault_mvp.dwh.user_activity ua
WHERE ua.date = CURRENT_DATE - 1
;


WITH
	email_opens AS (
		-- model daily user email opens for the past 8 days (to allow for whole week truncs)
		SELECT
			ue.shiro_user_id,
			ue.date,
			COUNT(*) AS email_opens
		FROM data_vault_mvp.dwh.user_emails ue
		WHERE ue.date >= DATEADD(DAY, -8, CURRENT_DATE)::DATE
		  AND ue.date <= TO_DATE(CURRENT_DATE)
		  AND ue.opens > 0 --any user with an open
		GROUP BY 1, 2
	),
	sessions AS (
		-- model daily user sessions for the past 8 days (to allow for whole week truncs)
		SELECT
			stba.attributed_user_id,
			stba.touch_start_tstamp::DATE            AS date,
			COUNT(DISTINCT stba.touch_id)            AS sessions,
			COUNT(DISTINCT
				  IFF(se.data.platform_from_touch_experience(stba.touch_experience) IS DISTINCT FROM 'native app',
					  stba.touch_id, NULL))          AS web_sessions,
			COUNT(DISTINCT IFF(se.data.platform_from_touch_experience(stba.touch_experience) = 'native app',
							   stba.touch_id, NULL)) AS app_sessions
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
		WHERE stba.touch_start_tstamp >= DATEADD(DAY, -8, CURRENT_DATE)::DATE
		  AND stba.touch_start_tstamp <= TO_DATE(CURRENT_DATE)
		  AND stba.stitched_identity_type = 'se_user_id' -- only sessions that have been associated to a se_user_id
		GROUP BY 1, 2
	),
	modelling AS (
		-- create a complete list of sessions and opens and enrich with fields necessary for agregation
		SELECT
			COALESCE(s.attributed_user_id, eo.shiro_user_id)::NUMBER                         AS shiro_user_id,
			COALESCE(s.date, eo.date)                                                        AS date,
			se_dev_robin.data.member_recency_status(sua.signup_tstamp,
													COALESCE(s.date, eo.date))               AS member_recency_status,
			se_dev_robin.data.posa_category_from_territory(sua.current_affiliate_territory)  AS current_affiliate_territory,
			se_dev_robin.data.posa_category_from_territory(sua.original_affiliate_territory) AS original_affiliate_territory,
			sessions,
			web_sessions,
			app_sessions,
			email_opens
		FROM sessions s
			FULL OUTER JOIN email_opens eo ON s.attributed_user_id = eo.shiro_user_id AND s.date = eo.date
			INNER JOIN      data_vault_mvp_dev_robin.dwh.user_attributes sua
							ON COALESCE(s.attributed_user_id, eo.shiro_user_id)::NUMBER = sua.shiro_user_id
	)
SELECT
	--aggregate to demand model grain
	member_recency_status,
	current_affiliate_territory,
	original_affiliate_territory,
	DATE_TRUNC(WEEK, date)                                                         AS week,
	COUNT(DISTINCT IFF(app_sessions > 0 OR web_sessions > 0, shiro_user_id, NULL)) AS wau_count_of_unique_users,
	COUNT(DISTINCT IFF(app_sessions > 0, shiro_user_id, NULL))                     AS wau_count_of_unique_user_app_sessions,
	COUNT(DISTINCT IFF(web_sessions > 0, shiro_user_id, NULL))                     AS wau_count_of_unique_user_web_sessions,
	COUNT(DISTINCT IFF(email_opens > 0, shiro_user_id, NULL))                      AS wau_count_of_unique_user_email_sessions
FROM modelling m
GROUP BY 1, 2, 3, 4

;


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

SELECT
	date,
	SUM(wau_count_of_unique_users)
FROM data_vault_mvp_dev_robin.bi.weekly_active_users
WHERE date BETWEEN '2024-01-01' AND CURRENT_DATE
GROUP BY 1
ORDER BY 1
;


SELECT
	date,
	SUM(wau_count_of_unique_users)
FROM data_vault_mvp_dev_robin.bi.weekly_active_users__step04__hash_data
WHERE date BETWEEN '2024-01-01' AND CURRENT_DATE
GROUP BY 1
;



SELECT
	se_dev_robin.data.member_recency_status(sua.signup_tstamp, ua.date::TIMESTAMP)   AS member_recency_status,
	se_dev_robin.data.posa_category_from_territory(sua.current_affiliate_territory)  AS current_affiliate_territory,
	se_dev_robin.data.posa_category_from_territory(sua.original_affiliate_territory) AS original_affiliate_territory,
	ua.date,
	SUM(IFF(ua.app_sessions_7d > 0 OR ua.web_sessions_7d > 0, 1, 0))                 AS wau,
	SUM(IFF(ua.app_sessions_7d > 0, 1, 0))                                           AS app_wau,
	SUM(IFF(ua.web_sessions_7d > 0, 1, 0))                                           AS web_wau,
	SUM(IFF(ua.emails_7d > 0, 1, 0))                                                 AS email_wau
FROM data_vault_mvp_dev_robin.dwh.user_activity ua
	INNER JOIN data_vault_mvp_dev_robin.dwh.user_attributes sua ON ua.shiro_user_id = sua.shiro_user_id
WHERE ua.date >= '2024-07-27 04:30:00'::DATE
  AND (ua.app_sessions_7d > 0
	OR ua.web_sessions_7d > 0
	OR ua.emails_7d > 0)
GROUP BY 1, 2, 3,
;


SELECT *
FROM data_vault_mvp_dev_robin.bi.weekly_active_users__step03__model_data
WHERE date = CURRENT_DATE - 7


SELECT
	date,
	SUM(wau_count_of_unique_users)
FROM data_vault_mvp_dev_robin.bi.weekly_active_users
WHERE date BETWEEN '2024-01-01' AND CURRENT_DATE
GROUP BY 1
ORDER BY 1
;

SELECT
	event_date,
	SUM(wau_count_of_unique_users)
FROM data_vault_mvp_dev_robin.bi.grain
WHERE event_date BETWEEN '2024-01-01' AND CURRENT_DATE
GROUP BY 1
ORDER BY 1
;

SELECT
	date,
	SUM(wau_count_of_unique_users)
FROM data_vault_mvp_dev_robin.bi.weekly_active_users
WHERE date BETWEEN CURRENT_DATE - 14 AND CURRENT_DATE
GROUP BY 1
ORDER BY 1
;


SELECT
	date,
	SUM(wau_count_of_unique_users)
FROM data_vault_mvp_dev_robin.bi.weekly_active_users__step04__hash_data
WHERE date BETWEEN CURRENT_DATE - 14 AND CURRENT_DATE
GROUP BY 1
ORDER BY 1
;


SELECT
	event_date,
	SUM(wau_count_of_unique_users)
FROM data_vault_mvp_dev_robin.bi.grain__step04__join_with_calendar
WHERE event_date BETWEEN CURRENT_DATE - 14 AND CURRENT_DATE
GROUP BY 1
ORDER BY 1
;

SELECT
-- 	w.id                           AS id,
-- 	w.member_recency_status        AS member_recency_status,
-- 	w.current_affiliate_territory  AS current_affiliate_territory,
-- 	w.original_affiliate_territory AS original_affiliate_territory,
w.date                                         AS event_date,
SUM(w.wau)                                     AS wau,
SUM(w.app_wau)                                 AS app_wau,
SUM(w.web_wau)                                 AS web_wau,
SUM(w.email_wau)                               AS email_wau,
SUM(w.wau_count_of_unique_users)               AS wau_count_of_unique_users,
SUM(w.wau_count_of_unique_user_web_sessions)   AS wau_count_of_unique_user_web_sessions,
SUM(w.wau_count_of_unique_user_app_sessions)   AS wau_count_of_unique_user_app_sessions,
SUM(w.wau_count_of_unique_user_email_sessions) AS wau_count_of_unique_user_email_sessions
FROM data_vault_mvp.bi.weekly_active_users w
WHERE date BETWEEN '2024-01-01' AND CURRENT_DATE
GROUP BY 1
;

SELECT
	stmeoi.event_tstamp::DATE                                                                 AS date,
	COUNT(*)                                                                                  AS bookings,
	COUNT(DISTINCT IFF(fb.booking_status_type IN ('live', 'cancelled'), fb.booking_id, NULL)) AS live_canx_bookings
FROM se.data.scv_touched_module_events_of_interest stmeoi
	LEFT JOIN se.data.fact_booking fb ON stmeoi.booking_id = fb.booking_id
WHERE stmeoi.event_tstamp::date = '2024-07-28'
  AND stmeoi.event_category = 'transaction'
  AND fb.territory = 'UK'
GROUP BY 1
;


WITH
	opensite_ff AS (
		SELECT DISTINCT
			stff.touch_id
		FROM se.data.scv_touched_feature_flags stff
		WHERE stff.feature_flag LIKE 'abtest.opensite.%'
	)

SELECT
	stt.event_tstamp::DATE,
	COUNT(*)
FROM se.data.scv_touched_transactions stt
	INNER JOIN se.data.scv_touch_basic_attributes stba
			   ON stt.touch_id = stba.touch_id AND stba.touch_start_tstamp >= '2024-01-01'
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
	INNER JOIN se.data.fact_booking fb
			   ON stt.booking_id = fb.booking_id AND fb.booking_status_type IN ('live', 'cancelled')
	INNER JOIN opensite_ff ff ON stt.touch_id = ff.touch_id
WHERE stba.touch_start_tstamp::DATE = '2024-07-28'
  AND stmc.touch_affiliate_territory = 'UK'
GROUP BY 1
;


------------------------------------------------------------------------------------------------------------------------


SELECT
-- 	w.id                           AS id,
-- 	w.member_recency_status        AS member_recency_status,
-- 	w.current_affiliate_territory  AS current_affiliate_territory,
-- 	w.original_affiliate_territory AS original_affiliate_territory,
w.week                                         AS event_date,
SUM(w.wau_count_of_unique_users)               AS wau_count_of_unique_users,
SUM(w.wau_count_of_unique_user_web_sessions)   AS wau_count_of_unique_user_web_sessions,
SUM(w.wau_count_of_unique_user_app_sessions)   AS wau_count_of_unique_user_app_sessions,
SUM(w.wau_count_of_unique_user_email_sessions) AS wau_count_of_unique_user_email_sessions
FROM data_vault_mvp_dev_robin.bi.weekly_active_users__step02__week_beginning_monday_sessions w
WHERE week BETWEEN '2024-01-01' AND CURRENT_DATE
GROUP BY 1
;


SELECT *
FROM se.bi.weekly_active_users wau
;

SELECT *
FROM se.bi.grain g
;

SELECT GET_DDL('table', 'se.bi.weekly_active_users')
;

USE ROLE pipelinerunner
;

CREATE OR REPLACE VIEW se.bi.weekly_active_users
AS
(
SELECT
	id,
	member_recency_status,
	current_affiliate_territory,
	original_affiliate_territory,
	date,
	wau,
	app_wau,
	web_wau,
	email_wau,
	wau_count_of_unique_users,
	wau_count_of_unique_user_web_sessions,
	wau_count_of_unique_user_app_sessions,
	wau_count_of_unique_user_email_sessions
FROM data_vault_mvp.bi.weekly_active_users
	)
;


SELECT GET_DDL('table', 'se.bi.grain')
;

CREATE OR REPLACE VIEW se.bi.grain AS
(
SELECT
	id,
	member_recency_status,
	current_affiliate_territory,
	original_affiliate_territory,
	event_date,
	day_name,
	year,
	se_year,
	se_week,
	month,
	month_name,
	day_of_month,
	day_of_week,
	week_start,
	yesterday,
	yesterday_last_week,
	this_week,
	this_week_wtd,
	last_week,
	last_week_wtd,
	this_month,
	this_month_mtd,
	last_month,
	last_month_mtd,
	sends,
	opens,
	unique_opens,
	clicks,
	unique_clicks,
	mau,
	app_mau,
	web_mau,
	email_mau,
	wau,
	app_wau,
	web_wau,
	email_wau,
	signups,
	mau_month_to_date,
	app_mau_month_to_date,
	web_mau_month_to_date,
	email_mau_month_to_date,
	wau_count_of_unique_users,
	wau_count_of_unique_user_web_sessions,
	wau_count_of_unique_user_app_sessions,
	wau_count_of_unique_user_email_sessions
FROM data_vault_mvp.bi.grain
	)
;

USE ROLE tableau
;

SELECT *
FROM se.bi.grain
;

SELECT *
FROM se.bi.weekly_active_users wau
;

GRANT SELECT ON TABLE se.bi.grain TO ROLE tableau

GRANT SELECT ON TABLE se.bi.weekly_active_users TO ROLE tableau
;


SELECT
	to_char(esl.request_time, 'dd : hh'),
	COUNT(*)
FROM latest_vault.iterable.email_send_log esl
WHERE esl.request_time >= current_date -7
GROUP BY 1
;


SELECT * FROM se.data.sales_kingfisher sk WHERE id = 'A58987'