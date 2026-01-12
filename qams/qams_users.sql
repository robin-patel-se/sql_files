SELECT
	sua.shiro_user_id,
	sua.signup_tstamp,
	sua.original_affiliate_territory,
	sua.current_affiliate_territory
FROM se.data.se_user_attributes sua
;



SELECT *
FROM se.data.user_segmentation us
;


-- https://docs.google.com/spreadsheets/d/1VqW8qBGuyXnVIQ--Lw0Ro3QpmRwDfu5kOlkEXIeV0sg/edit#gid=1149898206

-- original cohort -- simply the month they signed up
-- activation cohort -- once a user has met certain criteria update this month
-- if user has a session and they've signed up in the last 6 months then use their original cohort as their activation cohort
-- if a member has a transaction and signed up on the same day
-- a member has a session and their last session was more than 6 months ago
-- reactivated cohort -- after a period of inactivity, month when they have reactivated


------------------------------------------------------------------------------------------------------------------------
-- Assumptions list
-- using gross bookings -- otherwise activation cohort might change

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.qam_base_data AS (
	WITH
		session_bookings AS (
			SELECT
				stt.touch_id,
				COUNT(*)                                           AS gross_bookings,
				SUM(fb.gross_revenue_gbp_constant_currency)        AS gross_revenue_gbp,
				SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
			FROM se.data.scv_touched_transactions stt
				INNER JOIN se.data.fact_booking fb ON stt.booking_id = fb.booking_id
				AND fb.booking_status_type IN ('live', 'cancelled')
			GROUP BY 1
		)
			,
		modelling_data AS (
			SELECT
				stba.attributed_user_id                AS shiro_user_id,
				sua.original_affiliate_territory,
				sua.signup_tstamp::DATE                AS sign_up_date,
				stba.touch_start_tstamp::DATE          AS session_date,
				COUNT(DISTINCT stba.touch_id)          AS sessions,
				COALESCE(SUM(sb.gross_bookings), 0)    AS gross_bookings,
				COALESCE(SUM(sb.gross_revenue_gbp), 0) AS gross_revenue_gbp,
				COALESCE(SUM(sb.margin_gbp), 0)        AS margin_gbp
			FROM se.data_pii.scv_touch_basic_attributes stba
				INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
				-- disregard sessions before signup
				AND stba.touch_start_tstamp::DATE >= sua.signup_tstamp::DATE
				LEFT JOIN  session_bookings sb ON stba.touch_id = sb.touch_id
			WHERE stba.touch_se_brand = 'SE Brand'
			  AND stba.stitched_identity_type = 'se_user_id'
			GROUP BY 1, 2, 3, 4
		)

	SELECT
		shiro_user_id,
		original_affiliate_territory,
		sign_up_date,
		session_date,
		LAG(session_date) OVER (PARTITION BY md.shiro_user_id ORDER BY md.session_date ASC) AS last_session_date,
		sessions,
		gross_bookings,
		gross_revenue_gbp,
		margin_gbp
	FROM modelling_data md
)
;

SELECT *
FROM scratch.robinpatel.qam_base_data


SELECT *
FROM scratch.robinpatel.qam_base_data qbd
WHERE qbd.sign_up_date >= '2018-06-01'
;


SELECT *
FROM scratch.robinpatel.qam_base_data qbd
WHERE qbd.sign_up_date >= '2018-06-01'
;

QUALIFY count(*) OVER (PARTITION BY qbd.shiro_user_id)> 20


-- user with more than one session in 6 months of sign up

SELECT *
FROM scratch.robinpatel.qam_base_data qbd
WHERE qbd.shiro_user_id = 74166428

-- user with booking and multiple sessions within sign up
SELECT *
FROM scratch.robinpatel.qam_base_data qbd
WHERE qbd.shiro_user_id = 58542076

-- user  with booking on same day as sign up
SELECT *
FROM scratch.robinpatel.qam_base_data qbd
WHERE qbd.shiro_user_id = 75845010
;


/*
 -- find user with more than one reactivated date
 			SELECT *,
			   DATEDIFF(MONTH, qbd.last_session_date, qbd.session_date) AS diff_between_session_dates
			FROM scratch.robinpatel.qam_base_data qbd
			WHERE DATEDIFF(MONTH, qbd.last_session_date, qbd.session_date) >= 6

 */

--user with more than one reactivated date
SELECT *
FROM scratch.robinpatel.qam_base_data qbd
WHERE qbd.shiro_user_id = 19016267

------------------------------------------------------------------------------------------------------------------------
-- Activated cohort
------------------------------------------------------------------------------------------------------------------------
/*WITH
	sign_up_bookers AS (
-- users that signed up and booked on same day
		SELECT
			qbd.sign_up_date AS cohort_activated_date,
			qbd.shiro_user_id,
			qbd.original_affiliate_territory,
			qbd.sign_up_date,
			qbd.session_date
		FROM scratch.robinpatel.qam_base_data qbd
		WHERE qbd.session_date = qbd.sign_up_date AND qbd.gross_bookings > 0
-- 248K users
	),
	session_two_within_six_months AS (
-- users that signed up and have a second session within 6 months of sign up
		SELECT
			qbd.sign_up_date AS cohort_activated_date,
			qbd.shiro_user_id,
			qbd.original_affiliate_territory,
			qbd.sign_up_date,
			qbd.session_date
		FROM scratch.robinpatel.qam_base_data qbd
-- session is within 6 months of sign up (first month will be 0)
		WHERE DATEDIFF(MONTH, qbd.sign_up_date, qbd.session_date) < 6
--   to limit to just the first session session since sign up
		  AND qbd.last_session_date = qbd.sign_up_date
-- 11M users
	),
	session_two_outsite_six_months AS (
-- users that signed up and have a second session outside 6 months of sign up
		SELECT
			qbd.session_date AS cohort_activated_date,
			qbd.shiro_user_id,
			qbd.original_affiliate_territory,
			qbd.sign_up_date,
			qbd.session_date
		FROM scratch.robinpatel.qam_base_data qbd
-- session is outside 6 months of sign up (first month will be 0)
		WHERE DATEDIFF(MONTH, qbd.sign_up_date, qbd.session_date) >= 6
--   to limit to just the first session session since sign up
		  AND qbd.last_session_date = qbd.sign_up_date
-- 1.9M users
	),
	stack AS (

		SELECT *,
			   'booking on sign up day' AS cohort_activated_reason
		FROM sign_up_bookers
		UNION ALL
		SELECT *,
			   'second session within 6 months of sign up' AS cohort_activated_reason
		FROM session_two_within_six_months
		UNION ALL
		SELECT *,
			   'second session outside 6 months of sign up' AS cohort_activated_reason
		FROM session_two_outsite_six_months
	)
SELECT *
FROM stack s
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.shiro_user_id
	ORDER BY
		s.cohort_activated_date,
		CASE s.cohort_activated_reason
			WHEN 'booking on sign up day' THEN 1
			WHEN 'second session within 6 months of sign up' THEN 2
			WHEN 'second session outside 6 months of sign up' THEN 3
		END ASC
	) = 1
-- QUALIFY COUNT(*) OVER (PARTITION BY stack.shiro_user_id) > 1
*/

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.qam_activated_users AS (
	WITH
		session_level_cohort_activation AS (
-- session level categorisation
			SELECT
				CASE
					WHEN
						qbd.session_date = qbd.sign_up_date AND qbd.gross_bookings > 0
						THEN 'booking on sign up day'
					WHEN DATEDIFF(MONTH, qbd.sign_up_date, qbd.session_date) < 6
						AND qbd.sign_up_date != qbd.session_date
-- 						AND qbd.last_session_date = qbd.sign_up_date
						THEN 'second session within 6 months of sign up'
					WHEN DATEDIFF(MONTH, qbd.sign_up_date, qbd.session_date) >= 6
-- 						AND qbd.last_session_date = qbd.sign_up_date
						THEN 'second session outside 6 months of sign up'
				END                   AS cohort_activated_reason,

				IFF(cohort_activated_reason = 'second session outside 6 months of sign up',
					qbd.session_date,
					qbd.sign_up_date) AS cohort_activated_date,
				qbd.shiro_user_id,
				qbd.original_affiliate_territory,
				qbd.sign_up_date,
				qbd.session_date,
				qbd.last_session_date,
				qbd.sessions,
				qbd.gross_bookings
			FROM scratch.robinpatel.qam_base_data qbd
		),
		user_level_assignment AS (
			SELECT *
			FROM session_level_cohort_activation slca
			WHERE slca.cohort_activated_reason IS NOT NULL
			QUALIFY ROW_NUMBER() OVER (PARTITION BY slca.shiro_user_id
				ORDER BY
					slca.cohort_activated_date,
					CASE slca.cohort_activated_reason
						WHEN 'booking on sign up day' THEN 1
						WHEN 'second session within 6 months of sign up' THEN 2
						WHEN 'second session outside 6 months of sign up' THEN 3
					END ASC
				) = 1
		)
	SELECT *
	FROM user_level_assignment ula
)
;

------------------------------------------------------------------------------------------------------------------------
-- Reactivated cohort
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.qam_reactivated_users AS (
	WITH
		session_level_cohort_reactivation AS (
			-- take all sessions where the time between current session
			-- and previous session was greater or equal to 6 months apart
			SELECT *,
				   DATEDIFF(MONTH, qbd.last_session_date, qbd.session_date) AS diff_between_session_dates
			FROM scratch.robinpatel.qam_base_data qbd
			WHERE DATEDIFF(MONTH, qbd.last_session_date, qbd.session_date) >= 6
		)
	SELECT
		slcr.session_date AS cohort_reactivated_date,
		slcr.shiro_user_id,
		slcr.sign_up_date,
		slcr.session_date,
		slcr.last_session_date,
		slcr.sessions,
		slcr.diff_between_session_dates
	FROM session_level_cohort_reactivation slcr
-- return the most recent reactivation date per user
	QUALIFY ROW_NUMBER() OVER (PARTITION BY slcr.shiro_user_id ORDER BY slcr.session_date DESC) = 1
)
;

------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
-- combine activated and reactivated
------------------------------------------------------------------------------------------------------------------------

USE WAREHOUSE pipe_xlarge
;

SELECT *
FROM scratch.robinpatel.qam_user_base
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.qam_user_base AS (
	SELECT
		sua.shiro_user_id,
		sua.signup_tstamp::DATE                                         AS sign_up_date,
		sua.membership_account_status,
		qau.cohort_activated_reason,
		qau.cohort_activated_date,
		qau.cohort_activated_date IS NOT NULL                           AS qualified_active_member,
		IFF(qualified_active_member, qru.cohort_reactivated_date, NULL) AS cohort_reactivated_date
	FROM se.data.se_user_attributes sua
		LEFT JOIN scratch.robinpatel.qam_activated_users qau ON sua.shiro_user_id = qau.shiro_user_id
		LEFT JOIN scratch.robinpatel.qam_reactivated_users qru ON sua.shiro_user_id = qru.shiro_user_id
)
;


SELECT *
FROM scratch.robinpatel.qam_user_base
;

WITH
	user_cohorts AS
		(
			SELECT *
			FROM scratch.robinpatel.qam_user_base qub
			WHERE qub.sign_up_date >= '2018-01-01'
-- WHERE sua.shiro_user_id = 10527537
		)
-- SELECT
-- 	qualified_active_member,
-- 	COUNT(*)
-- FROM user_cohorts uc
-- GROUP BY 1
-- ;

-- Members signedup since 2018
-- 33,957,009

-- QUALIFIED_ACTIVE_MEMBER	COUNT(*)
-- true		21,981,989
-- false	11,975,020


SELECT
	DATE_TRUNC(MONTH, uc.sign_up_date)            AS original_cohort_month,
	DATE_TRUNC(MONTH, uc.cohort_activated_date)   AS activated_cohort_month,
	DATE_TRUNC(MONTH, uc.cohort_reactivated_date) AS reactivated_cohort_month,
	COUNT(*)                                      AS users,
FROM user_cohorts uc
WHERE uc.qualified_active_member
GROUP BY 1, 2, 3
;


CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.qam_event_data AS (
	SELECT
		qbd.shiro_user_id,
		qub.membership_account_status,
		qbd.original_affiliate_territory,
		qbd.sign_up_date,
		qub.cohort_activated_date,
		qub.cohort_activated_reason,
		qub.cohort_reactivated_date,
		qub.qualified_active_member,
		qbd.session_date,
		qbd.sessions,
		qbd.gross_bookings,
		qbd.gross_revenue_gbp,
		qbd.margin_gbp,
		qbd.last_session_date,
		DATEDIFF(MONTH, qbd.last_session_date, qbd.session_date)                                                                                 AS diff_between_session_dates,
		IFF(diff_between_session_dates >= 6, qbd.session_date, NULL)                                                                             AS reactivated_session_date,
		LAST_VALUE(reactivated_session_date)
				   IGNORE NULLS OVER (PARTITION BY qbd.shiro_user_id ORDER BY qbd.session_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS reactivated_session_date_pit
	FROM scratch.robinpatel.qam_base_data qbd
		INNER JOIN scratch.robinpatel.qam_user_base qub ON qbd.shiro_user_id = qub.shiro_user_id
-- WHERE qbd.shiro_user_id = 19016267
)
;


SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_event_data
;



SELECT *
FROM scratch.robinpatel.qam_event_data
WHERE qam_event_data.shiro_user_id = 10527537
;

-- for tableau
SELECT
	DATE_TRUNC(MONTH, qed.session_date)            AS event_month,
	qualified_active_member,
	DATE_TRUNC(MONTH, qed.sign_up_date)            AS original_cohort_month,
	DATE_TRUNC(MONTH, qed.cohort_activated_date)   AS activated_cohort_month,
	DATE_TRUNC(MONTH, qed.cohort_reactivated_date) AS reactivated_cohort_month,
	COUNT(DISTINCT qed.shiro_user_id)              AS users,
	SUM(qed.gross_bookings)                        AS gross_bookings,
	SUM(qed.gross_revenue_gbp)                     AS gross_gross_revenue_gbp,
	SUM(qed.margin_gbp)                            AS gross_margin_gbp
FROM scratch.robinpatel.qam_event_data qed
GROUP BY 1, 2, 3, 4, 5
;



SELECT
	qed.session_date,
	qed.qualified_active_member,
	qed.sign_up_date,
	qed.cohort_activated_date,
	qed.cohort_reactivated_date,
	qed.shiro_user_id,
	qed.gross_bookings,
	qed.gross_revenue_gbp,
	qed.margin_gbp
FROM scratch.robinpatel.qam_event_data qed


SELECT *
FROM scratch.robinpatel.qam_event_data qed
WHERE qed.shiro_user_id = 19016267

------------------------------------------------------------------------------------------------------------------------

-- is cohort of sign up vs ACTIVATED cohort
-- take all sessions where the time between current session
-- and previous session was greater or equal to 6 months apart


SELECT
	DATE_TRUNC(MONTH, qam_event_data.session_date) AS month,
	COUNT(DISTINCT qam_event_data.shiro_user_id)
FROM scratch.robinpatel.qam_event_data
GROUP BY 1
;


SELECT *
FROM se.data.se_user_attributes sua
WHERE DATE_TRUNC(YEAR, sua.signup_tstamp) = '2018-01-01'



SELECT
	DATE_TRUNC(MONTH, qed.session_date)            AS event_month,
	qualified_active_member,
	DATE_TRUNC(MONTH, qed.sign_up_date)            AS original_cohort_month,
	DATE_TRUNC(MONTH, qed.cohort_activated_date)   AS activated_cohort_month,
	DATE_TRUNC(MONTH, qed.cohort_reactivated_date) AS reactivated_cohort_month,
	COUNT(DISTINCT qed.shiro_user_id)              AS users,
	SUM(qed.gross_bookings)                        AS gross_bookings,
	SUM(qed.gross_revenue_gbp)                     AS gross_gross_revenue_gbp,
	SUM(qed.margin_gbp)                            AS gross_margin_gbp
FROM scratch.robinpatel.qam_event_data qed
GROUP BY 1, 2, 3, 4, 5
;


SELECT *
FROM scratch.robinpatel.qam_event_data qed
;

USE ROLE pipelinerunner
;

CREATE SCHEMA collab.qams
;

GRANT USAGE ON SCHEMA collab.qams TO ROLE data_team_basic
;

GRANT USAGE ON SCHEMA collab.qams TO ROLE tableau
;

GRANT SELECT ON ALL VIEWS IN SCHEMA collab.qams TO ROLE data_team_basic
;

GRANT SELECT ON ALL TABLES IN SCHEMA collab.qams TO ROLE data_team_basic
;

GRANT SELECT ON ALL VIEWS IN SCHEMA collab.qams TO ROLE tableau
;

GRANT SELECT ON ALL TABLES IN SCHEMA collab.qams TO ROLE tableau
;

CREATE OR REPLACE VIEW collab.qams.qam_event_data AS
SELECT *
FROM scratch.robinpatel.qam_event_data qed
;

------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM collab.qams.qam_event_data
WHERE DATE_TRUNC(MONTH, session_date) = '2023-08-01'

------------------------------------------------------------------------------------------------------------------------
-- infer activity for the last

-- need to insert extra rows into this table with 0 metrics but just a user id and event date
SELECT
	shiro_user_id,
	membership_account_status,
	original_affiliate_territory,
	sign_up_date,
	cohort_activated_date,
	cohort_activated_reason,
	cohort_reactivated_date,
	qualified_active_member,
	session_date,
	sessions,
	gross_bookings,
	gross_revenue_gbp,
	margin_gbp,
	last_session_date,
	month_diff_between_session_dates,
	reactivated_session_date,
	reactivated_session_date_pit
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_05_event_data
;

-- 6 months from activated date

WITH
	monthly_calendar AS (
		SELECT
			sc.date_value
		FROM se.data.se_calendar sc
		WHERE sc.day_of_month = 1 --first day of each month
		  AND sc.date_value BETWEEN '2018-01-01' AND CURRENT_DATE
	)
SELECT
	qub.shiro_user_id,
	qub.membership_account_status,
	qub.original_affiliate_territory,
	qub.sign_up_date,
	qub.cohort_activated_date,
	qub.cohort_activated_reason,
	qub.cohort_reactivated_date,
	qub.qualified_active_member,
	mc.date_value                       AS session_date,
	0                                   AS sessions,
	0                                   AS gross_bookings,
	0                                   AS gross_revenue_gbp,
	0                                   AS margin_gbp,
	0                                   AS last_session_date,
	COALESCE(LAG(mc.date_value) OVER (PARTITION BY qub.shiro_user_id ORDER BY mc.date_value),
			 qub.cohort_activated_date) AS last_session_date
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_04_user_base qub
	INNER JOIN monthly_calendar mc
			   ON mc.date_value BETWEEN DATEADD(MONTH, 1, qub.cohort_activated_date) AND DATEADD(MONTH, 6, qub.cohort_activated_date)
WHERE qub.shiro_user_id = 3890836
;


-- 6 months from reactivated date
WITH
	monthly_calendar AS (
		SELECT
			sc.date_value
		FROM se.data.se_calendar sc
		WHERE sc.day_of_month = 1 --first day of each month
		  AND sc.date_value BETWEEN '2018-01-01' AND CURRENT_DATE
	)
SELECT
	qub.shiro_user_id,
	qub.membership_account_status,
	qub.original_affiliate_territory,
	qub.sign_up_date,
	qub.cohort_activated_date,
	qub.cohort_activated_reason,
	qub.cohort_reactivated_date,
	qub.qualified_active_member,
	mc.date_value AS session_date,
	0             AS sessions,
	0             AS gross_bookings,
	0             AS gross_revenue_gbp,
	0             AS margin_gbp
-- 	DATEDIFF(MONTH, qbd.last_session_date, qbd.session_date) AS month_diff_between_session_dates
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_01_base_session_data qbd
	INNER JOIN dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_04_user_base qub
			   ON qbd.shiro_user_id = qub.shiro_user_id
	INNER JOIN monthly_calendar mc
			   ON mc.date_value BETWEEN DATEADD(MONTH, 1, qbd.session_date) AND DATEADD(MONTH, 6, qbd.session_date)
WHERE DATEDIFF(MONTH, qbd.last_session_date, qbd.session_date) > 6
  AND qbd.shiro_user_id = 54912869
;

USE WAREHOUSE pipe_default

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_07_inferred_reactivation_event_data
WHERE shiro_user_id = 19016267

------------------------------------------------------------------------------------------------------------------------

SELECT
	ed.shiro_user_id,
	ed.membership_account_status,
	ed.original_affiliate_territory,
	ed.sign_up_date,
	ed.cohort_activated_date,
	ed.cohort_activated_reason,
	ed.cohort_reactivated_date,
	ed.qualified_active_member,
	ed.session_date,
	ed.sessions,
	ed.gross_bookings,
	ed.gross_revenue_gbp,
	ed.margin_gbp,
	ed.last_session_date,
	ed.month_diff_between_session_dates,
	ed.reactivated_session_date,
	ed.reactivated_session_date_pit,
	'event data' AS data_source
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_05_event_data ed
UNION ALL
SELECT
	ad.shiro_user_id,
	ad.membership_account_status,
	ad.original_affiliate_territory,
	ad.sign_up_date,
	ad.cohort_activated_date,
	ad.cohort_activated_reason,
	ad.cohort_reactivated_date,
	ad.qualified_active_member,
	ad.session_date,
	ad.sessions,
	ad.gross_bookings,
	ad.gross_revenue_gbp,
	ad.margin_gbp,
	ad.last_session_date,
	NULL                       AS month_diff_between_session_dates,
	NULL                       AS reactivated_session_date,
	NULL                       AS reactivated_session_date_pit,
	'inferred activiated data' AS data_source
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_06_inferred_activation_event_data ad
UNION ALL
SELECT
	rd.shiro_user_id,
	rd.membership_account_status,
	rd.original_affiliate_territory,
	rd.sign_up_date,
	rd.cohort_activated_date,
	rd.cohort_activated_reason,
	rd.cohort_reactivated_date,
	rd.qualified_active_member,
	rd.session_date,
	rd.sessions,
	rd.gross_bookings,
	rd.gross_revenue_gbp,
	rd.margin_gbp,
	rd.last_session_date,
	NULL                         AS month_diff_between_session_dates,
	NULL                         AS reactivated_session_date,
	NULL                         AS reactivated_session_date_pit,
	'inferred reactiviated data' AS data_source
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_07_inferred_reactivation_event_data rd



SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_08_stack_event_data
;


SELECT
	DATE_TRUNC(MONTH, qed.session_date)            AS event_month,
	qualified_active_member,
	DATE_TRUNC(MONTH, qed.sign_up_date)            AS original_cohort_month,
	DATE_TRUNC(MONTH, qed.cohort_activated_date)   AS activated_cohort_month,
	DATE_TRUNC(MONTH, qed.cohort_reactivated_date) AS reactivated_cohort_month,
	COUNT(DISTINCT qed.shiro_user_id)              AS users,
	SUM(qed.gross_bookings)                        AS gross_bookings,
	SUM(qed.gross_revenue_gbp)                     AS gross_gross_revenue_gbp,
	SUM(qed.margin_gbp)                            AS gross_margin_gbp
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_08_stack_event_data qed
GROUP BY 1, 2, 3, 4, 5
;

SELECT
	DATE_TRUNC(MONTH, qed.session_date)            AS event_month,
	qualified_active_member,
	DATE_TRUNC(MONTH, qed.sign_up_date)            AS original_cohort_month,
	DATE_TRUNC(MONTH, qed.cohort_activated_date)   AS activated_cohort_month,
	DATE_TRUNC(MONTH, qed.cohort_reactivated_date) AS reactivated_cohort_month,
	COUNT(DISTINCT qed.shiro_user_id)              AS users,
	SUM(qed.gross_bookings)                        AS gross_bookings,
	SUM(qed.gross_revenue_gbp)                     AS gross_gross_revenue_gbp,
	SUM(qed.margin_gbp)                            AS gross_margin_gbp
FROM collab.qams.qam_event_data qed
GROUP BY 1, 2, 3, 4, 5


USE WAREHOUSE pipe_xlarge
;


SELECT
	COUNT(*)
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_08_stack_event_data
; -- 736,169,383
SELECT
	COUNT(*)
FROM collab.qams.qam_event_data
; --512,344,088


USE ROLE pipelinerunner
;

CREATE OR REPLACE VIEW collab.qams.qam_event_data COPY GRANTS AS
SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_08_stack_event_data
;


SELECT *
FROM collab.qams.qam_event_data
WHERE qam_event_data.shiro_user_id = 19016267



SELECT
	DATE_TRUNC(MONTH, qed.session_date)            AS event_month,
	qed.original_affiliate_territory,
	qualified_active_member,
	DATE_TRUNC(MONTH, qed.sign_up_date)            AS original_cohort_month,
	DATE_TRUNC(MONTH, qed.cohort_activated_date)   AS activated_cohort_month,
	DATE_TRUNC(MONTH, qed.cohort_reactivated_date) AS reactivated_cohort_month,
	COUNT(DISTINCT qed.shiro_user_id)              AS users,
	SUM(qed.gross_bookings)                        AS gross_bookings,
	SUM(qed.gross_revenue_gbp)                     AS gross_gross_revenue_gbp,
	SUM(qed.margin_gbp)                            AS gross_margin_gbp
FROM collab.qams.qam_event_data qed
WHERE qed.shiro_user_id = 74166428
GROUP BY 1, 2, 3, 4, 5, 6



SELECT
	DATE_TRUNC(MONTH, qed.session_date)                 AS event_month,
	DATE_TRUNC(MONTH, qed.sign_up_date)                 AS original_cohort_month,
	DATE_TRUNC(MONTH, qed.cohort_activated_date)        AS activated_cohort_month,
	DATE_TRUNC(MONTH, qed.reactivated_session_date_pit) AS reactivated_cohort_month,
	qed.qualified_active_member,
	qed.sign_up_date,
	qed.cohort_activated_date,
	qed.reactivated_session_date_pit,
	qed.session_date,
	qed.shiro_user_id,
	qed.membership_account_status,
	qed.original_affiliate_territory,
	qed.cohort_activated_reason,
	qed.cohort_reactivated_date,
	qed.qualified_active_member,
	qed.sessions,
	qed.gross_bookings,
	qed.gross_revenue_gbp,
	qed.margin_gbp,
	qed.last_session_date,
	qed.month_diff_between_session_dates,
	qed.reactivated_session_date,
	qed.data_source
FROM collab.qams.qam_event_data qed
WHERE qed.shiro_user_id = 19016267
;


-- understood that now activated pit filter will remove first session from qam counts, it will


SELECT *
FROM collab.qams.qam_event_data qed
WHERE qed.shiro_user_id = 19016267
;

SELECT
	DATE_TRUNC(MONTH, qed.session_date)                 AS event_month,
	DATE_TRUNC(MONTH, qed.sign_up_date)                 AS original_cohort_month,
	DATE_TRUNC(MONTH, qed.cohort_activated_date)        AS activated_cohort_month,
	DATE_TRUNC(MONTH, qed.reactivated_session_date_pit) AS reactivated_cohort_month_pit,
	qed.qualified_active_member,
	qed.qualified_active_member_pit,
	qed.original_affiliate_territory,
	CASE
		WHEN event_month = original_cohort_month
			AND original_cohort_month = activated_cohort_month
			THEN 'Brand New'
		WHEN event_month = activated_cohort_month
			AND DATEDIFF(MONTH, original_cohort_month, event_month) >= 6
			THEN 'Late Activator'
		WHEN event_month = reactivated_cohort_month_pit
			AND DATEDIFF(MONTH, original_cohort_month, event_month) >= 6
			AND DATEDIFF(MONTH, activated_cohort_month, event_month) >= 6
			THEN 'Reactivated'
		WHEN qed.data_source = 'event data'
			AND DATEDIFF(MONTH, GREATEST(activated_cohort_month, reactivated_cohort_month_pit), event_month) < 6
			THEN 'Retained Active in Month'
		WHEN qed.data_source IN ('inferred activiated data', 'inferred reactiviated data')
			AND DATEDIFF(MONTH, GREATEST(activated_cohort_month, reactivated_cohort_month_pit), event_month) < 6
			THEN 'Retained Not Active in Month'
	END                                                 AS qualified_active_member_type,
	COUNT(DISTINCT qed.shiro_user_id)                   AS users,
	SUM(qed.gross_bookings)                             AS gross_bookings,
	SUM(qed.gross_revenue_gbp)                          AS gross_gross_revenue_gbp,
	SUM(qed.margin_gbp)                                 AS gross_margin_gbp
FROM collab.qams.qam_event_data qed
WHERE qed.shiro_user_id = 19016267
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
;


CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.qam_modelled_data AS (
	WITH
		model_data AS (
			-- compute attributes necessary for aggregation
			SELECT
				DATE_TRUNC(MONTH, qed.session_date)                 AS event_month,
				DATE_TRUNC(MONTH, qed.sign_up_date)                 AS original_cohort_month,
				DATE_TRUNC(MONTH, qed.cohort_activated_date)        AS activated_cohort_month,
				DATE_TRUNC(MONTH, qed.reactivated_session_date_pit) AS reactivated_cohort_month_pit,
				qed.qualified_active_member,
				qed.qualified_active_member_pit,
				qed.original_affiliate_territory,
				qed.session_date,
				qed.data_source,
				FIRST_VALUE(qed.data_source)
							OVER (PARTITION BY qed.shiro_user_id,event_month ORDER BY
								CASE
									WHEN qed.data_source = 'event data' THEN 1
									WHEN qed.data_source = 'inferred activiated data' THEN 2
									WHEN qed.data_source = 'inferred reactiviated data' THEN 3
								END)                                AS data_source_month,
				qed.shiro_user_id,
				qed.gross_bookings,
				qed.gross_revenue_gbp,
				qed.margin_gbp
			FROM collab.qams.qam_event_data qed
		),
		aggregate_data AS (
-- 			aggregate to remove user and date grain
			SELECT
				'2024-02-01',
				md.original_cohort_month,
				md.activated_cohort_month,
				md.reactivated_cohort_month_pit,
				md.qualified_active_member,
				md.qualified_active_member_pit,
				md.original_affiliate_territory,
				md.data_source_month,
				COUNT(DISTINCT md.shiro_user_id) AS users,
				SUM(md.gross_bookings)           AS gross_bookings,
				SUM(md.gross_revenue_gbp)        AS gross_gross_revenue_gbp,
				SUM(md.margin_gbp)               AS gross_margin_gbp
			FROM model_data md
			GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
		)
	-- classify months based to create qam member type
	SELECT
		ad.event_month,
		ad.original_cohort_month,
		ad.activated_cohort_month,
		ad.reactivated_cohort_month_pit,
		CASE
			WHEN ad.event_month = ad.original_cohort_month
				AND ad.original_cohort_month = ad.activated_cohort_month
				THEN 'Brand New'
			WHEN ad.event_month = ad.activated_cohort_month
				AND DATEDIFF(MONTH, ad.original_cohort_month, ad.event_month) >= 6
				THEN 'Late Activator'
			WHEN ad.event_month = ad.reactivated_cohort_month_pit
				AND DATEDIFF(MONTH, ad.original_cohort_month, ad.event_month) >= 6
				AND DATEDIFF(MONTH, ad.activated_cohort_month, ad.event_month) >= 6
				THEN 'Reactivated'
			WHEN ad.data_source_month = 'event data'
				AND ad.activated_cohort_month IS NOT NULL
				THEN 'Retained Active in Month'
			WHEN ad.data_source_month IN ('inferred activiated data', 'inferred reactiviated data')
				AND
				 DATEDIFF(MONTH,
						  GREATEST(ad.activated_cohort_month, COALESCE(ad.reactivated_cohort_month_pit, '1970-01-01')),
						  ad.event_month) < 6
				THEN 'Retained Not Active in Month'
			WHEN ad.qualified_active_member_pit = FALSE
				THEN 'Never Qualified'
		END AS qualified_active_member_type,
		ad.qualified_active_member,
		ad.qualified_active_member_pit,
		ad.original_affiliate_territory,
		ad.data_source_month,
		ad.users,
		ad.gross_bookings,
		ad.gross_gross_revenue_gbp,
		ad.gross_margin_gbp
	FROM aggregate_data ad
)
;


SELECT *
FROM collab.qams.qam_event_data qed
WHERE qed.shiro_user_id = 74166428

CREATE OR REPLACE VIEW collab.qams.qam_modelled_data AS
SELECT *
FROM scratch.robinpatel.qam_modelled_data
;

GRANT SELECT ON TABLE collab.qams.qam_modelled_data TO ROLE data_team_basic
;

GRANT SELECT ON TABLE collab.qams.qam_modelled_data TO ROLE tableau
;


SELECT
	qam_modelled_data.event_month,
	qam_modelled_data.qualified_active_member_type,
	SUM(users)
FROM scratch.robinpatel.qam_modelled_data
GROUP BY 1, 2
;

SELECT *
FROM scratch.robinpatel.qam_modelled_data
WHERE qam_modelled_data.qualified_active_member_type IS NULL

SELECT *
FROM scratch.robinpatel.qam_modelled_data
;

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort
;

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort
WHERE qualified_active_member_type IS NULL


SELECT
	event_month,
	qualified_active_member_type,
	SUM(users)
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort dqc
GROUP BY 1, 2
;


CREATE OR REPLACE VIEW collab.qams.qam_modelled_data AS
SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort
;


------------------------------------------------------------------------------------------------------------------------
--why inferred drop off in m2

SELECT
	COUNT(DISTINCT iaed.shiro_user_id)
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_05_event_data iaed
WHERE iaed.cohort_activated_date = '2021-01-01'

SELECT
	COUNT(DISTINCT iaed.shiro_user_id)
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_06_inferred_activation_event_data iaed
WHERE iaed.cohort_activated_date = '2021-01-01'

SELECT
	COUNT(DISTINCT iaed.shiro_user_id)
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_08_stack_event_data iaed
WHERE iaed.cohort_activated_date = '2021-01-01'


SELECT
	COUNT(DISTINCT iaed.shiro_user_id)
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_08_stack_event_data iaed
WHERE DATE_TRUNC(MONTH, iaed.sign_up_date) = '2018-08-01'
  AND DATE_TRUNC(MONTH, iaed.session_date) = '2018-08-01'
  AND iaed.qualified_active_member_pit
;

SELECT
	COUNT(DISTINCT iaed.shiro_user_id)
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_08_stack_event_data iaed
WHERE DATE_TRUNC(MONTH, iaed.sign_up_date) = '2018-08-01'
  AND DATE_TRUNC(MONTH, iaed.session_date) = '2018-09-01'
  AND iaed.qualified_active_member_pit
;


WITH
	aug_users AS (
		SELECT DISTINCT
			iaed.shiro_user_id
		FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_08_stack_event_data iaed
		WHERE DATE_TRUNC(MONTH, iaed.sign_up_date) = '2018-08-01'
		  AND DATE_TRUNC(MONTH, iaed.session_date) = '2018-08-01'
		  AND iaed.qualified_active_member_pit

	)
SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_08_stack_event_data iaed
	LEFT JOIN aug_users su ON iaed.shiro_user_id = su.shiro_user_id
WHERE DATE_TRUNC(MONTH, iaed.sign_up_date) = '2018-08-01'
  AND DATE_TRUNC(MONTH, iaed.session_date) = '2018-09-01'
  AND iaed.qualified_active_member_pit
  AND su.shiro_user_id IS NULL
;

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_08_stack_event_data iaed
WHERE iaed.shiro_user_id = 57007858


SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '57007858'
;

USE WAREHOUSE pipe_xlarge
;


SELECT
	dq05ed.month_diff_between_session_dates,
	COUNT(*)
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_05_event_data dq05ed
WHERE dq05ed.gross_bookings > 0
GROUP BY 1
;

WITH
	bookings_lag AS (
		SELECT
			fcb.shiro_user_id,
			fcb.booking_completed_date,
			LAG(fcb.booking_completed_date)
				OVER (PARTITION BY fcb.shiro_user_id ORDER BY fcb.booking_completed_date) AS last_booking_date,
			DATEDIFF(MONTH, last_booking_date, fcb.booking_completed_date)                AS month_diff
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.se_brand = 'SE Brand'
	)
SELECT
	bl.month_diff,
	COUNT(*)
FROM bookings_lag bl
GROUP BY 1
;


------------------------------------------------------------------------------------------------------------------------

SELECT
	event_month,
	qualified_active_member_type,
	SUM(users)
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort dqc
GROUP BY 1, 2
;


SELECT
	event_month,
	qualified_active_member_type,
	SUM(users)
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort4m dqc
GROUP BY 1, 2
;

SELECT
	event_month,
	qualified_active_member_type,
	SUM(users)
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort3m dqc
GROUP BY 1, 2
;

------------------------------------------------------------------------------------------------------------------------

/*
QAM additions, add two new metrics
- average length of stay
- adr

split all metrics by:
domestic hotel
international hotel
package
  */

WITH
	booking_type AS (
		SELECT
			fb.booking_id,
			CASE
				WHEN fb.travel_type = 'Domestic' AND fb.booking_includes_flight = FALSE THEN 'domestic hotel'
				WHEN fb.travel_type = 'International' AND fb.booking_includes_flight = FALSE AND
					 ds.product_configuration IN ('Hotel', 'Hotel Plus') THEN 'international hotel'
				ELSE 'package'
			END AS qam_booking_type,
			fb.travel_type,
			fb.gross_revenue_gbp_constant_currency,
			fb.margin_gross_of_toms_gbp_constant_currency,
			fb.no_nights
		FROM se.data.fact_booking fb
			INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
		WHERE fb.booking_status_type IN ('live', 'cancelled')
	)

SELECT
	stt.touch_id,
	COUNT(DISTINCT stt.booking_id)                                             AS gross_bookings,
	SUM(fb.gross_revenue_gbp_constant_currency)                                AS gross_revenue_gbp,
	SUM(fb.margin_gross_of_toms_gbp_constant_currency)                         AS margin_gbp,
	SUM(fb.no_nights)                                                          AS nights,

	COUNT(DISTINCT
		  IFF(qam_booking_type = 'domestic hotel', stt.booking_id, NULL))      AS domestic_hotel_gross_bookings,
	SUM(IFF(qam_booking_type = 'domestic hotel', fb.gross_revenue_gbp_constant_currency,
			NULL))                                                             AS domestic_hotel_gross_revenue_gbp,
	SUM(IFF(qam_booking_type = 'domestic hotel', fb.margin_gross_of_toms_gbp_constant_currency,
			NULL))                                                             AS domestic_hotel_margin_gbp,
	SUM(IFF(qam_booking_type = 'domestic hotel', fb.no_nights, NULL))          AS domestic_hotel_nights,

	COUNT(DISTINCT
		  IFF(qam_booking_type = 'international hotel', stt.booking_id, NULL)) AS international_hotel_gross_bookings,
	SUM(IFF(qam_booking_type = 'international hotel', fb.gross_revenue_gbp_constant_currency,
			NULL))                                                             AS international_hotel_gross_revenue_gbp,
	SUM(IFF(qam_booking_type = 'international hotel', fb.margin_gross_of_toms_gbp_constant_currency,
			NULL))                                                             AS international_hotel_margin_gbp,
	SUM(IFF(qam_booking_type = 'international hotel', fb.no_nights, NULL))     AS international_hotel_nights,

	COUNT(DISTINCT
		  IFF(qam_booking_type = 'package', stt.booking_id, NULL))             AS package_gross_bookings,
	SUM(IFF(qam_booking_type = 'package', fb.gross_revenue_gbp_constant_currency,
			NULL))                                                             AS package_gross_revenue_gbp,
	SUM(IFF(qam_booking_type = 'package', fb.margin_gross_of_toms_gbp_constant_currency,
			NULL))                                                             AS package_margin_gbp,
	SUM(IFF(qam_booking_type = 'package', fb.no_nights, NULL))                 AS package_nights

FROM se.data.scv_touched_transactions stt
	INNER JOIN booking_type fb ON stt.booking_id = fb.booking_id
GROUP BY 1
;


-- split all metrics by:
-- domestic hotel
-- international hotel
-- package

SELECT
	fb.booking_id,
	fb.travel_type,
	fb.booking_includes_flight,
	ds.product_configuration,
	CASE
		WHEN ds.travel_type = 'Domestic' AND fb.booking_includes_flight = FALSE THEN 'domestic hotel'
		WHEN ds.travel_type = 'International' AND fb.booking_includes_flight = FALSE AND
			 ds.product_configuration IN ('Hotel', 'Hotel Plus') THEN 'international hotel'
		ELSE 'package'
	END AS qam_booking_type,
FROM se.data.fact_booking fb
	INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type IN ('live', 'cancelled')


;


SELECT
	fb.booking_id,
	booking_includes_flight,
	CASE
		WHEN fb.travel_type = 'Domestic' AND fb.booking_includes_flight = FALSE THEN 'domestic hotel'
		WHEN fb.travel_type = 'International' AND fb.booking_includes_flight = FALSE AND
			 ds.product_configuration IN ('Hotel', 'Hotel Plus') THEN 'international hotel'
		ELSE 'package'
	END AS qam_booking_type,
	fb.travel_type,
	fb.gross_revenue_gbp_constant_currency,
	fb.margin_gross_of_toms_gbp_constant_currency,
	fb.no_nights
FROM se.data.fact_booking fb
	INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type IN ('live', 'cancelled')
  AND fb.booking_includes_flight IS NULL
  AND fb.booking_completed_date >= '2018-01-01'


SELECT
	CASE
		WHEN fb.travel_type = 'Domestic' AND COALESCE(fb.booking_includes_flight, FALSE) = FALSE THEN 'domestic hotel'
		WHEN fb.travel_type = 'International' AND COALESCE(fb.booking_includes_flight, FALSE) = FALSE AND
			 ds.product_configuration IN ('Hotel', 'Hotel Plus') THEN 'international hotel'
		ELSE 'package'
	END AS qam_booking_type,
	ds.travel_type,
	ds.product_configuration,
	COUNT(*)
FROM se.data.fact_booking fb
	INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type IN ('live', 'cancelled')
  AND fb.booking_includes_flight IS NULL
  AND fb.booking_completed_date >= '2018-01-01'
GROUP BY 1, 2, 3
;

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_01_base_session_data dq01bsd
WHERE dq01bsd.gross_bookings > 1
;

SHOW VIEWS IN SCHEMA collab.qams
;

SELECT GET_DDL('table', 'collab.qams.QAM_MODELLED_DATA')
;


SHOW TABLES IN SCHEMA dbt_dev.dbt_robinpatel_data_platform
;

NAME

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort
;

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort3m
;

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort4m
;

USE ROLE pipelinerunner
;

CREATE OR REPLACE VIEW collab.qams.qam_6m_modelled_data COPY GRANTS AS
SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort
;

CREATE OR REPLACE VIEW collab.qams.qam_3m_modelled_data COPY GRANTS AS

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort3m
;

CREATE OR REPLACE VIEW collab.qams.qam_4m_modelled_data COPY GRANTS AS

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort4m
;

GRANT SELECT ON TABLE collab.qams.qam_6m_modelled_data TO ROLE data_team_basic
;

GRANT SELECT ON TABLE collab.qams.qam_6m_modelled_data TO ROLE tableau
;

GRANT SELECT ON TABLE collab.qams.qam_3m_modelled_data TO ROLE data_team_basic
;

GRANT SELECT ON TABLE collab.qams.qam_3m_modelled_data TO ROLE tableau
;

GRANT SELECT ON TABLE collab.qams.qam_4m_modelled_data TO ROLE data_team_basic
;

GRANT SELECT ON TABLE collab.qams.qam_4m_modelled_data TO ROLE tableau
;


USE ROLE personal_role__robinpatel
;

SELECT *
FROM collab.qams.qam_4m_modelled_data
;


SELECT *
FROM se.data.fact_booking fb
WHERE fb.booking_completed_date >= CURRENT_DATE - 1
  AND fb.booking_status_type = 'live'
  AND fb.promo_code_amount IS NOT NULL


SELECT *
FROM collab.qams.qam_event_data qed

SELECT GET_DDL('table', 'collab.qams.qam_event_data')
;

USE ROLE pipelinerunner
;

CREATE OR REPLACE VIEW collab.qams.qam_event_data COPY GRANTS AS
SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_08_stack_event_data
;


SELECT *
FROM collab.qams.qam_event_data
;

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_qam_cohort dqc
;


SELECT *,
	   CASE
		   WHEN '2024-02-01' = md.original_cohort_month
			   AND md.original_cohort_month = md.activated_cohort_month
			   THEN 'Brand New'
		   WHEN '2024-02-01' = md.activated_cohort_month
			   AND DATEDIFF(MONTH, md.original_cohort_month, '2024-02-01') >= 6
			   THEN 'Late Activator'
		   WHEN '2024-02-01' = md.reactivated_cohort_month_pit
			   AND DATEDIFF(MONTH, md.original_cohort_month, '2024-02-01') >= 6
			   AND DATEDIFF(MONTH, md.activated_cohort_month, '2024-02-01') >= 6
			   THEN 'Reactivated'
		   WHEN md.data_source_month = 'event data'
			   AND DATEDIFF(MONTH,
							GREATEST(md.activated_cohort_month,
									 COALESCE(md.reactivated_cohort_month_pit, '1970-01-01')),
							'2024-02-01') < 6
			   THEN 'New Retained Active in Month'
		   WHEN md.data_source_month = 'event data'
			   AND md.activated_cohort_month IS NOT NULL
			   THEN 'Retained Active in Month'
		   WHEN md.data_source_month IN ('inferred activiated data', 'inferred reactiviated data')
			   AND DATEDIFF(MONTH,
							GREATEST(md.activated_cohort_month,
									 COALESCE(md.reactivated_cohort_month_pit, '1970-01-01')),
							'2024-02-01') < 6
			   THEN 'Retained Not Active in Month'
		   WHEN md.qualified_active_member_pit = FALSE
			   THEN 'Never Qualified'
	   END AS qualified_active_member_type
FROM collab.qams.qam_6m_modelled_data md;


SELECT * FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_qam_08_stack_event_data dq08sed WHERE dq08sed.shiro_user_id = 4869775


SELECT get_ddl('table', 'collab.qams.qam_6m_modelled_data');

SELECT * FROM dbt.bi_data_platform.qam