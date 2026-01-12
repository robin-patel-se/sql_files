SET (from_date , to_date )= ('2022-01-01', '2023-02-28')
;

WITH
	downloads AS (
		SELECT
			sua.original_affiliate_territory              AS territory,
			a.category,
			stba.attributed_user_id,
			DATE_TRUNC(MONTH, sua.signup_tstamp)::DATE    AS signup_month,
			DATE_TRUNC(MONTH, MIN(ai.event_tstamp))::DATE AS download_month,
			COUNT(DISTINCT ai.touch_id)                   AS downloads
		FROM se.data_pii.scv_touch_basic_attributes stba
			INNER JOIN se.data.se_user_attributes sua ON sua.shiro_user_id::varchar = stba.attributed_user_id
			INNER JOIN se.data.se_affiliate a ON a.id = sua.original_affiliate_id
			INNER JOIN se.data.scv_touched_app_installs ai ON ai.touch_id = stba.touch_id
		WHERE first_event_for_user = 'TRUE'
		  AND stba.stitched_identity_type = 'se_user_id'
		GROUP BY 1, 2, 3, 4
	),
	grain AS
		(
			SELECT
				DATE_TRUNC(MONTH, date_value)::DATE AS grain_month,
				ac.category                         AS category,
				t.name                              AS territory
			FROM se.data.se_calendar
				CROSS JOIN se.data.se_affiliate ac
				CROSS JOIN se.data.se_territory t
			WHERE date_value::DATE >= $from_date
			  AND date_value::DATE <= $to_date
			  AND t.id IN (1, 2, 4, 11, 12)

			GROUP BY 1, 2, 3
		),


	session_bookings AS
		(
			SELECT
				t.touch_id,
				DATE_TRUNC(MONTH, t.event_tstamp)::DATE            AS booking_month,
				COUNT(ti.transaction_id)                           AS app_bookings,
				SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS app_margin

			FROM se.data.scv_touched_transactions t
				INNER JOIN se.data.fact_booking fb
						   ON fb.booking_id = t.booking_id
				INNER JOIN se.data_pii.scv_branch_purchase_events ti
						   ON ti.transaction_id = fb.transaction_id --on ti.touch_id = stba.touch_id
			GROUP BY 1, 2
		),


-- bookings from users who downloaded app in a speciffic month

	app_users_bookings AS (

		SELECT

			sua.original_affiliate_territory           AS territory,
			a.category,
			d.download_month,
			DATE_TRUNC(MONTH, sua.signup_tstamp)::DATE AS signup_month,
			sb.booking_month,
			SUM(sb.app_bookings)                       AS app_bookings,
			SUM(sb.app_margin)                         AS app_margin

		FROM se.data_pii.scv_touch_basic_attributes stba
			INNER JOIN se.data.se_user_attributes sua
					   ON sua.shiro_user_id::VARCHAR = stba.attributed_user_id
			INNER JOIN downloads d ON d.attributed_user_id = stba.attributed_user_id
			INNER JOIN se.data.se_affiliate a ON a.id = sua.original_affiliate_id
			INNER JOIN session_bookings sb ON sb.touch_id = stba.touch_id

		WHERE stba.stitched_identity_type = 'se_user_id' AND d.download_month IS NOT NULL
		GROUP BY 1, 2, 3, 4, 5
	),
	age AS (
		SELECT
			DATE_TRUNC('MONTH', sc.date_value::DATE)           AS month,
			DATE_TRUNC('MONTH', sc2.date_value::DATE)          AS event_month,
			DATEDIFF('MONTH', DATE_TRUNC('MONTH', sc2.date_value::DATE),
					 DATE_TRUNC('MONTH', sc.date_value::DATE)) AS age

		FROM se.data.se_calendar sc
			JOIN se.data.se_calendar sc2

		WHERE DATE_TRUNC('MONTH', sc.date_value::DATE) >= '2011-01-01' AND
			  DATE_TRUNC('MONTH', sc.date_value::DATE) <= getdate() AND
			  DATE_TRUNC('MONTH', sc2.date_value::DATE) >= '2011-01-01' AND
			  DATE_TRUNC('MONTH', sc2.date_value::DATE) <= getdate() AND
			  DATE_TRUNC('MONTH', sc2.date_value::DATE) <= DATE_TRUNC('MONTH', sc.date_value::DATE)
		GROUP BY 1, 2, 3
	)

SELECT
	a.event_month,
	a.age           AS user_app_age, -- difference between grain month (month) and event month
	a2.age          AS user_age,
	g.grain_month,
	g.category,
	g.territory,
	d.signup_month,
	d.download_month,
/*case when DATEDIFF(month, d.signup_month, d.download_month) < 0 then 'Pre_signup'
when DATEDIFF(month, d.signup_month, d.download_month)  = 0 then 'M1'
when DATEDIFF(month, d.signup_month, d.download_month)  = 1 then 'M2'
when DATEDIFF(month, d.signup_month, d.download_month)  = 2 then 'M3'
when DATEDIFF(month, d.signup_month, d.download_month)  <=14 then 'M4-15'else 'older' end AS user_cohort_age, */
	d.downloads,-- downloads count for users in cohort month
	b.booking_month AS booking_month,
	b.app_margin,
	b.app_bookings

FROM grain g
	INNER JOIN age a ON g.grain_month = a.month
	INNER JOIN age a2 ON g.grain_month = a2.month
	LEFT JOIN  downloads d ON d.category = g.category AND d.territory = g.territory AND d.download_month = a.event_month

	LEFT JOIN  downloads d2
			   ON d2.category = g.category AND d2.territory = g.territory AND d2.signup_month = a2.event_month AND
				  d2.signup_month = d.signup_month AND d2.download_month = d.download_month
	LEFT JOIN  app_users_bookings b
			   ON b.category = g.category AND b.territory = g.territory AND b.booking_month = g.grain_month
--and b.download_month = a.event_month

--and b.Signup_month = d.signup_month


GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12



WITH
	grain AS
		(
			SELECT
				DATE_TRUNC(MONTH, date_value)::DATE AS grain_month,
				ac.category                         AS category,
				t.name                              AS territory
			FROM se.data.se_calendar
				CROSS JOIN se.data.se_affiliate ac
				CROSS JOIN se.data.se_territory t
			WHERE date_value::DATE >= $from_date
			  AND date_value::DATE <= $to_date
			  AND t.id IN (1, 2, 4, 11, 12)

			GROUP BY 1, 2, 3
		),
	age AS (
		SELECT
			DATE_TRUNC('MONTH', sc.date_value::DATE)           AS month,
			DATE_TRUNC('MONTH', sc2.date_value::DATE)          AS event_month,
			DATEDIFF('MONTH', DATE_TRUNC('MONTH', sc2.date_value::DATE),
					 DATE_TRUNC('MONTH', sc.date_value::DATE)) AS age

		FROM se.data.se_calendar sc
			JOIN se.data.se_calendar sc2

		WHERE DATE_TRUNC('MONTH', sc.date_value::DATE) >= '2011-01-01' AND
			  DATE_TRUNC('MONTH', sc.date_value::DATE) <= getdate() AND
			  DATE_TRUNC('MONTH', sc2.date_value::DATE) >= '2011-01-01' AND
			  DATE_TRUNC('MONTH', sc2.date_value::DATE) <= getdate() AND
			  DATE_TRUNC('MONTH', sc2.date_value::DATE) <= DATE_TRUNC('MONTH', sc.date_value::DATE)
		GROUP BY 1, 2, 3
	)

SELECT
	a.event_month  AS download_month,
	a.age          AS user_app_age, -- difference between grain month (month) and event month
	a2.event_month AS signup_month,
	a2.age         AS user_age,
	g.grain_month,
	g.category,
	g.territory

FROM grain g
	INNER JOIN age a ON g.grain_month = a.month
	INNER JOIN age a2 ON g.grain_month = a2.month


;


WITH
	months AS (
		SELECT
			DATE_TRUNC('MONTH', sc.date_value::DATE) AS month
		FROM se.data.se_calendar sc
		WHERE DATE_TRUNC('MONTH', sc.date_value::DATE) >= '2018-01-01'
		  AND DATE_TRUNC('MONTH', sc.date_value::DATE) <= CURRENT_DATE()
		GROUP BY 1
	),
	explode_months AS (
		SELECT
			su.month AS signup_month,
			dl.month AS download_month
		FROM months su
			CROSS JOIN months dl
	),
	add_event_month AS (
		SELECT
			m.signup_month,
			m.download_month,
			em.month AS event_month
		FROM months em
			CROSS JOIN explode_months m
	),
	download_sessions AS (
		-- filter download sessions
		SELECT
			sua.original_affiliate_territory              AS territory,
			a.category,
			stba.touch_id,
			stba.attributed_user_id,
			DATE_TRUNC(MONTH, sua.signup_tstamp)::DATE    AS signup_month,
			DATE_TRUNC(MONTH, MIN(ai.event_tstamp))::DATE AS download_month -- have a think about if we should us most recent download
		FROM se.data_pii.scv_touch_basic_attributes stba
			INNER JOIN se.data.se_user_attributes sua ON sua.shiro_user_id::varchar = stba.attributed_user_id
			INNER JOIN se.data.se_affiliate a ON a.id = sua.original_affiliate_id
			INNER JOIN se.data.scv_touched_app_installs ai ON ai.touch_id = stba.touch_id
		WHERE first_event_for_user = 'TRUE'
		  AND stba.stitched_identity_type = 'se_user_id'
		GROUP BY 1, 2, 3, 4, 5
	),
	downloads AS (
		-- aggregate to get download counts
		SELECT
			de.territory,
			de.category,
			de.signup_month,
			de.download_month, -- have a think about if we should us most recent download
			COUNT(DISTINCT attributed_user_id) AS downloads
		FROM download_sessions de
		GROUP BY 1, 2, 3, 4
	),
	session_bookings AS
		(
			SELECT
				t.touch_id,
				DATE_TRUNC(MONTH, t.event_tstamp)::DATE            AS booking_month,
				COUNT(ti.transaction_id)                           AS app_bookings,
				SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS app_margin
			FROM se.data.scv_touched_transactions t
				INNER JOIN se.data.fact_booking fb ON fb.booking_id = t.booking_id
				INNER JOIN se.data_pii.scv_branch_purchase_events ti
						   ON ti.transaction_id = fb.transaction_id --on ti.touch_id = stba.touch_id
			GROUP BY 1, 2
		),
	app_users_bookings AS (
		-- aggregate to get app user bookings
		SELECT
			sua.original_affiliate_territory           AS territory,
			a.category,
			d.download_month,
			DATE_TRUNC(MONTH, sua.signup_tstamp)::DATE AS signup_month,
			sb.booking_month,
			SUM(sb.app_bookings)                       AS app_bookings,
			SUM(sb.app_margin)                         AS app_margin
		FROM se.data_pii.scv_touch_basic_attributes stba
			INNER JOIN se.data.se_user_attributes sua ON sua.shiro_user_id::VARCHAR = stba.attributed_user_id
			INNER JOIN download_sessions d ON d.attributed_user_id = stba.attributed_user_id
			INNER JOIN se.data.se_affiliate a ON a.id = sua.original_affiliate_id
			INNER JOIN session_bookings sb ON sb.touch_id = stba.touch_id
		WHERE stba.stitched_identity_type = 'se_user_id' AND d.download_month IS NOT NULL
		GROUP BY 1, 2, 3, 4, 5
	)
SELECT
	em.signup_month,
	em.download_month,
	em.event_month,
	DATEDIFF(MONTHS, em.signup_month, em.event_month)    AS user_age,
	DATEDIFF(MONTHS, em.signup_month, em.download_month) AS app_install_age,
	DATEDIFF(MONTHS, em.download_month, em.event_month)  AS app_cohort_age,
	dl.territory,
	dl.category,
	dl.downloads,
	aub.app_bookings,
	aub.app_margin
FROM add_event_month em
	INNER JOIN downloads dl ON em.event_month = dl.download_month
	AND em.download_month = dl.download_month
	AND em.signup_month = dl.signup_month
	INNER JOIN app_users_bookings aub ON
			dl.category = aub.category
		AND dl.territory = aub.territory
		AND em.event_month = aub.download_month
		AND em.download_month = aub.download_month
		AND em.signup_month = aub.signup_month;