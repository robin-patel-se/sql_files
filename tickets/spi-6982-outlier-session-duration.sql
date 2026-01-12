WITH
	duration_p95_per_month AS (
		SELECT
			DATE_TRUNC('month', stba.touch_start_tstamp::DATE)          AS month_of,
			APPROX_PERCENTILE((stba.touch_duration_seconds / 60), 0.95) AS p95_duration_mins
		FROM se.data.scv_touch_basic_attributes stba
		GROUP BY 1
	),
	modelling AS (
		SELECT
			stba.touch_id,
			stba.touch_start_tstamp::DATE                                                  AS touch_start_date,
			stba.touch_duration_seconds / 60                                               AS touch_duration_minutes,
			p95_duration_mins,
			(stba.touch_duration_seconds / 60) <= duration_p95_per_month.p95_duration_mins AS touch_duration_within_p95
		FROM se.data.scv_touch_basic_attributes stba
			LEFT JOIN duration_p95_per_month
					  ON DATE_TRUNC('month', stba.touch_start_tstamp::DATE) = duration_p95_per_month.month_of
		WHERE stba.touch_start_tstamp >= '2024-01-01'
		  AND stba.touch_se_brand = 'SE Brand'
	)
SELECT
	DATE_TRUNC(MONTH, modelling.touch_start_date) AS month,
	modelling.touch_duration_within_p95,
	AVG(modelling.touch_duration_minutes)         AS avg_duration
FROM modelling
GROUP BY ALL




