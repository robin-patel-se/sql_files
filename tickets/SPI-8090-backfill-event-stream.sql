-- reference Snowplow/Snowflake data-missing in tracking
-- of all the data checks.. this is perhaps the most significant
-- suggests late-arriving events..
WITH
	cte_hygiene_event_stream AS (
		SELECT
			'hygiene_vault'        AS type,
			HOUR(dvce_sent_tstamp) AS the_hour,
			COUNT(*)               AS ctr
		FROM hygiene_vault_mvp.snowplow.event_stream
		WHERE dvce_sent_tstamp::DATE = '2025-12-16'
		GROUP BY HOUR(dvce_sent_tstamp)
	),
	cte_atomic_events AS (
		SELECT
			'atomic_events'        AS type,
			HOUR(dvce_sent_tstamp) AS the_hour,
			COUNT(*)               AS ctr
		FROM snowplow.atomic.events
		WHERE dvce_sent_tstamp::DATE = '2025-12-16'
		GROUP BY HOUR(dvce_sent_tstamp)
	)
SELECT
	'dvce_sent_tstamp'                                       AS metric,
	incident.*,
	prior_period.*,
	prior_period.ctr - incident.ctr                          AS diff,
	((prior_period.ctr - incident.ctr) / incident.ctr) * 100 AS percent_difference

FROM cte_hygiene_event_stream incident
INNER JOIN cte_atomic_events prior_period
	ON incident.the_hour = prior_period.the_hour
ORDER BY incident.the_hour
;


SELECT
	fact_booking.booking_completed_date,
	se_user_attributes.current_affiliate_name,
	fact_booking.margin_current_gbp_constant_currency

FROM se.data.fact_booking
INNER JOIN se.data.se_user_attributes
	ON fact_booking.shiro_user_id = se_user_attributes.shiro_user_id
WHERE fact_booking.booking_status IN ('live', 'cancelled')
  AND fact_booking.se_brand = 'SE Brand'
  AND fact_booking.booking_completed_date BETWEEN '2025-01-01' AND CURRENT_DATE()


SELECT *
FROM se.data.fact_booking;