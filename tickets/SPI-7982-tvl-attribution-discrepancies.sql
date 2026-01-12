WITH
	events AS (
		SELECT
			ses.event_tstamp::DATE AS event_date,
			ses.unique_browser_id,
			ses.device_platform,
		FROM se.data_pii.scv_event_stream ses
		WHERE ses.se_brand = 'Travelist'
		  AND ses.event_name = 'page_view'
		  AND ses.event_tstamp >= CURRENT_DATE - 100
	),
	agg_to_ubid AS (
		SELECT
			events.event_date,
			events.unique_browser_id,
			COUNT(DISTINCT events.device_platform) AS device_count
		FROM events
		GROUP BY events.event_date,
				 events.unique_browser_id
	)
SELECT
	agg_to_ubid.event_date,
	COUNT(DISTINCT agg_to_ubid.unique_browser_id) AS num_ubids,
	SUM(IFF(agg_to_ubid.device_count = 1, 1, 0))  AS num_ubids_with_1_device,
	SUM(IFF(agg_to_ubid.device_count > 1, 1, 0))  AS num_ubids_with_multiple_devices,
	num_ubids_with_multiple_devices / num_ubids   AS perc_multiple_device_ubids
FROM agg_to_ubid
GROUP BY agg_to_ubid.event_date
;


-- WITH
-- 	base AS (
-- 		SELECT
-- 			unique_browser_id,
-- 			COUNT(DISTINCT device_platform) AS count_device,
-- 			COUNT(DISTINCT platform)        AS l_platform
-- 		FROM se.data_pii.scv_event_stream a
-- 		WHERE se_brand = 'Travelist' AND DATE(a.event_tstamp) > '2025-11-01'
-- 		GROUP BY 1
-- 		ORDER BY 2 DESC
-- 	)
-- SELECT
-- 	l_platform,
-- 	COUNT(DISTINCT unique_browser_id) AS liczba_przypadkow
-- FROM base
-- GROUP BY 1
-- ;

SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_se_brand = 'Travelist'
;

SELECT
	unique_browser_id,
	a.platform,
FROM se.data_pii.scv_event_stream a
WHERE se_brand = 'Travelist' AND DATE(a.event_tstamp) > '2025-11-01'
;


USE WAREHOUSE pipe_xlarge
;

WITH
	transactions AS (
		SELECT
			stt.touch_id,
			fb.booking_id,
			fb.currency,
			fb.gross_revenue_cc,
			fb.margin_gross_of_toms_cc,
			fb.booking_status_type,
			fb.se_brand
		FROM se.data.scv_touched_transactions stt
		INNER JOIN se.data.fact_booking fb
			ON stt.booking_id = fb.booking_id
			AND fb.currency = 'PLN' -- all travelist bookings are currency PLN at time of writing this query
		WHERE stt.event_tstamp >= '2025-07-01'
		  AND fb.se_brand = 'Travelist'
	),
	agg_bookings AS (
		SELECT
			transactions.touch_id,
			COUNT(DISTINCT transactions.booking_id)   AS bookings,
			SUM(transactions.gross_revenue_cc)        AS sum_gross_revenue_pln,
			SUM(transactions.margin_gross_of_toms_cc) AS sum_margin_pln,

			COUNT(DISTINCT IFF(transactions.booking_status_type = 'live', transactions.booking_id,
							   NULL))                 AS live_bookings,
			SUM(IFF(transactions.booking_status_type = 'live', transactions.gross_revenue_cc,
					NULL))                            AS live_sum_gross_revenue_pln,
			SUM(IFF(transactions.booking_status_type = 'live', transactions.margin_gross_of_toms_cc,
					NULL))                            AS live_sum_margin_pln
		FROM transactions
		GROUP BY transactions.touch_id
	)

SELECT
	stmc.touch_mkt_channel,
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	COUNT(*)                                   AS sessions,
	SUM(bookings)                              AS bookings,
	SUM(sum_gross_revenue_pln)                 AS sum_gross_revenue_pln,
	SUM(sum_margin_pln)                        AS sum_margin_pln,
	SUM(live_bookings)                         AS live_bookings,
	SUM(live_sum_gross_revenue_pln)            AS live_sum_gross_revenue_pln,
	SUM(live_sum_margin_pln)                   AS live_sum_margin_pln,
FROM se.data.scv_touch_basic_attributes stba
INNER JOIN se.data.scv_touch_attribution sta
	ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON sta.attributed_touch_id = stmc.touch_id
LEFT JOIN agg_bookings bookings
	ON stba.touch_id = bookings.touch_id
WHERE stba.touch_se_brand = 'Travelist'
  AND stba.touch_start_tstamp >= '2025-07-01'
GROUP BY ALL
;


WITH
	transactions AS (
		SELECT
			stt.touch_id,
			fb.booking_id,
			fb.currency,
			fb.gross_revenue_cc,
			fb.margin_gross_of_toms_cc,
			fb.booking_status_type,
			fb.se_brand
		FROM se.data.scv_touched_transactions stt
		INNER JOIN se.data.fact_booking fb
			ON stt.booking_id = fb.booking_id
			AND fb.currency = 'PLN' -- all travelist bookings are currency PLN at time of writing this query
		WHERE stt.event_tstamp >= '2025-07-01'
		  AND fb.se_brand = 'Travelist'
	),
	agg_bookings AS (
		SELECT
			transactions.touch_id,
			COUNT(DISTINCT transactions.booking_id)   AS bookings,
			SUM(transactions.gross_revenue_cc)        AS sum_gross_revenue_pln,
			SUM(transactions.margin_gross_of_toms_cc) AS sum_margin_pln,

			COUNT(DISTINCT IFF(transactions.booking_status_type = 'live', transactions.booking_id,
							   NULL))                 AS live_bookings,
			SUM(IFF(transactions.booking_status_type = 'live', transactions.gross_revenue_cc,
					NULL))                            AS live_sum_gross_revenue_pln,
			SUM(IFF(transactions.booking_status_type = 'live', transactions.margin_gross_of_toms_cc,
					NULL))                            AS live_sum_margin_pln
		FROM transactions
		GROUP BY transactions.touch_id
	)

SELECT
	stmc.touch_mkt_channel,
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	COUNT(*)                                   AS sessions,
	SUM(bookings)                              AS bookings,
	SUM(sum_gross_revenue_pln)                 AS sum_gross_revenue_pln,
	SUM(sum_margin_pln)                        AS sum_margin_pln,
	SUM(live_bookings)                         AS live_bookings,
	SUM(live_sum_gross_revenue_pln)            AS live_sum_gross_revenue_pln,
	SUM(live_sum_margin_pln)                   AS live_sum_margin_pln,
FROM se.data.scv_touch_basic_attributes stba
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
LEFT JOIN agg_bookings bookings
	ON stba.touch_id = bookings.touch_id
WHERE stba.touch_se_brand = 'Travelist'
  AND stba.touch_start_tstamp >= '2025-07-01'
GROUP BY ALL
;


SELECT
	tb.booking_id,
	tb.booking_status_type,
	tb.utm_source,
	tb.utm_campaign,
	tb.utm_medium,
	tb.utm_content,
	tb.utm_term,
	tb.sold_price_total_cc,
	tb.cost_price_total_cc
FROM se.data.tb_booking tb
WHERE tb.se_brand = 'Travelist'
  AND tb.created_at_dts BETWEEN '2025-09-01' AND '2025-09-30'
  AND tb.booking_status_type IN ('live', 'cancelled')
;

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge
;

WITH
	transactions AS (
		SELECT
			stt.touch_id,
			fb.booking_id,
			fb.currency,
			fb.gross_revenue_cc,
			fb.margin_gross_of_toms_cc,
			fb.booking_status_type,
			fb.se_brand
		FROM se.data.scv_touched_transactions stt
		INNER JOIN se.data.fact_booking fb
			ON stt.booking_id = fb.booking_id
			AND fb.currency = 'PLN' -- all travelist bookings are currency PLN at time of writing this query
		WHERE stt.event_tstamp >= '2025-07-01'
		  AND fb.se_brand = 'Travelist'
	),
	agg_bookings AS (
		SELECT
			transactions.touch_id,
			COUNT(DISTINCT transactions.booking_id)   AS bookings,
			SUM(transactions.gross_revenue_cc)        AS sum_gross_revenue_pln,
			SUM(transactions.margin_gross_of_toms_cc) AS sum_margin_pln,

			COUNT(DISTINCT IFF(transactions.booking_status_type = 'live', transactions.booking_id,
							   NULL))                 AS live_bookings,
			SUM(IFF(transactions.booking_status_type = 'live', transactions.gross_revenue_cc,
					NULL))                            AS live_sum_gross_revenue_pln,
			SUM(IFF(transactions.booking_status_type = 'live', transactions.margin_gross_of_toms_cc,
					NULL))                            AS live_sum_margin_pln
		FROM transactions
		GROUP BY transactions.touch_id
	)
		,
	modelling AS (
		SELECT
			stba.*,
			stmc.* EXCLUDE (touch_start_tstamp, touch_id), bookings.* EXCLUDE (touch_id)
		FROM se.data_pii.scv_touch_basic_attributes stba
		INNER JOIN se.data.scv_touch_marketing_channel stmc
			ON stba.touch_id = stmc.touch_id
		LEFT JOIN agg_bookings bookings
			ON stba.touch_id = bookings.touch_id
		WHERE stba.touch_se_brand = 'Travelist'
		  AND stba.touch_start_tstamp >= '2025-07-01'
		GROUP BY ALL
	)
SELECT
	modelling.referrer_hostname,
	modelling.referrer_medium,
	COUNT(*),
	SUM(modelling.bookings)
FROM modelling
WHERE modelling.touch_mkt_channel = 'Referral'
  AND modelling.bookings > 0
GROUP BY 1, 2
;

-- theory that there a lot of booking sessions that aren't being associated to a TVL user id
-- theory disproven numbers are far too small


-- looking at Referral Channel

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'Travelist'
  AND stba.touch_start_tstamp >= '2024-01-01'
  AND stmc.touch_mkt_channel = 'Referral'
  AND stba.touch_has_booking
;

SELECT
	DATE_TRUNC(WEEK, stba.touch_start_tstamp)                                         AS week,
	COUNT(*)                                                                          AS bookings,
	SUM(IFF(stmc.referrer_hostname IS NOT DISTINCT FROM 'accounts.google.com', 1, 0)) AS bookings_with_google_referrer,
	bookings_with_google_referrer / bookings
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'Travelist'
  AND stba.touch_start_tstamp >= '2024-01-01'
  AND stmc.touch_mkt_channel = 'Referral'
  AND stba.touch_has_booking
GROUP BY ALL
;

-- looks like a high proportion of sessions that are being channeled as Referral because theres a referrer hostname 'accounts.google.com'
-- which looks like oauth but we've not accommodated for that in SCV
--~15-20% of sessions and  ~ 40-50% of Referral sessions with a booking


-- looking at Source SEO Channel

SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	stba.touch_landing_pagepath,
	CASE
		WHEN stba.touch_landing_pagepath = '/search/' THEN stba.touch_landing_pagepath
		WHEN stba.touch_landing_pagepath = '/hotele' THEN stba.touch_landing_pagepath
		WHEN stba.touch_landing_pagepath = '/search/baltyk/' THEN stba.touch_landing_pagepath
		ELSE 'other'
	END                                        AS page_path_category,
	COUNT(*)
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'Travelist'
  AND stba.touch_start_tstamp >= '2025-07-01'
  AND stmc.touch_mkt_channel = 'Source SEO'
  AND stba.touch_has_booking
  AND page_path_category = 'other'
GROUP BY ALL
ORDER BY 4 DESC
;



SELECT
	stba.touch_start_tstamp,
	stba.stitched_identity_type,
	stba.attributed_user_id,
	stba.touch_landing_page,
	stmc.referrer_hostname,
	stmc.referrer_medium,
	stmc.touch_mkt_channel,
	stba.touch_referrer_url
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'Travelist'
  AND stba.touch_start_tstamp >= '2024-01-01'
  AND stba.touch_has_booking
  AND stba.touch_landing_page LIKE '%oauth%'
;

SELECT
	ses.event_tstamp,
	ses.event_name,
	ses.tvl_user_id,
	ses.unique_browser_id,
	ses.page_url,
	ses.page_referrer,
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_brand = 'Travelist'
  AND ses.event_tstamp::DATE = '2025-10-09'
  AND ses.unique_browser_id = '9b024e1d-eee8-45f9-a0eb-7b29b6cea2ca'

-- 	  ses.event_hash = '60fdbfde89f177b84ee4c8b09a883f87935bdabfce408393d201cbab9216e816'

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_brand = 'SE Brand'
  AND ses.event_tstamp >= CURRENT_DATE - 1
  AND ses.contexts_com_secretescapes_user_state_context_1 IS NOT NULL;


SELECT TRIM(' hello ');

SELECT * FROM se.bi.session_metrics sm WHERE sm.touch_se_brand = 'Travelist'

