USE WAREHOUSE pipe_large
;

WITH
	ses AS
		(
			SELECT
				stba.touch_start_tstamp,
				stba.touch_id,
				stba.touch_duration_seconds AS duration,
				stba.platform,
				CASE
					WHEN stba.touch_hostname LIKE '%magazyn.travelist.pl%' THEN 'MAG'
					ELSE 'TRAVELIST'
				END                         AS landing_page_klasyfikacja,
				stba.touch_landing_page,
				stba.touch_exit_pagepath,
				stba.touch_se_brand
			FROM se.data.scv_touch_basic_attributes stba
			WHERE stba.touch_se_brand = 'Travelist'
			  AND stba.touch_start_tstamp::DATE >= '2023-11-01'
		),

	events AS (
			SELECT
				ses.touch_id,
				CASE
					WHEN str.page_urlhost = 'magazyn.travelist.pl' THEN 'MAG'
					ELSE 'TRAVELIST'
				END AS klasyfikacja
			FROM ses
				INNER JOIN se.data_pii.scv_session_events_link sts
						   ON ses.touch_id = sts.touch_id AND sts.event_tstamp >= '2023-11-01'
				INNER JOIN se.data_pii.scv_event_stream str
						   ON sts.event_hash = str.event_hash AND str.se_brand = 'Travelist'
		),

	klas AS
		(
			SELECT
				e.touch_id,
				COUNT(DISTINCT e.klasyfikacja) AS lpm
			FROM events e
			GROUP BY 1
		),

	bx AS
		(
			SELECT
				ses.touch_id,
				COUNT(DISTINCT tb.booking_id) AS l_rez,
				SUM(tb.margin_cc)             AS gp
			FROM ses
				INNER JOIN se.data.scv_touched_transactions tt ON ses.touch_id = tt.touch_id
				INNER JOIN se.data.tb_booking tb ON tb.booking_id = tt.booking_id
			WHERE tb.se_brand = 'Travelist' AND order_status IN ('PROCESSING', 'COMPLETE')
			GROUP BY 1
		)
		,
	final AS (
			SELECT
				DATE(ses.touch_start_tstamp) AS session_date,
				ses.touch_id,
				ses.duration,
				ses.platform,
				ses.touch_landing_page,
				klas.lpm,
				ses.landing_page_klasyfikacja,
				stmc.touch_mkt_channel,
				stmc.channel_category,
				bx.l_rez,
				bx.gp
			FROM ses
				INNER JOIN se.data.scv_touch_marketing_channel stmc ON stmc.touch_id = ses.touch_id
				INNER JOIN klas ON klas.touch_id = ses.touch_id
				LEFT JOIN  bx ON ses.touch_id = bx.touch_id
		)
SELECT *
FROM final
WHERE lpm > 1
  AND final.l_rez > 0
;



SELECT
	sk.id                                                                         AS se_sale_id,
	record['editorial']                                                           AS editorial,
	record['editorial']['hotelDetails']::VARCHAR                                  AS hotel_details,
	se.data.remove_html_from_string(record['editorial']['hotelDetails']::VARCHAR) AS hotel_details_with_html
FROM se.data.sales_kingfisher sk
;


SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active AND ssa.posa_territory = 'UK'

SELECT *
FROM dbt.bi_product_analytics.