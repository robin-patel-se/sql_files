WITH
	input_data AS (
		SELECT
			ses.page_url,
			IFF(COALESCE(ses.page_urlpath, '') LIKE '%/checkout', SPLIT_PART(ses.page_urlpath, '/', 3),
				NULL)                                                                                                                     AS checkout_offer_id,
			IFF(
					ses.unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM 'booking confirmed',
					ses.se_sale_id,
					NULL)                                                                                                                 AS se_sale_id,
			t1.product_configuration                                                                                                      AS checkout_product_config,
			ses.contexts_com_secretescapes_secret_escapes_sale_context_1[0]['line']::VARCHAR                                              AS conversion_sale_line,
			COALESCE(ses.page_urlpath, '') LIKE '%/checkout'                                                                              AS checkout_page,
			ses.unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM 'booking confirmed' AS conversion_event,
			ses.se_brand
		FROM se.data_pii.scv_event_stream ses
			-- checkout product config
			LEFT JOIN se.data.tb_offer t1 ON SPLIT_PART(ses.page_urlpath, '/', 3) = t1.tb_offer_id
		WHERE ses.event_tstamp >= CURRENT_DATE
		  AND ((ses.page_urlpath LIKE '%/checkout' AND ses.is_server_side_event = FALSE)
			OR
			   (ses.unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM 'booking confirmed' AND
				ses.is_server_side_event)
			)
		  AND ses.se_brand = 'SE Brand'
	)
SELECT
	SUM(IFF(ind.checkout_page, 1, 0))    AS checkout_pages,
	SUM(IFF(ind.conversion_event, 1, 0)) AS conversions,
	conversions / NULLIF(checkout_pages, 0)
FROM input_data ind
WHERE ind.checkout_product_config IS DISTINCT FROM '3PP'
  AND ind.conversion_sale_line IS DISTINCT FROM '3PP'
;


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE AND
	  ses.unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM 'booking confirmed'
;

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE AND
	  ses.page_urlpath LIKE '%/checkout'
;

SELECT *
FROM se.data.tb_offer t
WHERE t.se_sale_id = 'A17426'


SELECT * FROM se.data.scv_touch_basic_attributes stba;