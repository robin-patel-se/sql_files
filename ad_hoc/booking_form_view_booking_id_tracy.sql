------------------------------------------------------------------------------------------------------------------------
-- looking at the booking id logic for tracy booking form views:

WITH
	cte_orders_orderproperty AS (
		SELECT
			orders_orderproperty.order_id,
			orders_orderproperty.value,
			orders_orderproperty.name
		FROM latest_vault.travelbird_mysql.orders_orderproperty orders_orderproperty
		WHERE orders_orderproperty.name = 'form_session_id'
		QUALIFY ROW_NUMBER() OVER (
			PARTITION BY
				orders_orderproperty.value,
				orders_orderproperty.name
			ORDER BY
				orders_orderproperty.updated_at_dts
				DESC) = 1
	),
	cte_booking_forms AS (
		SELECT
			event_stream.event_hash,
			module_touchification.touch_id,
			event_stream.event_tstamp,
			event_stream.se_sale_id,
			-- workaround for the event we receive being an graphql end point event - we need to use the ref page url here
			event_stream.page_referrer                                                  AS page_url,
			event_stream.refr_urlpath                                                   AS page_urlpath,
			event_stream.contexts_com_secretescapes_booking_context_1[0]['id']::VARCHAR AS booking_context_id,
			event_stream.ti_orderid

		FROM hygiene_vault_mvp.snowplow.event_stream event_stream
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification module_touchification
			ON event_stream.event_hash = module_touchification.event_hash
			AND module_touchification.updated_at >= TIMESTAMPADD('day', -1, '2026-03-04 02:00:00'::TIMESTAMP)
		WHERE event_stream.se_brand = 'SE Brand' -- non Travelist events only
		  -- Tracy platform
		  AND event_stream.contexts_com_secretescapes_product_display_context_1[0]['tech_platform']::VARCHAR IS NOT DISTINCT FROM 'Travelbird Platform'
		  -- Booking Form View logic
		  AND event_stream.contexts_com_secretescapes_content_context_1[0]['category']::VARCHAR = 'booking flow'
		  AND event_stream.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'booking form'
		  AND event_stream.contexts_com_secretescapes_content_context_1[0]['name']::VARCHAR = 'booking form'
		  AND event_stream.event_name = 'page_view'
		  AND event_stream.is_server_side_event
	)
-- enhancing with booking_id where possible
SELECT
	booking_forms.event_hash,
	booking_forms.touch_id,
	booking_forms.event_tstamp,
	COALESCE(
			'TB-' || orders_orderproperty_booking_id.order_id,
			'TB-' || orders_orderproperty_page_url.order_id
	) AS booking_id,
	booking_forms.se_sale_id,
	booking_forms.page_url,
	booking_forms.page_urlpath,
	COALESCE(booking_forms.booking_context_id, booking_forms.ti_orderid),
	SPLIT_PART(booking_forms.page_urlpath, '/', 5)
FROM cte_booking_forms booking_forms

	-- primary method -> match on the booking context
LEFT JOIN cte_orders_orderproperty orders_orderproperty_booking_id
	ON COALESCE(booking_forms.booking_context_id, booking_forms.ti_orderid) = orders_orderproperty_booking_id.value

	-- fallback method -> match on the page_url 5th element
LEFT JOIN cte_orders_orderproperty orders_orderproperty_page_url
	ON SPLIT_PART(booking_forms.page_urlpath, '/', 5) = orders_orderproperty_page_url.value
;



TB-22819951


SELECT
	orders_orderproperty.order_id,
	orders_orderproperty.value,
	orders_orderproperty.name
FROM latest_vault.travelbird_mysql.orders_orderproperty orders_orderproperty
WHERE orders_orderproperty.name = 'form_session_id'
  AND orders_orderproperty.order_id = 22819951;

SELECT * FROM latest_vault.travelbird_mysql.orders_orderproperty orders_orderproperty
WHERE orders_orderproperty.name = 'form_session_id'