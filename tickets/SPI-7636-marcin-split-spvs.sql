SELECT
	c.event_hash,
	a.touch_id,
	c.page_url,
	c.event_tstamp,
	c.is_server_side_event,
	c.page_referrer,
	c.device_platform,
	c.mkt_medium,
	c.mkt_source,
	c.mkt_term,
	c.mkt_content,
	c.mkt_campaign,
	a.num_spvs,
	c.user_id,
	c.unique_browser_id
FROM se.data_pii.scv_event_stream c

LEFT JOIN se.data_pii.scv_session_events_link b
			  ON c.event_hash = b.event_hash

LEFT JOIN se.data_pii.scv_touch_basic_attributes a
			  ON a.touch_id = b.touch_id
WHERE DATE(c.event_tstamp) = '2025-07-31'
  AND DATE(b.event_tstamp) = '2025-07-31'
  AND DATE(touch_start_tstamp) = '2025-07-31'
  AND unique_browser_id = 'ac743abf-0cf7-4cf2-b551-4f881dfe1e05'
  AND touch_se_brand = 'Travelist'
ORDER BY event_tstamp;


SELECT * FROm data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker murm

SELECT * FROM