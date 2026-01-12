USE WAREHOUSE pipe_xlarge;

SELECT
	ses.event_tstamp,
	ses.page_url,
	ses.page_urlpath,
	ses.device_platform                  AS touch_experience,
	SPLIT_PART(ses.page_urlpath, '/', 2) AS microsite_territory,
	SPLIT_PART(ses.page_urlpath, '/', 3) AS microsite_time_period,
	SPLIT_PART(ses.page_urlpath, '/', 4) AS microsite_campaign,
	ses.page_title,
	ssel.attributed_user_id,
	spse.page_duration_seconds
FROM se.data_pii.scv_event_stream ses
INNER JOIN se.data_pii.scv_page_screen_enrichment spse
	ON ses.event_hash = spse.event_hash
	AND spse.event_tstamp >= '2025-11-01'
INNER JOIN se.data_pii.scv_session_events_link ssel
	ON ssel.event_hash = ses.event_hash
	AND ssel.event_tstamp >= '2025-11-01'
INNER JOIN se.data.scv_touch_basic_attributes stba
	ON ssel.touch_id = stba.touch_id
	AND stba.touch_start_tstamp >= '2025-11-01'
WHERE ses.event_name = 'page_view'
  AND ses.page_urlhost LIKE 'mp%'

-- microsites
-- 		  AND ses.page_urlpath LIKE '/uk/2025/morocco/%'

SELECT ds.posa_country FROM se.data.dim_sale ds;

SELECT fb.margin_gross_of_toms_gbp_constant_currency FROM se.data.fact_booking fb;

SELECT sua.membership_account_status, count(*) FROM se.data.se_user_attributes sua GROUP BY ALL;