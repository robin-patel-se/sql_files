USE ROLE securityadmin
;

GRANT OWNERSHIP ON ALL semantic views IN SCHEMA se.data
TO ROLE ai_admin;

GRANT OWNERSHIP ON ALL semantic views IN SCHEMA se.bi
TO ROLE ai_admin COPY current GRANTS
;


SELECT *
FROM se.bi.session_metrics sm
WHERE sm.app_state_context IS NOT NULL
;


SELECT
	PARSE_URL(touch_referrer_url, 1)['host']::VARCHAR,
	COUNT(*)
FROM se.bi.session_metrics sm
WHERE sm.touch_start_tstamp >= CURRENT_DATE - 10
GROUP BY ALL
ORDER BY COUNT(*) DESC
;

- CFNetwork

PARSE_URL(TOUCH_REFERRER_URL,1)['HOST']::VARCHAR
          - www.facebook.com
          - www.google.com
          - www.secretescapes.de
          - com.google.android.googlequicksearchbox
          - travelist.pl
          - com.google.android.gm
          - www.secretescapes.com
          - instagram.com
          - ams.creativecdn.com
          - de.sales.secretescapes.com
          - paid.outbrain.com
          - trc.taboola.com
          - www.bing.com
          - co.uk.sales.secretescapes.com
          - www.tiktok.com



SELECT *
FROM se.data.se_event_calendar sec


SELECT
	stai.event_category,
	stai.event_subcategory,
	COUNT(*)
FROM se.data.scv_touched_app_installs stai
GROUP BY ALL