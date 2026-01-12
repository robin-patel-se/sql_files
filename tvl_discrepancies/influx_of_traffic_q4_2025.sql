SELECT *
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_start_tstamp >= '2025-10-01'
  AND sm.touch_mkt_channel = 'Direct' -- last click
  AND sm.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
;


SELECT
	sm.touch_start_tstamp::DATE,
	COUNT(*)
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_start_tstamp >= '2025-10-01'
  AND sm.touch_mkt_channel = 'Direct' -- last click
  AND sm.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
GROUP BY ALL
;

-- large spikes in direct traffic on the 6th and 7th of november
-- also 6th and 7th of december


-- plotting member non member traffic
SELECT
	sm.touch_start_tstamp::DATE                                                                 AS date,
	COUNT(DISTINCT sm.attributed_user_id)                                                       AS users,
	COUNT(*)                                                                                    AS sessions,
	COUNT(DISTINCT IFF(sm.stitched_identity_type = 'tvl_user_id', sm.attributed_user_id, NULL)) AS member_users,
	COUNT_IF(sm.stitched_identity_type = 'tvl_user_id')                                         AS member_sessions
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_start_tstamp::DATE >= '2025-10-01'
  AND sm.touch_mkt_channel = 'Direct' -- last click
  AND sm.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
GROUP BY ALL
;

-- influx appears to be from non member traffic
-- theory could be that non member traffic isn't being associated to members;


-- investigating direct traffic on the 7th november to look at non member traffic patterns

SELECT *
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_start_tstamp::DATE = '2025-11-07'
  AND sm.touch_mkt_channel = 'Direct' -- last click
  AND sm.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
;

-- weird disable gtm param
SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp::DATE >= '2025-11-01'
  AND ses.se_brand = 'Travelist'
  AND ses.unique_browser_id = '2d00fb43-7268-48e4-b44b-5c1f3cb682ad'
--   AND ses.event_name = 'page_view'
;

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp::DATE >= '2025-11-01'
  AND ses.se_brand = 'Travelist'
  AND ses.unique_browser_id = 'aa2c3fac-db41-4bce-8039-2e2322daff95'
--   AND ses.event_name = 'page_view'
;

-- checking my own traffic
SELECT *
FROM snowplow.atomic.events e
WHERE e.derived_tstamp >= CURRENT_DATE
  AND
	contexts_com_secretescapes_user_context_1[0]['unique_browser_id']::VARCHAR = 'de5c37bd-5a1b-4dfc-a8ba-0f0d292bb26f'
  AND e.event_name = 'page_view'
;



SELECT
	sm.touch_start_tstamp::DATE                                                                       AS date,
	COUNT(DISTINCT sm.attributed_user_id)                                                             AS users,
	COUNT(*)                                                                                          AS sessions,
	COUNT(DISTINCT IFF(sm.stitched_identity_type = 'tvl_user_id', sm.attributed_user_id, NULL))       AS member_users,
	COUNT_IF(sm.stitched_identity_type = 'tvl_user_id')                                               AS member_sessions,
	COUNT(DISTINCT IFF(sm.stitched_identity_type = 'unique_browser_id', sm.attributed_user_id, NULL)) AS ubid_users,
	COUNT_IF(sm.stitched_identity_type = 'unique_browser_id')                                         AS ubid_sessions,
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_start_tstamp::DATE >= '2025-11-01'
  AND sm.touch_mkt_channel = 'Direct' -- last click
  AND sm.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
GROUP BY ALL
;



SELECT
	contexts_com_secretescapes_user_context_1[0]['unique_browser_id']::VARCHAR,
	*
FROM snowplow.atomic.events e
WHERE e.user_id = '5480943' AND e.collector_tstamp::DATE = '2025-11-28'
;

WITH
	data AS
		(
			SELECT
				touch_id,
				stba.attributed_user_id,
				stba.num_spvs
			FROM se.data_pii.scv_touch_basic_attributes stba
			WHERE stba.touch_start_tstamp::DATE >= '2025-11-01'
			  AND stba.touch_se_brand = 'Travelist'
		)
SELECT
	data.attributed_user_id,
	COUNT(*) AS sessions
FROM data
GROUP BY 1
ORDER BY sessions DESC
;


SELECT
	touch_start_tstamp::DATE AS event_date,
	COUNT(*)
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '5480943'
  AND stba.touch_start_tstamp::DATE >= '2025-11-01'
GROUP BY 1
;


SELECT
	ses.event_tstamp::DATE,
	COUNT(DISTINCT ses.unique_browser_id) AS ubids,
	COUNT(DISTINCT ses.user_id)           AS userids
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_brand >= 'Travelist'
  AND ses.event_tstamp >= '2025-09-01'
  AND ses.event_name = 'page_view'
GROUP BY ALL
;



WITH
	data AS
		(
			SELECT
				touch_id,
				stba.attributed_user_id,
				stba.num_spvs
			FROM se.data_pii.scv_touch_basic_attributes stba
			WHERE stba.touch_start_tstamp::DATE >= '2025-11-01'
			  AND stba.touch_se_brand = 'Travelist'
		)
SELECT
	data.num_spvs,
	COUNT(DISTINCT data.attributed_user_id) AS users,
	COUNT(*)                                AS sessions
FROM data
GROUP BY 1
ORDER BY data.num_spvs DESC



-- ubid id
-- non mmeber traffic


-- identified:
-- direct traffic
-- non member
-- web platform


SELECT *
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_start_tstamp::DATE = '2025-12-07'
  AND sm.touch_mkt_channel = 'Direct' -- last click
  AND sm.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id'
  AND sm.touch_experience = 'web'
;


SELECT
	sm.geo_country,
	COUNT(*)
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_start_tstamp::DATE = '2025-11-07'
--   AND sm.touch_start_tstamp::DATE = '2025-12-07'
  AND sm.touch_mkt_channel = 'Direct' -- last click
  AND sm.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id'
  AND sm.touch_experience = 'web'
GROUP BY ALL
;


SELECT
	sm.touch_start_tstamp::DATE     AS date,
-- 	sm.geo_latitude,
-- 	sm.geo_longitude,
	COUNT(*)                        AS total_sessions,
	COUNT_IF(sm.geo_country = 'IE') AS ie_sessions,
	COUNT_IF(sm.geo_country = 'CN') AS cn_sessions,
	COUNT_IF(sm.geo_country = 'PL') AS pl_sessions,
	COUNT_IF(sm.geo_country = 'SG') AS sg_sessions,
	COUNT_IF(sm.geo_country = 'HK') AS hk_sessions,
	COUNT_IF(sm.geo_country = 'MX') AS mx_sessions,
	COUNT_IF(sm.geo_country = 'US') AS us_sessions,
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_start_tstamp::DATE >= '2025-01-01'
  AND sm.touch_mkt_channel = 'Direct' -- last click
  AND sm.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id'
  AND sm.touch_experience = 'web'
GROUP BY ALL
;


SELECT
-- 	sm.touch_start_tstamp::DATE AS date,
sm.geo_latitude,
sm.geo_longitude,
COUNT(*)
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_start_tstamp::DATE >= '2025-10-01'
  AND sm.touch_mkt_channel = 'Direct' -- last click
  AND sm.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id'
  AND sm.touch_experience = 'web'
  AND sm.geo_country = 'CN'
GROUP BY ALL
;


SELECT
-- 	sm.touch_start_tstamp::DATE AS date,
sm.geo_latitude,
sm.geo_longitude,
COUNT(*)
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_start_tstamp::DATE >= '2025-10-01'
  AND sm.touch_mkt_channel = 'Direct' -- last click
  AND sm.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id'
  AND sm.touch_experience = 'web'
  AND sm.geo_country = 'IE'
GROUP BY ALL
;



SELECT --sm.useragent,
	   sm.geo_country,
	   COUNT(*),
	   COUNT(DISTINCT sm.user_ipaddress)
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_start_tstamp::DATE >= '2025-10-01'
  AND sm.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
  AND sm.touch_mkt_channel = 'Direct'                          -- last click direct
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id' -- non member
  AND sm.touch_experience = 'web'                              -- web platform
  AND sm.geo_country IN ('IE', 'CN')
GROUP BY ALL
;


SELECT *
FROM snowflake.account_usage.query_history
WHERE LOWER(query_text) LIKE '%collab.covid_pii.dflo_view_booking_summary%'
  AND start_time >= CURRENT_DATE
;


USE ROLE pipelinerunner
;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage', 'collab.covid_pii.dflo_view_booking_summary')
;

SELECT *
FROM scratch.robinpatel.table_usage
;


-- plotting cn/ie traffic
SELECT
	sm.touch_start_tstamp::DATE                                                          AS date,
	COUNT(DISTINCT sm.attributed_user_id)                                                AS users,
	COUNT(DISTINCT IFF(sm.geo_country IN ('CN', 'IE'), sm.attributed_user_id, NULL))     AS cn_ie_users,
	COUNT(DISTINCT IFF(sm.geo_country NOT IN ('CN', 'IE'), sm.attributed_user_id, NULL)) AS non_cn_ie_users,
	cn_ie_users / users                                                                  AS cn_ie_users_perc,
	COUNT(*)                                                                             AS sessions,
	COUNT_IF(sm.geo_country IN ('CN', 'IE'))                                             AS cn_ie_sessions,
	COUNT_IF(sm.geo_country NOT IN ('CN', 'IE'))                                         AS non_cn_ie_sessions,
	cn_ie_sessions / sessions                                                            AS cn_ie_sessions_perc,
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_start_tstamp::DATE >= '2025-01-01'
  AND sm.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
  AND sm.touch_mkt_channel = 'Direct' -- last click
  AND sm.touch_experience = 'web'     -- web platform
GROUP BY ALL
;