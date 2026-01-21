SELECT
	YEAR(stba.touch_start_tstamp::date)     AS year_,
	MONTH(stba.touch_start_tstamp::date)    AS mnth_,
	COUNT(*)                                AS num_sessions,
	COUNT(DISTINCT attributed_user_id_hash) AS unique_users,
	SUM(num_spvs),
	SUM(num_trxs)
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp::DATE >= '2024-01-01'
  AND touch_se_brand = 'Travelist'
  AND touch_hostname_territory <> 'SE TECH'
  AND geo_country NOT IN ('CN', 'IE')
GROUP BY 1, 2
ORDER BY 1
;



SELECT
	MONTH(session_metrics.touch_start_tstamp::date) AS mnth_,
	COUNT(DISTINCT IFF(YEAR(session_metrics.touch_start_tstamp) = 2024, session_metrics.attributed_user_id,
					   NULL))                       AS unique_users_2024,
	COUNT(DISTINCT IFF(YEAR(session_metrics.touch_start_tstamp) = 2025, session_metrics.attributed_user_id,
					   NULL))                       AS unique_users_2025,
	unique_users_2025 / unique_users_2024 - 1       AS unique_user_growth
-- 	SUM(IFF(YEAR(session_metrics.touch_start_tstamp) = 2024, 1, 0))                    AS num_sessions_2024,
-- 	SUM(IFF(YEAR(session_metrics.touch_start_tstamp) = 2025, 1, 0))                    AS num_sessions_2025,
-- 	SUM(IFF(YEAR(session_metrics.touch_start_tstamp) = 2024, session_metrics.spvs, 0)) AS spvs_2024,
-- 	SUM(IFF(YEAR(session_metrics.touch_start_tstamp) = 2025, session_metrics.spvs, 0)) AS spvs_2025,
-- 	SUM(IFF(YEAR(session_metrics.touch_start_tstamp) = 2024, session_metrics.bookings, 0)) AS bookings_2024,
-- 	SUM(IFF(YEAR(session_metrics.touch_start_tstamp) = 2025, session_metrics.bookings, 0)) AS bookings_2025,
FROM se.bi.session_metrics
WHERE session_metrics.touch_start_tstamp::DATE >= '2024-01-01'
  AND session_metrics.touch_se_brand = 'Travelist'
  AND session_metrics.touch_affiliate_territory <> 'SE TECH'
  AND session_metrics.geo_country NOT IN ('CN', 'IE')
GROUP BY 1
ORDER BY 1
;



SELECT
	MONTH(session_metrics.touch_start_tstamp::date) AS mnth_,
	COUNT(DISTINCT IFF(YEAR(session_metrics.touch_start_tstamp) = 2024, session_metrics.attributed_user_id,
					   NULL))                       AS unique_users_2024,
	COUNT(DISTINCT IFF(YEAR(session_metrics.touch_start_tstamp) = 2025, session_metrics.attributed_user_id,
					   NULL))                       AS unique_users_2025,
	unique_users_2025 / unique_users_2024 - 1       AS unique_user_growth,

	COUNT(DISTINCT IFF(
			YEAR(session_metrics.touch_start_tstamp) = 2024 AND session_metrics.stitched_identity_type = 'tvl_user_id',
			session_metrics.attributed_user_id,
			NULL))                                  AS unique_members_2024,
	COUNT(DISTINCT IFF(
			YEAR(session_metrics.touch_start_tstamp) = 2025 AND session_metrics.stitched_identity_type = 'tvl_user_id',
			session_metrics.attributed_user_id,
			NULL))                                  AS unique_members_2025,

	COUNT(DISTINCT IFF(
			YEAR(session_metrics.touch_start_tstamp) = 2024 AND
			session_metrics.stitched_identity_type IS DISTINCT FROM 'tvl_user_id',
			session_metrics.attributed_user_id,
			NULL))                                  AS unique_non_members_2024,
	COUNT(DISTINCT IFF(
			YEAR(session_metrics.touch_start_tstamp) = 2025 AND
			session_metrics.stitched_identity_type IS DISTINCT FROM 'tvl_user_id',
			session_metrics.attributed_user_id,
			NULL))                                  AS unique_non_members_2025,
FROM se.bi.session_metrics
WHERE session_metrics.touch_start_tstamp::DATE >= '2024-01-01'
  AND session_metrics.touch_se_brand = 'Travelist'
  AND session_metrics.touch_affiliate_territory <> 'SE TECH'
  AND session_metrics.geo_country NOT IN ('CN', 'IE')
GROUP BY 1
ORDER BY 1
;

-- Unique member counts for member traffic in 2025 is actually below 2024 for October and Novemeber
-- the growth appears to be exclusively coming from non member sessions
-- biggest gap months are July, October and November

SELECT *
FROM se.bi.session_metrics
WHERE MONTH(session_metrics.touch_start_tstamp) IN (7, 10, 11)
  AND session_metrics.touch_se_brand = 'Travelist'
  AND session_metrics.touch_affiliate_territory <> 'SE TECH'
  AND session_metrics.geo_country NOT IN ('CN', 'IE')
  AND session_metrics.stitched_identity_type IS DISTINCT FROM 'tvl_user_id'
;


SELECT MONTH(CURRENT_DATE)

-- unique user growth appears only from mobile web
-- there aren't any outlier countries other than CN and IE that are causing growth, rest appears to be in PL


-- increases in october/november appear to be an influx in zero second sessions
-- influx is also attributed to direct channel


SELECT *
FROM se.bi.session_metrics
WHERE MONTH(session_metrics.touch_start_tstamp) IN (10, 11)
  AND session_metrics.touch_se_brand = 'Travelist'
  AND session_metrics.touch_affiliate_territory <> 'SE TECH'
  AND session_metrics.geo_country NOT IN ('CN', 'IE')
  AND session_metrics.stitched_identity_type IS DISTINCT FROM 'tvl_user_id'
  AND session_metrics.touch_experience = 'mobile web'
  AND session_metrics.touch_mkt_channel = 'Direct'
  AND session_metrics.touch_duration_seconds = 0
;

-- growth is not magazine landing page: `https://magazyn.travelist.pl`


SELECT
	session_metrics.touch_landing_pagepath,
	SPLIT_PART(session_metrics.touch_landing_pagepath, '/', 2),
	CASE
		WHEN SPLIT_PART(session_metrics.touch_landing_pagepath, '/', 2) IN ('hotele', 'search', 'odkryj') THEN 'search'
		WHEN SPLIT_PART(session_metrics.touch_landing_pagepath, '/', 2) IN ('booking') THEN 'booking'
		WHEN TRY_TO_NUMBER(SPLIT_PART(session_metrics.touch_landing_pagepath, '/', 2)) IS NOT NULL
			THEN 'spv'
	END AS first_page_path_dir,
	COUNT(*)
FROM se.bi.session_metrics
WHERE MONTH(session_metrics.touch_start_tstamp) IN (10, 11)
  AND session_metrics.touch_se_brand = 'Travelist'
  AND session_metrics.touch_affiliate_territory <> 'SE TECH'
  AND session_metrics.geo_country NOT IN ('CN', 'IE')
  AND session_metrics.stitched_identity_type IS DISTINCT FROM 'tvl_user_id'
  AND session_metrics.touch_experience = 'mobile web'
  AND session_metrics.touch_mkt_channel = 'Direct'
  AND session_metrics.touch_duration_seconds = 0
GROUP BY ALL
ORDER BY 4 DESC
;


-- didn't find a massive trend in the type of landing page


SELECT *,
	   SPLIT_PART(session_metrics.touch_landing_pagepath, '/', 2),
	   CASE
		   WHEN SPLIT_PART(session_metrics.touch_landing_pagepath, '/', 2) IN ('hotele', 'search', 'odkryj')
			   THEN 'search'
		   WHEN SPLIT_PART(session_metrics.touch_landing_pagepath, '/', 2) IN ('booking') THEN 'booking'
		   WHEN TRY_TO_NUMBER(SPLIT_PART(session_metrics.touch_landing_pagepath, '/', 2)) IS NOT NULL
			   THEN 'spv'
	   END AS first_page_path_dir,
FROM se.bi.session_metrics
WHERE MONTH(session_metrics.touch_start_tstamp) IN (10, 11)
  AND session_metrics.touch_se_brand = 'Travelist'
  AND session_metrics.touch_affiliate_territory <> 'SE TECH'
  AND session_metrics.geo_country NOT IN ('CN', 'IE')
  AND session_metrics.stitched_identity_type IS DISTINCT FROM 'tvl_user_id'
  AND session_metrics.touch_experience = 'mobile web'
  AND session_metrics.touch_mkt_channel = 'Direct'
  AND session_metrics.touch_duration_seconds = 0
;


-- Non SE TECH, CN & IE
-- October Unique users - 981K (vs 714K in 2024) - +267K
-- -- of the 981K 755K were non member


SELECT
	CASE
		WHEN session_metrics.touch_referrer_url LIKE 'https://travelist.pl/%' THEN 'Travelist Domain'
		WHEN session_metrics.touch_referrer_url IS NULL THEN 'Null'
		ELSE 'Other'
	END AS refererrer_type,
	*,
	SPLIT_PART(session_metrics.touch_landing_pagepath, '/', 2),
	CASE
		WHEN SPLIT_PART(session_metrics.touch_landing_pagepath, '/', 2) IN ('hotele', 'search', 'odkryj')
			THEN 'search'
		WHEN SPLIT_PART(session_metrics.touch_landing_pagepath, '/', 2) IN ('booking') THEN 'booking'
		WHEN TRY_TO_NUMBER(SPLIT_PART(session_metrics.touch_landing_pagepath, '/', 2)) IS NOT NULL
			THEN 'spv'
	END AS first_page_path_dir,
FROM se.bi.session_metrics
WHERE MONTH(session_metrics.touch_start_tstamp) IN (10, 11)
  AND session_metrics.touch_se_brand = 'Travelist'
  AND session_metrics.touch_affiliate_territory <> 'SE TECH'
  AND session_metrics.geo_country NOT IN ('CN', 'IE')
  AND session_metrics.stitched_identity_type IS DISTINCT FROM 'tvl_user_id'
  AND session_metrics.touch_experience = 'mobile web'
  AND session_metrics.touch_mkt_channel = 'Direct'
;



USE WAREHOUSE pipe_xlarge
;

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= '2025-11-01'
  AND ses.unique_browser_id = 'de5c37bd-5a1b-4dfc-a8ba-0f0d292bb26f'
;

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= '2025-11-01'
  AND ses.user_ipaddress = '81.107.44.250'
  AND ses.page_urlhost NOT LIKE '%secretescapes%'


SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	COUNT(DISTINCT sm.attributed_user_id)    AS distinct_users,
	COUNT(DISTINCT IFF(sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id', sm.attributed_user_id,
					   NULL))                AS distinct_users_non_member,
	COUNT(DISTINCT IFF(sm.stitched_identity_type = 'tvl_user_id', sm.attributed_user_id,
					   NULL))                AS distinct_users_member
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
GROUP BY ALL
;

WITH
	user_data AS
		(
			SELECT
				ses.user_id,
				ses.unique_browser_id,
				ses.event_tstamp
			FROM se.data_pii.scv_event_stream ses
			WHERE ses.se_brand = 'SE Brand'
			  AND ses.user_id IS NOT NULL
			  AND ses.event_name = 'page_view'
		)
		,
	aggregation AS (
			SELECT
				DATE_TRUNC(MONTH, user_data.event_tstamp)   AS month,
				user_data.user_id,
				COUNT(DISTINCT user_data.unique_browser_id) AS ubids
			FROM user_data
			GROUP BY ALL
		)

SELECT
	aggregation.month,
	COUNT(DISTINCT aggregation.user_id) AS users,
	SUM(aggregation.ubids)              AS ubids
FROM aggregation
GROUP BY ALL
;

USE WAREHOUSE pipe_xlarge
;

-- investigate if non member traffic have a lot of events in a short amount of time
-- bot scoring logic


SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	COUNT(DISTINCT sm.attributed_user_id)    AS distinct_users,

	COUNT(DISTINCT IFF(sm.stitched_identity_type = 'booking_id', sm.attributed_user_id,
					   NULL))                AS booking_id_user_count,
	COUNT(DISTINCT IFF(sm.stitched_identity_type = 'se_user_id', sm.attributed_user_id,
					   NULL))                AS se_user_id_user_count,
	COUNT(DISTINCT IFF(sm.stitched_identity_type = 'cookie_id', sm.attributed_user_id,
					   NULL))                AS cookie_id_user_count,
	COUNT(DISTINCT IFF(sm.stitched_identity_type = 'unique_browser_id', sm.attributed_user_id,
					   NULL))                AS unique_browser_id_user_count,
	COUNT(DISTINCT IFF(sm.stitched_identity_type = 'email_address', sm.attributed_user_id,
					   NULL))                AS email_address_user_count,
	COUNT(DISTINCT IFF(sm.stitched_identity_type = 'tvl_user_id', sm.attributed_user_id,
					   NULL))                AS tvl_user_id_user_count,

FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
GROUP BY ALL
;

SELECT DISTINCT
	sm.stitched_identity_type
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
;

------------------------------------------------------------------------------------------------------------------------
-- investigating if there's any engagement figures that might help us understand this user influx better

SELECT *
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) IN ('2025-10-01', '2024-10-01')
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id' -- non member traffic
  AND sm.touch_affiliate_territory IS DISTINCT FROM ('SE TECH')
  AND sm.geo_country NOT IN ('CN', 'IE')
;


SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	COUNT(*)                                 AS sessions,
	COUNT(DISTINCT sm.attributed_user_id)    AS users
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) IN ('2025-10-01', '2024-10-01')
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id' -- non member traffic
  AND sm.touch_affiliate_territory IS DISTINCT FROM ('SE TECH')
  AND sm.geo_country NOT IN ('CN', 'IE')
GROUP BY ALL
;

/*
MONTH	USERS
2024-10-01 00:00:00.000000000	408151
2025-10-01 00:00:00.000000000	704266 -- + ~300K users

*/

SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	sm.touch_duration_seconds = 0            AS zero_second_session,
	COUNT(DISTINCT sm.attributed_user_id)    AS users
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) IN ('2025-10-01', '2024-10-01')
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id' -- non member traffic
  AND sm.touch_affiliate_territory IS DISTINCT FROM ('SE TECH')
  AND sm.geo_country NOT IN ('CN', 'IE')
GROUP BY ALL
;

/*

MONTH							ZERO_SECON_SESSION	USERS
2024-10-01 00:00:00.000000000	false				233535
2025-10-01 00:00:00.000000000	false				380000
2024-10-01 00:00:00.000000000	true				207200
2025-10-01 00:00:00.000000000	true				377526

*/

SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	CASE
		WHEN has_booking = TRUE THEN 'Converted'
		WHEN has_booking_form_view = TRUE THEN 'High Engagement'
		WHEN has_spv = TRUE AND unique_spvs > 2 THEN 'Medium Engagement'
		WHEN has_user_search = TRUE THEN 'Medium Engagement'
		WHEN has_spv = TRUE THEN 'Low Engagement'
		WHEN has_search = TRUE THEN 'Low Engagement'
		ELSE 'Minimal Engagement'
	END                                      AS session_funnel_engagement,
	COUNT(DISTINCT sm.attributed_user_id)    AS users
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) IN ('2025-10-01', '2024-10-01')
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id' -- non member traffic
  AND sm.touch_affiliate_territory IS DISTINCT FROM ('SE TECH')
  AND sm.geo_country NOT IN ('CN', 'IE')
GROUP BY ALL
;

/*
MONTH							SESSION_FUNNEL_ENGAGEMENT	USERS
2024-10-01 00:00:00.000000000	Converted					107
2025-10-01 00:00:00.000000000	Converted					397
2024-10-01 00:00:00.000000000	High Engagement				1278
2025-10-01 00:00:00.000000000	High Engagement				630
2024-10-01 00:00:00.000000000	Low Engagement				327379
2025-10-01 00:00:00.000000000	Low Engagement				548173
2024-10-01 00:00:00.000000000	Medium Engagement			2899
2025-10-01 00:00:00.000000000	Medium Engagement			15758
2024-10-01 00:00:00.000000000	Minimal Engagement			86020
2025-10-01 00:00:00.000000000	Minimal Engagement			168603

*/

SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	sm.touch_mkt_channel,
	COUNT(DISTINCT sm.attributed_user_id)    AS users
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) IN ('2025-10-01', '2024-10-01')
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id' -- non member traffic
  AND sm.touch_affiliate_territory IS DISTINCT FROM ('SE TECH')
  AND sm.geo_country NOT IN ('CN', 'IE')
GROUP BY ALL
ORDER BY 2,1
;

/*
MONTH	TOUCH_MKT_CHANNEL	USERS
2024-10-01 00:00:00.000000000	Afiliacja	27732
2025-10-01 00:00:00.000000000	Afiliacja	21994
2024-10-01 00:00:00.000000000	Direct	44831
2025-10-01 00:00:00.000000000	Direct	200592
2024-10-01 00:00:00.000000000	Display	39714
2025-10-01 00:00:00.000000000	Display	58592
2024-10-01 00:00:00.000000000	Google Ads Brand	8245
2025-10-01 00:00:00.000000000	Google Ads Brand	24854
2024-10-01 00:00:00.000000000	Google Ads Generic	6199
2025-10-01 00:00:00.000000000	Google Ads Generic	16245
2024-10-01 00:00:00.000000000	Google Ads Inne	61899
2025-10-01 00:00:00.000000000	Google Ads Inne	85380
2024-10-01 00:00:00.000000000	Magazyn	276
2025-10-01 00:00:00.000000000	Magazyn	441
2024-10-01 00:00:00.000000000	Mailing	24757
2025-10-01 00:00:00.000000000	Mailing	13761
2024-10-01 00:00:00.000000000	Newsletter	3172
2025-10-01 00:00:00.000000000	Newsletter	8581
2024-10-01 00:00:00.000000000	Organic Social	74212
2025-10-01 00:00:00.000000000	Organic Social	123753
2024-10-01 00:00:00.000000000	Paid Social	57302
2025-10-01 00:00:00.000000000	Paid Social	65120
2024-10-01 00:00:00.000000000	Push	1716
2025-10-01 00:00:00.000000000	Push	4230
2024-10-01 00:00:00.000000000	Referral	4112
2025-10-01 00:00:00.000000000	Referral	20505
2024-10-01 00:00:00.000000000	Remarketing	9868
2025-10-01 00:00:00.000000000	Remarketing	11687
2025-10-01 00:00:00.000000000	SMS	66
2024-10-01 00:00:00.000000000	Source SEO	72468
2025-10-01 00:00:00.000000000	Source SEO	103074
2024-10-01 00:00:00.000000000	Test	49
2025-10-01 00:00:00.000000000	Test	3
2024-10-01 00:00:00.000000000	Video	20
2025-10-01 00:00:00.000000000	Video	4008

*/

SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	CASE
		WHEN SPLIT_PART(sm.touch_landing_pagepath, '/', 2) IN ('hotele', 'search', 'odkryj') THEN 'search'
		WHEN SPLIT_PART(sm.touch_landing_pagepath, '/', 2) IN ('booking') THEN 'booking'
		WHEN TRY_TO_NUMBER(SPLIT_PART(sm.touch_landing_pagepath, '/', 2)) IS NOT NULL
			THEN 'spv'
	END                                      AS first_page_path_dir,
	COUNT(DISTINCT sm.attributed_user_id)    AS users
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) IN ('2025-10-01', '2024-10-01')
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id' -- non member traffic
  AND sm.touch_affiliate_territory IS DISTINCT FROM ('SE TECH')
  AND sm.geo_country NOT IN ('CN', 'IE')
GROUP BY ALL
ORDER BY 2,1
;


-- ip to see if there are any common ones
SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	sm.user_ipaddress,
	COUNT(DISTINCT sm.attributed_user_id)    AS users
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) IN ('2025-10-01', '2024-10-01')
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id' -- non member traffic
  AND sm.touch_affiliate_territory IS DISTINCT FROM ('SE TECH')
  AND sm.geo_country NOT IN ('CN', 'IE')
GROUP BY ALL
ORDER BY 3 DESC
;
-- there are a few but again nothing substaintial to explain the 300K

SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	sm.geo_city,
	COUNT(DISTINCT sm.attributed_user_id)    AS users
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) IN ('2025-10-01', '2024-10-01')
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id' -- non member traffic
  AND sm.touch_affiliate_territory IS DISTINCT FROM ('SE TECH')
  AND sm.geo_country NOT IN ('CN', 'IE')
GROUP BY ALL
ORDER BY 3 DESC
;

SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	sm.useragent,
	COUNT(DISTINCT sm.attributed_user_id)    AS users
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) IN ('2025-10-01', '2024-10-01')
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id' -- non member traffic
  AND sm.touch_affiliate_territory IS DISTINCT FROM ('SE TECH')
  AND sm.geo_country NOT IN ('CN', 'IE')
GROUP BY ALL
ORDER BY 3 DESC
;


SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	sm.dvce_screenwidth || 'x' || sm.dvce_screenheight,
	COUNT(DISTINCT sm.attributed_user_id)    AS users
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) IN ('2025-10-01', '2024-10-01')
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id' -- non member traffic
  AND sm.touch_affiliate_territory IS DISTINCT FROM ('SE TECH')
  AND sm.geo_country NOT IN ('CN', 'IE')
GROUP BY ALL
ORDER BY 3 DESC
;


SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	CASE
		-- Android Reduction
		WHEN sm.useragent LIKE '%Android 10; K%' THEN 'Reduced (Android)'
		-- Mac Reduction
		WHEN sm.useragent LIKE '%Macintosh; Intel Mac OS X 10_15_7%' THEN 'Reduced (macOS)'
		-- Windows Reduction
		WHEN sm.useragent LIKE '%Windows NT 10.0; Win64; x64%'
			AND sm.useragent REGEXP '.*Chrome/[0-9]+\\.0\\.0\\.0.*' THEN 'Reduced (Windows)'
		ELSE 'Legacy / Detailed'
	END                                      AS ua_privacy_status,
	COUNT(DISTINCT sm.attributed_user_id)    AS users
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) IN ('2025-10-01', '2024-10-01')
  AND sm.stitched_identity_type IS DISTINCT FROM 'tvl_user_id' -- non member traffic
  AND sm.touch_affiliate_territory IS DISTINCT FROM ('SE TECH')
  AND sm.geo_country NOT IN ('CN', 'IE')
GROUP BY ALL
ORDER BY 3 DESC
;

WITH filtered_touches AS (
    SELECT
    user_ipaddress,
        stba.touch_id,
        platform,
        touch_duration_seconds,
        attributed_user_id,
        useragent,
        stba.touch_start_tstamp::DATE AS touch_date
    FROM se.data_pii.scv_touch_basic_attributes stba
    WHERE stba.touch_start_tstamp >= '2025-10-01'  -- Use timestamp comparison for index usage
      AND stba.touch_se_brand = 'Travelist'
      AND stba.touch_hostname_territory <> 'SE TECH'
      and useragent like '%OneTrust%'
)

select touch_mkt_channel, useragent, sum(l_ses)
from
(SELECT
    month(ft.touch_date),

    user_ipaddress,
    platform,
    attributed_user_id,
    useragent,
    stmc.touch_mkt_channel,
   -- touch_id,
count(distinct ft.touch_id) l_ses,
    COUNT(DISTINCT ft.TOUCH_ID) AS sessions_total,
     count(distinct attributed_user_id)

FROM filtered_touches ft
join SE.DATA.SCV_TOUCH_MARKETING_CHANNEL stmc on ft.touch_id=stmc.touch_id
where date(stmc.TOUCH_START_TSTAMP )>='2025-10-01'

GROUP BY 1,2,3,4,5,6
order by 2 desc
)zz
group by 1,2;


SELECT * FROM se.data_pii.scv_event_stream ses WHERE ses.event_tstamp::DATE = current_date