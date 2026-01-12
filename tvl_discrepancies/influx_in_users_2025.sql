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