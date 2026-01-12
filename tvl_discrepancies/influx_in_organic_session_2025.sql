SELECT
	sm.touch_mkt_channel,
	COUNT(*)
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_affiliate_territory <> 'SE TECH'
  AND geo_country NOT IN ('CN', 'IE')
GROUP BY 1
;

SELECT
	DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
	COUNT(*)
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_affiliate_territory <> 'SE TECH'
  AND geo_country NOT IN ('CN', 'IE')
  AND sm.touch_mkt_channel = 'Source SEO'
GROUP BY 1
;


SELECT *
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_affiliate_territory <> 'SE TECH'
  AND geo_country NOT IN ('CN', 'IE')
  AND sm.touch_mkt_channel = 'Source SEO'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) = '2025-10-01'
;

SELECT *
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_affiliate_territory <> 'SE TECH'
  AND geo_country NOT IN ('CN', 'IE')
  AND sm.touch_mkt_channel = 'Source SEO'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) = '2025-10-01'
  AND sm.attributed_user_id = '2839708'
;


SELECT
	sm.stitched_identity_type IS NOT DISTINCT FROM 'tvl_user_id' AS member,
	DATE_TRUNC(MONTH, sm.touch_start_tstamp)                     AS month,
	COUNT(*)
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_affiliate_territory <> 'SE TECH'
  AND geo_country NOT IN ('CN', 'IE')
  AND sm.touch_mkt_channel = 'Source SEO'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) = '2025-10-01'
GROUP BY ALL
;

SELECT
	sm.stitched_identity_type IS NOT DISTINCT FROM 'tvl_user_id' AS is_member,
	DATE_TRUNC(MONTH, sm.touch_start_tstamp)                     AS month,
	COUNT(*)
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_affiliate_territory <> 'SE TECH'
  AND geo_country NOT IN ('CN', 'IE')
  AND sm.touch_mkt_channel = 'Source SEO'
  AND YEAR(sm.touch_start_tstamp) = 2024
GROUP BY ALL
;

SELECT
	sm.stitched_identity_type IS NOT DISTINCT FROM 'tvl_user_id' AS is_member,
	DATE_TRUNC(MONTH, sm.touch_start_tstamp)                     AS month,
	COUNT(*)
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_affiliate_territory <> 'SE TECH'
  AND geo_country NOT IN ('CN', 'IE')
  AND sm.touch_mkt_channel = 'Source SEO'
  AND YEAR(sm.touch_start_tstamp) = 2025
GROUP BY ALL
;

-- there is an increase in non member sessions

SELECT *
FROM se.bi.session_metrics sm
WHERE sm.touch_se_brand = 'Travelist'
  AND sm.touch_affiliate_territory <> 'SE TECH'
  AND geo_country NOT IN ('CN', 'IE')
  AND sm.touch_mkt_channel = 'Source SEO'
  AND DATE_TRUNC(MONTH, sm.touch_start_tstamp) = '2025-10-01'
  AND sm.attributed_user_id = '9c406948-b38f-4bad-b68b-d7e6b1824a2b'
;
