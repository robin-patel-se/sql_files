SELECT
	psm.touch_id,
	SHA2(psm.attributed_user_id)                          AS attributed_user_id_hash,
	psm.stitched_identity_type,
	psm.touch_logged_in,
	psm.touch_start_tstamp::DATE                          AS touch_start_tstamp,
	psm.touch_end_tstamp::DATE                            AS touch_end_tstamp,
	psm.touch_duration_seconds,
	psm.touch_affiliate_territory,
	psm.touch_mkt_channel,
	psm.channel_category,
	psm.lnd_touch_mkt_channel,
	psm.lnd_channel_category,
	psm.touch_experience,
	psm.platform,
	psm.touch_landing_pagepath,
	CASE
		WHEN
			psm.touch_landing_pagepath LIKE '%current-sales%'
				OR psm.touch_landing_pagepath LIKE '%aktuelle-angebote%'
				OR psm.touch_landing_pagepath LIKE '%currentSales'
				OR psm.touch_landing_pagepath LIKE '%aanbedingen%' -- NL
				OR psm.touch_landing_pagepath LIKE '%offerte-in-corso%' -- IT
				OR psm.touch_landing_pagepath LIKE '%nuvaerende-salg%'
				OR psm.touch_landing_pagepath LIKE '%aktuella-kampanjer%'
				OR psm.touch_landing_pagepath = '/'
				OR (psm.touch_landing_pagepath IS NULL AND psm.touch_experience = 'web')
			THEN 'home'
		WHEN
			psm.touch_landing_pagepath LIKE '%/sale-hotel' -- client side
				OR (psm.touch_landing_pagepath LIKE '%/sale-offers' AND psm.touch_experience = 'web')
				OR psm.touch_landing_pagepath LIKE '%/sale' -- client side
				OR psm.touch_landing_page REGEXP
				   '.*\\/(sales.travelbird.([a-z,A-Z]{2}))\/[0-9]{1,6}\\/([a-z,A-Z]|-)*\\/.*' -- e.g. sales.travelbird.de
				OR psm.touch_landing_page REGEXP
				   '.*\\/(sales.([a-z,A-Z]{2}).travelbird.([a-z,A-Z]{2}))\\/[0-9]{1,6}\\/([a-z,A-Z]|-)*\\/.*' -- e.g. sales.fr.travelbird.be
				OR psm.touch_landing_page REGEXP
				   '.*\\/([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z]|-)*\\/.*' -- e.g. de.sales.secretescapes.com
				OR psm.touch_landing_page REGEXP
				   '.*\\/([a-z,A-Z]{2}\\.)([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z]|-)*\\/.*' -- e.g. co.uk.sales.secretescapes.com
			THEN 'sale page'
		WHEN psm.touch_landing_pagepath LIKE '%search%' THEN 'search'
		WHEN psm.touch_landing_pagepath LIKE '%filter%' THEN 'filter'
		WHEN psm.touch_landing_pagepath LIKE '%sale-offers' THEN 'offer'
		WHEN psm.touch_landing_pagepath LIKE '/instant-access%' THEN 'instant access'
		WHEN psm.touch_landing_pagepath LIKE '%book-hotel' THEN 'booking form'
		WHEN psm.touch_landing_pagepath REGEXP '\\/(de|uk)\\/2024.*' THEN 'summer sale'
		WHEN psm.touch_landing_pagepath LIKE '/geschenkgutscheine' THEN 'gift vouchers'
	END                                                   AS landing_page_category,
	psm.touch_hostname,
	psm.touch_exit_pagepath,
	PARSE_URL(psm.touch_referrer_url, 1)['path']::VARCHAR AS touch_referrer_pagepath,
	psm.touch_se_brand,
	psm.touch_event_count,
	psm.touch_has_booking,
	psm.is_se_internal_touch,
	psm.geo_country,
	psm.geo_city,
	psm.geo_region_name,
	psm.br_name,
	psm.br_family,
	psm.os_name,
	psm.os_family,
	psm.os_manufacturer,
	psm.first_login_type,
	psm.last_login_type,
	psm.login_types,
	psm.login_types_count,
	psm.has_pay_button_click,

	------------------------------------------------------------------------------------------------------------------------
	psm.spvs,
	psm.unique_spvs,
	psm.booking_form_views,
	psm.booking_form_views_hotel_plus,
	psm.bookings,
	psm.bookings_hotel_plus,
	psm.margin_gbp,
	psm.searches,
	psm.user_searches,
	psm.page_load_searches,
	psm.min_price_filter_searches,
	psm.max_price_filter_searches,
	psm.pay_button_clicks

FROM dbt.bi_product_analytics__intermediate.pda_session_metrics psm
WHERE psm.touch_start_tstamp >= '2024-01-01'


------------------------------------------------------------------------------------------------------------------------


SELECT
	psm.stitched_identity_type,
	psm.touch_logged_in,
	TO_DATE(psm.touch_start_tstamp)                       AS touch_start_tstamp,
	TO_DATE(psm.touch_end_tstamp)                         AS touch_end_tstamp,
	psm.touch_duration_seconds,
	psm.touch_affiliate_territory,
	psm.touch_mkt_channel,
	psm.channel_category,
	psm.lnd_touch_mkt_channel,
	psm.lnd_channel_category,
	psm.touch_experience,
	psm.platform,
	psm.touch_landing_pagepath,
	psm.touch_hostname,
	psm.touch_exit_pagepath,
	PARSE_URL(psm.touch_referrer_url, 1)['path']::VARCHAR AS touch_referrer_pagepath,
	psm.touch_se_brand,
	psm.touch_event_count,
	psm.touch_has_booking,
	psm.is_se_internal_touch,
	psm.geo_country,
	psm.geo_city,
	psm.geo_region_name,
-- 			psm.br_name,
	psm.br_family,
-- 			psm.os_name,
	psm.os_family,
-- 			psm.os_manufacturer,
-- 			psm.dvce_screenwidth,
-- 			psm.dvce_screenheight,
	CASE
		WHEN
			psm.touch_landing_pagepath LIKE '%current-sales%'
				OR psm.touch_landing_pagepath LIKE '%aktuelle-angebote%'
				OR psm.touch_landing_pagepath LIKE '%currentSales'
				OR psm.touch_landing_pagepath LIKE '%aanbedingen%' -- NL
				OR psm.touch_landing_pagepath LIKE '%offerte-in-corso%' -- IT
				OR psm.touch_landing_pagepath LIKE '%nuvaerende-salg%'
				OR psm.touch_landing_pagepath LIKE '%aktuella-kampanjer%'
				OR psm.touch_landing_pagepath = '/'
				OR (psm.touch_landing_pagepath IS NULL AND psm.touch_experience = 'web')
			THEN 'home'
		WHEN
			psm.touch_landing_pagepath LIKE '%/sale-hotel' -- client side
				OR (psm.touch_landing_pagepath LIKE '%/sale-offers' AND psm.touch_experience = 'web')
				OR psm.touch_landing_pagepath LIKE '%/sale' -- client side
				OR psm.touch_landing_page REGEXP
				   '.*\\/(sales.travelbird.([a-z,A-Z]{2}))\/[0-9]{1,6}\\/([a-z,A-Z]|-)*\\/.*' -- e.g. sales.travelbird.de
				OR psm.touch_landing_page REGEXP
				   '.*\\/(sales.([a-z,A-Z]{2}).travelbird.([a-z,A-Z]{2}))\\/[0-9]{1,6}\\/([a-z,A-Z]|-)*\\/.*' -- e.g. sales.fr.travelbird.be
				OR psm.touch_landing_page REGEXP
				   '.*\\/([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z]|-)*\\/.*' -- e.g. de.sales.secretescapes.com
				OR psm.touch_landing_page REGEXP
				   '.*\\/([a-z,A-Z]{2}\\.)([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z]|-)*\\/.*' -- e.g. co.uk.sales.secretescapes.com
			THEN 'sale page'
		WHEN psm.touch_landing_pagepath LIKE '%search%' THEN 'search'
		WHEN psm.touch_landing_pagepath LIKE '%filter%' THEN 'filter'
		WHEN psm.touch_landing_pagepath LIKE '%sale-offers' THEN 'offer'
		WHEN psm.touch_landing_pagepath LIKE '/instant-access%' THEN 'instant access'
		WHEN psm.touch_landing_pagepath LIKE '%book-hotel' THEN 'booking form'
		WHEN psm.touch_landing_pagepath REGEXP '\\/(de|uk)\\/2024.*' THEN 'summer sale'
		WHEN psm.touch_landing_pagepath LIKE '/geschenkgutscheine' THEN 'gift vouchers'
	END                                                   AS landing_page_category,
	psm.first_login_type,
	psm.last_login_type,
	psm.login_types,
	psm.login_types_count,
	psm.has_pay_button_click,

	------------------------------------------------------------------------------------------------------------------------

	COUNT(DISTINCT psm.attributed_user_id)                AS users,
	COUNT(DISTINCT psm.touch_id)                          AS sessions,
	SUM(psm.spvs)                                         AS spvs,
	SUM(psm.unique_spvs)                                  AS unique_spvs,
	SUM(psm.booking_form_views)                           AS booking_form_views,
	SUM(psm.booking_form_views_hotel_plus)                AS booking_form_views_hotel_plus,
	SUM(psm.bookings)                                     AS bookings,
	SUM(psm.bookings_hotel_plus)                          AS bookings_hotel_plus,
	SUM(psm.margin_gbp)                                   AS margin_gbp,
	SUM(psm.searches)                                     AS searches,
	SUM(psm.user_searches)                                AS user_searches,
	SUM(psm.page_load_searches)                           AS page_load_searches,
	SUM(psm.min_price_filter_searches)                    AS min_price_filter_searches,
	SUM(psm.max_price_filter_searches)                    AS max_price_filter_searches,
	SUM(psm.sort_by_searches)                             AS sort_by_searches,
	SUM(psm.pay_button_clicks)                            AS pay_button_clicks

FROM dbt.bi_product_analytics__intermediate.pda_session_metrics psm
WHERE psm.touch_start_tstamp >= '2024-01-01'
GROUP BY ALL
;


------------------------------------------------------------------------------------------------------------------------
-- reconciliation

-- sessions by month
SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	COUNT(*)                                   AS sessions
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stmc.touch_affiliate_territory NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'PL', 'SE TECH', 'pl', 'travelistpl')
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1
;

-- sessions by channel
SELECT
	stmc.touch_mkt_channel,
	COUNT(*) AS sessions
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stmc.touch_affiliate_territory NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'PL', 'SE TECH', 'pl', 'travelistpl')
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1
;

-- sessions by experience
SELECT
	stba.touch_experience,
	COUNT(*) AS sessions
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stmc.touch_affiliate_territory NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'PL', 'SE TECH', 'pl', 'travelistpl')
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1
;

-- sessions with an spv
SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	COUNT(DISTINCT stba.touch_id)              AS sessions
FROM se.data.scv_touched_spvs sts
	INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stmc.touch_affiliate_territory NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'PL', 'SE TECH', 'pl', 'travelistpl')
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1


-- sessions with an search
SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	COUNT(DISTINCT stba.touch_id)              AS sessions
FROM se.data.scv_touched_searches sts
	INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stmc.touch_affiliate_territory NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'PL', 'SE TECH', 'pl', 'travelistpl')
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1
;


-- sessions with an bfv
SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	COUNT(DISTINCT stba.touch_id)              AS sessions
FROM se.data.scv_touched_booking_form_views stbfv
	INNER JOIN se.data.scv_touch_basic_attributes stba ON stbfv.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stmc.touch_affiliate_territory NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'PL', 'SE TECH', 'pl', 'travelistpl')
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1
;

-- sessions with an transaction
SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	COUNT(DISTINCT stba.touch_id)              AS sessions
FROM se.data.scv_touched_transactions stt
	INNER JOIN se.data.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stmc.touch_affiliate_territory NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'PL', 'SE TECH', 'pl', 'travelistpl')
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1
;

-- spvs
SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	COUNT(sts.event_hash)                      AS spvs
FROM se.data.scv_touched_spvs sts
	INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stmc.touch_affiliate_territory NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'PL', 'SE TECH', 'pl', 'travelistpl')
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1


-- searches
SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	COUNT(sts.event_hash)                      AS searches,
	SUM(IFF(sts.triggered_by = 'user', 1, 0))  AS user_searches
FROM se.data.scv_touched_searches sts
	INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stmc.touch_affiliate_territory NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'PL', 'SE TECH', 'pl', 'travelistpl')
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1
;


-- bfvs
SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	COUNT(stbfv.event_hash)                    AS bfvs
FROM se.data.scv_touched_booking_form_views stbfv
	INNER JOIN se.data.scv_touch_basic_attributes stba ON stbfv.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stmc.touch_affiliate_territory NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'PL', 'SE TECH', 'pl', 'travelistpl')
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1
;

-- bookings
SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	COUNT(stt.event_hash)                      AS bookings
FROM se.data.scv_touched_transactions stt
	INNER JOIN se.data.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stmc.touch_affiliate_territory NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'PL', 'SE TECH', 'pl', 'travelistpl')
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1
;


SELECT
	COUNT(*)
FROM dbt.bi_product_analytics.pda_traffic_model ptm 246,211,032

;

SELECT se.data.page_url_categorisation()


SELECT *
FROM dbt_dev.dbt_robinpatel_product_analytics.pda_traffic_model
;

/*
'psm.stitched_identity_type',
'psm.touch_logged_in',
'TO_DATE(psm.touch_start_tstamp)',
'psm.touch_affiliate_territory',
'psm.touch_mkt_channel',
'psm.channel_category',
'psm.lnd_touch_mkt_channel',
'psm.lnd_channel_category',
'psm.touch_experience',
'psm.platform',
'psm.touch_landing_pagepath',
'psm.touch_hostname',
'PARSE_URL(psm.touch_referrer_url, 1)['path']::VARCHAR AS touch_referrer_pagepath',
'psm.touch_se_brand',
'IFF(psm.touch_duration_seconds = 0, TRUE, FALSE) AS zero_duration_session',
'psm.is_se_internal_touch',
'psm.br_family',
'psm.os_family',
'psm.landing_page_category',
'psm.first_login_type',
'psm.login_types_count',

 */


SELECT
	MD5(CAST(COALESCE(CAST(psm.stitched_identity_type AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.touch_logged_in AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(TO_DATE(psm.touch_start_tstamp) AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.touch_affiliate_territory AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.touch_mkt_channel AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.channel_category AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.lnd_touch_mkt_channel AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.lnd_channel_category AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.touch_experience AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.platform AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.touch_landing_pagepath AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.touch_hostname AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(PARSE_URL(psm.touch_referrer_url, 1):path::VARCHAR AS TEXT),
					  '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.touch_se_brand AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(IFF(psm.touch_duration_seconds = 0, TRUE, FALSE) AS TEXT),
					  '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.is_se_internal_touch AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.br_family AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.os_family AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.landing_page_category AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.first_login_type AS TEXT), '_dbt_utils_surrogate_key_null_') || '-' ||
			 COALESCE(CAST(psm.login_types_count AS TEXT), '_dbt_utils_surrogate_key_null_') AS TEXT)) AS id,
	psm.stitched_identity_type,
	psm.touch_logged_in,
	TO_DATE(psm.touch_start_tstamp)                                                                    AS touch_start_tstamp,
	TO_DATE(psm.touch_start_tstamp)                                                                    AS touch_start_date,
	psm.touch_affiliate_territory,
	psm.touch_mkt_channel,
	psm.channel_category,
	psm.lnd_touch_mkt_channel,
	psm.lnd_channel_category,
	psm.touch_experience,
	psm.platform,
	psm.touch_landing_pagepath,
	psm.touch_hostname,
	PARSE_URL(psm.touch_referrer_url, 1)['path']::VARCHAR                                              AS touch_referrer_pagepath,
	psm.touch_se_brand,
	IFF(psm.touch_duration_seconds = 0, TRUE, FALSE)                                                   AS zero_duration_session,
	psm.is_se_internal_touch,
	psm.br_family,
	psm.os_family,
	psm.landing_page_category,
	psm.first_login_type,
	psm.login_types_count,

	------------------------------------------------------------------------------------------------------------------------
	-- session count metrics
	COUNT(DISTINCT psm.touch_id)                                                                       AS sessions,

	COUNT(DISTINCT IFF(psm.has_search, psm.touch_id, NULL))                                            AS sessions_with_search,
	COUNT(DISTINCT IFF(psm.has_user_search, psm.touch_id, NULL))                                       AS sessions_with_user_search,
	COUNT(DISTINCT IFF(psm.has_page_load_search, psm.touch_id, NULL))                                  AS sessions_with_page_load_search,

	COUNT(DISTINCT IFF(psm.has_spv, psm.touch_id, NULL))                                               AS sessions_with_spv,
	COUNT(DISTINCT IFF(psm.has_spv_hotel_plus, psm.touch_id, NULL))                                    AS sessions_with_spv_hotel_plus,

	COUNT(DISTINCT IFF(psm.has_booking_form_view, psm.touch_id, NULL))                                 AS sessions_with_booking_form_view,
	COUNT(DISTINCT
		  IFF(psm.has_booking_form_view_hotel_plus, psm.touch_id, NULL))                               AS sessions_with_booking_form_view_hotel_plus,
	COUNT(DISTINCT
		  IFF(psm.has_booking_form_view_catalogue, psm.touch_id, NULL))                                AS sessions_with_booking_form_view_catalogue,

	COUNT(DISTINCT IFF(psm.has_pay_button_click, psm.touch_id, NULL))                                  AS sessions_with_pay_button_click,

	COUNT(DISTINCT IFF(psm.has_booking, psm.touch_id, NULL))                                           AS sessions_with_booking,
	COUNT(DISTINCT IFF(psm.has_booking_hotel_plus, psm.touch_id, NULL))                                AS sessions_with_booking_hotel_plus,

	COUNT(DISTINCT
		  IFF(psm.has_search AND psm.has_spv, psm.touch_id, NULL))                                     AS sessions_with_search_and_spv,
	COUNT(DISTINCT
		  IFF(psm.has_user_search AND psm.has_spv, psm.touch_id, NULL))                                AS sessions_with_user_search_and_spv,
	COUNT(DISTINCT
		  IFF(psm.has_page_load_search AND psm.has_spv, psm.touch_id, NULL))                           AS sessions_with_page_load_search_and_spv,
	COUNT(DISTINCT
		  IFF(psm.has_spv AND psm.has_booking, psm.touch_id, NULL))                                    AS sessions_with_spv_and_booking,
	COUNT(DISTINCT
		  IFF(psm.has_spv AND psm.has_booking_form_view, psm.touch_id, NULL))                          AS sessions_with_spv_and_booking_form_view,
	COUNT(DISTINCT IFF(psm.has_booking_form_view AND psm.has_booking, psm.touch_id,
					   NULL))                                                                          AS sessions_with_booking_form_view_and_booking,
	COUNT(DISTINCT IFF(psm.has_pay_button_click AND psm.has_booking, psm.touch_id,
					   NULL))                                                                          AS sessions_with_pay_button_click_and_booking,
	------------------------------------------------------------------------------------------------------------------------

	COUNT(DISTINCT psm.attributed_user_id)                                                             AS users,
	SUM(psm.spvs)                                                                                      AS spvs,
	SUM(psm.unique_spvs)                                                                               AS unique_spvs,
	SUM(psm.booking_form_views)                                                                        AS booking_form_views,
	SUM(psm.booking_form_views_hotel_plus)                                                             AS booking_form_views_hotel_plus,
	SUM(psm.bookings)                                                                                  AS bookings,
	SUM(psm.bookings_hotel_plus)                                                                       AS bookings_hotel_plus,
	SUM(psm.margin_gbp)                                                                                AS margin_gbp,
	SUM(psm.searches)                                                                                  AS searches,
	SUM(psm.user_searches)                                                                             AS user_searches,
	SUM(psm.page_load_searches)                                                                        AS page_load_searches,
	SUM(psm.min_price_filter_searches)                                                                 AS min_price_filter_searches,
	SUM(psm.max_price_filter_searches)                                                                 AS max_price_filter_searches,
	SUM(psm.sort_by_searches)                                                                          AS sort_by_searches,
	SUM(psm.pay_button_clicks)                                                                         AS pay_button_clicks,
	AVG(psm.touch_duration_seconds)                                                                    AS avg_duration_sessions,

FROM dbt_dev.dbt_robinpatel_product_analytics__intermediate.pda_session_metrics psm
WHERE psm.touch_start_tstamp::DATE >= '2023-01-01'
  AND psm.touch_start_tstamp::DATE <= CURRENT_DATE - 1 WHERE
        psm.touch_start_tstamp::DATE >= '2023-01-01'

                AND psm.touch_start_tstamp::DATE > (
                    SELECT MAX(target.touch_start_tstamp::DATE) - 10 FROM dbt_dev.dbt_robinpatel_product_analytics.pda_traffic_model AS target
                )

        AND psm.touch_start_tstamp::DATE <= CURRENT_DATE -1
GROUP BY ALL
;



SELECT
	DATE_TRUNC(WEEK, stba.touch_start_tstamp) AS week,
	stba.touch_experience,
	COUNT(stt.event_hash)                     AS bookings
FROM se.data.scv_touched_transactions stt
	INNER JOIN se.data.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
  AND stmc.touch_affiliate_territory NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'PL', 'SE TECH', 'pl', 'travelistpl')
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1, 2