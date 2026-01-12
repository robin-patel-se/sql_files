/*
WITH
	exploding_clicks AS (
		SELECT
			touchification.touch_id,
			page_screen_enrichment.event_tstamp,
			page_screen_enrichment.event_hash,
			page_screen_enrichment.page_url,
			page_screen_enrichment.content_interaction_array,
			clicks.value,
			clicks.value['element_category']::VARCHAR     AS element_category,
			clicks.value['element_sub_category']::VARCHAR AS element_sub_category,
			clicks.value['interaction_type']::VARCHAR     AS interaction_type,
			clicks.value['sale_id']::VARCHAR              AS se_sale_id,
		FROM data_vault_mvp.single_customer_view_stg.page_screen_enrichment page_screen_enrichment
				 INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification touchification
							ON page_screen_enrichment.event_hash = touchification.event_hash
								AND touchification.event_tstamp >= '2024-01-01',
			 LATERAL FLATTEN(INPUT => page_screen_enrichment.content_interaction_array, OUTER => TRUE) clicks
		WHERE page_screen_enrichment.event_tstamp >= '2024-01-01'
		  AND page_screen_enrichment.event_name = 'page_view'
		  AND page_screen_enrichment.page_url LIKE '%search/search%'
		  AND page_screen_enrichment.content_interaction_array IS NOT NULL
		  AND (
			clicks.value['element_category']::VARCHAR IS NULL
				OR clicks.value['element_category']::VARCHAR IN ('search results', 'kronos_recommended_for_you')
			)
	)
SELECT
	exploding_clicks.touch_id,
	COUNT(*)                                                                                AS search_clicks,
	SUM(IFF(exploding_clicks.element_category IS NOT DISTINCT FROM 'search results', 1, 0)) AS search_results_clicks,
	SUM(IFF(exploding_clicks.element_category IS NULL OR
			exploding_clicks.element_category = 'kronos_recommended_for_you', 1,
			0))                                                                             AS search_results_kronos_clicks,
	ARRAY_AGG(DISTINCT exploding_clicks.se_sale_id)                                         AS search_clicks_array,
	ARRAY_AGG(DISTINCT
			  IFF(exploding_clicks.element_category IS NOT DISTINCT FROM 'search results', exploding_clicks.se_sale_id,
				  NULL))                                                                    AS search_results_clicks_array,
	ARRAY_AGG(DISTINCT IFF(exploding_clicks.element_category IS NULL OR
						   exploding_clicks.element_category = 'kronos_recommended_for_you',
						   exploding_clicks.se_sale_id,
						   NULL))                                                           AS search_results_kronos_clicks_array,
FROM exploding_clicks
GROUP BY 1
;*/

USE WAREHOUSE pipe_xlarge


WITH
	explode_clicks AS (
		SELECT
			touchification.touch_id,
			page_screen_enrichment.event_tstamp,
			page_screen_enrichment.event_hash,
			page_screen_enrichment.page_url,
			page_screen_enrichment.content_interaction_array,
			PARSE_URL(page_screen_enrichment.page_url, 1)['parameters']['travelTypes']::VARCHAR AS page_url_travel_types,
			clicks.value,
			clicks.value['element_category']::VARCHAR                                           AS element_category,
			clicks.value['element_sub_category']::VARCHAR                                       AS element_sub_category,
			clicks.value['interaction_type']::VARCHAR                                           AS interaction_type,
			clicks.value['sale_id']::VARCHAR                                                    AS se_sale_id,
		FROM data_vault_mvp.single_customer_view_stg.page_screen_enrichment page_screen_enrichment
			 INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification touchification
							ON page_screen_enrichment.event_hash = touchification.event_hash
							AND touchification.event_tstamp >= '2025-07-01',
			 LATERAL FLATTEN(INPUT => page_screen_enrichment.content_interaction_array, OUTER => TRUE) clicks
		WHERE page_screen_enrichment.event_tstamp >= '2025-07-01'
		  AND page_screen_enrichment.event_name = 'page_view'
		  AND PARSE_URL(page_screen_enrichment.page_url, 1)['path']::VARCHAR = 'search/search'
		  AND page_screen_enrichment.content_interaction_array IS NOT NULL
		  AND (
			clicks.value['element_category']::VARCHAR IS NULL
				OR clicks.value['element_category']::VARCHAR IS NOT DISTINCT FROM 'kronos_recommended_for_you'
			)
		  AND (PARSE_URL(page_screen_enrichment.page_url, 1)['parameters']['travelTypes']::VARCHAR IS NULL
			OR PARSE_URL(page_screen_enrichment.page_url, 1)['parameters']['travelTypes']::VARCHAR IN
			   ('HOTEL_ONLY', 'WITH_FLIGHTS')
			)
	)

SELECT
	explode_clicks.event_tstamp::DATE                                  AS date,
	page_url_travel_types,
	COUNT(*)                                                           AS total_clicks,
	SUM(IFF(dim_sale.product_configuration = 'Hotel', 1, 0))           AS hotel_clicks,
	SUM(IFF(dim_sale.product_configuration = '3PP', 1, 0))             AS third_party_packages_clicks,
	SUM(IFF(dim_sale.product_configuration = 'WRD - direct', 1, 0))    AS wrd_direct_clicks,
	SUM(IFF(dim_sale.product_configuration = 'Catalogue', 1, 0))       AS catalogue_clicks,
	SUM(IFF(dim_sale.product_configuration = 'N/A', 1, 0))             AS na_clicks,
	SUM(IFF(dim_sale.product_configuration = 'IHP - dynamic', 1, 0))   AS ihp_dynamic_clicks,
	SUM(IFF(dim_sale.product_configuration = 'IHP - connected', 1, 0)) AS ihp_connected_clicks,
	SUM(IFF(dim_sale.product_configuration = 'IHP - static', 1, 0))    AS ihp_static_clicks,
	SUM(IFF(dim_sale.product_configuration = 'Hotel Plus', 1, 0))      AS hotel_plus_clicks,
	SUM(IFF(dim_sale.product_configuration = 'WRD', 1, 0))             AS wrd_clicks,
FROM explode_clicks
INNER JOIN se.data.dim_sale dim_sale
			   ON explode_clicks.se_sale_id = dim_sale.se_sale_id
GROUP BY 1, 2
;



https://www.secretescapes.com/search/search?travelTypes=WITH_FLIGHTS&query=porto
https://www.secretescapes.com/search/search?travelTypes=HOTEL_ONLY&query=porto


SELECT
	page_screen_enrichment.page_url,
	PARSE_URL(page_screen_enrichment.page_url, 1)['path']::VARCHAR                      AS page_url_path,
	PARSE_URL(page_screen_enrichment.page_url, 1)['parameters']['travelTypes']::VARCHAR AS page_url_travel_types
FROM data_vault_mvp.single_customer_view_stg.page_screen_enrichment page_screen_enrichment
WHERE page_screen_enrichment.event_tstamp >= '2025-07-01'
  AND page_screen_enrichment.event_name = 'page_view'
  AND PARSE_URL(page_screen_enrichment.page_url, 1)['path']::VARCHAR = 'search/search'
;

WITH
	agg_data AS (
		SELECT
			sds.spv_date,
			ds.product_configuration,
			SUM(sds.member_spvs) AS member_spvs
		FROM se.bi.sale_date_spvs sds
		INNER JOIN se.data.dim_sale ds
					   ON sds.se_sale_id = ds.se_sale_id
		WHERE sds.spv_date >= '2025-06-01'
		GROUP BY 1, 2
	)
SELECT *
FROM agg_data
	PIVOT ( SUM(member_spvs) FOR product_configuration IN (SELECT DISTINCT product_configuration FROM agg_data))
;


WITH
	agg_data AS (
		SELECT
			sds.spv_date,
			ds.product_type,
			SUM(sds.member_spvs) AS member_spvs
		FROM se.bi.sale_date_spvs sds
		INNER JOIN se.data.dim_sale ds
					   ON sds.se_sale_id = ds.se_sale_id
		WHERE sds.spv_date >= '2025-06-01'
		GROUP BY 1, 2
	)
SELECT *
FROM agg_data
	PIVOT ( SUM(member_spvs) FOR product_type IN (SELECT DISTINCT product_type FROM agg_data))
;


/*
if([Product Type]='Hotel' and [POSU Region] = 'DACH POSu' and [POSA Category] = 'DACH' and ([Se Year])=2025) then 'DACH Domestic 2023'
ELSEIF ([Product Type]='Hotel' and [POSU Region] <> 'DACH POSu' and [POSA Category] = 'DACH' and ([Se Year])=2025) then 'DACH International 2023'
ELSEIF ([Product Type]='Package' and [POSA Category] = 'DACH' and ([Se Year])=2025) then 'DACH Package 2023'

ELSEIF ([Product Type]='WRD' and [POSA Category] = 'DACH' and ([Se Year])=2025) then 'DACH Package 2023'

end*/

-- repliating code from deal model
WITH
	model_data AS (
		SELECT
			sds.spv_date,
-- 	dst.product_type,
			CASE
				WHEN dst.product_type = 'Hotel' AND dst.posu_region = 'DACH POSu' AND dst.posa_category = 'DACH'
					THEN 'DACH Domestic'
				WHEN dst.product_type = 'Hotel' AND dst.posu_region != 'DACH POSu' AND dst.posa_category = 'DACH'
					THEN 'DACH International'
				WHEN dst.product_type = 'Package' AND dst.posa_category = 'DACH'
					THEN 'DACH Package'
				WHEN dst.product_type = 'WRD' AND dst.posa_category = 'DACH'
					THEN 'DACH Package'
			END                  AS category,
			SUM(sds.member_spvs) AS member_spvs
		FROM se.bi.sale_date_spvs sds
		INNER JOIN se.bi.dim_sale_territory dst
					   ON sds.se_sale_id = dst.se_sale_id AND sds.posa_territory = dst.posa_territory
		WHERE sds.spv_date >= '2025-01-01'
		GROUP BY 1, 2
	)
SELECT
	model_data.spv_date,
	SUM(IFF(model_data.category = 'DACH Domestic', model_data.member_spvs, NULL))      AS dach_domestic_spvs,
	SUM(IFF(model_data.category = 'DACH International', model_data.member_spvs, NULL)) AS dach_international_spvs,
	SUM(IFF(model_data.category = 'DACH Package', model_data.member_spvs, NULL))       AS dach_package_spvs,
FROM model_data
GROUP BY 1
;


SELECT *
FROM se.bi.sale_date_spvs sds
INNER JOIN se.bi.dim_sale_territory dst
			   ON sds.se_sale_id = dst.se_sale_id AND sds.posa_territory = dst.posa_territory
WHERE sds.spv_date >= '2025-01-01'
;

-- checking that touched spvs show same movement
SELECT
	sts.event_tstamp::DATE AS spv_date,
-- 	ds.product_type,
	COUNT(*)
FROM se.data.scv_touched_spvs sts
INNER JOIN se.data.scv_touch_basic_attributes stba
			   ON sts.touch_id = stba.touch_id
			   AND stba.touch_start_tstamp >= '2025-01-01'
INNER JOIN se.data.scv_touch_marketing_channel stmc
			   ON sts.touch_id = stmc.touch_id
			   AND stmc.touch_start_tstamp >= '2025-01-01'
INNER JOIN se.data.dim_sale ds
			   ON sts.se_sale_id = ds.se_sale_id
WHERE sts.event_tstamp >= '2025-01-01'
  AND ds.product_type IN ('Package', 'WRD')                -- how package is derived in deal model
  AND stba.touch_se_brand = 'SE Brand'
  AND stba.stitched_identity_type = 'se_user_id'           -- member spvs
  AND stmc.touch_affiliate_territory IN ('DE', 'AT', 'CH') -- dach - to compare with deal model
GROUP BY ALL
;

-- this matches DACH member spvs in deal model

WITH
	spv_investigation AS (
		SELECT
			sts.*,
			sts.event_tstamp::DATE                                  AS spv_date,
			sc.se_week,
			stba.touch_experience,
			stmc.touch_affiliate_territory,
			ds.posa_territory,
			se.data.posa_category_from_territory(ds.posa_territory) AS posa_category,
			stmc.touch_mkt_channel,
			PARSE_URL(ses.page_referrer, 1)['host']::VARCHAR        AS referrer_host,
			PARSE_URL(ses.page_referrer, 1)['path']::VARCHAR        AS referrer_path,
			ses.page_referrer
		FROM se.data.scv_touched_spvs sts
		INNER JOIN se.data.scv_touch_basic_attributes stba
					   ON sts.touch_id = stba.touch_id
					   AND stba.touch_start_tstamp >= '2025-01-01'
		INNER JOIN se.data.scv_touch_attribution sta
					   ON sts.touch_id = sta.touch_id
					   AND sta.attribution_model = 'last non direct'
					   AND sta.touch_start_tstamp >= '2025-01-01'
		INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON sta.attributed_touch_id = stmc.touch_id
					   AND stmc.touch_start_tstamp >= '2025-01-01'
		INNER JOIN se.data.dim_sale ds
					   ON sts.se_sale_id = ds.se_sale_id
		INNER JOIN se.data_pii.scv_event_stream ses
					   ON sts.event_hash = ses.event_hash AND ses.event_tstamp >= '2025-01-01'
		INNER JOIN se.data.se_calendar sc
					   ON sts.event_tstamp::DATE = sc.date_value
		WHERE sts.event_tstamp >= '2025-01-01'
		  AND ds.product_type IN ('Package', 'WRD') -- how package is derived in deal model
		  AND stba.touch_se_brand = 'SE Brand'
		  AND stba.stitched_identity_type = 'se_user_id' -- member spvs
-- 		  AND stmc.touch_affiliate_territory IN ('DE', 'AT', 'CH') -- dach - to compare with deal model
	)
SELECT
	si.spv_date,
	si.se_sale_id,
	si.se_week,
	si.posa_category,
	si.touch_mkt_channel,
	si.touch_experience,
	si.event_category,
	si.referrer_host,
	si.referrer_path LIKE '%search/search%' AS referred_from_search,
	COUNT(*)                                AS spvs
FROM spv_investigation si
GROUP BY ALL
;

-- WHERE touch_affiliate_territory_category = 'DACH' -- to check matches query above

USE WAREHOUSE pipe_xlarge
;


WITH
	spv_investigation AS (
		SELECT
			sts.*,
			sts.event_tstamp::DATE                                  AS spv_date,
			sc.se_week,
			stba.touch_experience,
			ds.posa_territory,
			se.data.posa_category_from_territory(ds.posa_territory) AS posa_category,
		FROM se.data.scv_touched_spvs sts
		INNER JOIN se.data.scv_touch_basic_attributes stba
					   ON sts.touch_id = stba.touch_id
					   AND stba.touch_start_tstamp >= '2025-01-01'
		INNER JOIN se.data.dim_sale ds
					   ON sts.se_sale_id = ds.se_sale_id
		INNER JOIN se.data.se_calendar sc
					   ON sts.event_tstamp::DATE = sc.date_value
		WHERE sts.event_tstamp >= '2025-01-01'
		  AND ds.product_type IN ('Package', 'WRD') -- how package is derived in deal model
		  AND stba.touch_se_brand = 'SE Brand'
		  AND stba.stitched_identity_type = 'se_user_id' -- member spvs
-- 		  AND stmc.touch_affiliate_territory IN ('DE', 'AT', 'CH') -- dach - to compare with deal model
	),
	sale_aggregations AS (
		SELECT
			si.se_week,
			si.se_sale_id,
			COUNT(*) AS spvs
		FROM spv_investigation si
		GROUP BY ALL
	)
SELECT
	sagg.se_week,
	sagg.se_sale_id,
	MIN(sagg.se_week) OVER (PARTITION BY sagg.se_sale_id)                            AS first_spv_week,
	IFF(first_spv_week >= 25, 'went live after week 24', 'went live before week 24') AS go_live_category,
	sagg.spvs,
	SUM(sagg.spvs) OVER (PARTITION BY sagg.se_week)                                  AS total_week_spvs,
	sagg.spvs / total_week_spvs
FROM sale_aggregations sagg


SELECT
	sc.date_value,
	sc.se_week
FROM se.data.se_calendar sc
WHERE sc.date_value BETWEEN '2025-01-01' AND CURRENT_DATE

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.package_spv_events AS (
	SELECT
		sts.*,
		sts.event_tstamp::DATE                                  AS spv_date,
		sc.se_week,
		stba.touch_experience,
		stmc.touch_affiliate_territory,
		ds.posa_territory,
		se.data.posa_category_from_territory(ds.posa_territory) AS posa_category,
		stmc.touch_mkt_channel,
		PARSE_URL(ses.page_referrer, 1)['host']::VARCHAR        AS referrer_host,
		PARSE_URL(ses.page_referrer, 1)['path']::VARCHAR        AS referrer_path,
		ses.page_referrer
	FROM se.data.scv_touched_spvs sts
	INNER JOIN se.data.scv_touch_basic_attributes stba
				   ON sts.touch_id = stba.touch_id
				   AND stba.touch_start_tstamp >= '2025-01-01'
	INNER JOIN se.data.scv_touch_attribution sta
				   ON sts.touch_id = sta.touch_id
				   AND sta.attribution_model = 'last non direct'
				   AND sta.touch_start_tstamp >= '2025-01-01'
	INNER JOIN se.data.scv_touch_marketing_channel stmc
				   ON sta.attributed_touch_id = stmc.touch_id
				   AND stmc.touch_start_tstamp >= '2025-01-01'
	INNER JOIN se.data.dim_sale ds
				   ON sts.se_sale_id = ds.se_sale_id
	INNER JOIN se.data_pii.scv_event_stream ses
				   ON sts.event_hash = ses.event_hash AND ses.event_tstamp >= '2025-01-01'
	INNER JOIN se.data.se_calendar sc
				   ON sts.event_tstamp::DATE = sc.date_value
	WHERE sts.event_tstamp >= '2025-01-01'
	  AND ds.product_type IN ('Package', 'WRD') -- how package is derived in deal model
	  AND stba.touch_se_brand = 'SE Brand'
	  AND stba.stitched_identity_type = 'se_user_id' -- member spvs
)
;

USE WAREHOUSE pipe_xlarge
;

SELECT
	pse.se_week,
	SUM(IFF(pse.page_referrer IS NULL, 1, 0))     AS no_page_referrer,
	SUM(IFF(pse.page_referrer IS NOT NULL, 1, 0)) AS no_page_referrer
FROM scratch.robinpatel.package_spv_events pse
GROUP BY ALL
;


SELECT *
FROM scratch.robinpatel.package_spv_events pse
WHERE pse.page_referrer LIKE '%search%'
;

USE WAREHOUSE pipe_xlarge
;

SELECT *
FROM se.data.scv_touched_spvs sts
INNER JOIN se.data_pii.scv_event_stream ses
			   ON sts.event_hash = ses.event_hash AND ses.event_tstamp >= CURRENT_DATE - 1
WHERE sts.event_tstamp >= CURRENT_DATE - 1
  AND ses.page_referrer LIKE '%search%'
;

SELECT *
FROM se.data.scv_touched_spvs sts
INNER JOIN se.data_pii.scv_touch_basic_attributes stba
			   ON sts.touch_id = stba.touch_id
INNER JOIN se.data.dim_sale ds
			   ON sts.se_sale_id = ds.se_sale_id
INNER JOIN se.data_pii.scv_event_stream ses
			   ON sts.event_hash = ses.event_hash AND ses.event_tstamp >= CURRENT_DATE - 1
WHERE sts.event_tstamp >= CURRENT_DATE - 1
--   AND ds.tech_platform = 'TRAVELBIRD'
  AND ds.se_brand = 'SE Brand'
  AND stba.stitched_identity_type = 'se_user_id'
  AND stba.attributed_user_id = '67970160'
;

SELECT *
FROM se.data_pii.se_user_attributes sua
WHERE sua.email = 'robin.patel@secretescapes.com'
;


USE WAREHOUSE pipe_xlarge
;

WITH
	exploding_clicks AS (
		SELECT
			touchification.touch_id,
			page_screen_enrichment.event_tstamp,
			page_screen_enrichment.event_hash,
			page_screen_enrichment.page_url,
			page_screen_enrichment.content_interaction_array,
			clicks.value,
			clicks.value['element_category']::VARCHAR     AS element_category,
			clicks.value['element_sub_category']::VARCHAR AS element_sub_category,
			clicks.value['interaction_type']::VARCHAR     AS interaction_type,
			clicks.value['sale_id']::VARCHAR              AS se_sale_id,
		FROM data_vault_mvp.single_customer_view_stg.page_screen_enrichment page_screen_enrichment
			 INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification touchification
							ON page_screen_enrichment.event_hash = touchification.event_hash
							AND touchification.event_tstamp >= '2024-01-01',
			 LATERAL FLATTEN(INPUT => page_screen_enrichment.content_interaction_array, OUTER => TRUE) clicks
		WHERE page_screen_enrichment.event_tstamp >= '2025-01-01'
		  AND page_screen_enrichment.event_name = 'page_view'
		  AND page_screen_enrichment.page_url LIKE '%search/search%'
		  AND page_screen_enrichment.content_interaction_array IS NOT NULL
		  AND (
			clicks.value['element_category']::VARCHAR IS NULL
				OR clicks.value['element_category']::VARCHAR IN ('search results', 'kronos_recommended_for_you')
			)
	),
	sale_enrichment AS (
		SELECT
			ec.*,
			ds.product_configuration,
			ds.posa_territory,
			ds.product_type,
			ds.tech_platform
		FROM exploding_clicks ec
		INNER JOIN se.data.dim_sale ds
					   ON ec.se_sale_id = ds.se_sale_id
	)
SELECT
	sc.se_week,
	SUM(IFF(sale_enrichment.product_type IN ('Package', 'WRD'), 1, 0))     AS package_clicks, -- how package is derived in deal model
	SUM(IFF(sale_enrichment.product_type NOT IN ('Package', 'WRD'), 1, 0)) AS non_package_clicks
FROM sale_enrichment
INNER JOIN se.data.se_calendar sc
			   ON sale_enrichment.event_tstamp::DATE = sc.date_value
GROUP BY ALL