-- date
-- se week
-- se year
-- spvs
-- member spvs
-- non member spvs
-- spvs

-- deal model
-- touch basic attributes
-- touch basic attributes augemented
-- touched spvs


-- touched spvs
SELECT
	touched_spvs.event_tstamp::DATE                                                                 AS date,
	calendar.se_week,
	calendar.se_year,
	COUNT(*)                                                                                        AS spvs,
	SUM(IFF(touch_basic_attributes.stitched_identity_type IS NOT DISTINCT FROM 'se_user_id', 1, 0)) AS member_spvs,
	SUM(IFF(touch_basic_attributes.stitched_identity_type IS DISTINCT FROM 'se_user_id', 1, 0))     AS non_member_spvs,
FROM se.data.scv_touched_spvs touched_spvs
INNER JOIN se.data.scv_touch_basic_attributes touch_basic_attributes
			   ON touched_spvs.touch_id = touch_basic_attributes.touch_id
			   AND touch_basic_attributes.touch_start_tstamp >= '2024-01-01'
INNER JOIN se.data.se_calendar calendar
			   ON touched_spvs.event_tstamp::DATE = calendar.date_value
WHERE touch_basic_attributes.touch_se_brand = 'SE Brand'
  AND touched_spvs.event_tstamp >= '2024-01-01'
GROUP BY ALL


-- touch basic attributes
SELECT
	touch_basic_attributes.touch_start_tstamp::DATE AS date,
	calendar.se_week,
	calendar.se_year,
	SUM(touch_basic_attributes.num_spvs)            AS spvs,
	SUM(IFF(touch_basic_attributes.stitched_identity_type IS NOT DISTINCT FROM 'se_user_id',
			touch_basic_attributes.num_spvs, 0))    AS member_spvs,
	SUM(IFF(touch_basic_attributes.stitched_identity_type IS DISTINCT FROM 'se_user_id',
			touch_basic_attributes.num_spvs, 0))    AS non_member_spvs,
FROM se.data.scv_touch_basic_attributes touch_basic_attributes
INNER JOIN se.data.se_calendar calendar
			   ON touch_basic_attributes.touch_start_tstamp::DATE = calendar.date_value
WHERE touch_basic_attributes.touch_se_brand = 'SE Brand'
  AND touch_basic_attributes.touch_start_tstamp >= '2024-01-01'
GROUP BY ALL

-- touch basic attributes augmented
-- only member and se brand metrics in this dataset
SELECT
	touch_basic_attributes_augmented.touch_start_tstamp::DATE AS date,
	calendar.se_week,
	calendar.se_year,
	SUM(touch_basic_attributes_augmented.spvs)                AS member_spvs,
FROM se.data.touch_attributes_augmented touch_basic_attributes_augmented
INNER JOIN se.data.se_calendar calendar
			   ON touch_basic_attributes_augmented.touch_start_tstamp::DATE = calendar.date_value
WHERE touch_basic_attributes_augmented.touch_start_tstamp >= '2024-01-01'
GROUP BY ALL
;

-- fact sale metrics
WITH
	agg_to_date AS (
		SELECT
			fact_sale_metrics.date,
			SUM(fact_sale_metrics.member_spvs) AS member_spvs
		FROM se.bi.fact_sale_metrics fact_sale_metrics
		INNER JOIN se.data.dim_sale dim_sale
					   ON fact_sale_metrics.se_sale_id = dim_sale.se_sale_id
		WHERE dim_sale.se_brand = 'SE Brand'
		  AND fact_sale_metrics.date >= '2024-01-01'
		GROUP BY 1
	)
SELECT
	agg_to_date.date,
	calendar.se_week,
	calendar.se_year,
	agg_to_date.member_spvs
FROM agg_to_date
INNER JOIN se.data.se_calendar calendar
			   ON agg_to_date.date = calendar.date_value
;


------------------------------------------------------------------------------------------------------------------------
-- union of datasets

WITH
	agg_to_date AS (
		SELECT
			fact_sale_metrics.date,
			dim_sale_territory.posa_territory,
			SUM(fact_sale_metrics.member_spvs) AS member_spvs
		FROM se.bi.fact_sale_metrics fact_sale_metrics
		INNER JOIN se.bi.dim_sale_territory dim_sale_territory
					   ON fact_sale_metrics.se_sale_id = dim_sale_territory.se_sale_id
			AND fact_sale_metrics.posa_territory = dim_sale_territory.posa_territory
		INNER JOIN se.data.dim_sale dim_sale
					   ON dim_sale_territory.se_sale_id = dim_sale.se_sale_id
		WHERE dim_sale.se_brand = 'SE Brand'
		  AND fact_sale_metrics.date >= '2024-01-01'
		GROUP BY ALL
	)
SELECT
	'fact sale metrics'                                              AS source,
	agg_to_date.date,
	se.data.posa_category_from_territory(agg_to_date.posa_territory) AS posa_category,
	calendar.se_week,
	calendar.se_year,
	agg_to_date.member_spvs
FROM agg_to_date
INNER JOIN se.data.se_calendar calendar
			   ON agg_to_date.date = calendar.date_value

UNION ALL

SELECT
	'touch basic attributes augmented'                                                               AS source,
	touch_basic_attributes_augmented.touch_start_tstamp::DATE                                        AS date,
	se.data.posa_category_from_territory(touch_basic_attributes_augmented.touch_affiliate_territory) AS posa_category,
	calendar.se_week,
	calendar.se_year,
	SUM(touch_basic_attributes_augmented.spvs)                                                       AS member_spvs,
FROM se.data.touch_attributes_augmented touch_basic_attributes_augmented
INNER JOIN se.data.se_calendar calendar
			   ON touch_basic_attributes_augmented.touch_start_tstamp::DATE = calendar.date_value
WHERE touch_basic_attributes_augmented.touch_start_tstamp >= '2024-01-01'
GROUP BY ALL

UNION ALL

SELECT
	'touch basic attributes'                                                      AS source,
	touch_basic_attributes.touch_start_tstamp::DATE                               AS date,
	se.data.posa_category_from_territory(touch_channel.touch_affiliate_territory) AS posa_category,
	calendar.se_week,
	calendar.se_year,
	SUM(IFF(touch_basic_attributes.stitched_identity_type IS NOT DISTINCT FROM 'se_user_id',
			touch_basic_attributes.num_spvs, 0))                                  AS member_spvs,
FROM se.data.scv_touch_basic_attributes touch_basic_attributes
INNER JOIN se.data.scv_touch_marketing_channel touch_channel
			   ON touch_basic_attributes.touch_id = touch_channel.touch_id
			   AND touch_channel.touch_start_tstamp >= '2024-01-01'
INNER JOIN se.data.se_calendar calendar
			   ON touch_basic_attributes.touch_start_tstamp::DATE = calendar.date_value
WHERE touch_basic_attributes.touch_se_brand = 'SE Brand'
  AND touch_basic_attributes.touch_start_tstamp >= '2024-01-01'
GROUP BY ALL

UNION ALL

SELECT
	'touched spvs'                                                                                  AS source,
	touched_spvs.event_tstamp::DATE                                                                 AS date,
	se.data.posa_category_from_territory(touch_channel.touch_affiliate_territory)                   AS posa_category,
	calendar.se_week,
	calendar.se_year,
	SUM(IFF(touch_basic_attributes.stitched_identity_type IS NOT DISTINCT FROM 'se_user_id', 1, 0)) AS member_spvs,
FROM se.data.scv_touched_spvs touched_spvs
INNER JOIN se.data.scv_touch_basic_attributes touch_basic_attributes
			   ON touched_spvs.touch_id = touch_basic_attributes.touch_id
			   AND touch_basic_attributes.touch_start_tstamp >= '2024-01-01'
INNER JOIN se.data.scv_touch_marketing_channel touch_channel
			   ON touch_basic_attributes.touch_id = touch_channel.touch_id
			   AND touch_channel.touch_start_tstamp >= '2024-01-01'
INNER JOIN se.data.se_calendar calendar
			   ON touched_spvs.event_tstamp::DATE = calendar.date_value
WHERE touch_basic_attributes.touch_se_brand = 'SE Brand'
  AND touched_spvs.event_tstamp >= '2024-01-01'
GROUP BY ALL
;


-- 30th of may 2024;

SELECT TOP 100 *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp::DATE = '2024-05-30'
ORDER BY num_spvs DESC
;

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data.scv_touch_marketing_channel stmc
			   ON stba.touch_id = stmc.touch_id
WHERE stba.touch_id = '6c78d07febe3bb4fd42401db1ea95d54f83f3caaf7181c50e1678da3e0bdc884'
  AND stba.touch_start_tstamp::DATE = '2024-05-30'
;



SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON stba.touch_id = ssel.touch_id
			   AND ssel.event_tstamp >= '2024-05-30'
INNER JOIN se.data_pii.scv_event_stream ses
			   ON ssel.event_hash = ses.event_hash
			   AND ses.event_tstamp >= '2024-05-30'
WHERE stba.touch_id = '6c78d07febe3bb4fd42401db1ea95d54f83f3caaf7181c50e1678da3e0bdc884'
  AND stba.touch_start_tstamp::DATE = '2024-05-30'
;

-- 19th of august 2024

SELECT TOP 100 *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp::DATE = '2024-08-19'
ORDER BY num_spvs DESC
;
