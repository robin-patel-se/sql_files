WITH
	model_data AS (

		SELECT
			sts.event_tstamp::DATE                                     AS search_date,
			stba.touch_experience,
			stmc.touch_affiliate_territory,
-- 			sts.location                                AS location,
-- 			INITCAP(sts.location)                                AS location,
			TRIM(INITCAP(sts.location))                                AS location,
			sts.triggered_by,
			sts.check_in_date IS NOT NULL                              AS has_check_in_dates,
			DATEDIFF(MONTH, sts.event_tstamp::DATE, sts.check_in_date) AS search_lead_months,
			sts.num_results,
			sts.had_results
		FROM se.data.scv_touched_searches sts
			INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
		WHERE sts.event_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE - 1
		  AND NULLIF(sts.location, '') IS NOT NULL

	)
------------------------------------------------------------------------------------------------------------------------
-- to work out percentiles
		,
	agg_search_terms AS (
		SELECT
			md.location,
			COUNT(*) AS searches
		FROM model_data md
		GROUP BY 1
	),
	percentile AS (
		SELECT
			ast.location,
			ast.searches,
			SUM(ast.searches) OVER (ORDER BY ast.searches DESC) AS cumulative_search_total,
			SUM(ast.searches) OVER ()                           AS total_searches,
			cumulative_search_total / total_searches            AS search_term_percentile
		FROM agg_search_terms ast
	),
	percentile_grouping AS (
		SELECT
			location,
			searches,
			cumulative_search_total,
			total_searches,
			search_term_percentile,
			CASE
				WHEN p.search_term_percentile < 0.25 THEN '25%'
				WHEN p.search_term_percentile < 0.5 THEN '50%'
				WHEN p.search_term_percentile < 0.75 THEN '75%'
				WHEN p.search_term_percentile < 0.90 THEN '90%'
				WHEN p.search_term_percentile < 0.95 THEN '95%'
				WHEN p.search_term_percentile < 0.99 THEN '99%'
				WHEN p.search_term_percentile <= 1 THEN '100%'
			END AS percentile_group
		FROM percentile p
	)
SELECT
	ROUND(search_term_percentile, 2),
	COUNT(DISTINCT location),
	SUM(searches)
FROM percentile_grouping
GROUP BY 1

------------------------------------------------------------------------------------------------------------------------
SELECT
	md.location,
	COUNT(*)
FROM model_data md
GROUP BY 1
--
SELECT
	md.search_date,
	md.touch_experience,
	md.touch_affiliate_territory,
	md.location,
	md.triggered_by,
	md.has_check_in_dates,
	md.search_lead_months,
	AVG(md.num_results)                    AS avg_num_results,
	MIN(md.num_results)                    AS min_num_results,
	MAX(md.num_results)                    AS max_num_results,
	SUM(IFF(md.had_results = FALSE, 1, 0)) AS unulfilled_searches,
	COUNT(*)                               AS searches
FROM model_data md
GROUP BY 1, 2, 3, 4, 5, 6, 7
;



SELECT
	COUNT(*),
	SUM(IFF(NULLIF(sts.location, '') IS NOT NULL, 1, 0)) AS location_searches,
	location_searches / COUNT(*)
FROM se.data.scv_touched_searches sts
	INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE sts.event_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE - 1

SELECT *,
	   IFNULL(ARRAY_SIZE(sts.travel_types), 0) AS test,
	   IFNULL(ARRAY_SIZE(sts.trip_types), 0)   AS test2,
	   IFNULL(ARRAY_SIZE(sts.travel_types), 0) +
	   IFNULL(ARRAY_SIZE(sts.trip_types), 0)   AS test3,

FROM se.data.scv_touched_searches sts



WITH
	model_data AS (

		SELECT
			sts.event_tstamp::DATE                                     AS search_date,
			stba.touch_experience,
			stmc.touch_affiliate_territory,
			sts.location,
			sts.triggered_by,
			sts.check_in_date IS NOT NULL                              AS has_check_in_dates,
			DATEDIFF(MONTH, sts.event_tstamp::DATE, sts.check_in_date) AS search_lead_months,
			sts.num_results,
			sts.had_results,
			IFNULL(ARRAY_SIZE(sts.travel_types), 0) +
			IFNULL(ARRAY_SIZE(sts.trip_types), 0) > 0                  AS has_filters
		FROM se.data.scv_touched_searches sts
			INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
		WHERE sts.event_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE - 1
		  AND NULLIF(sts.location, '') IS NOT NULL

	)
SELECT
	md.search_date,
	md.touch_experience,
	md.touch_affiliate_territory,
	md.location,
	TRIM(INITCAP(md.location))             AS location,
	md.triggered_by,
	md.has_check_in_dates,
	md.search_lead_months,
	md.has_filters,
	AVG(md.num_results)                    AS avg_num_results,
	MIN(md.num_results)                    AS min_num_results,
	MAX(md.num_results)                    AS max_num_results,
	SUM(IFF(md.had_results = FALSE, 1, 0)) AS unulfilled_searches,
	COUNT(*)                               AS searches
FROM model_data md
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8


WITH
	model_data AS (
		SELECT
			sts.event_tstamp::DATE                                           AS search_date,
			stba.touch_experience,
			stmc.touch_affiliate_territory,
			sts.location                                                     AS location__o,
			sts.triggered_by,
			sts.check_in_date IS NOT NULL                                    AS has_check_in_dates,
			DATEDIFF(MONTH, sts.event_tstamp::DATE, sts.check_in_date)       AS search_lead_months,
			sts.num_results,
			sts.had_results,
			IFNULL(ARRAY_SIZE(sts.travel_types), 0) +
			IFNULL(ARRAY_SIZE(sts.trip_types), 0) > 0                        AS has_filters,
			IFF(stac.auto_complete IS NOT NULL, TRUE, FALSE)                 AS is_auto_complete,
			IFF(is_auto_complete, sts.location, TRIM(INITCAP(sts.location))) AS location
		FROM se.data.scv_touched_searches sts
			INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
			LEFT JOIN  data_vault_mvp.dwh.search_terms_auto_complete stac ON sts.location = stac.auto_complete
		WHERE sts.event_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE - 1
		  AND NULLIF(sts.location, '') IS NOT NULL
	)
SELECT
	md.search_date,
	md.touch_experience,
	md.touch_affiliate_territory,
	md.location,
	md.triggered_by,
	md.has_check_in_dates,
	md.search_lead_months,
	md.has_filters,
	md.is_auto_complete,
	AVG(md.num_results)                    AS avg_num_results,
	MIN(md.num_results)                    AS min_num_results,
	MAX(md.num_results)                    AS max_num_results,
	SUM(IFF(md.had_results = FALSE, 1, 0)) AS unulfilled_searches,
	COUNT(*)                               AS searches
FROM model_data md
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

------------------------------------------------------------------------------------------------------------------------


USE WAREHOUSE pipe_xlarge
;

WITH
	model_data AS (
		SELECT
			sts.event_tstamp::DATE                                                       AS search_date,
			stba.touch_experience,
			stmc.touch_affiliate_territory,
			sts.location                                                                 AS location__o,
			CASE
				WHEN stmc.touch_affiliate_territory = 'DE' THEN 'de'
				WHEN stmc.touch_affiliate_territory = 'CH' THEN 'de'
				WHEN stmc.touch_affiliate_territory = 'AT' THEN 'de'
				WHEN stmc.touch_affiliate_territory = 'IT' THEN 'it'
				WHEN stmc.touch_affiliate_territory = 'SE' THEN 'sv'
			END                                                                          AS territory_translation_locale,
			snowflake.cortex.translate(sts.location, territory_translation_locale, 'en') AS location_en,
			sts.triggered_by,
			sts.check_in_date IS NOT NULL                                                AS has_check_in_dates,
			DATEDIFF(MONTH, sts.event_tstamp::DATE, sts.check_in_date)                   AS search_lead_months,
			sts.num_results,
			sts.had_results,
			IFNULL(ARRAY_SIZE(sts.travel_types), 0) +
			IFNULL(ARRAY_SIZE(sts.trip_types), 0) > 0                                    AS has_filters,
			IFF(stac.auto_complete IS NOT NULL, TRUE, FALSE)                             AS is_auto_complete,
			IFF(is_auto_complete, location_en, TRIM(INITCAP(sts.location)))              AS location
		FROM se.data.scv_touched_searches sts
			INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
			LEFT JOIN  data_vault_mvp.dwh.search_terms_auto_complete stac ON sts.location = stac.auto_complete
			AND stmc.touch_affiliate_territory = stac.territory
		WHERE sts.event_tstamp::DATE BETWEEN '2023-01-01' AND CURRENT_DATE - 1
		  AND NULLIF(sts.location, '') IS NOT NULL
		  AND stmc.touch_affiliate_territory IN ('UK', 'DE', 'IT')
	)
SELECT
	md.search_date,
	md.touch_experience,
	md.touch_affiliate_territory,
	md.location,
	md.triggered_by,
	md.has_check_in_dates,
	md.search_lead_months,
	md.has_filters,
	md.is_auto_complete,
	AVG(md.num_results)                    AS avg_num_results,
	MIN(md.num_results)                    AS min_num_results,
	MAX(md.num_results)                    AS max_num_results,
	SUM(IFF(md.had_results = FALSE, 1, 0)) AS unulfilled_searches,
	COUNT(*)                               AS searches
FROM model_data md
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
;

USE WAREHOUSE pipe_xlarge
;


/*
English	en'
French:	fr'
German	de'
Italian	it'
Japanese	ja'
Korean	ko'
Polish	pl'
Portuguese	pt'
Russian	ru'
Spanish	es'
Swedish	sv'
*/

USE WAREHOUSE pipe_xlarge
;

WITH
	model_data AS (
		SELECT
			sts.event_tstamp::DATE                                                       AS search_date,
			stba.touch_experience,
			stmc.touch_affiliate_territory,
			sts.location                                                                 AS location__o,
			CASE
				WHEN stmc.touch_affiliate_territory = 'DE' THEN 'de'
				WHEN stmc.touch_affiliate_territory = 'CH' THEN 'de'
				WHEN stmc.touch_affiliate_territory = 'AT' THEN 'de'
				WHEN stmc.touch_affiliate_territory = 'IT' THEN 'it'
				WHEN stmc.touch_affiliate_territory = 'SE' THEN 'sv'
			END                                                                          AS territory_translation_locale,
			snowflake.cortex.translate(sts.location, territory_translation_locale, 'en') AS location_en,
			sts.triggered_by,
			sts.check_in_date IS NOT NULL                                                AS has_check_in_dates,
			DATEDIFF(MONTH, sts.event_tstamp::DATE, sts.check_in_date)                   AS search_lead_months,
			sts.num_results,
			sts.had_results,
			IFNULL(ARRAY_SIZE(sts.travel_types), 0) +
			IFNULL(ARRAY_SIZE(sts.trip_types), 0) > 0                                    AS has_filters,
			IFF(stac.auto_complete IS NOT NULL, TRUE, FALSE)                             AS is_auto_complete,
			IFF(is_auto_complete, location_en, TRIM(INITCAP(sts.location)))              AS location
		FROM se.data.scv_touched_searches sts
			INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
			LEFT JOIN  data_vault_mvp.dwh.search_terms_auto_complete stac ON sts.location = stac.auto_complete
			AND stmc.touch_affiliate_territory = stac.territory
		WHERE sts.event_tstamp::DATE BETWEEN '202-01-01' AND CURRENT_DATE - 1
		  AND NULLIF(sts.location, '') IS NOT NULL
		  AND stmc.touch_affiliate_territory IN ('UK', 'DE', 'IT', 'AT', 'CH', 'IE', 'SE')
	)
SELECT
	md.search_date,
	md.touch_experience,
	md.touch_affiliate_territory,
	md.location,
	md.triggered_by,
	md.has_check_in_dates,
	md.search_lead_months,
	md.has_filters,
	md.is_auto_complete,
	AVG(md.num_results)                    AS avg_num_results,
	MIN(md.num_results)                    AS min_num_results,
	MAX(md.num_results)                    AS max_num_results,
	SUM(IFF(md.had_results = FALSE, 1, 0)) AS unulfilled_searches,
	COUNT(*)                               AS searches
FROM model_data md
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
;


-- exisitng model for 2024 to date showed 10.6M
-- gareth bets 6M
-- robin bets 9.5M

WITH
	model_data AS (
		SELECT
			sts.event_tstamp::DATE                                                              AS search_date,
			stba.touch_experience,
			stmc.touch_affiliate_territory,
			sts.location                                                                        AS location__o,
			sts.triggered_by,
			sts.check_in_date IS NOT NULL                                                       AS has_check_in_dates,
			DATEDIFF(MONTH, sts.event_tstamp::DATE, sts.check_in_date)                          AS search_lead_months,
			sts.num_results,
			sts.had_results,
			IFNULL(ARRAY_SIZE(sts.travel_types), 0) + IFNULL(ARRAY_SIZE(sts.trip_types), 0) > 0 AS has_filters,
			IFF(stac.auto_complete IS NOT NULL, TRUE, FALSE)                                    AS is_auto_complete
		FROM se.data.scv_touched_searches sts
			INNER JOIN se.data.scv_touch_basic_attributes stba
					   ON sts.touch_id = stba.touch_id
						   AND stba.touch_start_tstamp >= CURRENT_DATE - 7
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
			LEFT JOIN  data_vault_mvp.dwh.search_terms_auto_complete stac ON sts.location = stac.auto_complete
			AND stmc.touch_affiliate_territory = stac.territory
-- 		WHERE sts.event_tstamp::DATE BETWEEN '2023-01-01' AND CURRENT_DATE - 1
		WHERE sts.event_tstamp::DATE >= CURRENT_DATE - 7
		  AND NULLIF(sts.location, '') IS NOT NULL
		  AND stmc.touch_affiliate_territory IN ('UK', 'DE', 'IT', 'AT', 'CH', 'IE', 'SE')
	),
	input_terms AS (
		SELECT DISTINCT
			md.location__o,
			IFF(md.is_auto_complete, md.location__o, TRIM(INITCAP(md.location__o))) AS location,
			md.touch_affiliate_territory,
			CASE
				WHEN md.touch_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'de'
				WHEN md.touch_affiliate_territory IN ('UK', 'IE') THEN 'en'
				WHEN md.touch_affiliate_territory = 'IT' THEN 'it'
				WHEN md.touch_affiliate_territory = 'SE' THEN 'sv'
			END                                                                     AS territory_translation_locale
		FROM model_data md
	),
	distinct_translations AS (
		SELECT DISTINCT
			inp.location__o, -- join key
			snowflake.cortex.translate(inp.location, inp.territory_translation_locale, 'en') AS location_en
		FROM input_terms inp
	)
SELECT
	m.search_date,
	m.touch_experience,
	m.touch_affiliate_territory,
	dt.location_en                        AS location,
	m.triggered_by,
	m.has_check_in_dates,
	m.search_lead_months,
	m.has_filters,
	m.is_auto_complete,
	AVG(m.num_results)                    AS avg_num_results,
	MIN(m.num_results)                    AS min_num_results,
	MAX(m.num_results)                    AS max_num_results,
	SUM(IFF(m.had_results = FALSE, 1, 0)) AS unulfilled_searches,
	COUNT(*)                              AS searches
FROM model_data m
	INNER JOIN distinct_translations dt ON m.location__o = dt.location__o
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
;


USE WAREHOUSE pipe_xlarge
;

WITH
	model_data AS (
		SELECT
			sts.event_tstamp::DATE                                     AS search_date,
			stba.touch_experience,
			stmc.touch_affiliate_territory,
			sts.location                                               AS location__o,
			CASE
				WHEN stmc.touch_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'de'
				WHEN stmc.touch_affiliate_territory IN ('UK', 'IE') THEN 'en'
				WHEN stmc.touch_affiliate_territory = 'IT' THEN 'it'
				WHEN stmc.touch_affiliate_territory = 'SE' THEN 'sv'
			END                                                        AS territory_translation_locale,
			IFF(stac.auto_complete IS NOT NULL, TRUE, FALSE)           AS is_auto_complete,
			sts.triggered_by,
			sts.check_in_date IS NOT NULL                              AS has_check_in_dates,
			DATEDIFF(MONTH, sts.event_tstamp::DATE, sts.check_in_date) AS search_lead_months,
			sts.num_results,
			sts.had_results,
			IFNULL(ARRAY_SIZE(sts.travel_types), 0) +
			IFNULL(ARRAY_SIZE(sts.trip_types), 0) > 0                  AS has_filters
		FROM se.data.scv_touched_searches sts
			INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
			LEFT JOIN  data_vault_mvp.dwh.search_terms_auto_complete stac ON sts.location = stac.auto_complete
			AND stmc.touch_affiliate_territory = stac.territory
-- 		WHERE sts.event_tstamp::DATE BETWEEN '2023-01-01' AND CURRENT_DATE - 1
		WHERE sts.event_tstamp::DATE >= CURRENT_DATE - 1 -- TODO update
		  AND NULLIF(sts.location, '') IS NOT NULL
		  AND stmc.touch_affiliate_territory IN ('UK', 'DE', 'IT', 'AT', 'CH', 'IE', 'SE')
		LIMIT 100 -- TODO REMOVE
	),
	translate_location AS (
		-- translate the location field to english
		SELECT *,
			   snowflake.cortex.translate(
					   IFF(is_auto_complete, md.location__o,
						   TRIM(INITCAP(md.location__o))), -- use the auto complete output or hygiene the location field
					   territory_translation_locale, -- input language
					   'en' -- translation language
			   ) AS location
		FROM model_data md
	)
SELECT
	tl.search_date,
	tl.touch_experience,
	tl.touch_affiliate_territory,
	tl.location,
	tl.triggered_by,
	tl.has_check_in_dates,
	tl.search_lead_months,
	tl.has_filters,
	tl.is_auto_complete,
	AVG(tl.num_results)                    AS avg_num_results,
	MIN(tl.num_results)                    AS min_num_results,
	MAX(tl.num_results)                    AS max_num_results,
	SUM(IFF(tl.had_results = FALSE, 1, 0)) AS unulfilled_searches,
	COUNT(*)                               AS searches
FROM translate_location tl
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
;


-- create a merge job that would only add the distinct list of locations from touched searches
-- add a translation to the location when we add a new row.

SELECT
	location,
	NULL AS translated_location
FROM se.data.scv_touched_searches sts
WHERE NULLIF(sts.location, '') IS NOT NULL -- to remove empty and null strings
  AND sts.event_tstamp::DATE >= CURRENT_DATE - 1
-- TODO update

--9,783
--115,230

CREATE TABLE IF NOT EXISTS scratch.robinpatel.location_translation
(
	location                     VARCHAR,
	territory_translation_locale VARCHAR,
	is_auto_complete             BOOLEAN,
	location_translated          VARCHAR
)
;


MERGE INTO scratch.robinpatel.location_translation AS target
	USING (
		SELECT DISTINCT
			location,
			CASE
				WHEN stmc.touch_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'de'
				WHEN stmc.touch_affiliate_territory IN ('UK', 'IE') THEN 'en'
				WHEN stmc.touch_affiliate_territory = 'IT' THEN 'it'
				WHEN stmc.touch_affiliate_territory = 'SE' THEN 'sv'
			END                                              AS territory_translation_locale,
			IFF(stac.auto_complete IS NOT NULL, TRUE, FALSE) AS is_auto_complete,
			NULL                                             AS translated_location
		FROM se.data.scv_touched_searches sts
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
			LEFT JOIN  data_vault_mvp.dwh.search_terms_auto_complete stac ON sts.location = stac.auto_complete
			AND stmc.touch_affiliate_territory = stac.territory
		WHERE NULLIF(sts.location, '') IS NOT NULL -- to remove empty and null strings
		  AND sts.event_tstamp::DATE >= CURRENT_DATE - 1 -- TODO update
-- 		LIMIT 100
	) AS batch ON target.location = batch.location
	WHEN NOT MATCHED
		THEN INSERT VALUES (batch.location,
							batch.territory_translation_locale,
							batch.is_auto_complete,
							snowflake.cortex.translate(
									IFF(batch.is_auto_complete, batch.location,
										TRIM(INITCAP(batch.location))), -- use the auto complete output or hygiene the location field
									territory_translation_locale, -- input language
									'en' -- translation language
							))
;


SELECT
	COUNT(DISTINCT location),
	COUNT(DISTINCT location_translated)
FROM scratch.robinpatel.location_translation
;


SELECT *
FROM se.data.search_location_translation slt
;


SELECT DISTINCT
	location,
	location_translated,
	COUNT(location) OVER ( PARTITION BY location) AS location_dupes

FROM se.data.search_location_translation

QUALIFY location_dupes > 1
;


SELECT *
FROM se.data.search_location_translation slt
WHERE slt.location = ' Barcelona, Spain'