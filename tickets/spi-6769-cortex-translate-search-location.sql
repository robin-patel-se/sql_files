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
		FROM data_vault_mvp.single_customer_view_stg.module_touched_searches sts
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
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


SELECT count(DISTINCT location),
       count(DISTINCT location_translated)
FROM scratch.robinpatel.location_translation
;

SELECT * FROM scratch.robinpatel.location_translation;

------------------------------------------------------------------------------------------------------------------------
USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.search_terms_auto_complete
CLONE data_vault_mvp.dwh.search_terms_auto_complete;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.search_location_translation
CLONE data_vault_mvp.dwh.search_location_translation;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.search.search_location_translation.py' \
    --method 'run' \
    --start '2025-01-17 00:00:00' \
    --end '2025-01-17 00:00:00'


                     SELECT * FROM data_vault_mvp_dev_robin.dwh.search_location_translation;


DROP TABLE data_vault_mvp_dev_robin.dwh.search_location_translation;

SELECT MIN(event_tstamp) FROm se.data.scv_touched_searches sts
-- 2020-03-06 14:50:32.196000000


SELECT COUNT(*) FROm data_vault_mvp_dev_robin.dwh.search_location_translation__step01_model_distinct_locations; --1,913,124


./scripts/mwaa-cli production "dags backfill --start-date '2023-06-01 00:00:00' --end-date '2023-06-02 00:00:00' --donot-pickle dwh__search_location_translation__daily_at_06h00"


USE ROLE personal_role__robinpatel;


SELECT * FROm se.data.search_location_translation;
