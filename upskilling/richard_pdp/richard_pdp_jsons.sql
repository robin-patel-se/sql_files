-- querying JSON data in Snowflake
-- search contexts

SELECT
	e.collector_tstamp,
	e.contexts_com_secretescapes_search_context_1,
	e.contexts_com_secretescapes_search_context_1[0],
	e.contexts_com_secretescapes_search_context_1[0]['check_in_date']       AS check_in_date,
-- 	e.contexts_com_secretescapes_search_context_1[0]:check_in_date AS check_in_date, -- less desired
-- 	TO_DATE(e.contexts_com_secretescapes_search_context_1[0]['check_in_date']) AS check_in_date,
	e.contexts_com_secretescapes_search_context_1[0]['check_in_date']::DATE AS check_in_date,
	e.contexts_com_secretescapes_search_context_1[0]['num_results']::NUMBER AS num_results,
	e.contexts_com_secretescapes_search_context_1[0]['flexible_search']     AS flexible_search,
	e.contexts_com_secretescapes_search_context_1[0]['travel_types']        AS travel_types,
	e.user_id,
	e.page_url
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= '2023-10-20'
  AND e.contexts_com_secretescapes_search_context_1 IS NOT NULL
;

-- parsing urls

SELECT
	e.page_url,
	PARSE_URL(e.page_url)                  AS parsed_url,
	PARSE_URL(e.page_url)['host']::VARCHAR AS url_host,
	PARSE_URL(e.page_url)['parameters']    AS url_host
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= '2023-10-20'
  AND e.contexts_com_secretescapes_search_context_1 IS NOT NULL
;

-- using the function on a random url
SELECT
	PARSE_URL('https://www.secretescapes.de/current-sales?affiliate=goo-cpl-brand-de&utmadgroupid=138632967018&awadposition=&utmcampaignid=17420310849&awcreative=642053837434&awdevice=m&awkeyword=secret+escapes&awloc_interest_ms=&awloc_physical_ms=9117374&awmatchtype=e&awplacement=&awtargetid=aud-920607971465%3Akwd-12680113420&saff=DECPL+Brand+Pure+All-Destinations+Phrase+Non-Member+Combined&utm_campaign=DECPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_adgroup=DECPL+Brand+Pure+Exact+All-Destinations+Non-Member+Pure&gclid=CjwKCAjwp8OpBhAFEiwAG7NaEtUqSAvAUpmdptM3wruyip5Zovxp6IfKpoFAKIGLRvWVjgc9KT4ZHxoCiO0QAvD_BwE&affiliateUrlString=goo-cpl-brand-de')


-- looking at page/screen enrichment for the benefit of understanding arrays
WITH
	limit_viewed AS (
		SELECT
			es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
			es.contexts_com_secretescapes_content_element_viewed_context_1[0]       AS content_viewed
		FROM hygiene_vault_mvp.snowplow.event_stream es
		WHERE es.event_tstamp >= TIMESTAMPADD('day', -1, '2023-10-19 03:00:00'::TIMESTAMP)
		  AND es.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
		  AND es.device_platform NOT IN ('native app ios', 'native app android') -- to remove app content interaction
		  -- belt and braces to avoid the array max size limit if a person has interacted with the site several times
		QUALIFY ROW_NUMBER() OVER (
			PARTITION BY es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR ORDER BY es.event_tstamp) <=
				100
	)

SELECT
	lv.web_page_id,
	ARRAY_AGG(lv.content_viewed) AS content_viewed_array
FROM limit_viewed lv
GROUP BY 1


SELECT *
FROM data_vault_mvp.single_customer_view_stg.page_screen_enrichment pse
;



WITH
	input_data AS (
		SELECT ['London', 'New York', 'Berlin'] AS array_of_cities
	)
SELECT
	input_data.array_of_cities,
-- 	city.seq,
-- 	city.key,
-- 	city.path,
	city.index,
	city.value::VARCHAR AS city_name
-- 	city.this
FROM input_data,
	 LATERAL FLATTEN(INPUT => array_of_cities, OUTER => TRUE) city
;

WITH
	input_data AS (
		SELECT
			PARSE_JSON('
		   {
			  "postCode": "NW6",
			  "city": "London",
			  "country": "UK"
			}
		') AS address_object
	)
SELECT
	input_data.address_object,
-- 	address_lines.seq,
-- 	address_lines.path,
-- 	address_lines.index,
	address_lines.key,
	address_lines.value::VARCHAR AS value
-- 	address_lines.this
FROM input_data,
	 LATERAL FLATTEN(INPUT => address_object, OUTER => TRUE) address_lines
;

-- using recusive to explode out multiple paths within a json
WITH
	input_data AS (
		SELECT
			PARSE_JSON('
		   {
			  "postCode": "NW6",
			  "city": "London",
			  "country": "UK",
			  "phoneNumbers": [12232213, 6746545645, 234262762]
			           }
		') AS address_object
	)
SELECT
	input_data.address_object,
-- 	address_lines.seq,
-- 	address_lines.path,
-- 	address_lines.index,
-- 	address_lines.this
	address_lines.key,
	address_lines.value::VARCHAR AS value,
	address_lines.*
-- 	phone_numbers.*
FROM input_data,
	 LATERAL FLATTEN(INPUT => address_object, RECURSIVE => TRUE) address_lines
;

-- note share the learnings from: RECURSIVE => TRUE

WITH
	input_data AS (
		SELECT
			PARSE_JSON('
		   {
			  "postCode": "NW6",
			  "city": "London",
			  "country": "UK",
			  "phoneNumbers": [12232213, 6746545645, 234262762],
			  "userCurrencies": ["EUR", "GBP"]
			           }
		') AS address_object
	)
SELECT
	input_data.address_object,
	address_lines.key,
	address_lines.value::VARCHAR AS value,
	phone_numbers.*
FROM input_data,
	 LATERAL FLATTEN(INPUT => address_object, OUTER => TRUE) address_lines,
	 LATERAL FLATTEN(INPUT => address_lines.value, OUTER => TRUE) phone_numbers
;

SELECT
	e.contexts_com_secretescapes_user_state_context_1,
	user_state_object.*
FROM snowplow.atomic.events e,
	 LATERAL FLATTEN(INPUT => e.contexts_com_secretescapes_user_state_context_1[0], OUTER => TRUE) user_state_object
WHERE e.collector_tstamp >= CURRENT_DATE
  AND e.contexts_com_secretescapes_user_state_context_1 IS NOT NULL
;


-- get list of distinct keys
SELECT DISTINCT
	user_state_object.key
FROM snowplow.atomic.events e,
	 LATERAL FLATTEN(INPUT => e.contexts_com_secretescapes_user_state_context_1[0], OUTER => TRUE) user_state_object
WHERE e.collector_tstamp >= CURRENT_DATE
  AND e.contexts_com_secretescapes_user_state_context_1 IS NOT NULL
;


-- get a count of how many times the key are used across events
SELECT
	user_state_object.key,
	COUNT(*)
FROM snowplow.atomic.events e,
	 LATERAL FLATTEN(INPUT => e.contexts_com_secretescapes_user_state_context_1[0], OUTER => TRUE) user_state_object
WHERE e.collector_tstamp >= CURRENT_DATE
  AND e.contexts_com_secretescapes_user_state_context_1 IS NOT NULL
GROUP BY 1
;

-- unpack the content viewed array

--isolate the sale object within the rendered object
SELECT
	e.v_tracker,
	e.dvce_type,
	e.contexts_com_secretescapes_content_elements_rendered_context_1,
	e.contexts_com_secretescapes_content_elements_rendered_context_1[0],
	e.contexts_com_secretescapes_content_elements_rendered_context_1[0]['sales']
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= CURRENT_DATE
  AND e.contexts_com_secretescapes_content_elements_rendered_context_1 IS NOT NULL
;


-- unpack sales within rendered object
SELECT
	e.v_tracker,
	e.dvce_type,
	e.contexts_com_secretescapes_content_elements_rendered_context_1,
	e.contexts_com_secretescapes_content_elements_rendered_context_1[0]['sales'] AS list_of_sale_objected_rendered,
	sale_rendered.*,
	sale_rendered.value['sale_id']::VARCHAR                                      AS sale_id
FROM snowplow.atomic.events e,
	 LATERAL FLATTEN(INPUT => list_of_sale_objected_rendered, OUTER => TRUE) sale_rendered
WHERE e.collector_tstamp >= CURRENT_DATE
  AND e.contexts_com_secretescapes_content_elements_rendered_context_1 IS NOT NULL
;

SELECT *
FROM unload_vault_mvp.iterable.user_profile_activity_first_quartile__20231001t030000__daily_at_03h00 u
	high_potential_gpv_ab_test_analysis



WITH
	customer_base AS
		(
			SELECT DISTINCT
				sua.shiro_user_id,
				current_affiliate_territory AS territory,
				sua.country,
				sua.main_affiliate_id,
				rfv_segment
			FROM se.data_pii.se_user_attributes sua
--          from  {{ ref('stg_data_pii__se_user_attributes') }} sua
				LEFT JOIN dbt.bi_customer_insight.ci_rfv_segments rcb
--             left join {{ ref('ci_rfv_segments') }} rcb
						  ON sua.shiro_user_id = rcb.shiro_user_id
--             left join   {{ ref('ci_rfv_customer_base') }} crcb
				LEFT JOIN dbt.bi_customer_insight.ci_rfv_customer_base crcb ON sua.shiro_user_id = crcb.shiro_user_id
				LEFT JOIN (
							  SELECT DISTINCT
								  (shiro_user_id) AS shiro_user_id
							  FROM dbt.bi_customer_insight.ci_iterable_suppression_lists
						  ) sl
						  ON sl.shiro_user_id = sua.shiro_user_id
			WHERE 1 = 1
			  AND sua.current_affiliate_territory IN ('UK', 'DE')
			  AND sua.main_affiliate_id IN (24, 362)
			  AND email_opt_in_status IN ('daily', 'weekly')
			  AND sl.shiro_user_id IS NULL --not on suppression list
			  AND crcb.last_session >= DATEADD(DAY, (-364 * 3), CURRENT_DATE()) --  session in last 3 years
			-- last in  last 3 years filter
		)
		,
	past_bookings AS
		(
			SELECT
				fcb.shiro_user_id,
				booking_id,
				ds.haul_type,
				ds.holiday_type,
				fcb.check_in_date,
				ds.se_sale_id,
				ROW_NUMBER() OVER (PARTITION BY fcb.shiro_user_id ORDER BY check_in_date DESC) AS booking_rank
			FROM se.data.fact_booking fcb
--     from {{ ref('base_dwh__fact_booking') }}    fcb
				INNER JOIN customer_base cb USING (shiro_user_id)
				LEFT JOIN  customer_insight.temp.ga_holiday_type ds
--             left join {{ ref('ci_holiday_haul_type_flag') }}.  ds
						   ON fcb.se_sale_id = ds.se_sale_id
							   AND posa_territory = cb.territory
			WHERE fcb.booking_status_type IN ('live', 'cancelled')
--          and fcb.check_in_date > dateadd(day, -(365 * 4), current_date())

		)
		,
	past_spvs AS
		(
			SELECT
				b.*,
				sts.event_hash,
				DATE(stba.touch_start_tstamp)                                           AS spv_date,
				ds.haul_type,
				ds.holiday_type,
				ROW_NUMBER() OVER (PARTITION BY b.shiro_user_id ORDER BY spv_date DESC) AS spv_rank
			FROM customer_base b
				LEFT JOIN se.data_pii.scv_touch_basic_attributes stba
--             left join  {{ ref('base_scv__module_touch_basic_attributes') }} stba
						  ON b.shiro_user_id = TRY_TO_NUMBER(stba.attributed_user_id)
				LEFT JOIN se.data.scv_touched_spvs sts
--             left join {{ ref('base_scv__module_touched_spvs') }}   sts
						  ON sts.touch_id = stba.touch_id
				LEFT JOIN customer_insight.temp.ga_holiday_type ds
--             left join {{ ref('ci_holiday_haul_type_flag') }}.  ds
						  ON sts.se_sale_id = ds.se_sale_id
							  AND posa_territory = b.territory
			WHERE 1 = 1
			  AND sts.touch_id IS NOT NULL --aka get rid of sessions without spvs
		)


SELECT
	cb.shiro_user_id,
	COALESCE(pb.haul_type, ps.haul_type)       AS haul_type,
	COALESCE(pb.holiday_type, ps.holiday_type) AS holiday_type,
	pb.se_sale_id                              AS last_booking_sale_id
FROM customer_base cb
	LEFT JOIN past_bookings pb ON cb.shiro_user_id = pb.shiro_user_id AND pb.booking_rank = 1
	LEFT JOIN past_spvs ps ON cb.shiro_user_id = ps.shiro_user_id AND ps.spv_rank = 1





