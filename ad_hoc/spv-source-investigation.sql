SET (start_date, end_date) = ('2024-01-01', '2025-08-01')
;

USE WAREHOUSE pipe_2xlarge
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.spv_source__events_data
AS
SELECT
	session.touch_id,
	channel.touch_mkt_channel,
	channel.channel_category,
	link.event_hash,
	events.page_url,
	events.event_name,
	events.device_platform,
	events.page_referrer,
	events.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR         AS screen_name,
	LAG(events.page_url) OVER (PARTITION BY link.touch_id ORDER BY link.event_tstamp ASC) AS previous_page_url,
	LAG(events.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR)
		OVER (PARTITION BY link.touch_id ORDER BY link.event_tstamp ASC)                  AS previous_screen_name,
	LAG(events.se_sale_id)
		OVER (PARTITION BY link.touch_id ORDER BY link.event_tstamp ASC)                  AS previous_se_sale_id,
	channel.touch_affiliate_territory
FROM se.data_pii.scv_touch_basic_attributes session
INNER JOIN se.data.scv_touch_marketing_channel channel
	ON session.touch_id = channel.touch_id
	AND UPPER(channel.touch_mkt_channel) NOT IN ('ANOMALOUS', 'NON_VERIFIED', 'SE TECH', 'TEST')
INNER JOIN se.data_pii.scv_session_events_link link
	ON session.touch_id = link.touch_id
	AND link.event_tstamp BETWEEN $start_date AND $end_date
INNER JOIN se.data_pii.scv_event_stream events
	ON link.event_hash = events.event_hash
	AND events.event_tstamp BETWEEN $start_date AND $end_date
	AND events.event_name IN ('page_view', 'screen_view')
WHERE session.touch_start_tstamp BETWEEN $start_date AND $end_date
  AND session.touch_se_brand = 'SE Brand'
  AND session.num_spvs > 0
-- remove duplicated spvs between client side and server side
QUALIFY IFF(events.page_url = LAG(events.page_url) OVER (PARTITION BY link.touch_id ORDER BY link.event_tstamp ASC),
			1, 0) = 0
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.spv_source__model_spvs
AS
	-- using list of touched spvs, attach modelled event data to obtain previous page/screen
SELECT
	spvs.event_hash,
	spvs.touch_id,
	spvs.event_tstamp,
	spvs.se_sale_id,
	spvs.tb_offer_id,
	spvs.event_category,
	spvs.event_subcategory,
	events.device_platform,
	events.touch_affiliate_territory,
	spvs.page_url,
	events.page_referrer,
	events.previous_page_url,
	IFF(events.page_referrer IS NULL, NULL,
		se.data.page_url_categorisation(events.page_referrer))           AS page_referrer_categorisation,
	IFF(events.previous_page_url IS NULL, NULL,
		se.data.page_url_categorisation(events.previous_page_url))       AS previous_page_url_categorisation,
	events.screen_name,
	events.previous_screen_name,
	events.previous_se_sale_id,
	IFF(events.previous_screen_name IS NULL, NULL,
		se.data.screen_view_classification(events.previous_screen_name)) AS previous_screen_name_categorisation,
FROM se.data.scv_touched_spvs spvs
INNER JOIN scratch.robinpatel.spv_source__events_data events
	ON spvs.event_hash = events.event_hash
WHERE spvs.event_tstamp BETWEEN $start_date AND $end_date
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.spv_source__spv_referrer AS
	-- add logic to choose the most trusted previous page/screen
SELECT
	model_spvs.event_hash,
	model_spvs.touch_id,
	model_spvs.event_tstamp,
	model_spvs.se_sale_id,
	model_spvs.tb_offer_id,
	model_spvs.event_category,
	model_spvs.event_subcategory,
	model_spvs.device_platform,
	model_spvs.touch_affiliate_territory,
	model_spvs.page_url,
	model_spvs.page_referrer,
	model_spvs.previous_page_url,
	model_spvs.page_referrer_categorisation,
	model_spvs.previous_page_url_categorisation,
	model_spvs.screen_name,
	model_spvs.previous_screen_name,
	model_spvs.previous_se_sale_id,
	model_spvs.previous_screen_name_categorisation,
	dim_sale.tech_platform,
	dim_sale.product_configuration,
	dim_sale.sale_name,
	dim_sale.posa_territory,
	CASE
		WHEN (
				 dim_sale.product_type IN ('Hotel')
					 AND dim_sale.travel_type IN ('International')
				 )
			OR
			 (
				 dim_sale.product_type NOT IN ('Package', 'WRD')
					 AND dim_sale.travel_type IN ('International')
				 )
			THEN 'International'
		WHEN (
				 dim_sale.product_type IN ('Hotel')
					 AND dim_sale.travel_type IN ('Domestic')
				 )
			OR
			 (
				 dim_sale.product_type NOT IN ('Package', 'WRD')
					 AND dim_sale.travel_type IN ('Domestic')
				 )
			THEN 'Domestic'
		WHEN dim_sale.product_type IN ('Package', 'WRD')
			THEN 'Package'
	END AS product_line,
	-- if tracy due to redirect stripping url params in referrer take previous page url
	-- app spvs for tracy powered deals are also served on web
	CASE
		WHEN dim_sale.tech_platform = 'TRAVELBIRD' THEN
			COALESCE(model_spvs.previous_page_url_categorisation,
					 model_spvs.page_referrer_categorisation,
					 model_spvs.previous_screen_name_categorisation
			)
		ELSE COALESCE(model_spvs.page_referrer_categorisation,
					  model_spvs.previous_screen_name_categorisation
			 )
	END AS previous_page_screen_categorisation
FROM scratch.robinpatel.spv_source__model_spvs model_spvs
INNER JOIN se.data.dim_sale
	ON model_spvs.se_sale_id = dim_sale.se_sale_id
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.spv_source AS
SELECT
	spvs.event_hash,
	spvs.touch_id,
	spvs.event_tstamp,
	spvs.se_sale_id,
	spvs.tb_offer_id,
	spvs.event_category,
	spvs.event_subcategory,
	spvs.touch_affiliate_territory,
	spvs.device_platform,
	spvs.page_url,
	spvs.page_referrer,
	spvs.previous_page_url,
	spvs.page_referrer_categorisation,
	spvs.previous_page_url_categorisation,
	spvs.screen_name,
	spvs.previous_screen_name,
	spvs.previous_screen_name_categorisation,
	spvs.previous_se_sale_id,
	spvs.tech_platform,
	spvs.posa_territory,
	spvs.product_line,
	spvs.product_configuration,
	spvs.sale_name,
	spvs.previous_page_screen_categorisation,
	channel.touch_mkt_channel,
	-- if no previous page/screen or classed as external url, it can be categorised then must be landing page spv
	IFF(spvs.previous_page_screen_categorisation IS NULL OR
		spvs.previous_page_screen_categorisation = 'external url',
		'Landing page - ' || channel.touch_mkt_channel,
		spvs.previous_page_screen_categorisation) AS spv_source
FROM scratch.robinpatel.spv_source__spv_referrer spvs
INNER JOIN se.data.scv_touch_marketing_channel channel
	ON spvs.touch_id = channel.touch_id
;

SELECT
	DATE_TRUNC(MONTH, spv_source.event_tstamp) AS month,
	spv_source.device_platform,
	spv_source.product_line,
	spv_source.touch_affiliate_territory,
	spv_source.spv_source,
	COUNT(*)                                   AS spvs
FROM scratch.robinpatel.spv_source
WHERE month = '2025-07-01'
  AND touch_affiliate_territory = 'UK'
GROUP BY ALL
;


------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM scratch.robinpatel.spv_source
;

/*
Things to investigate thread :thread:
sale page source
whitelabel spvs will all show landing page due to external url in udf
email source spvs alex was classifying them as search
sense check filter page, seems a bit low
from app referrer
*/

-- sale page source

SELECT
	PARSE_URL(ss.page_url)['path'] IS NOT DISTINCT FROM PARSE_URL(ss.page_referrer)['path'],
	se.data.platform_from_touch_experience(ss.device_platform) AS device_platform,
	COUNT(*)
FROM scratch.robinpatel.spv_source ss
WHERE ss.spv_source = 'sale page'
  AND DATE_TRUNC(MONTH, event_tstamp) = '2025-07-01'
GROUP BY ALL


SELECT *
FROM scratch.robinpatel.spv_source ss
WHERE ss.spv_source = 'sale page'
  AND DATE_TRUNC(MONTH, event_tstamp) = '2025-07-01'
  AND PARSE_URL(ss.page_url)['path'] IS NOT DISTINCT FROM PARSE_URL(ss.page_referrer)['path']
;

-- investigating spv source sale page for native app

SELECT
	ss.se_sale_id = previous_se_sale_id,
	COUNT(*)
FROM scratch.robinpatel.spv_source ss
WHERE ss.spv_source = 'sale page'
  AND DATE_TRUNC(MONTH, event_tstamp) = '2025-07-01'
  AND se.data.platform_from_touch_experience(ss.device_platform) = 'Native App'
GROUP BY ALL
;


-- whitelabel spvs will all show landing page due to external url in udf

SELECT
	ss.spv_source,
	COUNT(*)
FROM scratch.robinpatel.spv_source ss
WHERE DATE_TRUNC(MONTH, event_tstamp) = '2025-07-01'
GROUP BY 1
;

-- email source spvs alex was classifying them as search
SELECT
	ss.page_referrer IS NULL,
	COUNT(*)
FROM scratch.robinpatel.spv_source ss
WHERE DATE_TRUNC(MONTH, event_tstamp) = '2025-07-01'
  AND ss.spv_source LIKE 'Landing page - Email%'
GROUP BY 1
;


SELECT
	ss.previous_page_screen_categorisation,
	COUNT(*)
FROM scratch.robinpatel.spv_source ss
WHERE DATE_TRUNC(MONTH, event_tstamp) = '2025-07-01'
  AND ss.spv_source LIKE 'Landing page - Email%'
GROUP BY 1

SELECT *
FROM scratch.robinpatel.spv_source ss
WHERE DATE_TRUNC(MONTH, event_tstamp) = '2025-07-01'
  AND ss.spv_source LIKE 'Landing page - Email%'
  AND ss.previous_page_screen_categorisation = 'external url'
  AND ss.page_referrer LIKE '%search%'


-- sense check filter page, seems a bit low

SELECT *
FROM scratch.robinpatel.spv_source ss
WHERE DATE_TRUNC(MONTH, event_tstamp) = '2025-07-01'
  AND ss.spv_source IS DISTINCT FROM 'filter page'
  AND ss.page_referrer LIKE '%/filter%'
;


SELECT *
FROM scratch.robinpatel.spv_source ss
WHERE DATE_TRUNC(MONTH, event_tstamp) = '2025-07-01'
  AND ss.spv_source = 'Landing page - Direct'
  AND ss.page_referrer IS NOT NULL
;


SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_landing_page_categorisation = 'external url'
  AND DATE_TRUNC(MONTH, stba.touch_start_tstamp) = '2025-07-01';


SELECT * FROM collab.quality_assurance.commission_qa_v2;