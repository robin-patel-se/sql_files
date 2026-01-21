/*
 Searched for Secret Escapes on Google

Clicked Ad, and arrived with following url: https://www.secretescapes.com/current-sales?affiliate=goo-cpl-brand-uk&utmadgroupid=148328393708&awadposition=&utmcampaignid=17960206281&awcreative=618964799688&awdevice=c&awkeyword=secret+escapes.&awloc_interest_ms=&awloc_physical_ms=9046034&awmatchtype=e&awplacement=&awtargetid=kwd-61789019645&saff=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_campaign=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_adgroup={utmadgroup&gad_source=1&gad_campaignid=17960206281&gbraid=0AAAAADRSWbrVe1tsm3uktsm-QJ3B7wljS&gclid=CjwKCAiAmp3LBhAkEiwAJM2JUMSLC0FALRUuvJ54XwCF3oO6GZWbJMgTPRgxOqB3Q4eTHhU2AT3KFRoCTagQAvD_BwE&affiliateUrlString=goo-cpl-brand-uk

Searched homepage search bar for London, 13th Feb to 16th Feb

Got redirected to search page with lots of utms still persisted: `https://www.secretescapes.com/search/search?affiliate=goo-cpl-brand-uk&utmadgroupid=148328393708&awadposition=&utmcampaignid=17960206281&awcreative=618964799688&awdevice=c&awkeyword=secret+escapes.&awloc_interest_ms=&awloc_physical_ms=9046034&awmatchtype=e&awplacement=&awtargetid=kwd-61789019645&saff=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_campaign=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_adgroup={utmadgroup&gad_source=1&gad_campaignid=17960206281&gbraid=0AAAAADRSWbrVe1tsm3uktsm-QJ3B7wljS&gclid=CjwKCAiAmp3LBhAkEiwAJM2JUMSLC0FALRUuvJ54XwCF3oO6GZWbJMgTPRgxOqB3Q4eTHhU2AT3KFRoCTagQAvD_BwE&affiliateUrlString=goo-cpl-brand-uk&checkin=2026-02-13&checkout=2026-02-16&travelTypes=HOTEL_ONLY&query=London%2C+England&travellersSelection=AA


 */

SELECT
	PARSE_URL('https://www.secretescapes.com/current-sales?affiliate=goo-cpl-brand-uk&utmadgroupid=148328393708&awadposition=&utmcampaignid=17960206281&awcreative=618964799688&awdevice=c&awkeyword=secret+escapes.&awloc_interest_ms=&awloc_physical_ms=9046034&awmatchtype=e&awplacement=&awtargetid=kwd-61789019645&saff=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_campaign=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_adgroup={utmadgroup&gad_source=1&gad_campaignid=17960206281&gbraid=0AAAAADRSWbrVe1tsm3uktsm-QJ3B7wljS&gclid=CjwKCAiAmp3LBhAkEiwAJM2JUMSLC0FALRUuvJ54XwCF3oO6GZWbJMgTPRgxOqB3Q4eTHhU2AT3KFRoCTagQAvD_BwE&affiliateUrlString=goo-cpl-brand-uk')

/*

{
 "fragment": null,
 "host": "www.secretescapes.com",
 "parameters": {
   "affiliate": "goo-cpl-brand-uk",
   "affiliateUrlString": "goo-cpl-brand-uk",
   "awadposition": "",
   "awcreative": "618964799688",
   "awdevice": "c",
   "awkeyword": "secret+escapes.",
   "awloc_interest_ms": "",
   "awloc_physical_ms": "9046034",
   "awmatchtype": "e",
   "awplacement": "",
   "awtargetid": "kwd-61789019645",
   "gad_campaignid": "17960206281",
   "gad_source": "1",
   "gbraid": "0AAAAADRSWbrVe1tsm3uktsm-QJ3B7wljS",
   "gclid": "CjwKCAiAmp3LBhAkEiwAJM2JUMSLC0FALRUuvJ54XwCF3oO6GZWbJMgTPRgxOqB3Q4eTHhU2AT3KFRoCTagQAvD_BwE",
   "saff": "UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined",
   "utm_adgroup": "{utmadgroup",
   "utm_campaign": "UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined",
   "utmadgroupid": "148328393708",
   "utmcampaignid": "17960206281"
 },
 "path": "current-sales",
 "port": null,
 "query": "affiliate=goo-cpl-brand-uk&utmadgroupid=148328393708&awadposition=&utmcampaignid=17960206281&awcreative=618964799688&awdevice=c&awkeyword=secret+escapes.&awloc_interest_ms=&awloc_physical_ms=9046034&awmatchtype=e&awplacement=&awtargetid=kwd-61789019645&saff=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_campaign=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_adgroup={utmadgroup&gad_source=1&gad_campaignid=17960206281&gbraid=0AAAAADRSWbrVe1tsm3uktsm-QJ3B7wljS&gclid=CjwKCAiAmp3LBhAkEiwAJM2JUMSLC0FALRUuvJ54XwCF3oO6GZWbJMgTPRgxOqB3Q4eTHhU2AT3KFRoCTagQAvD_BwE&affiliateUrlString=goo-cpl-brand-uk",
 "scheme": "https"
}
*/


SELECT
	PARSE_URL('https://www.secretescapes.com/search/search?affiliate=goo-cpl-brand-uk&utmadgroupid=148328393708&awadposition=&utmcampaignid=17960206281&awcreative=618964799688&awdevice=c&awkeyword=secret+escapes.&awloc_interest_ms=&awloc_physical_ms=9046034&awmatchtype=e&awplacement=&awtargetid=kwd-61789019645&saff=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_campaign=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_adgroup=%7Butmadgroup&gad_source=1&gad_campaignid=17960206281&gbraid=0AAAAADRSWbrVe1tsm3uktsm-QJ3B7wljS&gclid=CjwKCAiAmp3LBhAkEiwAJM2JUMSLC0FALRUuvJ54XwCF3oO6GZWbJMgTPRgxOqB3Q4eTHhU2AT3KFRoCTagQAvD_BwE&affiliateUrlString=goo-cpl-brand-uk&checkin=2026-02-13&checkout=2026-02-16&travelTypes=HOTEL_ONLY&query=London%2C+England&travellersSelection=AA')


/*
{
 "fragment": null,
 "host": "www.secretescapes.com",
 "parameters": {
   "affiliate": "goo-cpl-brand-uk",
   "affiliateUrlString": "goo-cpl-brand-uk",
   "awadposition": "",
   "awcreative": "618964799688",
   "awdevice": "c",
   "awkeyword": "secret+escapes.",
   "awloc_interest_ms": "",
   "awloc_physical_ms": "9046034",
   "awmatchtype": "e",
   "awplacement": "",
   "awtargetid": "kwd-61789019645",
   "checkin": "2026-02-13",
   "checkout": "2026-02-16",
   "gad_campaignid": "17960206281",
   "gad_source": "1",
   "gbraid": "0AAAAADRSWbrVe1tsm3uktsm-QJ3B7wljS",
   "gclid": "CjwKCAiAmp3LBhAkEiwAJM2JUMSLC0FALRUuvJ54XwCF3oO6GZWbJMgTPRgxOqB3Q4eTHhU2AT3KFRoCTagQAvD_BwE",
   "query": "London%2C+England",
   "saff": "UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined",
   "travelTypes": "HOTEL_ONLY",
   "travellersSelection": "AA",
   "utm_adgroup": "%7Butmadgroup",
   "utm_campaign": "UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined",
   "utmadgroupid": "148328393708",
   "utmcampaignid": "17960206281"
 },
 "path": "search/search",
 "port": null,
 "query": "affiliate=goo-cpl-brand-uk&utmadgroupid=148328393708&awadposition=&utmcampaignid=17960206281&awcreative=618964799688&awdevice=c&awkeyword=secret+escapes.&awloc_interest_ms=&awloc_physical_ms=9046034&awmatchtype=e&awplacement=&awtargetid=kwd-61789019645&saff=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_campaign=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_adgroup=%7Butmadgroup&gad_source=1&gad_campaignid=17960206281&gbraid=0AAAAADRSWbrVe1tsm3uktsm-QJ3B7wljS&gclid=CjwKCAiAmp3LBhAkEiwAJM2JUMSLC0FALRUuvJ54XwCF3oO6GZWbJMgTPRgxOqB3Q4eTHhU2AT3KFRoCTagQAvD_BwE&affiliateUrlString=goo-cpl-brand-uk&checkin=2026-02-13&checkout=2026-02-16&travelTypes=HOTEL_ONLY&query=London%2C+England&travellersSelection=AA",
 "scheme": "https"
}
*/


SELECT *
FROM se.bi.session_metrics sm
WHERE sm.attributed_user_id = '67970160'
  AND sm.touch_start_tstamp::DATE = '2026-01-14'



-- this did created three sessions:
/*TOUCH_ID
65f110a6661a94be61f54515870a4016b33b1c3090a4c59b0d78dcb885aa1678
9a38ba1685dc9c7b2c61ff67b1b129ecb2c7d89cb129391c9edc42cf730f5dee
7f0e80d52ef371c3ef05bec26c0c8b4cccf841e61d1520a29812e6f66e7d88e1*/

SELECT
	sm.touch_id,
	sm.attributed_user_id,
	sm.touch_start_tstamp,
	sm.touch_mkt_channel,
	sm.landing_page_category,
	sm.touch_landing_page,
	sm.touch_experience,
	PARSE_URL(sm.touch_landing_page)                           AS parsed_landing_page,
	parsed_landing_page['parameters']['utm_campaign']::VARCHAR AS utm_campaign,
	parsed_landing_page['parameters']['utm_medium']::VARCHAR   AS utm_medium,
	parsed_landing_page['parameters']['utm_source']::VARCHAR   AS utm_source,
	parsed_landing_page['parameters']['utm_term']::VARCHAR     AS utm_term,
	parsed_landing_page['parameters']['utm_content']::VARCHAR  AS utm_content,
	parsed_landing_page['parameters']['gclid']::VARCHAR        AS gclid,
FROM se.bi.session_metrics sm
WHERE sm.attributed_user_id = '67970160'
  AND sm.touch_start_tstamp::DATE = '2026-01-14'
ORDER BY sm.touch_start_tstamp
;

USE WAREHOUSE pipe_xlarge
;

SELECT
	mt.touch_id,
	mt.attributed_user_id,
	murm.event_hash,
	murm.event_tstamp,
	murm.attributed_user_id,
	murm.stitched_identity_type,
	murm.utm_ref_partition,
	es.page_url,
	es.event_name,
	es.page_referrer,
	es.device_platform,
	es.contexts_com_secretescapes_search_context_1,
	mep.utm_campaign,
	mep.utm_medium,
	mep.utm_source,
	mep.utm_term,
	mep.utm_content,
	mep.click_id,
	-- logic to nullify referrer hostname for facebook oauth redirects
	-- on travelist
	IFF(
			es.page_referrer = 'https://www.facebook.com/'
				AND es.page_urlhost LIKE 'travelist.%'
				AND mep.utm_campaign IS NULL
				AND mep.utm_medium IS NULL
				AND mep.utm_source IS NULL
				AND mep.utm_term IS NULL
				AND mep.utm_content IS NULL
				AND mep.click_id IS NULL,
			NULL,
			es.refr_urlhost
	) AS url_host,
	SHA2(
			NULLIF(
					COALESCE(mep.utm_campaign, '') ||
					COALESCE(mep.utm_medium, '') ||
					COALESCE(mep.utm_source, '') ||
					COALESCE(mep.utm_term, '') ||
					COALESCE(mep.utm_content, '') ||
					COALESCE(mep.click_id, '') ||
					COALESCE(url_host, ''),
					'')
	) AS partition_marker,
	mep.sub_affiliate_name,
	mep.from_app,
	mep.snowplow_id,
	mep.affiliate,
	mep.awcampaignid,
	mep.awadgroupid,
	mep.account_verified,
	mep.message_id,
	mep.utm_platform,
	mep.url_parameters,
FROM data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker murm
INNER JOIN hygiene_vault_mvp.snowplow.event_stream es
	ON murm.event_hash = es.event_hash AND es.event_tstamp::DATE = '2026-01-14'
INNER JOIN data_vault_mvp.single_customer_view_stg.module_extracted_params mep
	ON es.page_url = mep.url
INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
	ON es.event_hash = mt.event_hash AND mt.event_tstamp::DATE = '2026-01-14'
WHERE murm.attributed_user_id = '67970160' AND murm.event_tstamp::DATE = '2026-01-14'
;

--
-- utm_campaign
-- utm_medium
-- utm_source
-- utm_term
-- utm_content
-- click_id;


SELECT *
FROM se.bi.session_metrics sm
WHERE sm.touch_start_tstamp::DATE = '2026-01-14' AND attributed_user_id = '67970160'
;

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp::DATE = '2026-01-14' AND stba.attributed_user_id = '67970160'
;

USE WAREHOUSE pipe_xlarge
;

SELECT
	mt.created_at,
	mt.updated_at,
	mt.event_hash,
	mt.attributed_user_id,
	mt.stitched_identity_type,
	mt.event_tstamp,
	mt.touch_id,
	mt.event_index_within_touch,
-- 	m.created_at,
-- 	m.updated_at,
	m.event_hash,
	m.utm_ref_partition,
-- 	mtdm.created_at,
-- 	mtdm.updated_at,
	mtdm.time_diff_partition,
	events.device_platform,
	events.page_url,
	events.page_referrer,
	PARSE_URL(events.page_url, 1)['parameters']    AS page_url_parameters,
	PARSE_URL(events.page_url, 1)['host']::VARCHAR AS page_url_host,
	IFF(
			events.page_referrer = 'https://www.facebook.com/'
				AND page_url_host LIKE 'travelist.%'
				AND page_url_parameters['utm_campaign']::VARCHAR IS NULL
				AND page_url_parameters['utm_medium']::VARCHAR IS NULL
				AND page_url_parameters['utm_source']::VARCHAR IS NULL
				AND page_url_parameters['utm_term']::VARCHAR IS NULL
				AND page_url_parameters['utm_content']::VARCHAR IS NULL
				AND page_url_parameters['fbclid']::VARCHAR IS NULL,
			NULL,
			referrer.url_hostname
	)                                              AS url_host,
	SHA2(
			NULLIF(
					COALESCE(mep.utm_campaign, '') ||
					COALESCE(mep.utm_medium, '') ||
					COALESCE(mep.utm_source, '') ||
					COALESCE(mep.utm_term, '') ||
					COALESCE(mep.utm_content, '') ||
					COALESCE(mep.click_id, '') ||
					COALESCE(url_host, ''),
					'')
	)                                              AS partition_marker,
	mep.utm_campaign,
	mep.utm_medium,
	mep.utm_source,
	mep.utm_term,
	mep.utm_content,
	mep.click_id,
	mep.sub_affiliate_name,
	mep.from_app,
	mep.snowplow_id,
	mep.affiliate,
	mep.awcampaignid,
	mep.awadgroupid,
	mep.account_verified,
	mep.message_id,
	mep.utm_platform,
	mep.url_parameters,
	referrer.url_hostname                          AS referrer_url_hostname,
	referrer.url_medium                            AS referrer_url_medium
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
INNER JOIN data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker m
	ON mt.event_hash = m.event_hash AND m.event_tstamp::DATE = '2026-01-14'
INNER JOIN data_vault_mvp.single_customer_view_stg.module_time_diff_marker mtdm
	ON mt.event_hash = mtdm.event_hash
INNER JOIN hygiene_vault_mvp.snowplow.event_stream events
	ON mt.event_hash = events.event_hash AND events.event_tstamp::DATE = '2026-01-14'
LEFT JOIN data_vault_mvp.single_customer_view_stg.module_extracted_params mep
	ON events.page_url = mep.url
LEFT JOIN data_vault_mvp.single_customer_view_stg.module_url_hostname referrer
	ON events.page_referrer = referrer.url
	AND referrer.url_medium NOT IN ('internal', 'payment_gateway', 'oauth')
WHERE mt.attributed_user_id = '67970160' AND mt.event_tstamp::DATE = '2026-01-14'
ORDER BY mt.event_tstamp
;



https://www.secretescapes.com/current-sales?affiliate=goo-cpl-brand-uk&utmadgroupid=148328393708&awadposition=&utmcampaignid=17960206281&awcreative=618964799688&awdevice=c&awkeyword=secret+escapes.&awloc_interest_ms=&awloc_physical_ms=9046034&awmatchtype=e&awplacement=&awtargetid=kwd-61789019645&saff=UKCPL+Brand+Pure+ALL-Destinations+Exact+Non-Member+Combined&utm_campaign=UKCPL+Brand+Pure+ALL-Destinations+Exact+Non-Member+Combined&utm_adgroup=%7Butmadgroup&gad_source=1&gad_campaignid=17960206281&gbraid=0AAAAADRSWbrVe1tsm3uktsm-QJ3B7wljS&gclid=CjwKCAiAmp3LBhAkEiwAJM2JUMSLC0FALRUuvJ54XwCF3oO6GZWbJMgTPRgxOqB3Q4eTHhU2AT3KFRoCTagQAvD_BwE&affiliateUrlString=goo-cpl-brand-uk
https://www.secretescapes.com/search/search?affiliate=goo-cpl-brand-uk&utmadgroupid=148328393708&awadposition=&utmcampaignid=17960206281&awcreative=618964799688&awdevice=c&awkeyword=secret+escapes.&awloc_interest_ms=&awloc_physical_ms=9046034&awmatchtype=e&awplacement=&awtargetid=kwd-61789019645&saff=UKCPL+Brand+Pure+ALL-Destinations+Exact+Non-Member+Combined&utm_campaign=UKCPL+Brand+Pure+ALL-Destinations+Exact+Non-Member+Combined&utm_adgroup=%7Butmadgroup&gad_source=1&gad_campaignid=17960206281&gbraid=0AAAAADRSWbrVe1tsm3uktsm-QJ3B7wljS&gclid=CjwKCAiAmp3LBhAkEiwAJM2JUMSLC0FALRUuvJ54XwCF3oO6GZWbJMgTPRgxOqB3Q4eTHhU2AT3KFRoCTagQAvD_BwE&affiliateUrlString=goo-cpl-brand-uk&checkin=2026-02-13&checkout=2026-02-16&travelTypes=HOTEL_ONLY&query=London%2C+England&travellersSelection=AA


------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/03_touchification/02_01_utm_or_referrer_hostname_marker.py make clones


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
	CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
	CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params
	CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
	CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker
	CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker
;

/*
self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.03_touchification.02_01_utm_or_referrer_hostname_marker.py' \
    --method 'run' \
    --start '2026-01-15 00:00:00' \
    --end '2026-01-15 00:00:00'
*/

;

USE WAREHOUSE pipe_xlarge

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker__step03__persist_partition_marker
WHERE attributed_user_id = '67970160'
;

SELECT SHA2('' || '')


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker__step04__create_partition_flag
WHERE attributed_user_id = '67970160'
;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker
WHERE attributed_user_id = '67970160'
  AND event_tstamp::DATE = '2026-01-14'
;

SELECT
	COUNT(*)                                                AS events,
	COUNT(DISTINCT attributed_user_id)                      AS users,
	COUNT(DISTINCT attributed_user_id || utm_ref_partition) AS user_partitions
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker
;

/*
EVENTS		USERS		USER_PARTITIONS
3124305191	155762097	346291235
*/

SELECT
	COUNT(*)                                                AS events,
	COUNT(DISTINCT attributed_user_id)                      AS users,
	COUNT(DISTINCT attributed_user_id || utm_ref_partition) AS user_partitions
FROM data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker
;

/*
EVENTS		USERS		USER_PARTITIONS
3124305191	155762097	367837000
 */

-- prod USER_PARTITIONS: 367,837,000
-- dev USER_PARTITIONS: 346,291,235
-- diff: -21,545,765
-- var: -5.86%


SELECT
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba -- 562,070,846 -- 4% reduction in overall sessions

------------------------------------------------------------------------------------------------------------------------
-- need to run downstream scv to see impact

-- biapp/task_catalogue/dv/dwh/scv/03_touchification/03_touchification.py


	use role personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
	CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching
;

-- use dev version
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker
-- CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker
	CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
	CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/03_touchification/03_touchification.py'  --method 'run' --start '2026-01-14 00:00:00' --end '2026-01-14 00:00:00'

SELECT
	COUNT(*)                           AS events,
	COUNT(DISTINCT touch_id)           AS sessions,
	COUNT(DISTINCT attributed_user_id) AS users,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
;

/*
EVENTS		SESSIONS	USERS
3124305191	544201926	155762097

*/


SELECT
	COUNT(*)                           AS events,
	COUNT(DISTINCT touch_id)           AS sessions,
	COUNT(DISTINCT attributed_user_id) AS users,
FROM data_vault_mvp.single_customer_view_stg.module_touchification


/*
EVENTS		SESSIONS	USERS
3124305191	562070846	155762097
*/

-- prod
SELECT
	DATE_TRUNC(MONTH, module_touchification.event_tstamp) AS month,
	COUNT(*)                                              AS events,
	COUNT(DISTINCT touch_id)                              AS sessions,
	COUNT(DISTINCT attributed_user_id)                    AS users,
FROM data_vault_mvp.single_customer_view_stg.module_touchification
GROUP BY ALL
;

-- dev
SELECT
	DATE_TRUNC(MONTH, module_touchification.event_tstamp) AS month,
	COUNT(*)                                              AS events,
	COUNT(DISTINCT touch_id)                              AS sessions,
	COUNT(DISTINCT attributed_user_id)                    AS users,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
GROUP BY ALL
;


-- dev -- checking updated rows
SELECT
	module_touchification.updated_at::DATE,
	COUNT(*),
	COUNT(DISTINCT touch_id) AS sessions,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
GROUP BY ALL
;

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/01_module_touched_spvs.py
-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/01_module_touched_spvs.py make clones


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

-- use dev version
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
	CLONE data_vault_mvp.dwh.se_sale
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/01_module_touched_spvs.py'  --method 'run' --start '2026-01-14 00:00:00' --end '2026-01-14 00:00:00'

-- prod

SELECT
	COUNT(*)                     AS spvs,
	COUNT(DISTINCT mts.touch_id) AS sessions_w_spv,
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
;

/*
SPVS		SESSIONS_W_SPV
836284789	334418940

*/

-- dev
SELECT
	COUNT(*)                     AS spvs,
	COUNT(DISTINCT mts.touch_id) AS sessions_w_spv,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs mts

/*
SPVS		SESSIONS_W_SPV
836280052	329904492
*/


-- prod
SELECT
	DATE_TRUNC(MONTH, event_tstamp) AS month,
	COUNT(*)                        AS spvs,
	COUNT(DISTINCT touch_id)        AS sessions,
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs
GROUP BY ALL
;

-- dev
SELECT
	DATE_TRUNC(MONTH, event_tstamp) AS month,
	COUNT(*)                        AS spvs,
	COUNT(DISTINCT touch_id)        AS sessions,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
GROUP BY ALL
;


-- dev -- checking updated rows
SELECT
	updated_at::DATE,
	COUNT(*),
	COUNT(DISTINCT touch_id) AS sessions,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs mts
GROUP BY ALL
;


------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/02_module_touched_transactions.py
-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/02_module_touched_transactions.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

-- use dev version
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

-- already cloned
-- CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;
--
-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
-- CLONE hygiene_vault_mvp.snowplow.event_stream;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/02_module_touched_transactions.py'  --method 'run' --start '2026-01-14 00:00:00' --end '2026-01-14 00:00:00'



-- prod

SELECT
	COUNT(*)                     AS transactions,
	COUNT(DISTINCT mts.touch_id) AS sessions_w_transaction,
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions mts
;

/*
TRANSACTIONS	SESSIONS_W_TRANSACTION
2030487			1983901


*/

-- dev
SELECT
	COUNT(*)                     AS transactions,
	COUNT(DISTINCT mts.touch_id) AS sessions_w_transaction,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions mts

	/*
	TRANSACTIONS	SESSIONS_W_TRANSACTION
	2030487			1983241

	*/

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/03_module_touched_searches.py
-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/03_module_touched_searches.py make clones


	use role personal_role__robinpatel
;

-- CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
-- CLONE hygiene_vault_mvp.snowplow.event_stream;

-- CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/03_module_touched_searches.py'  --method 'run' --start '2026-01-14 00:00:00' --end '2026-01-14 00:00:00'

-- prod

SELECT
	COUNT(*)                     AS searches,
	COUNT(DISTINCT mts.touch_id) AS sessions_w_search,
FROM data_vault_mvp.single_customer_view_stg.module_touched_searches mts
;

/*
SEARCHES	SESSIONS_W_SEARCH
459437919	125024532



*/

-- dev
SELECT
	COUNT(*)                     AS searches,
	COUNT(DISTINCT mts.touch_id) AS sessions_w_search,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches mts

/*
SEARCHES	SESSIONS_W_SEARCH
459437919	118787293
*/

-- prod
SELECT
	DATE_TRUNC(MONTH, event_tstamp) AS month,
	COUNT(*)                        AS searches,
	COUNT(DISTINCT touch_id)        AS sessions_w_search,
FROM data_vault_mvp.single_customer_view_stg.module_touched_searches
GROUP BY ALL
;

-- dev
SELECT
	DATE_TRUNC(MONTH, event_tstamp) AS month,
	COUNT(*)                        AS searches,
	COUNT(DISTINCT touch_id)        AS sessions_w_search,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
GROUP BY ALL
;



------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/04_module_touched_app_installs.py
-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/04_module_touched_app_installs.py make clones


USE ROLE personal_role__robinpatel
;

-- CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
--
-- CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;
--
-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
-- CLONE hygiene_vault_mvp.snowplow.event_stream;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/04_module_touched_app_installs.py'  --method 'run' --start '2026-01-14 00:00:00' --end '2026-01-14 00:00:00'


-- prod

SELECT
	COUNT(*)                 AS app_installs,
	COUNT(DISTINCT touch_id) AS sessions_w_app_install,
FROM data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

/*
APP_INSTALLS	SESSIONS_W_APP_INSTALL
2314129			2305178
*/

-- dev
SELECT
	COUNT(*)                 AS app_installs,
	COUNT(DISTINCT touch_id) AS sessions_w_app_install,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs

/*
APP_INSTALLS	SESSIONS_W_APP_INSTALL
2314129			2305168
*/


-- prod
SELECT
	DATE_TRUNC(MONTH, event_tstamp) AS month,
	COUNT(*)                        AS app_installs,
	COUNT(DISTINCT touch_id)        AS sessions_w_app_install,
FROM data_vault_mvp.single_customer_view_stg.module_touched_app_installs
GROUP BY ALL
;

-- dev
SELECT
	DATE_TRUNC(MONTH, event_tstamp) AS month,
	COUNT(*)                        AS app_installs,
	COUNT(DISTINCT touch_id)        AS sessions_w_app_install,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
GROUP BY ALL
;


------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/06_module_touched_booking_form_views.py
-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/06_module_touched_booking_form_views.py make clones

USE ROLE personal_role__robinpatel
;

--
-- CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

-- CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;
--
-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
-- CLONE hygiene_vault_mvp.snowplow.event_stream;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.travelbird_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_orderproperty
	CLONE latest_vault.travelbird_mysql.orders_orderproperty
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/06_module_touched_booking_form_views.py'  --method 'run' --start '2026-01-14 00:00:00' --end '2026-01-14 00:00:00'

-- prod

SELECT
	COUNT(*)                 AS booking_form_views,
	COUNT(DISTINCT touch_id) AS sessions_w_booking_form_view,
FROM data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views
;

/*
BOOKING_FORM_VIEWS	SESSIONS_W_BOOKING_FORM_VIEW
20073391			14523356

*/

-- dev
SELECT
	COUNT(*)                 AS booking_form_views,
	COUNT(DISTINCT touch_id) AS sessions_w_booking_form_view,
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views

	/*
	BOOKING_FORM_VIEWS	SESSIONS_W_BOOKING_FORM_VIEW
	20073391			14466007

	*/


------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/09_module_events_of_interest.py
-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/09_module_events_of_interest.py make clones


	use role personal_role__robinpatel
;

-- CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
-- CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
-- CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
-- CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
-- CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
-- CLONE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_in_app_notification_events
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_in_app_notification_events
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_pay_button_clicks
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_pay_button_clicks
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest
	CLONE data_vault_mvp.single_customer_view_stg.module_events_of_interest
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/09_module_events_of_interest.py'  --method 'run' --start '2026-01-14 00:00:00' --end '2026-01-14 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/00_anomalous_user_dates.py
-- module=/biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/00_anomalous_user_dates.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
-- CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates
	CLONE data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/00_anomalous_user_dates.py'  --method 'run' --start '2026-01-14 00:00:00' --end '2026-01-14 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/01_module_touch_basic_attributes.py
-- module=/biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/01_module_touch_basic_attributes.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
-- CLONE hygiene_vault_mvp.snowplow.event_stream;
--
-- CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates
-- CLONE data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest
-- CLONE data_vault_mvp.single_customer_view_stg.module_events_of_interest;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
	CLONE data_vault_mvp.single_customer_view_stg.page_screen_enrichment
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

-- optional statement to create the module target table --

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2026-01-14 00:00:00' --end '2026-01-14 00:00:00'

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/01_module_touch_utm_referrer.py
-- module=/biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/01_module_touch_utm_referrer.py make clones

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params
	CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
	CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname
;

-- optional statement to create the module target table --
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/01_module_touch_utm_referrer.py'  --method 'run' --start '2026-01-14 00:00:00' --end '2026-01-14 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/02_module_touch_marketing_channel.py
-- module=/biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/02_module_touch_marketing_channel.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.affiliate
	CLONE latest_vault.cms_mysql.affiliate
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
-- CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory
	CLONE latest_vault.cms_mysql.territory
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

-- optional statement to create the module target table --
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2026-01-14 00:00:00' --end '2026-01-14 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/dv/dwh/scv/07_touch_attribution/01_module_touch_attribution.py
-- optional statement to create the module target table --

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/07_touch_attribution/01_module_touch_attribution.py'  --method 'run' --start '2026-01-14 00:00:00' --end '2026-01-14 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/dv/bi/scv/session_metrics/session_metrics__events_of_interest.py

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

-- CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest
-- CLONE data_vault_mvp.single_customer_view_stg.module_events_of_interest;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.product_reservation
	CLONE latest_vault.cms_mysql.product_reservation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.reservation
	CLONE latest_vault.cms_mysql.reservation
;

DROP TABLE data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest
;
-- optional statement to create the module target table --
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest
	CLONE data_vault_mvp.bi.session_metrics__events_of_interest
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.scv.session_metrics.session_metrics__events_of_interest.py' \
    --method 'run' \
    --start '2026-01-15 00:00:00' \
    --end '2026-01-15 00:00:00'

------------------------------------------------------------------------------------------------------------------------

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__feature_flags
	CLONE data_vault_mvp.bi.session_metrics__feature_flags
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.scv.session_metrics.session_metrics__feature_flags.py' \
    --method 'run' \
    --start '2026-01-15 00:00:00' \
    --end '2026-01-15 00:00:00'

------------------------------------------------------------------------------------------------------------------------

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
-- CLONE hygiene_vault_mvp.snowplow.event_stream;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

-- optional statement to create the module target table --
DROP TABLE data_vault_mvp_dev_robin.bi.session_metrics__login_types

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.bi.session_metrics__login_types
	CLONE data_vault_mvp.bi.session_metrics__login_types
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.scv.session_metrics.session_metrics__login_types.py' \
    --method 'run' \
    --start '2026-01-15 00:00:00' \
    --end '2026-01-15 00:00:00'

------------------------------------------------------------------------------------------------------------------------

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.bi
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest
-- CLONE data_vault_mvp.bi.session_metrics__events_of_interest;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__feature_flags
-- CLONE data_vault_mvp.bi.session_metrics__feature_flags;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__login_types
-- CLONE data_vault_mvp.bi.session_metrics__login_types;

-- optional statement to create the module target table --
DROP TABLE data_vault_mvp_dev_robin.bi.session_metrics
;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.bi.session_metrics
	CLONE data_vault_mvp.bi.session_metrics
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.scv.session_metrics.session_metrics.py' \
    --method 'run' \
    --start '2026-01-15 00:00:00' \
    --end '2026-01-15 00:00:00'

------------------------------------------------------------------------------------------------------------------------

SELECT
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
;

SELECT
	COUNT(*)
FROM data_vault_mvp.bi.session_metrics sm
;

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
;

-- prod
SELECT
	sm.touch_se_brand,
	sm.touch_mkt_channel,
	COUNT(*)
FROM data_vault_mvp.bi.session_metrics sm
GROUP BY ALL
;

-- dev
SELECT
	sm.touch_se_brand,
	sm.touch_mkt_channel,
	COUNT(*)
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
GROUP BY ALL
;

------------------------------------------------------------------------------------------------------------------------
SELECT
	mtba.touch_id
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp::DATE = '2026-01-14'

EXCEPT

SELECT
	mtba.touch_id
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp::DATE = '2026-01-14'
;

SELECT *
FROM se.data_pii.scv_session_events_link ssel
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash AND ses.event_tstamp::DATE = '2026-01-14'
WHERE ssel.event_tstamp::DATE = '2026-01-14'
  AND ssel.touch_id IN (
						'66d36e362dade6663df8f4f5032f43b3e00c714b57ce962e279e0c273a9e69af',
						'75cdca8dc0ebe59ffc23a5d7e97364348add2bbdbf87eeedc12a5608f63e8cbb',
						'f0aba415299981212b99e9beccf205448e45611cd98eeaba517c2c34fc798e95',
						'81c440054478ac1f57cc73134471a82f383def25e36b6a2344d32cb1af88f6d2',
						'23ec678a6643760689901622913163e8fb178b3f92ce4fc54f54a64de1b099a0',
						'69e0ac8218222b0f85dfd3f2b69d14b4ad8c9954cdf72e49f1a928ef425347e1',
						'dd92e6dab5cd80927c9b0a2cf834aab678872aeead9ec35d19042b4249685a3a',
						'a65c4ab2a6cc6cfcb32576a332ec20e6e44d75190908862bec2a660439f3d13d',
						'769a348c43fa78765c78f9167babcb88fb60ffe4300fdf602f07d5c232e2fcec',
						'70166989d651fbf81864d4bd071af27a1e902a73ffa2ac731e4b2e8c5a161656'
	)
;


SELECT *
FROM se.bi.session_metrics sm
WHERE sm.attributed_user_id = '56638575'
ORDER BY touch_start_tstamp
;

SELECT *
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
WHERE sm.attributed_user_id = '56638575'
--   AND sm.touch_start_tstamp::DATE = '2026-01-14'
;

https://www.secretescapes.de/current-sales?affiliate=goo-cpl-brand-de&utmadgroupid=138632967018&awadposition=&utmcampaignid=17420310849&awcreative=642053837434&awdevice=c&awkeyword=secret+escapes&awloc_interest_ms=&awloc_physical_ms=9044765&awmatchtype=e&awplacement=&awtargetid=kwd-12680113420&saff=DECPL+Brand+Pure+ALL-Destinations+Phrase+Non-Member+Combined&utm_campaign=DECPL+Brand+Pure+ALL-Destinations+Exact+Non-Member+Combined&utm_adgroup=DECPL+Brand+Pure+Exact+ALL-Destinations+Non-Member+Pure&gad_source=1&gad_campaignid=17420310849&gclid=EAIaIQobChMIldqDnfuIkgMVGpKDBx2yCy8XEAAYASAAEgI1L_D_BwE&affiliateUrlString=goo-cpl-brand-de
https://www.secretescapes.de/current-sales?affiliate=goo-cpl-brand-de&utmadgroupid=138632967018&awadposition=&utmcampaignid=17420310849&awcreative=642053837434&awdevice=c&awkeyword=secret+escapes&awloc_interest_ms=&awloc_physical_ms=9044765&awmatchtype=e&awplacement=&awtargetid=kwd-12680113420&saff=DECPL+Brand+Pure+ALL-Destinations+Phrase+Non-Member+Combined&utm_campaign=DECPL+Brand+Pure+ALL-Destinations+Exact+Non-Member+Combined&utm_adgroup=DECPL+Brand+Pure+Exact+ALL-Destinations+Non-Member+Pure&gad_source=1&gad_campaignid=17420310849&gclid=EAIaIQobChMIuof0roCJkgMVGM5EBx29JQGIEAAYASAAEgJv0fD_BwE&affiliateUrlString=goo-cpl-brand-de
------------------------------------------------------------------------------------------------------------------------

WITH
	prod AS (
		SELECT
			DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
			sm.touch_mkt_channel,
			last_non_direct_touch_mkt_channel,
			sm.touch_se_brand,
			COUNT(*)                                 AS prod_sessions,
			SUM(sm.spvs)                             AS prod_spvs,
			SUM(sm.booking_form_views)               AS prod_bfvs,
			SUM(sm.bookings)                         AS prod_bookings
		FROM data_vault_mvp.bi.session_metrics sm
		GROUP BY ALL


	),
	dev AS (
		SELECT
			DATE_TRUNC(MONTH, sm.touch_start_tstamp) AS month,
			sm.touch_mkt_channel,
			sm.last_non_direct_touch_mkt_channel,
			sm.touch_se_brand,
			COUNT(*)                                 AS dev_sessions,
			SUM(sm.spvs)                             AS dev_spvs,
			SUM(sm.booking_form_views)               AS dev_bfvs,
			SUM(sm.bookings)                         AS dev_bookings
		FROM data_vault_mvp_dev_robin.bi.session_metrics sm
		GROUP BY ALL
	)
SELECT
	prod.month,
	prod.touch_mkt_channel,
	prod.last_non_direct_touch_mkt_channel,
	prod.touch_se_brand,
	prod.prod_sessions,
	dev.dev_sessions,
	prod.prod_spvs,
	dev.dev_spvs,
	prod.prod_bfvs,
	dev.dev_bfvs,
	prod.prod_bookings,
	dev.dev_bookings
FROM prod
LEFT JOIN dev
	ON prod.month = dev.month
	AND prod.touch_mkt_channel = dev.touch_mkt_channel
	AND prod.last_non_direct_touch_mkt_channel = dev.last_non_direct_touch_mkt_channel
	AND prod.touch_se_brand = dev.touch_se_brand
;

------------------------------------------------------------------------------------------------------------------------
-- looking for sessions that were organic search brand that aren't in dev table

SELECT
	sm.touch_id
FROM data_vault_mvp.bi.session_metrics sm
WHERE sm.touch_start_tstamp::DATE = '2026-01-14'
  AND sm.last_non_direct_touch_mkt_channel = 'Organic Search Non-Brand'
EXCEPT
SELECT
	sm.touch_id
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
WHERE sm.touch_start_tstamp::DATE = '2026-01-14'
;


SELECT *
FROM se.data_pii.scv_session_events_link ssel
WHERE ssel.event_tstamp::DATE = '2026-01-14'
  AND ssel.touch_id IN (
						'f9ebf8f97a63677c4d30dd259e8e68f7bbc465e14cdbce24d1ab1d61a165623b',
						'f1fb950592e9de5781b810f4b6712405962f5812cca225fffa76665104490c83',
						'd41b4b0ebf1650b38b9ec64ac0c3a2734c39629cd70ef5054ff8e5f5e66b1c67',
						'68a750a18c011207297d832a3932d0b865ca4bb358a0b9afb8a75b4e25e68f0b',
						'42a2b870ea46f9d54c59ee6bcd6e4d47e3a1f4bdf68bcb0a585ba98dd81bdeaa',
						'0ca41f841ba3ebc5cc86dfe5f2746f70e25b7cb329789b6e499435b5a198973c',
						'fc1096d3e05508cd014e8e14674ef46e5cf152222fcc786df2ad05b2f502bcf3',
						'171b8805ed16e98e8d2e9fbe72caaabbf34c23145bf58abe7611fad56d5329f1',
						'ca39a35ed63e5e432a307b40f8d0df37e9faf5f8f2927d99b36685a5c3db5a9e',
						'ee7acdf48ef2e4a9a77375bf3f4c7656b3665c5392686465fe28abc224f999af'
	)
;


SELECT *
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
WHERE sm.touch_start_tstamp::DATE = '2026-01-14'
  AND sm.touch_id IN (
					  'f9ebf8f97a63677c4d30dd259e8e68f7bbc465e14cdbce24d1ab1d61a165623b',
					  'f1fb950592e9de5781b810f4b6712405962f5812cca225fffa76665104490c83',
					  'd41b4b0ebf1650b38b9ec64ac0c3a2734c39629cd70ef5054ff8e5f5e66b1c67',
					  '68a750a18c011207297d832a3932d0b865ca4bb358a0b9afb8a75b4e25e68f0b',
					  '42a2b870ea46f9d54c59ee6bcd6e4d47e3a1f4bdf68bcb0a585ba98dd81bdeaa',
					  '0ca41f841ba3ebc5cc86dfe5f2746f70e25b7cb329789b6e499435b5a198973c',
					  'fc1096d3e05508cd014e8e14674ef46e5cf152222fcc786df2ad05b2f502bcf3',
					  '171b8805ed16e98e8d2e9fbe72caaabbf34c23145bf58abe7611fad56d5329f1',
					  'ca39a35ed63e5e432a307b40f8d0df37e9faf5f8f2927d99b36685a5c3db5a9e',
					  'ee7acdf48ef2e4a9a77375bf3f4c7656b3665c5392686465fe28abc224f999af'
	)
;


SELECT *
FROM se.bi.session_metrics sm
WHERE sm.attributed_user_id = '85440847'
  AND sm.touch_start_tstamp::DATE = '2026-01-14'
;


SELECT *
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
WHERE sm.attributed_user_id = '85440847'
--   AND sm.touch_start_tstamp::DATE = '2026-01-14'
;


SELECT *
FROM se.data_pii.scv_session_events_link ssel
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash
	AND ses.event_tstamp::DATE = '2026-01-14'
WHERE ssel.touch_id IN (
						'b053d3450979ba3baad19d2fb9261e05b452e2c3bb4debb15b4796901fc8ce63',
						'0ca41f841ba3ebc5cc86dfe5f2746f70e25b7cb329789b6e499435b5a198973c'
	)
  AND ssel.event_tstamp::DATE = '2026-01-14'
;



SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification ssel
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash
	AND ses.event_tstamp::DATE = '2026-01-14'
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker murm
	ON ssel.event_hash = murm.event_hash
	AND murm.event_tstamp::DATE = '2026-01-14'
WHERE ssel.touch_id IN (
						'b053d3450979ba3baad19d2fb9261e05b452e2c3bb4debb15b4796901fc8ce63',
						'0ca41f841ba3ebc5cc86dfe5f2746f70e25b7cb329789b6e499435b5a198973c',
						'9ddf7f57ff71a65cf42b8112bc646d58ee63c371f7a7ff5cf1d4471c21ddbe85'
	)
  AND ssel.event_tstamp::DATE = '2026-01-14'
;

USE WAREHOUSE pipe_xlarge
;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker__step04__create_partition_flag
WHERE attributed_user_id = '85440847'


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker__step03__persist_partition_marker
WHERE attributed_user_id = '85440847'
;

-- referrer marker didn't change even though it should have:
-- copying code logic and trying to understand why

WITH
	module_utm_referrer_marker__step03__persist_partition_marker AS (
		SELECT
			events.event_hash,
			events.event_tstamp,
			events.attributed_user_id,
			events.stitched_identity_type,
			events.page_url,
			events.page_referrer,
			events.url_hostname                                                             AS referrer_url_hostname,
			events.referrer_url_host, -- with facebook oauth logic applied
			events.utm_partition_marker,
			events.referrer_partition_marker,
			LAST_VALUE(events.utm_partition_marker) IGNORE NULLS OVER (
				PARTITION BY
					events.attributed_user_id,
					events.stitched_identity_type
				ORDER BY
					events.event_tstamp,
					events.derived_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS persisted_utm_partition_marker,
			LAST_VALUE(events.referrer_partition_marker) IGNORE NULLS OVER (
				PARTITION BY
					events.attributed_user_id,
					events.stitched_identity_type
				ORDER BY
					events.event_tstamp,
					events.derived_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS persisted_referrer_partition_marker,
			SHA2(
					COALESCE(persisted_utm_partition_marker, '') ||
					COALESCE(persisted_referrer_partition_marker, '')
			)                                                                               AS partition_group
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker__step02__utm_referrer_marker events
	)
SELECT *
FROM module_utm_referrer_marker__step03__persist_partition_marker
WHERE attributed_user_id = '85440847'
;


------------------------------------------------------------------------------------------------------------------------
-- investigation into 4604688 user who had a PPC brand

SELECT *
FROM data_vault_mvp.bi.session_metrics sm
WHERE sm.touch_start_tstamp::DATE = '2026-01-14'
  AND sm.attributed_user_id = '4604688'
;

SELECT *
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
WHERE sm.touch_start_tstamp::DATE = '2026-01-14'
  AND sm.attributed_user_id = '4604688'
;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification ssel
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash
	AND ses.event_tstamp::DATE = '2026-01-14'
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker murm
	ON ssel.event_hash = murm.event_hash
	AND murm.event_tstamp::DATE = '2026-01-14'
WHERE murm.attributed_user_id = '4604688'
  AND ssel.event_tstamp::DATE = '2026-01-14'
;



SELECT
	PARSE_URL('https://www.secretescapes.com/search/search?affiliate=goo-cpl-brand-uk&utmadgroupid=148328393708&awadposition=&utmcampaignid=17960206281&awcreative=621971089574&awdevice=c&awkeyword=secretescapes&awloc_interest_ms=&awloc_physical_ms=9046034&awmatchtype=e&awplacement=&awtargetid=kwd-10850663191&saff=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_campaign=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_adgroup=%7Butmadgroup&gad_source=1&gad_campaignid=17960206281&gbraid=0AAAAADRSWbq11xhCocQYLdHLbfLcJOysE&gclid=CjwKCAiAybfLBhAjEiwAI0mBBvI7lS0W2uV9yqefx02pJmVgocgH3tEiC4Ft06qfu1y7uMYoRQNQ6BoCVoEQAvD_BwE&affiliateUrlString=goo-cpl-brand-uk&travelTypes=HOTEL_ONLY&query=London%2C+England&travellersSelection=AA')
;

SELECT
	PARSE_URL('https://www.secretescapes.com/current-sales?affiliate=goo-cpl-brand-uk&utmadgroupid=148328393708&awadposition=&utmcampaignid=17960206281&awcreative=621971089574&awdevice=c&awkeyword=secretescapes&awloc_interest_ms=&awloc_physical_ms=9046034&awmatchtype=e&awplacement=&awtargetid=kwd-10850663191&saff=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_campaign=UKCPL+Brand+Pure+All-Destinations+Exact+Non-Member+Combined&utm_adgroup=%7Butmadgroup&gad_source=1&gad_campaignid=17960206281&gbraid=0AAAAADRSWbq11xhCocQYLdHLbfLcJOysE&gclid=CjwKCAiAybfLBhAjEiwAI0mBBvI7lS0W2uV9yqefx02pJmVgocgH3tEiC4Ft06qfu1y7uMYoRQNQ6BoCVoEQAvD_BwE&affiliateUrlString=goo-cpl-brand-uk')
;



SELECT *
FROM se.bi.session_metrics sm
WHERE sm.attributed_user_id = '67970160'
  AND sm.touch_start_tstamp::DATE = '2026-01-14'
;



SELECT *
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
WHERE sm.attributed_user_id = '67970160'
  AND sm.touch_start_tstamp::DATE = '2026-01-14'
;

-- create a list of sessions that had a booking last year via

SELECT
	sm.touch_id
FROM data_vault_mvp.bi.session_metrics sm
WHERE sm.touch_start_tstamp::DATE = '2025-01-14'
  AND sm.last_non_direct_touch_mkt_channel = 'Organic Search Non-Brand'
  AND sm.has_booking
EXCEPT
SELECT
	sm.touch_id
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
WHERE sm.touch_start_tstamp::DATE = '2025-01-14'
  AND sm.has_booking
;

USE WAREHOUSE pipe_xlarge
;
--prod
SELECT *
FROM data_vault_mvp.bi.session_metrics sm
WHERE sm.touch_id = 'a02a4a8fc922d446c5b8a0e4052c3ec5cd55efd6d93ee1cf406d0ea88846a1bd'
  AND sm.touch_start_tstamp::DATE = '2025-01-14'
;


--prod
SELECT *
FROM data_vault_mvp.bi.session_metrics sm
-- WHERE sm.touch_id = 'a02a4a8fc922d446c5b8a0e4052c3ec5cd55efd6d93ee1cf406d0ea88846a1bd'
WHERE sm.attributed_user_id = '57293686'
  AND sm.touch_start_tstamp::DATE = '2025-01-14'
;

-- dev
SELECT *
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
-- WHERE sm.touch_id = 'a02a4a8fc922d446c5b8a0e4052c3ec5cd55efd6d93ee1cf406d0ea88846a1bd'
WHERE sm.attributed_user_id = '57293686'
  AND sm.touch_start_tstamp::DATE = '2025-01-14';



SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification ssel
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash
	AND ses.event_tstamp::DATE = '2025-01-14'
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker murm
	ON ssel.event_hash = murm.event_hash
	AND murm.event_tstamp::DATE = '2025-01-14'
WHERE murm.attributed_user_id = '57293686'
  AND ssel.event_tstamp::DATE = '2025-01-14'
;


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification ssel
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash
	AND ses.event_tstamp::DATE = '2025-01-14'
INNER JOIN data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker murm
	ON ssel.event_hash = murm.event_hash
	AND murm.event_tstamp::DATE = '2025-01-14'
WHERE murm.attributed_user_id = '57293686'
  AND ssel.event_tstamp::DATE = '2025-01-14'
;\



Identifies the browser family used by the user during the session. This field
captures the primary browser type (e.g., Chrome, Safari, Firefox, Edge) and
includes browser engine variants (e.g., WebKit, Chromium-based browsers).

Browser family information is essential for diagnosing browser-specific technical
issues, prioritizing cross-browser compatibility testing, and optimizing user
experience for the most common browser types. It helps identify rendering issues,
JavaScript compatibility problems, and performance variations across different
browser engines.

Use this field to segment traffic by browser type, identify browser-specific
conversion rate differences, prioritize QA testing efforts, and ensure feature
compatibility with your user base's most popular browsers.