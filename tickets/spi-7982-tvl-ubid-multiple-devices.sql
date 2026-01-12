WITH
	events AS (
		SELECT
			ses.event_tstamp::DATE AS event_date,
			ses.unique_browser_id,
			ses.device_platform,
		FROM se.data_pii.scv_event_stream ses
		WHERE ses.se_brand = 'Travelist'
-- 		  AND ses.event_name = 'page_view'
		  AND ses.event_tstamp >= CURRENT_DATE - 100
	),
	agg_to_ubid AS (
		SELECT
			events.event_date,
			events.unique_browser_id,
			COUNT(DISTINCT events.device_platform) AS device_count
		FROM events
		GROUP BY events.event_date,
				 events.unique_browser_id
	)
SELECT
	agg_to_ubid.event_date,
	COUNT(DISTINCT agg_to_ubid.unique_browser_id) AS num_ubids,
	SUM(IFF(agg_to_ubid.device_count = 1, 1, 0))  AS num_ubids_with_1_device,
	SUM(IFF(agg_to_ubid.device_count > 1, 1, 0))  AS num_ubids_with_multiple_devices,
	num_ubids_with_multiple_devices / num_ubids   AS perc_multiple_device_ubids
FROM agg_to_ubid
GROUP BY agg_to_ubid.event_date
;

-- less than one percent of ubids in a day have multiple devices

------------------------------------------------------------------------------------------------------------------------

WITH
	events AS (
		SELECT
			ses.event_tstamp::DATE AS event_date,
			ses.unique_browser_id,
			ses.device_platform,
		FROM se.data_pii.scv_event_stream ses
		WHERE ses.se_brand = 'Travelist'
-- 		  AND ses.event_name = 'page_view'
		  AND ses.event_tstamp >= CURRENT_DATE - 100
	),
	agg_to_ubid AS (
		SELECT
			events.event_date,
			events.unique_browser_id,
			COUNT(DISTINCT events.device_platform) AS device_count
		FROM events
		GROUP BY events.event_date,
				 events.unique_browser_id
	)
SELECT *
FROM agg_to_ubid
WHERE agg_to_ubid.device_count > 1
;

-- look at one example of a ubid that has multiple devices
SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_brand = 'Travelist'
-- 		  AND ses.event_name = 'page_view'
  AND ses.event_tstamp::DATE = '2025-11-17'
  AND ses.unique_browser_id = 'aeb5849b-96c6-4824-ad73-4d224f942416';

-- trivago

-- found that there was a lot with useragent ='trivago-AdvertiserQualityAgent/1.0'
-- will remove these from query and recheck numbers



WITH
	events AS (
		SELECT
			ses.event_tstamp::DATE AS event_date,
			ses.unique_browser_id,
			ses.device_platform,
		FROM se.data_pii.scv_event_stream ses
		WHERE ses.se_brand = 'Travelist'
		  AND ses.event_name = 'page_view'
		  AND ses.useragent IS DISTINCT FROM 'trivago-AdvertiserQualityAgent/1.0'
		  AND ses.event_tstamp >= CURRENT_DATE - 100
	),
	agg_to_ubid AS (
		SELECT
			events.event_date,
			events.unique_browser_id,
			COUNT(DISTINCT events.device_platform) AS device_count
		FROM events
		GROUP BY events.event_date,
				 events.unique_browser_id
	)
SELECT
	agg_to_ubid.event_date,
	COUNT(DISTINCT agg_to_ubid.unique_browser_id) AS num_ubids,
	SUM(IFF(agg_to_ubid.device_count = 1, 1, 0))  AS num_ubids_with_1_device,
	SUM(IFF(agg_to_ubid.device_count > 1, 1, 0))  AS num_ubids_with_multiple_devices,
	num_ubids_with_multiple_devices / num_ubids   AS perc_multiple_device_ubids
FROM agg_to_ubid
GROUP BY agg_to_ubid.event_date
;

-- removing trivago does drop numbers but not dramatically


-- look at one example of a ubid that has multiple devices
SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_brand = 'Travelist'
-- 		  AND ses.event_name = 'page_view'
  AND ses.event_tstamp::DATE = '2025-11-12'
  AND ses.unique_browser_id = '869f8907-36ec-481f-ac0c-22a2f213e284';
-- bot: Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Googlebot/2.1; +http://www.google.com/bot.html) Chrome/141.0.7390.122 Safari/537.36



-- look at one example of a ubid that has multiple devices
SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_brand = 'Travelist'
-- 		  AND ses.event_name = 'page_view'
  AND ses.event_tstamp::DATE = '2025-11-10'
  AND ses.unique_browser_id = '88d284a0-4124-4992-8d96-9473aa5f445f';
-- mix of user and bot: Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Googlebot/2.1; +http://www.google.com/bot.html) Chrome/141.0.7390.122 Safari/537.36


-- look at one example of a ubid that has multiple devices
SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_brand = 'Travelist'
-- 		  AND ses.event_name = 'page_view'
  AND ses.event_tstamp::DATE = '2025-11-02'
  AND ses.unique_browser_id = 'ca7b38ee-f87c-4aa8-bc64-1f72479abbb4';
-- bot: Mozilla/5.0 (compatible; YandexRenderResourcesBot/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0


-- look at one example of a ubid that has multiple devices
SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_brand = 'Travelist'
-- 		  AND ses.event_name = 'page_view'
  AND ses.event_tstamp::DATE = '2025-10-29'
  AND ses.unique_browser_id = '6bf06d11-8bbd-46ba-9aa3-72caec3b36d9';
-- bot: AdsBot-Google (+http://www.google.com/adsbot.html)

-- look at one example of a ubid that has multiple devices
SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_brand = 'Travelist'
-- 		  AND ses.event_name = 'page_view'
  AND ses.event_tstamp::DATE = '2025-10-25'
  AND ses.unique_browser_id = 'a7571322-6252-4648-874f-a9a77d40333b';
-- bot: Mozilla/5.0 (compatible; YandexRenderResourcesBot/1.0; +http://yandex.com/bots) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0


SELECT
	ses.event_tstamp,
	ses.page_url,
	ses.page_referrer,
	ses.device_platform,
	ssel.touch_id,
	ssel.attributed_user_id,
	ses.useragent,
	ses.is_server_side_event
FROM se.data_pii.scv_event_stream ses
INNER JOIN se.data_pii.scv_session_events_link ssel
	ON ses.event_hash = ssel.event_hash
	AND ssel.event_tstamp::DATE >= '2025-11-04'
	AND ssel.event_tstamp::DATE <= '2025-11-05'
WHERE ses.se_brand = 'Travelist'
  AND ses.event_tstamp::DATE >= '2025-11-04'
  AND ses.event_tstamp::DATE <= '2025-11-05'
  AND ses.unique_browser_id = '140c594a-08cb-4066-9f9d-b3685dfceeaa'
ORDER BY ses.event_tstamp
