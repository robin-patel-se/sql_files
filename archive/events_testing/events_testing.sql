-- Here's the events for review, I've checked in snowflake and they're in there
-- https://docs.google.com/spreadsheets/d/1MhTDnsI6GO5UssFRbDCm7XBnNQ3DwyWc39ApC-WO4ZA/edit#gid=1449444246
-- https://secretescapes.atlassian.net/browse/DEV-46427 - ticket

--  I used the qs param recon_test_2020_08_21, it's on the cs and SS events, in Snowflake and on the page visit events sent to Mongo

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.page_url LIKE '%recon_test_2020_08_21%'
  AND es.event_name IN ('page_view', 'screen_view');

--serverside test events
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.page_url LIKE '%recon_test%'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
ORDER BY 2;

--clientside test events
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.page_url LIKE '%recon_test%'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
ORDER BY 2;

--first test shows 4 events in snowplow (excluding the page pings). 2 cs and 2 ss
-- EVENT_HASH
-- 179a53d6932f2723b29ec97267933d8b07b8ebab49c771a73a96bb2ee50451a2
-- 87eb0a874a1bf854954246505b9de3114e6e09625b0dc97741e0136f801c863a
-- 0d2e600b2c1be6fca0458fa0fa35423f7965edb47322e5fb7e146b4f1df57215
-- d0c63df1688856ad7c6276f0f9282a1d040f94d8123fdf711a5ba1ee17716c51


USE WAREHOUSE pipe_large;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test%';

------------------------------------------------------------------------------------------------------------------------
-- Test 1
-- SPV, Web, SE, not logged in
USE WAREHOUSE pipe_large;

-- mongo test 1
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_21%'
  AND epv.sale_id = 'A13373'
  AND epv.user_id IS NULL
ORDER BY event_tstamp;

-- sp cs test 1
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_21%'
  AND es.se_sale_id = 'A13373'
  AND es.se_user_id IS NULL
ORDER BY 2;

-- sp ss test 1
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
  AND es.page_url LIKE '%recon_test_2020_08_21%'
  AND es.se_sale_id = 'A13373'
  AND es.se_user_id IS NULL
ORDER BY 2;

-- scv test 1
SELECT sts.page_url,
       sts.se_sale_id,
       stba.touch_logged_in,
       stba.attributed_user_id
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= '2020-08-20'
  AND sts.page_url =
      'https://www.secretescapes.com/chic-boutique-resort-on-the-smallest-flegrean-island-of-procida-fully-refundable-la-suite-boutique-hotel-and-spa-italy/sale-hotel?recon_test_2020_08_21'
;


------------------------------------------------------------------------------------------------------------------------
-- Test 2
-- SPV, Web, SE, logged in

-- mongo test 2
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_21%'
  AND epv.sale_id = 'A13373'
  AND epv.user_id IS NOT NULL
ORDER BY event_tstamp;

-- sp cs test 2
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_21%'
  AND es.se_sale_id = 'A13373'
  AND es.se_user_id IS NOT NULL
ORDER BY 2;

-- sp ss test 2
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
  AND es.page_url LIKE '%recon_test_2020_08_21%'
  AND es.se_sale_id = 'A13373'
  AND es.se_user_id IS NOT NULL
ORDER BY 2;

-- scv test 2
SELECT sts.page_url,
       sts.se_sale_id,
       stba.touch_logged_in,
       stba.attributed_user_id
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= '2020-08-20'
  AND sts.page_url =
      'https://www.secretescapes.com/chic-boutique-resort-on-the-smallest-flegrean-island-of-procida-fully-refundable-la-suite-boutique-hotel-and-spa-italy/sale-hotel?recon_test_2020_08_21'
;

------------------------------------------------------------------------------------------------------------------------
-- Test 31
-- Logged in user who signed up on a different domain to the one being accessed

-- mongo test 31
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_24%'
ORDER BY event_tstamp;

-- sp cs test 31
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_24%'
ORDER BY 2;

-- sp ss test 31
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
  AND es.page_url LIKE '%recon_test_2020_08_24%'
ORDER BY 2;

-- scv test 31
SELECT sts.page_url,
       sts.se_sale_id,
       stba.touch_logged_in,
       stba.attributed_user_id
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= '2020-08-20'
  AND sts.page_url =
      'https://www.secretescapes.com/venezianischer-palazzo-am-canal-grande-kostenfrei-stornierbar-casagredo-hotel-venedig-italien/sale-hotel?recon_test_2020_08_24'
;

------------------------------------------------------------------------------------------------------------------------
-- Test 7
-- SPV, Web, WL - UK, not logged in

-- mongo test 7
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_25%'
  AND epv.user_id IS NULL
ORDER BY event_tstamp;


-- sp cs test 7
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_25%'
  AND es.se_user_id IS NULL
ORDER BY 2;

-- sp ss test 7
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_25%'
  AND es.se_user_id IS NULL
ORDER BY 2;

------------------------------------------------------------------------------------------------------------------------
-- Test 8
-- SPV, Web, WL - UK, logged in

-- mongo test 8
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_25_correct%'
  AND epv.user_id IS NOT NULL
ORDER BY event_tstamp;

-- sp cs test 8
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_25_correct%'
  AND es.se_user_id IS NOT NULL
ORDER BY 2;

-- sp ss test 8
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
  AND es.page_url LIKE '%recon_test_2020_08_25_correct%'
  AND es.se_user_id IS NOT NULL
ORDER BY 2;

-- scv test 8
SELECT sts.page_url,
       sts.se_sale_id,
       stba.touch_logged_in,
       stba.attributed_user_id
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= '2020-08-20'
  AND sts.page_url =
      'https://www.hand-picked.telegraph.co.uk/picturesque-jersey-break-with-award-winning-dining-the-atlantic-hotel-st-brelade/sale?recon_test_2020_08_25_correct'
;

------------------------------------------------------------------------------------------------------------------------
-- Test 9
-- Robot SPV, Web, SE, not logged in

-- mongo test 9
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_28%'
  AND epv.user_id IS NULL
ORDER BY event_tstamp;

-- sp cs test 9
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_28%'
  AND es.se_user_id IS NULL
ORDER BY 2;

-- sp ss test 9
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
  AND es.page_url LIKE '%recon_test_2020_08_28%'
  AND es.se_user_id IS NULL
ORDER BY 2;

-- scv test 9
SELECT sts.page_url,
       sts.se_sale_id,
       stba.touch_logged_in,
       stba.attributed_user_id
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= '2020-08-20'
  AND sts.page_url =
      'https://www.secretescapes.com/chic-boutique-resort-on-the-smallest-flegrean-island-of-procida-fully-refundable-la-suite-boutique-hotel-and-spa-italy/sale-hotel?recon_test_2020_08_28'
;
--only returns the non robot event

------------------------------------------------------------------------------------------------------------------------
-- Test 10
-- Robot SPV, Web, SE, logged in

-- mongo test 10
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_28%'
  AND epv.user_id IS NOT NULL
ORDER BY event_tstamp;

-- sp cs test 10
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_28%'
  AND es.se_user_id IS NOT NULL
ORDER BY 2;


-- sp ss test 10
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
  AND es.page_url LIKE '%recon_test_2020_08_28%'
  AND es.se_user_id IS NOT NULL
ORDER BY 2;

-- scv test 10
SELECT sts.page_url,
       sts.se_sale_id,
       stba.touch_logged_in,
       stba.attributed_user_id
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= '2020-08-20'
  AND sts.page_url =
      'https://www.secretescapes.com/chic-boutique-resort-on-the-smallest-flegrean-island-of-procida-fully-refundable-la-suite-boutique-hotel-and-spa-italy/sale-hotel?recon_test_2020_08_28'
;
--only returns the non robot event

------------------------------------------------------------------------------------------------------------------------
-- Test 11
-- Expired SPV, Web, SE, not logged in

-- mongo test 11
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_25%'
  AND epv.user_id IS NULL
  AND epv.sale_id = '15835'
ORDER BY event_tstamp;

-- sp cs test 11
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_25%'
  AND es.se_user_id IS NULL
  AND es.se_sale_id = '15835'
ORDER BY 2;

-- sp ss test 11
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
  AND es.page_url LIKE '%recon_test_2020_08_25%'
  AND es.se_user_id IS NULL
  AND es.se_sale_id = '15835'
ORDER BY 2;

-- scv test 11
SELECT sts.event_hash,
       sts.page_url,
       sts.se_sale_id,
       stba.touch_logged_in,
       stba.attributed_user_id,
       es.se_user_id
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON sts.event_hash = es.event_hash
WHERE sts.event_tstamp >= '2020-08-20'
  AND sts.page_url =
      'https://www.secretescapes.com/carbis-bay-hotel-and-spa-st-ives-cornwall/sale?recon_test_2020_08_25'
;

------------------------------------------------------------------------------------------------------------------------
-- Test 12
-- Expired SPV, Web, SE, logged in

-- mongo test 12
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_25%'
  AND epv.user_id IS NOT NULL
  AND epv.sale_id = '15835'
ORDER BY event_tstamp;

-- sp cs test 12
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_25%'
  AND es.se_user_id IS NOT NULL
  AND es.se_sale_id = '15835'
ORDER BY 2;

-- sp ss test 12
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
  AND es.page_url LIKE '%recon_test_2020_08_25%'
  AND es.se_user_id IS NOT NULL
  AND es.se_sale_id = '15835'
ORDER BY 2;

-- scv test 12
SELECT sts.event_hash,
       sts.page_url,
       sts.se_sale_id,
       stba.touch_logged_in,
       stba.attributed_user_id,
       es.se_user_id
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON sts.event_hash = es.event_hash
WHERE sts.event_tstamp >= '2020-08-20'
  AND sts.page_url =
      'https://www.secretescapes.com/carbis-bay-hotel-and-spa-st-ives-cornwall/sale?recon_test_2020_08_25'
;

------------------------------------------------------------------------------------------------------------------------
-- Test 14
-- Redirect SPV, Web, SE, not logged in

-- mongo test 14
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*)', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_25%'
  AND epv.user_id IS NULL
ORDER BY event_tstamp;

-- sp cs test 14
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_25%'
  AND es.se_user_id IS NULL
ORDER BY 2;

SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE 'https://co.uk.sales.secretescapes.com%'
  AND es.page_url LIKE '%?%'
  AND es.se_user_id IS NULL
ORDER BY 2;

-- sp ss test 14
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
  AND es.page_url LIKE '%recon_test_2020_08_25%'
  AND es.se_user_id IS NULL
ORDER BY 2;

-- scv test 14
SELECT sts.event_hash,
       sts.page_url,
       sts.se_sale_id,
       stba.touch_logged_in,
       stba.attributed_user_id
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= '2020-08-20'
  AND sts.page_url =
      'https://co.uk.sales.secretescapes.com/115035/puglia-real-masseria-experience-italy/?recon_test_2020_08_25'
;

------------------------------------------------------------------------------------------------------------------------
-- Test 15
-- Redirect SPV, Web, SE, logged in

-- mongo test 15
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*)', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_25%'
  AND epv.user_id IS NOT NULL
ORDER BY event_tstamp;

-- sp cs test 15
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_25%'
  AND es.se_user_id IS NOT NULL
ORDER BY 2;

-- sp ss test 15
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
  AND es.page_url LIKE '%recon_test_2020_08_25%'
  AND es.se_user_id IS NOT NULL
ORDER BY 2;

-- scv test 15
SELECT sts.event_hash,
       sts.page_url,
       sts.se_sale_id,
       stba.touch_logged_in,
       stba.attributed_user_id
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= '2020-08-20'
  AND sts.page_url =
      'https://co.uk.sales.secretescapes.com/115035/puglia-real-masseria-experience-italy/?recon_test_2020_08_25'
;

------------------------------------------------------------------------------------------------------------------------
-- Test 17
-- Ad blocker SPV, Web, SE, not logged in

-- mongo test 17
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*)', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_25_correct%'
  AND epv.user_id IS NULL
ORDER BY event_tstamp;

-- sp cs test 17
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_25_correct%'
  AND es.se_user_id IS NULL
ORDER BY 2;

-- sp ss test 17
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
  AND es.page_url LIKE '%recon_test_2020_08_25_correct%'
  AND es.se_user_id IS NULL
ORDER BY 2;

-- scv test 17
SELECT sts.event_hash,
       sts.page_url,
       sts.se_sale_id,
       stba.touch_logged_in,
       stba.attributed_user_id,
       es.se_user_id
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON sts.event_hash = es.event_hash
WHERE sts.event_tstamp >= '2020-08-20'
  AND sts.page_url =
      'https://www.secretescapes.com/chic-boutique-resort-on-the-smallest-flegrean-island-of-procida-fully-refundable-la-suite-boutique-hotel-and-spa-italy/sale-hotel?recon_test_2020_08_25_correct'

------------------------------------------------------------------------------------------------------------------------
-- Test 18
-- Ad blocker SPV, Web, SE, logged in

-- mongo test 18
SELECT regexp_substr(epv.page_url, '\\\?(recon_test.*)', 1, 1, 'e') AS test_string,
       epv.event_id,
       epv.event_tstamp,
       epv.user_id,
       epv.url_string,
       epv.page_url,
       epv.sale_id,
       epv.referrer,
       epv.tr_id,
       epv.record__o,
       epv.user_id__o,
       epv.event_tstamp__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.events_page_visit epv
WHERE epv.event_tstamp >= '2020-08-20'
  AND epv.page_url LIKE '%recon_test_2020_08_25%'
  AND epv.user_id IS NOT NULL
ORDER BY event_tstamp;

-- sp cs test 18
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event = FALSE
  AND es.page_url LIKE '%recon_test_2020_08_25%'
  AND es.se_user_id IS NOT NULL
ORDER BY 2;

-- sp ss test 18
SELECT es.page_urlquery,
       es.event_tstamp,
       es.page_url,
       es.page_urlhost,
       es.page_urlpath,
       es.se_user_id,
       es.se_sale_id,
       es.page_referrer,
       es.useragent,
       es.is_robot_spider_event,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-08-20'
  AND es.event_name IN ('page_view', 'screen_view')
  AND es.is_server_side_event
  AND es.page_url LIKE '%recon_test_2020_08_25%'
  AND es.se_user_id IS NOT NULL
ORDER BY 2;


-- scv test 18
SELECT sts.event_hash,
       sts.page_url,
       sts.se_sale_id,
       stba.touch_logged_in,
       stba.attributed_user_id
FROM se.data.scv_touched_spvs AS sts
         LEFT JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= '2020-08-20'
  AND sts.page_url =
      'https://www.secretescapes.com/chic-boutique-resort-on-the-smallest-flegrean-island-of-procida-fully-refundable-la-suite-boutique-hotel-and-spa-italy/sale-hotel?recon_test_2020_08_25&affiliate=es'



SELECT es.event_tstamp,
       es.contexts_com_secretescapes_sale_page_context_1[0]['sale_id']::VARCHAR
FROM hygiene_vault_mvp.snowplow.event_stream es;


SELECT bs.record__o['user']['address1'],
       bs.record__o['user']['city']
       FROM hygiene_vault_mvp.cms_mongodb.booking_summary bs