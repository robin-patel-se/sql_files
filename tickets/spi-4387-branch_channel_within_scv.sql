SELECT DISTINCT
	stai.channel

FROM se.data_pii.scv_touched_app_installs stai
;

-- first event for user = true , do we want to do this??
-- can we identify reinstall app events
-- there are campaigns targeting people who have already installed the app
-- theory being these are deeplinked ad campaigns

SELECT *
FROM se.data_pii.se_user_attributes sua
;


SELECT
	stmc.touch_mkt_channel,
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_experience LIKE 'native app %'
  AND stba.touch_start_tstamp >= CURRENT_DATE - 1
GROUP BY 1
;


SELECT *
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_experience LIKE 'native app %'
  AND stba.touch_start_tstamp >= CURRENT_DATE - 1
  AND stmc.touch_mkt_channel IS DISTINCT FROM 'Direct'
;


SELECT *
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_experience LIKE 'native app %'
  AND stba.touch_start_tstamp >= CURRENT_DATE - 1
  AND stmc.touch_landing_page IS NOT NULL
;
-- Campaign and channel branch events come from an install app campaign


SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_experience LIKE 'native app %' AND
	  stba.touch_start_tstamp >= CURRENT_DATE - 1
;


-- app install campaigns is the main headache


-- partner_name
-- campaign
-- channel


SELECT *
FROM se.data.scv_touched_app_installs stai
WHERE stai.channel IS NOT NULL
  AND stai.campaign IS NULL
;


SELECT DISTINCT
	stai.partner_name
FROM se.data.scv_touched_app_installs stai


SELECT *
FROM se.data_pii.scv_touched_app_installs stai
WHERE stai.event_tstamp >= CURRENT_DATE - 30
;

SELECT
	stai.partner_name,
	COUNT(*),
	MAX(stai.event_tstamp)
FROM se.data_pii.scv_touched_app_installs stai
WHERE stai.event_tstamp >= CURRENT_DATE - 365
GROUP BY 1
;


-- Google AdWords
-- Apple Search Ads

-- Apple Search Ads not seen since september 2023

SELECT
	stai.partner_name,
	COUNT(*)
FROM se.data_pii.scv_touched_app_installs stai
-- WHERE stai.event_tstamp >= CURRENT_DATE - 365
GROUP BY 1
;

SELECT
	event_hash,
	touch_id,
	event_tstamp,
	event_category,
	event_subcategory,
	app_install_context,
	idfv,
	aaid,
	attributed,
	event_name,
	existing_user,
	partner_name,
	campaign,
	campaign_type,
	channel,
	branch_ad_format,
-- 	brand, --device brand
-- 	model, --device model
-- 	os, --device os
-- 	os_version, --device os_version
-- 	sdk_version, --device sdk_version
	days_from_last_attributed_touch_to_event,
	deep_linked,
	first_event_for_user
FROM se.data_pii.scv_touched_app_installs stai


SELECT
	stai.partner_name,
	stai.channel,
	stai.branch_ad_format,
	stai.deep_linked,
	COUNT(*)
FROM se.data_pii.scv_touched_app_installs stai
GROUP BY ALL


-- understanding google adwords campaigns
SELECT DISTINCT
	stai.partner_name,
	stai.channel,
	stai.branch_ad_format,
	stai.deep_linked,
	stai.campaign,
	stai.campaign_type,
	COUNT(*) AS installs
FROM se.data_pii.scv_touched_app_installs stai
WHERE stai.partner_name = 'Google AdWords'
GROUP BY ALL
;


-- understanding apple search ads campaigns
SELECT DISTINCT
	stai.partner_name,
	stai.channel,
	stai.branch_ad_format,
	stai.deep_linked,
	stai.campaign,
	stai.campaign_type,
	COUNT(*) AS installs
FROM se.data_pii.scv_touched_app_installs stai
WHERE stai.partner_name = 'Apple Search Ads'
GROUP BY ALL
;


SELECT DISTINCT platform FROM se.data.scv_touch_basic_attributes stba