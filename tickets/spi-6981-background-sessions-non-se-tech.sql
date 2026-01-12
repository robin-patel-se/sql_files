/*
Why are some sessions flagged as background sessions but aren't in setech
*/


SELECT
	stmc.touch_mkt_channel,
	stmc.touch_affiliate_territory,
	stba.is_app_background_session
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_se_brand = 'SE Brand'
-- AND stba.is_app_background_session


SELECT
	stba.is_app_background_session,
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= '2024-10-15'
GROUP BY ALL


-- currently not finding any sessions that are in the is app background session category


