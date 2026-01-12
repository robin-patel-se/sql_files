SELECT
	sts.location,
	stmc.touch_affiliate_territory,
	CASE
		WHEN stmc.touch_affiliate_territory = 'DE' THEN 'de'
	END                                                                                       AS translation_language,
	IFF(sts.location IS NOT NULL, snowflake.cortex.translate(sts.location, 'de', 'en'), NULL) AS translated,
	COUNT(*)
FROM se.data.scv_touched_searches sts
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.triggered_by = 'user'
  AND NULLIF(sts.location, '') IS NOT NULL
  AND sts.event_tstamp >= CURRENT_DATE - 10
GROUP BY 1, 2
;


USE WAREHOUSE pipe_xlarge
;


SELECT * FROm latest_vault.iterable.email_send_log esl