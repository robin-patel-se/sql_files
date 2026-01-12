SELECT *
FROM latest_vault.iterable.email_send_log esl
WHERE esl.campaign_id = 8608899
;

SELECT
	*
FROM se.data.email_performance er
WHERE er.email_id = 'IT-8608899'
;


SELECT *
FROM se.data.scv_touch_marketing_channel stmc
	INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
WHERE stba.touch_start_tstamp >= '2023-12-01'
AND stmc.utm_campaign = '8608899';


SELECT
	*
FROM se.data.email_performance er
WHERE er.email_id = 'IT-3981175'
;

SELECT *
FROM se.data.scv_touch_marketing_channel stmc
	INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
WHERE stba.touch_start_tstamp >= '2023-12-01'
AND stmc.utm_campaign = '3981175';
