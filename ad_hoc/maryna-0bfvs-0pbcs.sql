SELECT *
FROM se.data.scv_touched_transactions stt
	INNER JOIN se.data_pii.scv_touch_basic_attributes stba
			   ON stt.touch_id = stba.touch_id AND stba.touch_start_tstamp >= '2025-01-01'
WHERE stt.event_tstamp >= '2025-01-01' AND
	  stt.touch_id IN ('4cbd89219aa512ab8ef4a83fec4347de355c237cc75cbe4c3d99ac5469379f84',
					   'eec3057b84340781e38e2cc209776bbc33f28dd48ffa1500c079c0bb9e55d886',
					   'e220defe9aa795305533490b2e21a0070eee751135df4ce9b7304d49f78fddad',
					   '2fe7b41c89903d95bcd0d4a4281fcd9107f0b6afc764bb7ed2c13752b2c28715')
;

/*

EVENT_HASH	TOUCH_ID	EVENT_TSTAMP	BOOKING_ID	ATTRIBUTED_USER_ID
4cbd89219aa512ab8ef4a83fec4347de355c237cc75cbe4c3d99ac5469379f84	4cbd89219aa512ab8ef4a83fec4347de355c237cc75cbe4c3d99ac5469379f84	2025-06-30 15:28:50.000000000	A23968553	75249212
8c910c70c06c90fd6d0bd16a7b05c0611d1541e8eba038a423b351b5c355a237	eec3057b84340781e38e2cc209776bbc33f28dd48ffa1500c079c0bb9e55d886	2025-06-30 15:16:19.000000000	A23968347	63565379
e220defe9aa795305533490b2e21a0070eee751135df4ce9b7304d49f78fddad	e220defe9aa795305533490b2e21a0070eee751135df4ce9b7304d49f78fddad	2025-06-30 15:58:31.000000000	A23968851	79906482
2fe7b41c89903d95bcd0d4a4281fcd9107f0b6afc764bb7ed2c13752b2c28715	2fe7b41c89903d95bcd0d4a4281fcd9107f0b6afc764bb7ed2c13752b2c28715	2025-01-24 17:19:24.000000000	A22092261	2368355
*/

------------------------------------------------------------------------------------------------------------------------
-- 4cbd89219aa512ab8ef4a83fec4347de355c237cc75cbe4c3d99ac5469379f84
SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= '2025-06-01' AND stba.attributed_user_id = '75249212'
;


SELECT *
FROM se.data.fact_booking fb
WHERE fb.booking_id IN (
	'A23968553'
	)
;


SELECT *
FROM se.data_pii.scv_session_events_link ssel
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON ssel.event_hash = ses.event_hash AND ses.event_tstamp::DATE = '2025-06-30'
WHERE ssel.event_tstamp::DATE >= '2025-06-30' AND ssel.attributed_user_id = '75249212'
;

-- this example the completed tstamp on the booking appears to be before the time we assume the web activity occurred.
------------------------------------------------------------------------------------------------------------------------
-- eec3057b84340781e38e2cc209776bbc33f28dd48ffa1500c079c0bb9e55d886
SELECT *
FROM se.data_pii.scv_session_events_link ssel
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON ssel.event_hash = ses.event_hash AND ses.event_tstamp::DATE = '2025-06-30'
WHERE ssel.event_tstamp::DATE >= '2025-06-30' AND ssel.attributed_user_id = '63565379'
;

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp::DATE = '2025-06-30'
  AND stba.attributed_user_id = '63565379'
;


------------------------------------------------------------------------------------------------------------------------
-- e220defe9aa795305533490b2e21a0070eee751135df4ce9b7304d49f78fddad
SELECT *
FROM se.data_pii.scv_session_events_link ssel
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON ssel.event_hash = ses.event_hash AND ses.event_tstamp::DATE = '2025-06-30'
WHERE ssel.event_tstamp::DATE >= '2025-06-30' AND ssel.attributed_user_id = '79906482'
;
;

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp::DATE = '2025-06-30'
  AND stba.attributed_user_id = '79906482'
;

------------------------------------------------------------------------------------------------------------------------
WITH
	metrics AS (
		SELECT

			(stba.num_bfvs = 0
				AND stba.num_pay_button_clicks = 0
				) AS booking_without_bfv_or_pbc,
			*
		FROM se.data.scv_touch_basic_attributes stba
		WHERE stba.num_trxs > 0
		  AND stba.touch_start_tstamp >= '2025-06-01'
		  AND stba.touch_se_brand = 'SE Brand'
	)
SELECT
	metrics.booking_without_bfv_or_pbc,
	COUNT(*)
FROM metrics
GROUP BY 1



GROUP BY
touch_basic_attributes.attributed_user_id,
PARSE_URL(touched_spvs.page_url, 1)['parameters']['utm_campaign']::VARCHAR,
PARSE_URL(touched_spvs.page_url, 1)['parameters']['messageId']::VARCHAR,
touched_spvs.se_sale_id