SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 2
  AND stba.touch_has_booking;

--chose a sample of 3 sessions that have 20 events associated to them

SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 2
  AND stba.touch_has_booking
  AND stba.touch_id IN (
                        'e528935bdac1d2e6b2d399b67f30ae175806db772c2ba64b203ff810caa67157',
                        '5d9c12f8a85db45d07292669e5bf11213340ca8d38e47b282d04c0b33ffecf1e',
                        '2c99707db12219b932e7ef8ba2982020fa1afbd3eafd789ae86eb9135a95d9bd'
    );

USE WAREHOUSE pipe_xlarge;
--attach events for each of these sessions
SELECT *
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data_pii.scv_session_events_link ssel ON stba.touch_id = ssel.touch_id
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 2
  AND stba.touch_has_booking
  AND stba.touch_id IN (
                        'e528935bdac1d2e6b2d399b67f30ae175806db772c2ba64b203ff810caa67157',
                        '5d9c12f8a85db45d07292669e5bf11213340ca8d38e47b282d04c0b33ffecf1e',
                        '2c99707db12219b932e7ef8ba2982020fa1afbd3eafd789ae86eb9135a95d9bd'
    );