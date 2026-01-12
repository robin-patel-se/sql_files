SELECT DATE_TRUNC(WEEK, stba.touch_start_tstamp) AS week,
       stba.touch_hostname,
       COUNT(*)
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stmc.touch_mkt_channel = 'Other'
  AND stba.touch_start_tstamp >= CURRENT_DATE - 7
GROUP BY 1, 2;



SELECT se.data.se_week(stba.touch_start_tstamp::DATE) AS week,
       stmc.touch_mkt_channel,
       COUNT(*)
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2021-01-01'
  AND stmc.touch_affiliate_territory = 'UK'
GROUP BY 1, 2;

SELECT se.data.se_week(stba.touch_start_tstamp::DATE) AS week,
       stmc.touch_mkt_channel,
       COUNT(*)
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_attribution sta
                    ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2021-01-01'
  AND stmc.touch_affiliate_territory = 'UK'
GROUP BY 1, 2;


--lc uk bookings
SELECT se.data.se_week(stt.event_tstamp::DATE) AS week,
       stmc.touch_mkt_channel,
       COUNT(*)
FROM se.data.scv_touched_transactions stt
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
WHERE stt.event_tstamp >= '2021-01-01'
  AND stmc.touch_affiliate_territory = 'UK'
GROUP BY 1, 2;

--lnd uk bookings
SELECT se.data.se_week(stt.event_tstamp::DATE) AS week,
       stmc.touch_mkt_channel,
       COUNT(*)
FROM se.data.scv_touched_transactions stt
         INNER JOIN se.data.scv_touch_attribution sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE stt.event_tstamp >= '2021-01-01'
  AND stmc.touch_affiliate_territory = 'UK'
GROUP BY 1, 2;



SELECT stmc.*
FROM se.data.scv_touched_transactions stt
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
WHERE stt.event_tstamp >= '2021-01-01'
  AND stmc.touch_affiliate_territory = 'UK'
  AND stmc.touch_mkt_channel = 'Other';


SELECT se.data.se_week(stt.event_tstamp::DATE) AS week,
       stmc.touch_hostname,
       COUNT(*)
FROM se.data.scv_touched_transactions stt
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
WHERE stt.event_tstamp >= '2021-01-01'
  AND stmc.touch_affiliate_territory = 'UK'
  AND stmc.touch_mkt_channel = 'Other'
GROUP BY 1, 2;


SELECT PARSE_URL(
               'https://www.lateluxury.com/sale/book-hotel?startDate=2021-8-11&endDate=2021-8-12&rooms=1&offerId=16846&saleId=21822&agentId=&numberOfAdults=2&selectedFlightJsonString=&flightIndex=&numberOfFlightResults=&maxAvailableRooms=10&singleResult=false&gce_pbf=&rateCodes=SBVRB&staffBooking=false')
           AS self_describing_task --include 'dv/dwh/master_booking_list/master_se_booking_list.py'  --method 'run' --start '2021-02-25 00:00:00' --end '2021-02-25 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list msbl;

