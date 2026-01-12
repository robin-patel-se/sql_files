SELECT stmc.touch_mkt_channel,
       stmc.touch_affiliate_territory,
       stmc.utm_campaign,
       stmc.utm_content,
       stmc.utm_medium,
       stmc.utm_source,
       stmc.utm_term,
       stmc.click_id,
       stmc.affiliate,
       stmc.referrer_hostname,
       stmc.referrer_medium,
       COUNT(DISTINCT stba.touch_id)  AS sessions,
       COUNT(DISTINCT stt.booking_id) AS bookings
FROM se.data.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         LEFT JOIN se.data.scv_touched_transactions stt ON stba.touch_id = stt.touch_id
WHERE stba.touch_start_tstamp::DATE >= '2020-08-31' --week 36
  AND stba.touch_start_tstamp::DATE <= '2020-09-06'
  AND stmc.touch_mkt_channel = 'Other'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;