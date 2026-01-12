-- django and python

--order events is a snapshot of the order items aggregated
--need to bring in the order_events
--need to assess whether its possible to produce the same netsuite report and what language to replicate this in.

--changelog:
-- has different sheets with different data in it
-- total of 6 tabs in that sheet

------------------------------------------------------------------------------------------------------------------------

SELECT stmc.touch_id,
       stba.touch_start_tstamp,
       stmc.touch_mkt_channel,
       stmc.touch_landing_page,
       stmc.touch_hostname,
       stmc.touch_hostname_territory,
       stmc.attributed_user_id_hash,
       stmc.utm_campaign,
       stmc.utm_medium,
       stmc.utm_source,
       stmc.utm_term,
       stmc.utm_content,
       stmc.click_id,
       stmc.sub_affiliate_name,
       stmc.affiliate,
       stmc.touch_affiliate_territory,
       stmc.awadgroupid,
       stmc.awcampaignid,
       stmc.referrer_hostname,
       stmc.referrer_medium
FROM se.data.scv_touch_basic_attributes stba
         LEFT JOIN se.data.scv_touch_marketing_channel stmc

------------------------------------------------------------------------------------------------------------------------
WITH users_with_bookings AS (
    SELECT ua.original_affiliate_id   AS affiliate_original,
           ua.original_affiliate_name AS affiliate_original_name,
           ua.shiro_user_id,
           ua.signup_tstamp           AS signup_date,
           fcb.booking_id             AS booking_id
    FROM se.data.se_user_attributes ua
             JOIN se.data.fact_complete_booking fcb ON fcb.shiro_user_id = ua.shiro_user_id
    WHERE fcb.booking_completed_date >= '2020-08-01'
      AND fcb.booking_completed_date <= '2020-08-30'
),
     sess_bookings AS (
         SELECT stt.touch_id,
                stt.booking_id               AS booking_id,
                stt.event_tstamp             AS booking_date,
                fcb.margin_gross_of_toms_gbp AS margin
         FROM se.data.scv_touched_transactions stt
                  INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
         WHERE stt.event_tstamp >= '2020-08-01'
           AND stt.event_tstamp <= '2020-08-30'
     )
SELECT CAST(ao.signup_date AS DATE)                                                         AS signup_date,
       ao.affiliate_original_name,
       CAST(b.booking_date AS DATE)                                                         AS booking_date,
       b.booking_date::DATE - ao.signup_date::DATE                                          AS date_diff,
       CASE WHEN b.booking_date::DATE = ao.signup_date::DATE THEN 'New' ELSE 'Existing' END AS member_status,
       stmc.touch_affiliate_territory,
       stmc.affiliate,
       stmc.touch_mkt_channel,
       b.margin
FROM se.data.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         LEFT JOIN sess_bookings b ON stba.touch_id = b.touch_id
         INNER JOIN se.data.scv_touch_attribution a ON stba.touch_id = a.touch_id
         JOIN users_with_bookings ao ON ao.booking_id = b.booking_id
WHERE stba.touch_start_tstamp >= '2020-08-01'
  AND stmc.touch_affiliate_territory IN ('UK', 'DE', 'IT')
  AND a.attribution_model = 'last non direct'
;