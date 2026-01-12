SELECT se_year                                                       AS year,
       se_week                                                       AS week,
       MAX(fcb.booking_completed_date)                               AS end_date,
       se.data.channel_category(stmc.touch_mkt_channel)              AS channel,
       COUNT(DISTINCT fcb.booking_id)                                AS bookings,
       COUNT(DISTINCT CASE
                          WHEN stba.touch_experience LIKE 'native app%' THEN fcb.booking_id
                          ELSE NULL END)                             AS app_bookings,
       COUNT(DISTINCT CASE
                          WHEN stba.touch_experience NOT LIKE 'native app%' THEN fcb.booking_id
                          ELSE NULL END)                             AS web_bookings,
       ROUND(SUM(fcb.margin_gross_of_toms_gbp_constant_currency), 2) AS margin,
       ROUND(SUM(CASE
                     WHEN stba.touch_experience LIKE 'native app%' THEN fcb.margin_gross_of_toms_gbp_constant_currency
                     ELSE NULL END),
             2)                                                      AS app_margin,
       ROUND(SUM(CASE
                     WHEN stba.touch_experience NOT LIKE 'native app%' THEN fcb.margin_gross_of_toms_gbp_constant_currency
                     ELSE NULL END),
             2)                                                      AS web_margin,
       SUM(fcb.no_nights)                                            AS nights,
       SUM(CASE
               WHEN stba.touch_experience LIKE 'native app%'
                   THEN fcb.no_nights
               ELSE NULL END)                                        AS app_nights,
       SUM(CASE
               WHEN stba.touch_experience NOT LIKE 'native app%'
                   THEN fcb.no_nights
               ELSE NULL END)                                        AS web_nights
FROM se.data.fact_complete_booking fcb --se.data.se_booking sb
         LEFT JOIN se.data.scv_touched_transactions stt
                   ON stt.booking_id = fcb.booking_id
         INNER JOIN se.data.scv_touch_attribution sta
                    ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc
                    ON sta.attributed_touch_id = stmc.touch_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
                   ON stba.touch_id = stmc.touch_id
         LEFT JOIN se.data.se_calendar se ON fcb.booking_completed_date = se.date_value
WHERE touch_affiliate_territory IN ('UK')
  AND stba.stitched_identity_type = 'se_user_id'
  AND EXTRACT(YEAR FROM booking_completed_date) >= 2019
GROUP BY 1, 2, 4;