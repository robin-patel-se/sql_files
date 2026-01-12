-- number of users in june
-- number of total transactions in june


SELECT DATE_TRUNC(MONTH, stba.touch_start_tstamp)                             AS month,
       COUNT(*)                                                               AS sessions,
       COUNT(DISTINCT stba.attributed_user_id_hash)                           AS users,
       COUNT(DISTINCT stt.booking_id)                                         AS bookings,
       COUNT(DISTINCT IFF(ds.product_type = 'Hotel', fcb.booking_id, NULL))   AS hotel_bookings,
       COUNT(DISTINCT IFF(ds.product_type = 'Package', fcb.booking_id, NULL)) AS package_bookings,
       bookings / sessions                                                    AS cvr
FROM se.data.scv_touch_basic_attributes stba
         LEFT JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         LEFT JOIN se.data.scv_touched_transactions stt ON stba.touch_id = stt.touch_id
         LEFT JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
         LEFT JOIN se.data.dim_sale ds ON fcb.sale_id = ds.se_sale_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp >= '2020-01-01'
  AND stba.touch_experience != 'native app'
  AND stmc.touch_affiliate_territory = 'UK'
GROUP BY 1
ORDER BY 1;

SELECT *
FROM se.data.dim_sale ds;

SELECT count(*)
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date >= '2020-01-01'
  AND fcb.booking_completed_date <= '2020-01-31';

SELECT count(*)
FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.booking_completed_date >= '2020-01-01'
  AND sb.booking_completed_date <= '2020-01-31'
  AND sb.booking_status = 'COMPLETE'
  AND sb.territory = 'UK';


SELECT *
FROM se.data.scv_touched_transactions stt;


SELECT DATE_TRUNC(MONTH, DATE('2020-07-22'));