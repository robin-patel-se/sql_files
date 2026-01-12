SELECT *
FROM data_vault_mvp.chiasma_sql_server_snapshots.dim_customers_snapshot dcs

SELECT *
FROM se.data_pii.se_user_attributes sua;



--average money spent in the app or web
--last 6 months

SELECT fcb.device_platform,
       ROUND(SUM(fcb.customer_total_price_gbp), 2) AS total_customer_total_price_gbp,
       ROUND(SUM(fcb.margin_gross_of_toms_gbp), 2) AS total_margin_gross_of_toms_gbp,
       ROUND(AVG(fcb.customer_total_price_gbp), 2) AS avg_customer_total_price_gbp,
       ROUND(AVG(fcb.margin_gross_of_toms_gbp), 2) AS avg_margin_gross_of_toms_gbp,
       count(*)                                    AS bookings
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date >= DATEADD(MONTH, -6, current_date)
GROUP BY 1;



------------------------------------------------------------------------------------------------------------------------ 
--lucy
SELECT sbse.customername,
       sbse.usercountry,
       sbse.transactionid,
       sbse.booking_id,
       sbse.datebooked,
       sbse.checkin,
       sbse.nonights,
       sbse.rooms,
       sbse.offername, --doesn't always show board basis
       --Total paid to supplier (net)  - X
       sb.customer_total_price_cc,
       sb.customer_total_price_gbp,
       sb.customer_total_price_sc,
       sb.margin_gross_of_toms_sc,
       sb.offer_name

FROM se.data_pii.se_booking_summary_extended sbse
         LEFT JOIN se.data.se_booking sb  ON sbse.booking_id = sb.booking_id
LIMIT 1;

-- Full name of customer  ?
-- Customer country   ?
-- BOOKING_ID
-- BOOKING_COMPLETED_DATE
-- CHECK_IN_DATE
-- NO_NIGHTS
-- Room booked  ?
-- Board basis  ?
-- Total paid by customer  ?
-- Total paid to supplier (net)  ?
-- SE commission  ?

SELECT * FROm se.data.scv_touch_marketing_channel stmc WHERE stmc.touch_mkt_channel = 'Email - Triggers';

