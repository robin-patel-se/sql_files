SELECT *
FROM se.data.se_credit_model scm;

SELECT credit_type, count(*)
FROM se.data.se_credit_model scm
GROUP BY 1;


--get list of desired gift vouchers

SELECT *
FROM se.data.se_credit_model scm
         LEFT JOIN se.data.se_voucher_model svm ON scm.original_voucher_id = svm.voucher_id
WHERE
--       scm.credit_type = 'GIFT_VOUCHER'
--   AND scm.credit_status != 'DELETED'
--   AND
svm.voucher_code IS NOT NULL;



SELECT svm.voucher_id,
       svm.voucher_code,
--        svm.order_code AS voucher_order_code,
       svm.gifter_id               AS voucher_gifter_id,
       svm.giftee_id               AS voucher_giftee_id,
--        svm.gifter_join_date AS voucher_gifter_join_date,
--        svm.giftee_join_date AS voucher_giftee_join_date,
--        svm.payment_id AS voucher_payment_id,
       svm.voucher_date_created,
       svm.voucher_expires_on,
       svm.voucher_last_updated,
       svm.voucher_redeemed_date,
       svm.voucher_type,
       svm.voucher_status,
       svm.physical_voucher        AS voucher_physical_voucher,
       svm.voucher_currency,
       svm.voucher_territory,
--        svm.payment_type AS voucher_payment_type,
       svm.payment_status          AS voucher_payment_status,
       svm.delivery_charge         AS voucher_delivery_charge,
       svm.payment_amount          AS voucher_payment_amount,

       scm.credit_id,
       scm.billing_id              AS credit_billing_id,
       scm.user_id                 AS credit_user_id,
       scm.user_join_date          AS credit_user_join_date,
       scm.original_booking_id     AS credit_original_booking_id,
       scm.original_external_id    AS credit_original_external_id,
       scm.original_reservation_id AS credit_original_reservation_id,
       scm.original_se_booking_id  AS credit_original_se_booking_id,
       scm.original_voucher_id     AS credit_original_voucher_id,
       scm.redeemed_booking_id     AS credit_redeemed_booking_id,
       scm.redeemed_reservation_id AS credit_redeemed_reservation_id,
       scm.redeemed_se_booking_id  AS credit_redeemed_se_booking_id,
       scm.credit_date_created,
       scm.credit_expires_on,
       scm.credit_last_updated,
       scm.credit_type,
       scm.credit_status,
       scm.credit_reason,
       scm.credit_territory,
       scm.credit_currency,
       scm.credit_amount,

--        fcb.booking_id,
--        fcb.booking_status,
--        fcb.sale_id,
--        fcb.shiro_user_id,
--        fcb.check_in_date,
--        fcb.check_out_date,
--        fcb.booking_lead_time_days,
--        fcb.booking_created_date,
--        fcb.booking_completed_date,
--        fcb.gross_booking_value_gbp,
--        fcb.commission_ex_vat_gbp,
--        fcb.booking_fee_net_rate_gbp,
--        fcb.payment_surcharge_net_rate_gbp,
--        fcb.insurance_commission_gbp,
--        fcb.margin_gross_of_toms_gbp,
--        fcb.no_nights,
--        fcb.adult_guests,
--        fcb.child_guests,
--        fcb.infant_guests,
--        fcb.price_per_night,
--        fcb.price_per_person_per_night,
--        fcb.tech_platform,
       ''
FROM se.data.se_voucher_model svm
         LEFT JOIN se.data.se_credit_model scm ON svm.voucher_id = scm.original_voucher_id
--          LEFT JOIN se.data.fact_complete_booking fcb ON scm.redeemed_se_booking_id = fcb.booking_id
WHERE svm.voucher_id = 147069;


--look at multiple redeemed vouchers
SELECT svm.voucher_id,
       count(*)
FROM se.data.se_credit_model scm
         INNER JOIN se.data.se_voucher_model svm ON scm.original_voucher_id = svm.voucher_id
GROUP BY 1
HAVING count(*) > 1
ORDER BY 2 DESC;

SELECT svm.giftee_id,
       count(*)
FROM se.data.se_voucher_model svm
GROUP BY 1
HAVING count(*) > 1
ORDER BY 2 DESC;

CREATE OR REPLACE VIEW collab.partnerships_pii.gift_voucher_report COPY GRANTS AS
(
WITH voucher_details AS (
    SELECT svm.voucher_id,
           svm.voucher_code,
--        svm.order_code AS voucher_order_code,
           svm.gifter_id        AS voucher_gifter_id,
           svm.giftee_id        AS voucher_giftee_id,
--        svm.gifter_join_date AS voucher_gifter_join_date,
--        svm.giftee_join_date AS voucher_giftee_join_date,
--        svm.payment_id AS voucher_payment_id,
           svm.voucher_date_created,
           svm.voucher_expires_on,
           svm.voucher_last_updated,
           svm.voucher_redeemed_date,
           svm.voucher_type,
           svm.voucher_status,
           svm.physical_voucher AS voucher_physical_voucher,
           svm.voucher_currency,
           svm.voucher_territory,
--        svm.payment_type AS voucher_payment_type,
           svm.payment_status   AS voucher_payment_status,
           svm.delivery_charge  AS voucher_delivery_charge,
           svm.payment_amount   AS voucher_payment_amount

    FROM se.data.se_voucher_model svm
--     WHERE svm.voucher_id = 139667 --just for testing
),
     credit_booking_info AS (
         --aggregate credit data to voucher level, multiple credits can be assigned to one voucher (eg. if someone
         --part redeems their voucher). So we aggregate credits, for both used and not used (cancelled bookings will still
         --appear as used credits).
         SELECT vd.voucher_id,
                COUNT(DISTINCT scm.credit_id)                                                  AS total_credits,
                COUNT(DISTINCT IFF(scm.credit_status = 'USED', scm.credit_id, NULL))           AS used_credits,
                --exclude cancelled bookings
                SUM(IFF(scm.credit_status = 'USED' AND fcb.booking_id IS NOT NULL, scm.credit_amount,
                        NULL))                                                                 AS used_credit_amount,
                COUNT(DISTINCT IFF(scm.credit_status = 'ACTIVE', scm.credit_id, NULL))         AS active_credits,
                SUM(IFF(scm.credit_status = 'ACTIVE', scm.credit_amount, NULL))                AS active_credit_amount,
                COUNT(DISTINCT fcb.booking_id)                                                 AS total_bookings,
                MIN(fcb.booking_completed_date)::DATE                                          AS first_booking_date,
                LISTAGG(fcb.booking_id, ', ')                                                  AS booking_list,
                SUM(fcb.gross_booking_value_gbp)                                               AS total_order_value,
                AVG(fcb.gross_booking_value_gbp)                                               AS average_order_value,
                COUNT(DISTINCT IFF(LOWER(ssa.product_type) = 'hotel', fcb.booking_id, NULL))   AS hotel_bookings,
                COUNT(DISTINCT IFF(LOWER(ssa.product_type) = 'package', fcb.booking_id, NULL)) AS package_bookings

         FROM se.data.se_credit_model scm
                  INNER JOIN voucher_details vd ON scm.original_voucher_id = vd.voucher_id
                  LEFT JOIN se.data.fact_complete_booking fcb ON scm.redeemed_se_booking_id = fcb.booking_id
                  LEFT JOIN se.data.se_sale_attributes ssa ON fcb.sale_id = ssa.se_sale_id
         GROUP BY 1
     ),
     user_details AS (
         SELECT sua.shiro_user_id,
                sua.email,
                sua.city,
                sua.country,
                sua.original_affiliate_name,
                sua.original_affiliate_territory,
                sua.signup_tstamp::DATE AS signup_date
         FROM se.data_pii.se_user_attributes sua
                  INNER JOIN voucher_details vd ON sua.shiro_user_id = vd.voucher_giftee_id
     ),
     voucher_lifetime_details AS (
--for each voucher, work out user's lifetime aggregate booking values prior to the voucher being redeemed.
         SELECT vd.voucher_id,
                COUNT(DISTINCT f.booking_id)    AS lifetime_bookings,
                SUM(f.margin_gross_of_toms_gbp) AS lifetime_margin,
                SUM(f.gross_booking_value_gbp)  AS lifetime_order_value
         FROM voucher_details vd
                  LEFT JOIN se.data.fact_complete_booking f
                            ON vd.voucher_giftee_id = f.shiro_user_id AND vd.voucher_redeemed_date > f.booking_completed_date
         GROUP BY 1
     )
SELECT vd.voucher_id,
       vd.voucher_code,
       vd.voucher_gifter_id,
       vd.voucher_giftee_id,
       vd.voucher_date_created,
       vd.voucher_expires_on,
       vd.voucher_last_updated,
       vd.voucher_redeemed_date,
       vd.voucher_type,
       vd.voucher_status,
       vd.voucher_physical_voucher,
       vd.voucher_currency,
       vd.voucher_territory,
       vd.voucher_payment_status,
       vd.voucher_delivery_charge,
       vd.voucher_payment_amount,
       sua.email                        AS giftee_email,
       sua.city                         AS giftee_city,
       sua.country                      AS giftee_country,
       sua.original_affiliate_name      AS giftee_original_affiliate_name,
       sua.original_affiliate_territory AS giftee_original_affiliate_territory,
       sua.signup_tstamp::DATE          AS giftee_signup_date,
       cbi.total_credits                AS cr_total_credits,
       cbi.used_credits                 AS cr_used_credits,
       cbi.used_credit_amount           AS cr_used_credit_amount,
       cbi.active_credits               AS cr_active_credits,
       cbi.active_credit_amount         AS cr_active_credit_amount,
       cbi.total_bookings               AS vb_total_bookings,
       cbi.first_booking_date           AS vb_first_booking_date,
       cbi.booking_list                 AS vb_booking_list,
       cbi.total_order_value            AS vb_total_order_value,
       cbi.average_order_value          AS vb_average_order_value,
       cbi.hotel_bookings               AS vb_hotel_bookings,
       cbi.package_bookings             AS vb_package_bookings,
       vld.lifetime_bookings,
       vld.lifetime_margin,
       vld.lifetime_order_value
FROM voucher_details vd
         LEFT JOIN se.data_pii.se_user_attributes sua ON vd.voucher_giftee_id = sua.shiro_user_id
         LEFT JOIN credit_booking_info cbi ON vd.voucher_id = cbi.voucher_id
         LEFT JOIN voucher_lifetime_details vld ON vd.voucher_id = vld.voucher_id
    );



SELECT gvr.voucher_id,
       gvr.voucher_code,
       gvr.voucher_gifter_id,
       gvr.voucher_giftee_id,
       gvr.voucher_date_created,
       gvr.voucher_expires_on,
       gvr.voucher_last_updated,
       gvr.voucher_redeemed_date,
       gvr.voucher_type,
       gvr.voucher_status,
       gvr.voucher_physical_voucher,
       gvr.voucher_currency,
       gvr.voucher_territory,
       gvr.voucher_payment_status,
       gvr.voucher_delivery_charge,
       gvr.voucher_payment_amount,
       gvr.giftee_email,
       gvr.giftee_city,
       gvr.giftee_country,
       gvr.giftee_original_affiliate_name,
       gvr.giftee_original_affiliate_territory,
       gvr.giftee_signup_date,
       gvr.cr_total_credits,
       gvr.cr_used_credits,
       gvr.cr_used_credit_amount,
       gvr.cr_active_credits,
       gvr.cr_active_credit_amount,
       gvr.vb_total_bookings,
       gvr.vb_first_booking_date,
       gvr.vb_booking_list,
       gvr.vb_total_order_value,
       gvr.vb_average_order_value,
       gvr.vb_hotel_bookings,
       gvr.vb_package_bookings,
       gvr.lifetime_bookings,
       gvr.lifetime_margin,
       gvr.lifetime_order_value
FROM collab.partnerships_pii.gift_voucher_report gvr
WHERE gvr.voucher_giftee_id = 6089264
ORDER BY gvr.voucher_date_created;

GRANT SELECT ON VIEW collab.partnerships_pii.gift_voucher_report TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.partnerships_pii.gift_voucher_report TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.partnerships_pii.gift_voucher_report TO ROLE personal_role__sheilawilliams;



SELECT territory,
       type,
       sale_dimension,
       check_in_date,
       DATEDIFF('DAY', current_date, check_in_date)                            AS dtd,
       CASE
           WHEN dtd BETWEEN 1 AND 14 THEN '1. 14 Days'
           WHEN dtd BETWEEN 15 AND 30 THEN '2. 30 Days'
           WHEN dtd BETWEEN 31 AND 60 THEN '3. 60 Days'
           WHEN dtd BETWEEN 61 AND 90 THEN '4. 90 Days'
           WHEN dtd BETWEEN 91 AND 180 THEN '5. 180 Days'
           WHEN dtd BETWEEN 181 AND 360 THEN '6. 360 Days'
           WHEN dtd > 360 THEN '7. Greater than 360 days'
           WHEN dtd < 1 THEN '8. Past Check in'
           ELSE '9. Not Defined' END                                           AS brackets,
       SUM(CASE WHEN refunded = FALSE AND cancelled = FALSE THEN 1 ELSE 0 END) AS live_bookings,
       SUM(CASE WHEN refunded = TRUE OR cancelled = TRUE THEN 1 ELSE 0 END)    AS canx_bookings,
       SUM(CASE WHEN lower(sf_view) LIKE ('%refundable%') THEN 1 ELSE 0 END)   AS salesforce_canx_request
FROM se.data.master_se_booking_list
WHERE date_booked::DATE >= '2020-05-01'
GROUP BY 1, 2, 3, 4, 5;

SELECT *
FROM se.data.se_room_type_rooms_and_rates srtrar
WHERE srtrar.hotel_name LIKE '4 MOODS%'
  AND date_trunc(MONTH, srtrar.rate_date)::DATE = '2020-09-01';

SELECT *
FROM se.data.se_hotel_rooms_and_rates shrar
WHERE shrar.hotel_name LIKE '4 MOODS%'
  AND date_trunc(MONTH, date)::DATE = '2020-09-01';

WITH hotel_by_day_lead_rate AS (
    --aggregate rates up to hotel by date for percent allocations calculation
    --cannot nest aggregations
    SELECT hs.code                          AS hotel_code,
           rtra.rate_currency,
           rtra.rate_date                   AS date,
           MIN(rtra.rt_lead_rate)           AS hotel_lead_rate,
           MIN(rtra.rt_available_lead_rate) AS hotel_available_lead_rate
    FROM se.data.se_room_type_rooms_and_rates rtra
             INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts
                        ON rtra.room_type_id = rts.id
             INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
    WHERE rtra.hotel_name LIKE '4 MOODS%'
      AND date_trunc(MONTH, date)::DATE = '2020-09-01'
    GROUP BY 1, 2, 3
)
SELECT hs.code                                        AS hotel_code,
       hs.name                                        AS hotel_name,
       rtra.rate_date                                 AS date,
       sc.day_name,
       rtra.rate_currency,
       hdlr.hotel_lead_rate,
--        SUM(rtra.rt_no_total_rooms)                    AS no_total_rooms,
       SUM(rtra.rt_no_available_rooms)                AS no_available_rooms,
--        SUM(rtra.rt_no_booked_rooms)                   AS no_booked_rooms,
--        SUM(rtra.rt_no_closedout_rooms)                AS no_closedout_rooms,
--        SUM(rtra.rt_no_rates)                          AS no_rates,
       MIN(rtra.rt_lead_rate)                         AS lead_rate,
--        MAX(rtra.rt_top_discount_percentage)           AS top_discount_percentage,

       SUM(IFF(rtra.rt_lead_rate = hdlr.hotel_lead_rate,
               rtra.rt_no_available_rooms, 0))        AS lead_rate_rooms,
--        SUM(IFF(rtra.rt_lead_rate = hdlr.hotel_lead_rate, rtra.rt_no_available_rooms, 0)) /
--        SUM(rtra.rt_no_total_rooms)                    AS percent_rooms_at_lead_rate,

       MIN(rtra.rt_available_lead_rate)               AS available_lead_rate,
       SUM(IFF(rtra.rt_available_lead_rate = hdlr.hotel_available_lead_rate,
               rtra.rt_available_lead_rate_rooms, 0)) AS available_lead_rate_rooms

FROM se.data.se_room_type_rooms_and_rates rtra
         INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts
                    ON rtra.room_type_id = rts.id
         INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
         LEFT JOIN hotel_by_day_lead_rate hdlr ON hs.code = hdlr.hotel_code AND rtra.rate_date = hdlr.date
         LEFT JOIN se.data.se_calendar sc ON rtra.rate_date = sc.date_value
WHERE rtra.hotel_name LIKE '4 MOODS%'
  AND date_trunc(MONTH, date)::DATE = '2020-09-01'
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY hotel_code, date;



SELECT * FROM se.data.se_hotel_rooms_and_rates shrar WHERE shrar.hotel_name LIKE 'Amadria%'
AND date_trunc(MONTH, date)::DATE = '2020-08-01';


                SELECT *
                -- but somehow hardcode values eg
                now() AS loaded_at,
                true AS is_deleted_in_source
                FROM {self.prod_ingest_table_ref}
                WHERE {pk} NOT IN (
                    SELECT {pk_source} AS {pk}
                    FROM {self.primary_keys_table_ref}
                )
                AND {self.is_deleted_column.expr} IS DISTINCT FROM true
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY {pk} ORDER BY loaded_at DESC
                ) = 1

SELECT * FROM snowflake.information_schema.columns WHERE table_name = {table_name};