--swiss re bonded bookings
--check number of swiss re bonded catalogue bookings

WITH swiss_re_bookings AS (
    --swiss re bonded bookings, approx 11.1K bookings
    SELECT msbl.booking_id,
           msbl.transaction_id,
           msbl.shiro_user_id,
           msbl.check_in_date,
           msbl.supplier,
           msbl.currency,
           msbl.sale_dimension,
           msbl.territory,
           msbl.refunded,
           msbl.cancelled,
           msbl.total_sell_rate_in_currency,
           msbl.total_sell_rate,
           msbl.customer_total_price,
           msbl.payment_type,
           msbl.credits_used,
           msbl.cr_credit_active,
           msbl.cr_credit_refunded_cash,
           msbl.cr_credit_used
    FROM se.data.master_se_booking_list msbl
    WHERE msbl.check_in_date BETWEEN '2020-03-01' AND '2020-12-31'
      AND (
            (msbl.sale_dimension = 'HotelPlus'
                AND msbl.dynamic_flight_booked = 'y')
            OR
            msbl.sale_dimension IN ('IHP - Static',
                                    'IHP - dynamic',
                                    'IHP - Connected',
                                    'Third Party Package'
                )
                AND msbl.supplier IN ('Secret Escapes GmbH', 'Secret Escapes Limited')
        )
      AND msbl.territory != 'UK'
),
     current_user_active_credit AS (
         --work out the current active credit amount for every user from the list
         --of swiss re bonded bookings
         SELECT sc.shiro_user_id,
                sc.credit_currency,
                SUM(IFF(sc.credit_status = 'ACTIVE' AND (sc.credit_expires_on >= CURRENT_DATE OR sc.credit_expires_on IS NULL),
                        sc.credit_amount, 0))                               AS users_total_active_credit,
                SUM(IFF(sc.credit_status = 'USED', sc.credit_amount, 0))    AS users_total_used_credit,
                SUM(IFF(sc.credit_status = 'DELETED', sc.credit_amount, 0)) AS users_total_deleted_credit,
                SUM(IFF(sc.credit_status = 'USED_TB', sc.credit_amount, 0)) AS users_total_used_tb_credit
         FROM se.data.se_credit sc
         WHERE sc.shiro_user_id IN (
             SELECT DISTINCT sr.shiro_user_id
             FROM swiss_re_bookings sr
         )
         GROUP BY 1, 2
     )

--attach current user credit figures onto the swiss re bonded booking list.
SELECT srb.booking_id,
       srb.transaction_id,
       srb.shiro_user_id,
       srb.check_in_date,
       srb.sale_dimension,
       srb.supplier,
       srb.refunded,
       srb.cancelled,
       srb.territory,
       srb.currency,
       srb.total_sell_rate_in_currency,
       srb.total_sell_rate, -- gbp equivalent
       srb.customer_total_price,
       srb.payment_type,
       srb.credits_used,
       srb.credits_used > 0 AS is_credit_redemption_booking,
       srb.cr_credit_used,
       srb.cr_credit_active,
       srb.cr_credit_refunded_cash,
       cuac.credit_currency,
       cuac.users_total_active_credit,
       cuac.users_total_used_credit,
       cuac.users_total_deleted_credit,
       cuac.users_total_used_tb_credit
FROM swiss_re_bookings srb
         LEFT JOIN current_user_active_credit cuac ON srb.shiro_user_id = cuac.shiro_user_id;
WHERE srb.booking_id = 'A693767';
;

SELECT *
FROM se.data.se_credit sc
WHERE sc.shiro_user_id = 69784584;


------------------------------------------------------------------------------------------------------------------------


WITH tb AS (
    SELECT transaction_id,
           REGEXP_SUBSTR(transaction_id, '-(.*)', 1, 1, 'e') AS trx_id_substr,
           booking_id,
           booking_status,
           cb_tb_lost_amount,
           cb_tb_pending_amount,
           cb_tb_won_amount,
           territory_name,
           customer_email,
           flight_sold_price
    FROM se.data_pii.master_tb_booking_list tbl
    WHERE travel_date >= '2020-03-01'
      AND travel_date <= '2020-12-31'
)
SELECT tb.transaction_id,
       tb.booking_id,
       tb.booking_status,
       tb.cb_tb_lost_amount,
       tb.cb_tb_pending_amount,
       cm.original_external_reference_id,
       cm.credit_status,
       cm.credit_amount,
       tb.cb_tb_won_amount,
       tb.territory_name,
       tb.customer_email,
       tb.flight_sold_price
FROM tb
         LEFT JOIN se.data.se_credit_model cm ON tb.trx_id_substr = cm.original_external_reference_id



SELECT DISTINCT credit_status
FROM se.data.se_credit sc;