SELECT ua.original_affiliate_name,
       voucher.voucher_id,
       voucher.voucher_date_created,
       voucher.voucher_type,
       voucher.voucher_expires_on,
       voucher.voucher_status,
       voucher.voucher_territory,
       credit.credit_territory,
       LISTAGG(credit.redeemed_se_booking_id, '/')  AS booking_ids,
       COUNT(credit.redeemed_se_booking_id)         AS no_bookings,
       LISTAGG(booking.booking_completed_date, '/') AS dates_booked
FROM se.data.se_voucher_model voucher
         LEFT JOIN se.data.se_credit_model credit ON credit.original_voucher_id = voucher.voucher_id
         LEFT JOIN se.data.se_booking booking ON credit.redeemed_se_booking_id = booking.booking_id
         LEFT JOIN se.data.se_user_attributes ua ON ua.shiro_user_id = voucher.gifter_id
WHERE voucher_expires_on::DATE > CURRENT_DATE
   OR (voucher_expires_on IS NULL
    AND
       voucher_date_created::DATE >=
       (CASE
            WHEN COALESCE(voucher_territory, 'XX') = 'DE' THEN CURRENT_DATE - 1095
            ELSE CURRENT_DATE - 365
           END))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

SELECT credit_status,
       sc.credit_status__o,
       COUNT(*),
       SUM(sc.credit_amount_gbp)
FROM se.data.se_credit sc
GROUP BY 1, 2