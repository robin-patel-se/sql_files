-- Hi team, can somebody please get me a query to show active covid credit value, GBV, TID and Territory on packages cancelled due
-- to covid so combining, CANCELLED=TRUE, REFUNDED=TRUE, cancellation reason 'covid _19_cancellation' , where sale dimension is
-- either IHP, Catalogue or 3PP and supplier is SE and where H+ and includes dynamic flight. Maybe could also look at 'STATUS_PKG'
-- and 'Credits issued'

WITH covid_canx_bookings AS (
    SELECT fb.booking_id,
           COALESCE(ssa.product_configuration, t.product_configuration) AS product_configuration,
           fb.cancellation_reason,
           COALESCE(ssa.supplier_name, t.supplier_name)                 AS supplier_name
    FROM se.data.fact_booking fb
             LEFT JOIN se.data.se_sale_attributes ssa
                       ON fb.se_sale_id = ssa.se_sale_id AND ssa.product_configuration IS DISTINCT FROM 'WRD'
             LEFT JOIN se.data.tb_offer t ON fb.se_sale_id = t.se_sale_id
             LEFT JOIN se.data.se_booking sb ON fb.booking_id = sb.booking_id
    WHERE fb.booking_status_type = 'cancelled'
      AND LOWER(fb.cancellation_reason) LIKE 'covid_19%'
      AND ((ssa.product_configuration IN (
                                          'IHP - connected',
                                          'IHP - dynamic',
                                          'IHP - static',
                                          '3PP',
                                          'Hotel Plus'
        ) AND ssa.supplier_name LIKE 'Secret Escapes%')
        OR
           (
                   t.product_configuration = 'Catalogue'
                   AND
                   t.supplier_name LIKE 'Secret Escapes%'
               )
        )
)
SELECT sc.credit_id,
       sc.credit_date_created,
       sc.credit_expiration_date,
       sc.credit_last_updated,
       sc.credit_type,
       sc.is_cash_credit,
       sc.credit_never_expires,
       sc.credit_status,
       sc.credit_currency,
       sc.credit_amount,
       sc.cc_rate_to_gbp,
       sc.credit_amount_gbp,
       sc.cc_rate_to_gbp_constant_currency,
       sc.credit_amount_gbp_constant_currency,
       sc.shiro_user_id,
       sc.user_signup_tstamp,
       sc.user_original_territory,
       sc.user_current_territory,
       sc.redeemed_se_booking_id,
       sc.original_external_id,
       sc.original_external_reference_id,
       sc.original_voucher_id,
       sc.original_se_booking_id,
       sc.original_se_booking_tech_platform,
       ccb.product_configuration AS original_se_booking_product_configuration,
       ccb.cancellation_reason   AS original_se_booking_cancellation_reason,
       ccb.supplier_name         AS original_se_booking_supplier_name
FROM se.data.se_credit sc
         INNER JOIN covid_canx_bookings ccb ON sc.original_se_booking_id = ccb.booking_id
WHERE sc.credit_status = 'ACTIVE'
;


airflow backfill --start_date '2021-03-31 03:00:00' --end_date '2021-03-31 03:00:00' --task_regex '.*' dwh__cash_flow__stripe_refund__daily_at_03h00

DROP TABLE hygiene_vault_mvp.travelbird_mysql.orders_order;
DROP TABLE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order;

--2021-03-01 16:48:18.975281000
airflow backfill --start_date '2021-03-01 01:00:00' --end_date '2021-03-01 01:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__orders_order__daily_at_01h00
airflow clear --start_date '2021-03-01 01:00:00' --end_date '2021-03-01 01:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__orders_order__daily_at_01h00

SELECT *
FROM se.data.fact_complete_booking fb;

airflow backfill --start_date '2021-04-06 03:00:00' --end_date '2021-04-06 03:00:00' --task_regex '.*' dwh__transactional__booking__daily_at_03h00

SELECT *
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_lead_time_days IS NULL
  AND fcb.tech_platform = 'TRAVELBIRD';



WITH covid_canx_bookings AS (
    SELECT fb.booking_id,
           COALESCE(ssa.product_configuration, t.product_configuration) AS product_configuration,
           fb.cancellation_reason,
           COALESCE(ssa.supplier_name, t.supplier_name)                 AS supplier_name
    FROM se.data.fact_booking fb
             LEFT JOIN se.data.se_sale_attributes ssa
                       ON fb.se_sale_id = ssa.se_sale_id AND ssa.product_configuration IS DISTINCT FROM 'WRD'
             LEFT JOIN se.data.tb_offer t ON fb.se_sale_id = t.se_sale_id
             LEFT JOIN se.data.se_booking sb ON fb.booking_id = sb.booking_id
    WHERE fb.booking_status_type = 'cancelled'
      AND LOWER(fb.cancellation_reason) LIKE 'covid_19%'
      AND ((ssa.product_configuration IN (
                                          'IHP - connected',
                                          'IHP - dynamic',
                                          'IHP - static',
                                          '3PP'
        ) AND ssa.supplier_name LIKE 'Secret Escapes%')
        OR
           (ssa.product_configuration = 'Hotel Plus'
               AND
            sb.has_flights
               )
        OR
           (
                   t.product_configuration = 'Catalogue'
                   AND
                   t.supplier_name LIKE 'Secret Escapes%'
               )
        )
)
SELECT sc.credit_id,
       sc.credit_date_created,
       sc.credit_expiration_date,
       sc.credit_last_updated,
       sc.credit_type,
       sc.is_cash_credit,
       sc.credit_never_expires,
       sc.credit_status,
       sc.credit_currency,
       sc.credit_amount,
       sc.cc_rate_to_gbp,
       sc.credit_amount_gbp,
       sc.cc_rate_to_gbp_constant_currency,
       sc.credit_amount_gbp_constant_currency,
       sc.shiro_user_id,
       sc.user_signup_tstamp,
       sc.user_original_territory,
       sc.user_current_territory,
       sc.redeemed_se_booking_id,
       sc.original_external_id,
       sc.original_external_reference_id,
       sc.original_voucher_id,
       sc.original_se_booking_id,
       sc.original_se_booking_tech_platform,
       ccb.product_configuration AS original_se_booking_product_configuration,
       ccb.cancellation_reason   AS original_se_booking_cancellation_reason,
       ccb.supplier_name         AS original_se_booking_supplier_name
FROM se.data.se_credit sc
         INNER JOIN covid_canx_bookings ccb ON sc.original_se_booking_id = ccb.booking_id
WHERE sc.credit_status = 'ACTIVE';

