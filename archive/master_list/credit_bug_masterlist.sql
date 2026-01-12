SELECT *
FROM se.data.se_credit_model
WHERE user_id = '74012375';


SELECT booking_id,
       customer_email,
       territory_name,
       booking_status,
       booking_date,
       paid_with_deposit,
       payment_currency,
       sell_rate_eur,
       cr_credit_active,
       cr_credit_deleted,
       cr_credit_used,
       cr_credit_used_tb,
       cr_credit_refunded_cash,
       stripe_currency,
       stripe_refunded_amount,
       m_bacs_amount_in_customer_currency,
       cb_tb_lost_amount
FROM se.data_pii.master_tb_booking_list
WHERE travel_date >= '2020-03-18'
  AND travel_date <= '2020-12-31'
  AND (cr_credit_active + cr_credit_deleted + cr_credit_used + cr_credit_used_tb) > 0
  AND (stripe_refunded_amount <> 0 OR m_bacs_amount_in_customer_currency > 0 OR cb_tb_lost_amount > 0)
  AND shiro_user_id = 74012375;


SELECT *
FROM se.data.se_credit_model
WHERE user_id = '22461586';



SELECT booking_id,
       customer_email,
       territory_name,
       booking_status,
       booking_date,
       paid_with_deposit,
       payment_currency,
       sell_rate_eur,
       cr_credit_active,
       cr_credit_deleted,
       cr_credit_used,
       cr_credit_used_tb,
       cr_credit_refunded_cash,
       stripe_currency,
       stripe_refunded_amount,
       m_bacs_amount_in_customer_currency,
       cb_tb_lost_amount
FROM se.data_pii.master_tb_booking_list
WHERE travel_date >= '2020-03-18'
  AND travel_date <= '2020-12-31'
  AND (cr_credit_active + cr_credit_deleted + cr_credit_used + cr_credit_used_tb) > 0
  AND (stripe_refunded_amount <> 0 OR m_bacs_amount_in_customer_currency > 0 OR cb_tb_lost_amount > 0)
  AND shiro_user_id = 22461586;



SELECT booking_id,
       shiro_user_id,
       customer_email,
       territory_name,
       booking_status,
       booking_date,
       paid_with_deposit,
       payment_currency,
       sell_rate_eur,
       cr_credit_active,
       cr_credit_deleted,
       cr_credit_used,
       cr_credit_used_tb,
       cr_credit_refunded_cash,
       stripe_currency,
       stripe_refunded_amount,
       m_bacs_amount_in_customer_currency,
       cb_tb_lost_amount
FROM se.data_pii.master_tb_booking_list
WHERE booking_id IN
      ('TB-21883608',
       'TB-21891290',
       'TB-21875190',
       'TB-21888649',
       'TB-21875622',
       'TB-21880107',
       'TB-21894913',
       'TB-21884997',
       'TB-21884413',
       'TB-21888170',
       'TB-21873036');



SELECT
       scm.user_id,
       scm.original_booking_id,
       scm.original_external_id,
       scm.original_reservation_id,
       scm.original_se_booking_id,
       scm.original_voucher_id,
       scm.redeemed_booking_id,
       scm.redeemed_reservation_id,
       scm.redeemed_se_booking_id,
       scm.credit_status,
       scm.credit_currency,
       scm.credit_amount
FROM se.data.se_credit_model scm
WHERE scm.user_id IN ('72032328',
                      '35983816',
                      '63351990',
                      '39514096',
                      '31845428',
                      '59240668',
                      '57730436',
                      '19566778',
                      '27328658',
                      '23508103',
                      '72093353'
    )



