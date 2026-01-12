CREATE SCHEMA raw_vault_mvp_dev_robin.broadway_travel;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.broadway_travel.wrd_booking CLONE raw_vault_mvp.broadway_travel.wrd_booking;

self_describing_task --include 'staging/hygiene/broadway_travel/wrd_booking.py'  --method 'run' --start '2021-06-01 00:00:00' --end '2021-06-01 00:00:00'

SELECT *
FROM hygiene_vault_mvp_dev_robin.broadway_travel.wrd_booking;

SELECT MIN(loaded_at)
FROM raw_vault_mvp.broadway_travel.wrd_booking; --2021-06-11 16:21:05.157791000

CREATE SCHEMA raw_vault_mvp_dev_robin.jetline_travel;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.jetline_travel.wrd_booking CLONE raw_vault_mvp.jetline_travel.wrd_booking;

self_describing_task --include 'staging/hygiene/jetline_travel/wrd_booking.py'  --method 'run' --start '2021-06-07 00:00:00' --end '2021-06-07 00:00:00'


SELECT *
FROM hygiene_vault_mvp_dev_robin.jetline_travel.wrd_booking;

SELECT MIN(loaded_at)
FROM raw_vault_mvp.jetline_travel.wrd_booking;
--2021-06-07 16:17:41.759116000

------------------------------------------------------------------------------------------------------------------------
DROP TABLE hygiene_vault_mvp.broadway_travel.wrd_booking;
DROP TABLE hygiene_snapshot_vault_mvp.broadway_travel.wrd_booking;

airflow clear --start_date '2021-06-11 00:00:00' --end_date '2021-06-12 00:00:00' --task_regex '.*' hygiene_snapshots__broadway_travel__wrd_booking__daily_at_03h00
airflow backfill --start_date '2021-06-11 00:00:00' --end_date '2021-06-12 00:00:00' --task_regex '.*' hygiene_snapshots__broadway_travel__wrd_booking__daily_at_03h00

DROP TABLE hygiene_vault_mvp.jetline_travel.wrd_booking;
DROP TABLE hygiene_snapshot_vault_mvp.jetline_travel.wrd_booking;

airflow clear --start_date '2021-06-07 00:00:00' --end_date '2021-06-08 00:00:00' --task_regex '.*' hygiene_snapshots__jetline_travel__wrd_booking__daily_at_03h00
airflow backfill --start_date '2021-06-07 00:00:00' --end_date '2021-06-08 00:00:00' --task_regex '.*' hygiene_snapshots__jetline_travel__wrd_booking__daily_at_03h00 --reset_dagruns



SELECT wrd.booking_id,
       wrd.booking_id || '-' || wrd.se_sale_id                                     AS transaction_id,
       wrd.booking_status,
       CASE
           WHEN wrd.booking_status = 'BOOKED' THEN 'live'
           WHEN wrd.booking_status = 'CANCELLED' THEN 'cancelled'
           ELSE 'other'
           END                                                                     AS booking_status_type,
       wrd.se_sale_id,
       ua.shiro_user_id,
       wrd.check_in_date,
       wrd.check_out_date,
       DATEDIFF(DAY, wrd.booking_completed_date_time::DATE, wrd.check_in_date)     AS booking_lead_time_days,
       wrd.booking_created_date_time::DATE                                         AS booking_created_date,
       wrd.booking_completed_date_time::DATE                                       AS booking_completed_date,
       wrd.booking_completed_date_time                                             AS booking_completed_timestamp,
       NULL                                                                        AS booking_transaction_completed_date,
       wrd.customer_currency                                                       AS currency,
       wrd.gross_revenue_customer_currency                                         AS gross_revenue_cc,
       wrd.margin_gross_of_toms_customer_currency                                  AS margin_gross_of_toms_cc,
       wrd.gross_revenue_customer_currency * wrd.rate_to_gbp_from_cc               AS gross_revenue_gbp,
       wrd.gross_revenue_gbp_constant_currency,
       wrd.gross_revenue_eur_constant_currency,
       wrd.gross_revenue_customer_currency * wrd.rate_to_gbp_from_cc               AS customer_total_price_gbp,
       wrd.gross_revenue_gbp_constant_currency                                     AS customer_total_price_gbp_constant_currency,
       wrd.gross_revenue_customer_currency * wrd.rate_to_gbp_from_cc               AS gross_booking_value_gbp,
       NULL                                                                        AS commission_ex_vat_gbp,
       NULL                                                                        AS booking_fee_net_rate_gbp,
       NULL                                                                        AS payment_surcharge_net_rate_gbp,
       NULL                                                                        AS insurance_commission_gbp,
       wrd.margin_gross_of_toms_customer_currency * wrd.rate_to_gbp_from_cc        AS margin_gross_of_toms_gbp,
       wrd.margin_gross_of_toms_gbp_constant_currency,
       wrd.margin_gross_of_toms_eur_constant_currency,
       wrd.no_nights,
       wrd.adult_guests,
       wrd.child_guests,
       wrd.infant_guests,
       gross_revenue_gbp / wrd.no_nights                                           AS price_per_night,
       price_per_night / (wrd.adult_guests + wrd.child_guests + wrd.infant_guests) AS price_per_person_per_night,
       wrd.rooms,
       wrd.rooms * wrd.no_nights                                                   AS room_nights,
       device_platform,
       NULL                                                                        AS booking_full_payment_complete,
       IFF(booking_status = 'CANCELLED', last_updated_date_time, NULL)             AS cancellation_date,
       NULL                                                                        AS cancellation_reason,
       wrd.territory,
       wrd.travel_type,
       NULL                                                                        AS booking_includes_flight,
       'WRD-' || wrd_provider                                                      AS tech_platform
FROM data_vault_mvp_dev_kirsten.dwh.wrd_booking wrd
         LEFT JOIN data_vault_mvp.dwh.user_attributes ua ON ua.email = wrd.customer_identifier
;



SELECT *
FROM data_vault_mvp.dwh.fact_booking "FB"
WHERE tech_platform LIKE 'WRD%';



SELECT *
FROM se.data.fact_complete_booking
WHERE tech_platform LIKE 'WRD%';

SELECT GET_DDL('table', 'SE.DATA.fact_booking');

SELECT DISTINCT stmc.affiliate
FROM se.data.scv_touch_marketing_channel stmc
WHERE stmc.touch_hostname_territory = 'UK'
  AND stmc.touch_affiliate_territory = 'DE';


self_describing_task --include 'se/data/dwh/fact_booking.py'  --method 'run' --start '2021-06-29 00:00:00' --end '2021-06-29 00:00:00'


SELECT *
FROM se.data.fact_booking
WHERE tech_platform LIKE 'WRD%';

SELECT ttmi.transaction_tstamp::DATE,
       COUNT(*)
FROM se.finance.travel_trust_money_in ttmi
GROUP BY 1;



WITH money_in AS (
    SELECT ttmi.transaction_tstamp::DATE AS date,
           SUM(ttmi.transaction_amount)  AS money_in
    FROM se.finance.travel_trust_money_in ttmi
    WHERE ttmi.transaction_tstamp >= '2021-06-14'
    GROUP BY 1
),
     money_out AS (
         SELECT ttmo.transaction_tstamp::DATE AS date,
                SUM(ttmo.settlement_amount)   AS money_out
         FROM se.finance.travel_trust_money_out ttmo
         GROUP BY 1
     )
SELECT sc.date_value                                AS date,
       mi.money_in,
       SUM(money_in) OVER (ORDER BY sc.date_value)  AS money_in_cumulative,
       mo.money_out,
       SUM(money_out) OVER (ORDER BY sc.date_value) AS money_out_cumulative,
       money_in - money_out                         AS net_settlement_amount,
       money_in_cumulative - money_out_cumulative    AS net_settlement_amount_cumulative
FROM se.data.se_calendar sc
         LEFT JOIN money_in mi ON sc.date_value = mi.date
         LEFT JOIN money_out mo ON sc.date_value = mo.date
WHERE sc.date_value BETWEEN '2021-06-14' AND CURRENT_DATE
ORDER BY 1 ASC;
