SELECT msbl.transaction_id,
       msbl.booking_id,
       msbl.margin_gross_of_toms_gbp,
       msbl.shiro_user_id,
       msbl.country,
       msbl.type                                                   AS sale_type,
       msbl.date_booked,
       COALESCE(msbl.adjusted_check_in_date, msbl.check_in_date)   AS check_in_date,
       COALESCE(msbl.adjusted_check_out_date, msbl.check_out_date) AS check_out_date,
       msbl.currency,
       msbl.territory,
       msbl.customer_total_price,
       msbl.finance_net_amount_paid_fx,
       msbl.finance_net_amount_paid_gbp,
       msbl.finance_non_flight_spls_cash_held,
       msbl.finance_non_flight_vcc_held,
       msbl.finance_flight_refunds_received_gbp,
       msbl.finance_total_held_gbp,
       msbl.finance_perc_held,
       msbl.finance_flight_and_non_flight_components_held,
       msbl.finance_amount_inc_margin_adj,
       msbl.supplier,
       msbl.dynamic_flight_booked,
       msbl.carrier,
       CASE
           WHEN refunded OR cancelled
               THEN 'CANX'
           ELSE 'LIVE'
           END                                                     AS booking_status,
       msbl.worldpay_currency,
       COALESCE(msbl.worldpay_amount, 0)                           AS worldpay_refunded_amount,
       msbl.ratepay_currency,
       COALESCE(msbl.ratepay_amount, 0)                            AS ratepay_refunded_amount,
       NULL                                                        AS stripe_currency,
       0                                                           AS stripe_amount,
       CASE
           WHEN lower(msbl.cb_se_status) = 'lost'
               THEN msbl.cb_se_payment_amount
           ELSE 0
           END                                                     AS chargeback_amount_lost,
       CASE
           WHEN lower(msbl.cb_se_status) = 'won'
               THEN msbl.cb_se_payment_amount
           ELSE 0
           END                                                     AS chargeback_amount_won,
       CASE
           WHEN lower(msbl.cb_se_status) = 'pending'
               THEN msbl.cb_se_payment_amount
           ELSE 0
           END                                                     AS chargeback_amount_pending,

       COALESCE(msbl.m_baoc_amount_in_customer_currency, 0)        AS manual_baoc_refunded,
       (worldpay_refunded_amount
           + ratepay_refunded_amount
           + manual_baoc_refunded
           + chargeback_amount_lost
           )                                                       AS total_refunded_cash,
       COALESCE(cr_credit_active, 0)                               AS active_credit,
       COALESCE(cr_credit_deleted, 0)                              AS deleted_credit,
       COALESCE(cr_credit_used, 0)                                 AS used_credit,
       COALESCE(cr_credit_used_tb, 0)                              AS used_tb_credit,
       COALESCE(cr_credit_refunded_cash, 0)                        AS refunded_cash_credit,
       total_refunded_cash + active_credit                         AS total_cash_and_active_credit,
       msbl.sf_case_number,
       msbl.sf_case_owner_full_name,
       msbl.sf_subject,
       msbl.sf_opportunity_sale_id,
       msbl.sf_status                                              AS sf_status__o,
       msbl.sf_number_dup_cases_solved,
       msbl.sf_case_origin,
       msbl.sf_view,
       msbl.sf_booking_lookup_check_in_date,
       msbl.sf_booking_lookup_check_out_date,
       msbl.sf_requested_rebooking_date,
       msbl.sf_postponed_booking_request,
       msbl.sf_booking_lookup_store_id,
       msbl.sf_booking_lookup_supplier_territory,
       msbl.sf_contact_reason,
       msbl.sf_last_modified_by_full_name,
       msbl.sf_overbooking_rebooking_stage,
       msbl.sf_reason,
       msbl.sf_case_id,
       msbl.sf_date_time_opened,
       msbl.sf_case_name,
       msbl.sf_last_modified_date,
       msbl.sf_last_modified_by_case_overview,
       msbl.sf_priority_type,
       msbl.sf_covid19_member_resolution_oc,
       msbl.sf_case_overview_id,
       msbl.sf_case_thread_id,
       msbl.sf_priority,
       msbl.sf_se_status                                           AS sf_status,
       msbl.bk_cnx_refund_channel,
       msbl.bk_cnx_refund_type,
       msbl.bk_cnx_who_pays,
       msbl.cb_se_status                                           AS cb_status,
       msbl.car_flight_pnr,
       msbl.car_total_flights,

       msbl.car1_airline_name,
       msbl.car1_supplier,
       msbl.car1_overall_booking_status,
       msbl.car1_flight_booking_status,
       msbl.car1_cost_in_buying_currency,
       msbl.car1_cost_in_gbp,
       msbl.car1_member_refund_type,
       msbl.car1_booking_system,
       msbl.car1_mapping_updated,
       msbl.car1_mapping_flight_carrier,
       msbl.car1_mapping_type,
       msbl.car1_mapping_refund_type,
       msbl.car1_mapping_reported_refund_type,

       msbl.car2_airline_name,
       msbl.car2_supplier,
       msbl.car2_overall_booking_status,
       msbl.car2_flight_booking_status,
       msbl.car2_cost_in_buying_currency,
       msbl.car2_cost_in_gbp,
       msbl.car2_member_refund_type,
       msbl.car2_booking_system,
       msbl.car2_mapping_updated,
       msbl.car2_mapping_flight_carrier,
       msbl.car2_mapping_type,
       msbl.car2_mapping_refund_type,
       msbl.car2_mapping_reported_refund_type,

       msbl.car3_airline_name,
       msbl.car3_supplier,
       msbl.car3_overall_booking_status,
       msbl.car3_flight_booking_status,
       msbl.car3_cost_in_buying_currency,
       msbl.car3_cost_in_gbp,
       msbl.car3_member_refund_type,
       msbl.car3_booking_system,
       msbl.car3_mapping_updated,
       msbl.car3_mapping_flight_carrier,
       msbl.car3_mapping_type,
       msbl.car3_mapping_refund_type,
       msbl.car3_mapping_reported_refund_type,

       msbl.car4_airline_name,
       msbl.car4_supplier,
       msbl.car4_overall_booking_status,
       msbl.car4_flight_booking_status,
       msbl.car4_cost_in_buying_currency,
       msbl.car4_cost_in_gbp,
       msbl.car4_member_refund_type,
       msbl.car4_booking_system,
       msbl.car4_mapping_updated,
       msbl.car4_mapping_flight_carrier,
       msbl.car4_mapping_type,
       msbl.car4_mapping_refund_type,
       msbl.car4_mapping_reported_refund_type,

       'SECRET_ESCAPES'                                            AS tech_platform

FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list msbl

UNION

SELECT mtbl.transaction_id,
       mtbl.booking_id,
       mtbl.margin_gross_of_toms_gbp,
       mtbl.shiro_user_id,
       mtbl.country,
       mtbl.sale_type,
       mtbl.booking_date                                    AS date_booked,
       mtbl.travel_date                                     AS check_in_date,
       mtbl.return_date                                     AS check_out_date,
       mtbl.payment_currency                                AS currency,
       mtbl.territory_name                                  AS territory,
       mtbl.sold_price_in_currency,
       mtbl.finance_net_amount_paid_fx,
       mtbl.finance_net_amount_paid_gbp,
       mtbl.finance_non_flight_spls_cash_held,
       mtbl.finance_non_flight_vcc_held,
       mtbl.finance_flight_refunds_received_gbp,
       mtbl.finance_total_held_gbp,
       mtbl.finance_perc_held,
       mtbl.finance_flight_and_non_flight_components_held,
       mtbl.finance_amount_inc_margin_adj,
       mtbl.supplier,
       CASE
           WHEN lower(flight_transaction_state) IN ('complete', 'manual')
               THEN 'y'
           ELSE 'n'
           END                                              AS dynamic_flight_booked,
       mtbl.airline                                         AS carrier,
       mtbl.booking_status,
       NULL                                                 AS worldpay_currency,
       0                                                    AS worldpay_refunded_amount,
       NULL                                                 AS ratepay_currency,
       0                                                    AS ratepay_refunded_amount,
       mtbl.stripe_currency                                 AS stripe_currency,
       COALESCE(mtbl.stripe_refunded_amount, 0)             AS stripe_refunded_amount,
       COALESCE(mtbl.cb_tb_lost_amount, 0)                  AS chargeback_amount_lost,
       COALESCE(mtbl.cb_tb_won_amount, 0)                   AS chargeback_amount_won,
       COALESCE(mtbl.cb_tb_pending_amount, 0)               AS chargeback_amount_pending,

       COALESCE(mtbl.m_baoc_amount_in_customer_currency, 0) AS manual_baoc_refunded,
       (stripe_refunded_amount
           + manual_baoc_refunded
           + chargeback_amount_lost
           )                                                AS total_refunded_cash,
       COALESCE(cr_credit_active, 0)                        AS active_credit,
       COALESCE(cr_credit_deleted, 0)                       AS deleted_credit,
       COALESCE(cr_credit_used, 0)                          AS used_credit,
       COALESCE(cr_credit_used_tb, 0)                       AS used_tb_credit,
       COALESCE(cr_credit_refunded_cash, 0)                 AS refunded_cash_credit,
       total_refunded_cash + active_credit                  AS total_cash_and_active_credit,

       mtbl.sf_case_number,
       mtbl.sf_case_owner_full_name,
       mtbl.sf_subject,
       mtbl.sf_opportunity_sale_id,
       mtbl.sf_status                                       AS sf_status__o,
       mtbl.sf_number_dup_cases_solved,
       mtbl.sf_case_origin,
       mtbl.sf_view,
       mtbl.sf_booking_lookup_check_in_date,
       mtbl.sf_booking_lookup_check_out_date,
       mtbl.sf_requested_rebooking_date,
       mtbl.sf_postponed_booking_request,
       mtbl.sf_booking_lookup_store_id,
       mtbl.sf_booking_lookup_supplier_territory,
       mtbl.sf_contact_reason,
       mtbl.sf_last_modified_by_full_name,
       mtbl.sf_overbooking_rebooking_stage,
       mtbl.sf_reason,
       mtbl.sf_case_id,
       mtbl.sf_date_time_opened,
       mtbl.sf_case_name,
       mtbl.sf_last_modified_date,
       mtbl.sf_last_modified_by_case_overview,
       mtbl.sf_priority_type,
       mtbl.sf_covid19_member_resolution_oc,
       mtbl.sf_case_overview_id,
       mtbl.sf_case_thread_id,
       mtbl.sf_priority,
       mtbl.sf_tb_status                                    AS sf_status,
       NULL                                                 AS bk_cnx_refund_channel,
       NULL                                                 AS bk_cnx_refund_type,
       NULL                                                 AS bk_cnx_who_pays,
       NULL                                                 AS cb_status,
       mtbl.car_flight_pnr,
       mtbl.car_total_flights,

       mtbl.car1_airline_name,
       mtbl.car1_supplier,
       mtbl.car1_overall_booking_status,
       mtbl.car1_flight_booking_status,
       mtbl.car1_cost_in_buying_currency,
       mtbl.car1_cost_in_gbp,
       mtbl.car1_member_refund_type,
       mtbl.car1_booking_system,
       mtbl.car1_mapping_updated,
       mtbl.car1_mapping_flight_carrier,
       mtbl.car1_mapping_type,
       mtbl.car1_mapping_refund_type,
       mtbl.car1_mapping_reported_refund_type,

       mtbl.car2_airline_name,
       mtbl.car2_supplier,
       mtbl.car2_overall_booking_status,
       mtbl.car2_flight_booking_status,
       mtbl.car2_cost_in_buying_currency,
       mtbl.car2_cost_in_gbp,
       mtbl.car2_member_refund_type,
       mtbl.car2_booking_system,
       mtbl.car2_mapping_updated,
       mtbl.car2_mapping_flight_carrier,
       mtbl.car2_mapping_type,
       mtbl.car2_mapping_refund_type,
       mtbl.car2_mapping_reported_refund_type,

       mtbl.car3_airline_name,
       mtbl.car3_supplier,
       mtbl.car3_overall_booking_status,
       mtbl.car3_flight_booking_status,
       mtbl.car3_cost_in_buying_currency,
       mtbl.car3_cost_in_gbp,
       mtbl.car3_member_refund_type,
       mtbl.car3_booking_system,
       mtbl.car3_mapping_updated,
       mtbl.car3_mapping_flight_carrier,
       mtbl.car3_mapping_type,
       mtbl.car3_mapping_refund_type,
       mtbl.car3_mapping_reported_refund_type,

       mtbl.car4_airline_name,
       mtbl.car4_supplier,
       mtbl.car4_overall_booking_status,
       mtbl.car4_flight_booking_status,
       mtbl.car4_cost_in_buying_currency,
       mtbl.car4_cost_in_gbp,
       mtbl.car4_member_refund_type,
       mtbl.car4_booking_system,
       mtbl.car4_mapping_updated,
       mtbl.car4_mapping_flight_carrier,
       mtbl.car4_mapping_type,
       mtbl.car4_mapping_refund_type,
       mtbl.car4_mapping_reported_refund_type,

       'TRAVELBIRD'                                         AS tech_platform
FROM data_vault_mvp_dev_robin.dwh.master_tb_booking_list mtbl;



self_describing_task --include 'dv/dwh/transactional/tb_booking'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/fact_complete_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/tb_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb;


SELECT sb.shiro_user_id
FROM data_vault_mvp_dev_robin.dwh.se_booking sb;

self_describing_task --include 'dv/dwh/master_booking_list/master_tb_booking_list'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_tb_booking_list mtbl;
SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list mtbl;

SELECT *
FROM se.data.se_credit_model scm;

self_describing_task --include 'se/data_pii/master_all_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM se_dev_robin.data_pii.master_all_booking_list
WHERE tech_platform = 'TRAVELBIRD';


self_describing_task --include 'se/data_pii/master_se_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data_pii/master_se_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data_pii/master_tb_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM se_dev_robin.data_pii.master_se_booking_list;

SELECT *
FROM se_dev_robin.data_pii.master_tb_booking_list;



self_describing_task --include 'se/data_pii/master_all_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/master_se_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/master_tb_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT *
FROM se.data_pii.master_se_booking_list;

SELECT *
FROM se.data_pii.master_tb_booking_list;

SELECT *
FROM se.data_pii.master_all_booking_list;

SELECT *
FROM se.data_pii.master_all_booking_list mabl;


SELECT *
FROM data_vault_mvp.travelbird_cms.order ops;
SELECT *
FROM data_vault_mvp.travelbird_cms.orders_person_snapshot ops;

self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'dv/dwh/master_booking_list/master_tb_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'dv/dwh/master_booking_list/master_se_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data_pii/master_se_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data_pii/master_tb_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data_pii/master_all_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/master_se_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/master_tb_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/master_all_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT *
FROM se.data_pii.master_se_booking_list msbl;

self_describing_task --include 'staging/hygiene/snowplow/events'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT DISTINCT ops.name
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order oo
         LEFT JOIN data_vault_mvp.travelbird_cms.orders_paymentmethod_snapshot ops
                   ON oo.payment_method_id = ops.id;

SELECT DISTINCT paymenttype
FROM se.data_pii.se_booking_summary_extended sbse;


SELECT sa.company_name,
       sa.posu_country,
       LISTAGG(DISTINCT (oc.name), ' - ')
FROM se.data.se_sale_attributes AS sa
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot AS oc ON sa.contractor_id = oc.id
WHERE lower(sa.product_type) = 'hotel'
GROUP BY 1, 2
ORDER BY sa.posu_country ASC


SELECT sa.company_name,
       sa.posu_country,
       sa.date_created,
       sa.start_date,
       oc.name
FROM se.data.se_sale_attributes AS sa
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot AS oc ON sa.contractor_id = oc.id
WHERE lower(sa.product_type) = 'hotel'
  AND sa.company_name = 'HavsVidden'
ORDER BY sa.posu_country ASC;


SELECT ssa.se_sale_id,
       ssa.contractor_id,
       LAST_VALUE(ssa.contractor_id) OVER (PARTITION BY ssa.company_id ORDER BY ssa.date_created) AS last_contractor
FROM se.data.se_sale_attributes ssa
WHERE ssa.company_name = 'HavsVidden';

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss;

SELECT sa.company_name,
       sa.product_configuration,
       sa.posu_country,
       LISTAGG(DISTINCT (oc.name), ' - ')
FROM data_vault_mvp_dev_robin.dwh.se_sale AS sa
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot AS oc ON sa.current_contractor_id = oc.id
WHERE lower(sa.product_type) = 'hotel'
GROUP BY 1, 2, 3
ORDER BY sa.posu_country ASC;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ssa
WHERE ssa.company_name = 'Living Hotel Kaiser Franz Joseph'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking
WHERE se_booking.rebooked;

SELECT *
FROM data_vault_mvp.dwh.se_sale ss


------------------------------------------------------------------------------------------------------------------------

WITH last_attributes AS
         (
             SELECT ssa.se_sale_id,
                    ssa.contractor_id,
                    ssa.company_name,
                    ssa.posu_country,
                    ssa.product_type,
                    LAST_VALUE(
                            ssa.contractor_id)
                            OVER (PARTITION BY ssa.company_id ORDER BY ssa.date_created) AS last_contractor
             FROM se.data.se_sale_attributes ssa
         )
SELECT sa.company_name,
       sa.posu_country,
       sa.last_contractor,
       oc.name
FROM last_attributes AS sa
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot AS oc ON sa.last_contractor = oc.id
WHERE lower(sa.product_type) = 'hotel'
ORDER BY sa.posu_country ASC

------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM data_vault_mvp.dwh.tb_booking tb;
SELECT *
FROM se_dev_robin.data_pii.master_tb_booking_list mtbl;
SELECT mtbl.unique_transaction_reference
FROM se_dev_robin.data_pii.master_se_booking_list mtbl;
SELECT *
FROM se_dev_robin.data_pii.master_all_booking_list mtbl;

SELECT filename,
       LOWER(SPLIT_PART(filename, '/', -1)) AS filename2,
       LENGTH(filename2)
FROM hygiene_vault_mvp.worldpay.transaction_summary
WHERE LENGTH(filename2) > 46;

SELECT *
FROM se.data_pii.master_all_booking_list mabl;

SELECT booking_id
FROM data_vault_mvp.dwh.master_tb_booking_list mtbl
GROUP BY 1
HAVING count(*) > 1;

SELECT *
FROM data_vault_mvp.dwh.master_tb_booking_list mtbl
WHERE mtbl.booking_id = 'TB-21880768'

SELECT *
FROM raw_vault_mvp.sfsc.rebooking_request_cases_pkg rrcp
WHERE rrcp.case_number IN (2689505, 2687653);

UPDATE raw_vault_mvp.sfsc.rebooking_request_cases_pkg rrcp
SET rrcp.booking_id = 'A4365-SED-21880768'
WHERE rrcp.case_number = 2689505;


SELECT id,
       ss.name,
       ss.customer_support_email
FROM data_vault_mvp.cms_mysql_snapshots.supplier_snapshot ss;

SELECT *
FROM data_vault_mvp.dwh.master_tb_booking_list mtbl
WHERE mtbl.lifetime_bookings > 1;


SELECT customer_id,
       count(*)
FROM data_vault_mvp.dwh.tb_booking tb
WHERE payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE')
GROUP BY 1
HAVING count(*) > 1;


SELECT oos.customer_id,
       count(*)
FROM data_vault_mvp.travelbird_cms.orders_order_snapshot oos
GROUP BY 1
HAVING count(*) > 1;

SELECT ops.email, count(*)
FROM data_vault_mvp.travelbird_cms.orders_person_snapshot ops
GROUP BY 1
HAVING count(*) > 1;


SELECT *
FROM data_vault_mvp.travelbird_cms.orders_person_snapshot ops;
SELECT *
FROM raw_vault_mvp.travelbird_mysql.;

SELECT ops.email,
       COUNT(1)                             AS lifetime_bookings,
       SUM(tb.margin_gbp)::DOUBLE PRECISION AS lifetime_margin_gbp
FROM data_vault_mvp.dwh.tb_booking tb
         LEFT JOIN data_vault_mvp.travelbird_cms.orders_person_snapshot ops ON tb.customer_id = ops.id
WHERE payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE')
GROUP BY 1;

self_describing_task --include 'dv/dwh/master_booking_list/master_tb_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT * FROM data_vault_mvp_dev_robin.dwh.master_tb_booking_list mtbl
WHERE mtbl.lifetime_bookings >1

------------------------------------------------------------------------------------------------------------------------

SELECT cs.order_code,
       cs.booking_id,
       hcs.booking_id,
       sb.booking_id
FROM raw_vault_mvp.finance_gsheets.chargebacks_se cs
    LEFT JOIN data_vault_mvp.dwh.se_booking sb ON cs.order_code = sb.unique_transaction_reference
         LEFT JOIN hygiene_snapshot_vault_mvp.finance_gsheets.chargebacks_se hcs ON cs.order_code = hcs.order_code
WHERE sb.booking_id IS NULL;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.finance_gsheets.chargebacks_se clone raw_vault_mvp.finance_gsheets.chargebacks_se;

self_describing_task --include 'staging/hygiene/finance_gsheets/chargebacks_se.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
biapp/task_catalogue/staging/hygiene/finance_gsheets/chargebacks_se.py
