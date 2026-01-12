--incoming via paymentsinstallments
--outgoing via refunds


--refund events
CREATE OR REPLACE VIEW collab.travel_trust.tb_netsuite_refunds COPY GRANTS AS
(
SELECT COALESCE('PSP-' || psprr.id, 'CP-' || rrc.id, 'MBT-' || rrbt.id, 'RR-' || ors.id)    AS refund_id,
       ors.id                                                                               AS refund_request_id,
       o.id                                                                                 AS order_id,
       oo.id                                                                                AS offer_id,
       oo.product_line,
       cc.code                                                                              AS currency,
       o.price_total                                                                        AS total_booking_amount,
       o.vat                                                                                AS total_booking_vat,
       se.data.posa_territory_from_tb_site_id(oo.site_id)                                   AS territory,
--        IFF(ors.status = 20, 'Cancellation', 'Order Change') AS type_of_payment,
       ors.created_at_dts                                                                   AS refund_request_created_at_dts,
       ors.updated_at_dts                                                                   AS refund_request_updated_at_dts,
       ors.status                                                                           AS refund_request_status_id,
       se.data.refund_status_name_from_tb_refund_status(ors.status)                         AS refund_request_status,

       ors.initiator,
       ors.reason,
       ors.complaint_responsibility,
       ors.complaint_comment,
       ors.comments,
       ors.payment_method                                                                   AS refund_payment_method_id,
       se.data.refund_payment_method_name_from_tb_refund_payment_method(ors.payment_method) AS refund_payment_method,
       ors.amount                                                                           AS refund_total_amount,
       ors.calculated_amount                                                                AS refund_total_calculated_amount,
       ors.paid_at_dts                                                                      AS refund_paid_at_dts,
       ors.created_by_id,
       ors.finance_reviewer_id,
       ors.first_reviewer_id,
       ors.initial_request_id,
       ors.paid_by_id,
       ors.cancellation_reason,
       ors.approved_at_dts,
       -- psp refund payment info
       psprr.id                                                                             AS refund_psppayments_id,
       psprr.created_at_dts,
       psprr.updated_at_dts,
       psprr.payment_id,
       psprr.amount                                                                         AS psp_refund_request_amount,
       psprr.notes,
       -- psp refund info, actual psp refund data
       ops.id                                                                               AS psp_refund_id,
       ops.created_at_dts                                                                   AS psp_refund_created_at_dts,
       ops.updated_at_dts                                                                   AS psp_refund_updated_at_dts,
       ops.amount                                                                           AS psp_refund_amount,
       ops.status                                                                           AS psp_refund_status,
       PARSE_JSON(ops.raw)                                                                  AS raw_psp_refund,
       ops.error,
       ops.polymorphic_ctype_id                                                             AS psp_refund_polymorphic_ctype_id,

       -- coupon refund info
       rrc.id                                                                               AS refund_couponpayment_id,
       rrc.created_at_dts                                                                   AS coupon_refund_created_at_dts,
       rrc.updated_at_dts                                                                   AS coupon_refund_updated_at_dts,
       rrc.related_payment_id,
       --coupon info
       ccs.id                                                                               AS coupon_id,
       ccs.created_at_dts                                                                   AS coupon_created_at_dts,
       ccs.updated_at_dts                                                                   AS coupon_updated_at_dts,
       ccs.code                                                                             AS coupon_code,
       ccs.active                                                                           AS coupon_active,
       ccs.config_id                                                                        AS coupon_config_id,

       --manual refund info
       rrbt.id                                                                              AS refund_bankpayment_id,
       rrbt.iban                                                                            AS refund_bank_account_iban,              --PCI
       rrbt.bic                                                                             AS refund_bank_account_bic,               --PCI
       rrbt.holder_name                                                                     AS refund_bank_account_holder_name,       --PII
       rrbt.address                                                                         AS refund_bank_account_holder_address,    --PII
       rrbt.city                                                                            AS refund_bank_account_holder_city,       --PII
       rrbt.country                                                                         AS refund_bank_account_holder_country,    --PII
       rrbt.postal_code                                                                     AS refund_bank_account_holder_postal_code --PII

FROM data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequest_snapshot ors
         INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order o ON ors.order_id = o.id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.offers_offer_snapshot oo ON o.offer_id = oo.id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.common_sitesettings_snapshot cs ON o.site_id = cs.site_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.currency_currency_snapshot cc ON cs.site_currency_id = cc.id

    --refunds that occur via a psp
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequestpsppayments_snapshot psprr
                   ON ors.id = psprr.refund_request_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_psprefund_snapshot ops ON psprr.psp_refund_id = ops.id

    --refunds that occur via a coupon
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequestcoupons_snapshot rrc
                   ON ors.id = rrc.refund_request_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.coupons_coupon_snapshot ccs ON rrc.coupon_id = ccs.id

    --refunds that occur via a manual bank transfer
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequestbankaccountdetails_snapshot rrbt
                   ON ors.id = rrbt.refund_request_id
    );


-- --cancellation psp refund request
-- SELECT *
-- FROM data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequest_snapshot ors
--          INNER JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequestpsppayments_snapshot psprr
--                     ON ors.id = psprr.refund_request_id
-- WHERE ors.status IN (4, 6, 7, 10)
--   AND ors.payment_method = 1
--   AND ors.order_id = 21872662;
--
--
-- --cancellation coupon refund request
-- SELECT *
-- FROM data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequest_snapshot ors
--          INNER JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequestcoupons_snapshot crr
--                     ON ors.id = crr.refund_request_id
--          INNER JOIN data_vault_mvp.travelbird_mysql_snapshots.coupons_coupon_snapshot cc ON crr.coupon_id = cc.id
-- WHERE ors.status IN (4, 6, 7, 10)
--   AND ors.payment_method = 1;


------------------------------------------------------------------------------------------------------------------------
--order payments
CREATE OR REPLACE VIEW collab.travel_trust.tb_netsuite_payments COPY GRANTS AS
(
SELECT ops.id,
       ops.deposit_percentage,
       ops.initial_amount,
       ops.num_planned_payments,
       ops.num_change_payments,
       ops.order_id,
       CASE
           WHEN ois.id IS NOT NULL THEN 'Planned Payment'
           WHEN ois2.id IS NOT NULL THEN 'Change Payment' END             AS payment_instalment_type,
       COALESCE(ois.id, ois.id)                                           AS installment_id,
       COALESCE(ois.created_at_dts, ois.created_at_dts)                   AS created_at_dts,
       COALESCE(ois.updated_at_dts, ois.updated_at_dts)                   AS updated_at_dts,
       COALESCE(ois.amount, ois.amount)                                   AS instalment_amount,
       COALESCE(ois.session_validity, ois.session_validity)               AS instalment_session_validity,
       COALESCE(ois.payment_method_id, ois.payment_method_id)             AS instalment_payment_method_id,
       COALESCE(ois.currency_id, ois.currency_id)                         AS instalment_currency_id,
       COALESCE(ois.due_date, ois.due_date)                               AS instalment_due_date,
       COALESCE(ois.num_payment, ois.num_payment)                         AS instalment_num_payment,
       COALESCE(ois.real_due_date, ois.real_due_date)                     AS instalment_real_due_date,
       COALESCE(ois.payments_plan_id, ois.payments_plan_id)               AS instalment_payments_plan_id,
       COALESCE(ois.payments_plan_change_id, ois.payments_plan_change_id) AS instalment_payments_plan_change_id,
       COALESCE(ois.deferred, ois.deferred)                               AS instalment_deferred,
       op.id                                                              AS payment_id,
       op.type                                                            AS payment_type,
       op.source_id                                                       AS payment_source_id,
       op.state                                                           AS payment_state,
       op.created                                                         AS payment_created,
       op.updated                                                         AS payment_updated,
       op.amount                                                          AS payment_amount,
       op.remark                                                          AS payment_remark,
       op.currency_id                                                     AS payment_currency_id,
       cc.code                                                            AS currency,
       op.order_id                                                        AS payment_order_id,
       op.balance                                                         AS payment_balance,
       op.classification                                                  AS payment_classification,
       op.source_content_type_id                                          AS payment_source_content_type_id,
       op.planned_payment_id                                              AS payment_planned_payment_id,
       op.polymorphic_ctype_id                                            AS payment_polymorphic_ctype_id

FROM data_vault_mvp.travelbird_mysql_snapshots.orders_paymentsplan_snapshot ops
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_instalment_snapshot ois ON ops.id = ois.payments_plan_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_instalment_snapshot ois2
                   ON ops.id = ois.payments_plan_change_id
    -- join on the instalment
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_payment_snapshot op
                   ON COALESCE(ois.id, ois2.id) = op.planned_payment_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.currency_currency_snapshot cc ON op.currency_id = cc.id
    );



GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_refunds TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_refunds TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_refunds TO ROLE personal_role__tanithspinelli;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_refunds TO ROLE personal_role__ezraphilips;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_refunds TO ROLE personal_role__sebastianmaczka;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_refunds TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_refunds TO ROLE personal_role__barbarazacchino;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_refunds TO ROLE personal_role__ailiemcderment;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_refunds TO ROLE personal_role__andypauer;


GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_payments TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_payments TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_payments TO ROLE personal_role__tanithspinelli;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_payments TO ROLE personal_role__ezraphilips;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_payments TO ROLE personal_role__sebastianmaczka;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_payments TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_payments TO ROLE personal_role__barbarazacchino;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_payments TO ROLE personal_role__ailiemcderment;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_payments TO ROLE personal_role__andypauer;

GRANT USAGE ON SCHEMA collab.travel_trust TO ROLE personal_role__ailiemcderment;
GRANT USAGE ON SCHEMA collab.travel_trust TO ROLE personal_role__ezraphilips;
GRANT USAGE ON SCHEMA collab.travel_trust TO ROLE personal_role__sebastianmaczka;


SELECT fb.se_sale_id,
       tbo.product_configuration,
       tbo.product_type,
       tbo.posa_territory,
       tbo.posu_country,
       fb.booking_completed_date::date                                    AS date_booked,
       DATE_TRUNC(MONTH, fb.check_in_date)                                AS month_of_check_in,
       fb.booking_status_type,
       COUNT(*)                                                           AS no_of_bookings,
       SUM(fb.customer_total_price_gbp::DECIMAL(13, 2))                   AS total_customer_total_price_gbp,
       SUM(fb.margin_gross_of_toms_gbp::DECIMAL(13, 2))                   AS total_margin_gross_of_toms_gbp,
       SUM(fb.margin_gross_of_toms_eur_constant_currency::DECIMAL(13, 2)) AS total_margin_gross_of_toms_eur_cc
FROM se.data.fact_booking fb
         LEFT JOIN se.data.tb_offer tbo ON tbo.se_sale_id = fb.se_sale_id
WHERE fb.tech_platform = 'TRAVELBIRD'
  AND fb.booking_completed_date::date > '2020-01-01'
  AND fb.se_sale_id NOT LIKE 'TVL%'
  AND fb.booking_status_type != 'abandoned'
  AND fb.se_sale_id = 'A24680'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY date_booked DESC;

SELECT *
FROM se.data.tb_offer t
WHERE t.se_sale_id = 'A24680';
SELECT *
FROM se.data.tb_booking tb
WHERE tb.offer_id = 117011;


SELECT event_date,
       SUM(aer.impressions) AS impressions,
       SUM(aer.clicks)      AS clicks
FROM se.data.athena_email_reporting aer
GROUP BY 1;

SELECT event_date,
       SUM(a.impressions) AS impressions,
       SUM(a.clicks)      AS clicks
FROM data_vault_mvp.dwh.athena_email_reporting_20210216 a
GROUP BY 1;

SELECT *
FROM collab.travel_trust.tb_netsuite_refunds tnr
    QUALIFY COUNT(*) OVER (PARTITION BY tnr.refund_id) > 1;


SELECT * FROM collab.travel_trust.tb_netsuite_refunds tnr WHERE tnr.order_id = 21879460;

