-- Query from tanith on Stripe Sigma
/*- ALL_TRANSACTIONS_DAILY
-- On a daily schedule, this report will include all of the
-- previous day's charges, disputes and refunds.
-- CHARGES TABLE
WITH cha AS (
    SELECT id                          AS id,
           amount/100.00 							 AS amount,
           currency                    AS currency,
           created                     AS date_created,
           status                      AS status,
           'charge'                    AS type
    FROM charges
),
	   -- DISPUTES TABLE
     dis AS (
         SELECT id                           AS id,
                amount/100.00								 AS amount,
                currency                     AS currency,
                created                     AS date_created,
                status                       AS status,
                'dispute'                    AS type
         FROM disputes
     ),
     -- REFUNDS TABLE
     ref AS (
         SELECT id                           AS id,
                amount/100.00								 AS amount,
                currency                     AS currency,
                created                     AS date_created,
                status                       AS status,
                'refund'                     AS type
         FROM refunds
     )
-- UNION ALL TABLES
SELECT *
FROM (SELECT *
      FROM cha
      UNION ALL
      SELECT *
      FROM dis
      UNION ALL
      SELECT *
      FROM ref) result
-- DATE FILTERS
-- If we schedule this daily, we will get all of the previous
-- day's processed data
where date_trunc('day', result.date_created) = date_trunc('day', date_add('month',-1,data_load_time))
ORDER BY result.date_created desc;*/

SELECT c.id,
       c.object,
       c.amount,
       c.amount_captured,
       c.amount_refunded,
       c.balance_transaction,
       c.billing_details,
       c.calculated_statement_descriptor,
       c.captured,
       c.created,
       c.currency,
       c.customer,
       c.description,
       c.dispute,
       c.disputed,
       c.failure_code,
       c.failure_message,
       c.fraud_details,
       c.invoice,
       c.livemode,
       c.metadata,
       c.order_id,
       c.record__o:payment_id::INT AS payment_id,
       c.outcome,
       c.paid,
       c.payment_intent,
       c.payment_method,
       c.payment_method_details,
       c.receipt_email,
       c.receipt_number,
       c.receipt_url,
       c.refunded,
       c.refunds,
       c.review,
       c.shipping,
       c.source,
       c.statement_descriptor,
       c.statement_descriptor_suffix,
       c.status,
       c.record__o
FROM hygiene_snapshot_vault_mvp.stripe.charges c;
SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.stripetb_charge_snapshot;

SELECT DISTINCT toi.event_type
FROM se.data.tb_order_item toi;

SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.orders_payment_snapshot op;

CREATE SCHEMA raw_vault_mvp_dev_robin.stripe;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.stripe.charges CLONE raw_vault_mvp.stripe.charges;

SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.stripe.charges; -- 2021-03-01 14:32:58.329945000

self_describing_task --include 'staging/hygiene_snapshots/stripe/charges.py'  --method 'run' --start '2021-03-01 00:00:00' --end '2021-03-01 00:00:00'
self_describing_task --include 'staging/hygiene/stripe/charges.py'  --method 'run' --start '2021-03-01 00:00:00' --end '2021-03-01 00:00:00'

DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.stripe.charges;
DROP TABLE hygiene_vault_mvp_dev_robin.stripe.charges;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.stripe.charges;

airflow clear --start_date '2021-03-01 01:00:00' --end_date '2021-03-01 01:00:00' --task_regex '.*' hygiene_snapshots__stripe__charges__daily_at_01h00
airflow backfill --start_date '2021-03-01 01:00:00' --end_date '2021-03-01 01:00:00' --task_regex '.*' hygiene_snapshots__stripe__charges__daily_at_01h00

------------------------------------------------------------------------------------------------------------------------
--Charges - Money in
CREATE OR REPLACE VIEW collab.travel_trust.money_in_cash_on_booking COPY GRANTS AS
(
SELECT c.id                                         AS transaction_id,
       c.created                                    AS transaction_tstamp,
       'stripe'                                     AS payment_service_provider,
       'charges'                                    AS payment_service_provider_transaction_type,
       'money in'                                   AS cashflow_direction,
       'cash on booking'                            AS cashflow_tye,
       c.amount / 100::DECIMAL(13, 4)               AS transaction_amount,
       UPPER(c.currency)                            AS transaction_currency,
       c.captured,
       c.outcome:network_status::VARCHAR            AS transaction_network_status,
       c.failure_code,
       c.failure_message,
       c.balance_transaction,
       c.tb_payment_id,
       c.tb_order_id,
       c.tb_offer_id,
       'TB-' || c.tb_order_id                       AS booking_id,
       c.payment_method_details,
       c.payment_method_details: TYPE::VARCHAR      AS payment_type,
       c.payment_method_details:card                AS payment_card,
       c.payment_method_details:card:brand::VARCHAR AS payment_card_brand,
       c.payment_method_details:sofort              AS payment_sofort,
       c.payment_method_details:bancontact          AS payment_bancontact,
       c.payment_method_details:ideal               AS payment_ideal,
       op.classification                            AS orders_paymemt_classification,
       op.planned_payment_id                        AS orders_payment_planned_payment_id,
       c.record__o
FROM hygiene_snapshot_vault_mvp.stripe.charges c
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_payment_snapshot op ON c.tb_payment_id::INT = op.id
    );

SELECT *
FROM hygiene_snapshot_vault_mvp.stripe.charges c;

-- PAYMENT_TYPE
-- card
-- sofort
-- bancontact
-- ideal


GRANT SELECT ON VIEW collab.travel_trust.money_in_cash_on_booking TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.money_in_cash_on_booking TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.travel_trust.money_in_cash_on_booking TO ROLE personal_role__tanithspinelli;
GRANT SELECT ON VIEW collab.travel_trust.money_in_cash_on_booking TO ROLE personal_role__ezraphilips;
GRANT SELECT ON VIEW collab.travel_trust.money_in_cash_on_booking TO ROLE personal_role__sebastianmaczka;
GRANT SELECT ON VIEW collab.travel_trust.money_in_cash_on_booking TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.travel_trust.money_in_cash_on_booking TO ROLE personal_role__barbarazacchino;
GRANT SELECT ON VIEW collab.travel_trust.money_in_cash_on_booking TO ROLE personal_role__roshnidattani;

SELECT *
FROM collab.travel_trust.money_in_cash_on_booking
WHERE money_in_cash_on_booking.booking_id = 'TB-21876212';

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW collab.travel_trust.money_out_psp_refunds COPY GRANTS AS
(
SELECT r.id                           AS transaction_id,
       r.created                      AS transaction_tstamp,
       'stripe'                       AS payment_service_provider,
       'charges'                      AS payment_service_provider_transaction_type,
       'money out'                    AS cashflow_direction,
       'booking psp refund'           AS cashflow_tye,
       r.amount / 100::DECIMAL(13, 4) AS transaction_amount,
       UPPER(r.currency)              AS transaction_currency,
       r.status                       AS transaction_charge_status,
       r.failure_balance_transaction,
       r.failure_reason,
       r.balance_transaction,
       r.charge                       AS transaction_charge_id,
       r.tb_payment_id,
       r.tb_order_id,
       'TB-' || tb_order_id           AS booking_id,
       r.payment_intent               AS payment_intent_id,
       r.reason,
       r.receipt_number,
       orp.refund_request_id,
       o.initiator,
       o.comments,
       o.complaint_comment,
       o.cancellation_reason,
       r.record__o
FROM hygiene_snapshot_vault_mvp.stripe.refunds r
         LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_psprefund op ON r.id = PARSE_JSON(op.raw):id::VARCHAR
         LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_refundrequestpsppayments orp
                   ON op.id = orp.psp_refund_id
         LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_refundrequest o ON orp.refund_request_id = o.id
    );


GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_refunds TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_refunds TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_refunds TO ROLE personal_role__tanithspinelli;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_refunds TO ROLE personal_role__ezraphilips;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_refunds TO ROLE personal_role__sebastianmaczka;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_refunds TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_refunds TO ROLE personal_role__barbarazacchino;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_refunds TO ROLE personal_role__roshnidattani;

SELECT *
FROM hygiene_snapshot_vault_mvp.stripe.refunds r;

SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequest_snapshot ors
    QUALIFY COUNT(*) OVER (PARTITION BY ors.order_id) > 1
ORDER BY order_id;

SELECT *
FROM se.data.tb_booking tb
WHERE tb.order_id = '21866836';

SELECT *
FROM hygiene_snapshot_vault_mvp.stripe.disputes d;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.stripe.refunds CLONE raw_vault_mvp.stripe.refunds;

SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.stripe.refunds r; --2021-03-15 10:22:54.272396000


SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequestpsppayments_snapshot ors;


SELECT PARSE_JSON(ops.raw):id::VARCHAR AS refund_transaction_id,
       ops.amount,
       ops.status,
       ops.payment_id,
       ors.refund_request_id,
       PARSE_JSON(ops.raw)             AS raw,
       o.*

FROM data_vault_mvp.travelbird_mysql_snapshots.orders_psprefund_snapshot ops
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequestpsppayments_snapshot ors
                   ON ops.id = ors.psp_refund_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequest_snapshot o ON ors.refund_request_id = o.id;


------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW collab.travel_trust.money_out_psp_chargebacks COPY GRANTS AS
(
SELECT d.id                           AS transaction_id,
       d.created                      AS transaction_tstamp,
       'stripe'                       AS payment_service_provider,
       'disputes'                     AS payment_service_provider_transaction_type,
       'money out'                    AS cashflow_direction,
       'customer psp dispute'         AS cashflow_tye,
       d.amount / 100::DECIMAL(13, 4) AS transaction_amount,
       UPPER(d.currency)              AS transaction_currency,
       d.status                       AS transaction_dispute_status,
       d.balance_transaction,
       d.balance_transactions,
       d.charge                       AS transaction_charge_id,
       c.tb_order_id,
       d.evidence,
       d.evidence_details,
       d.is_charge_refundable,
       d.livemode,
       d.payment_intent,
       d.reason,
       d.record__o
FROM hygiene_snapshot_vault_mvp.stripe.disputes d
         LEFT JOIN hygiene_snapshot_vault_mvp.stripe.charges c ON d.charge = c.id
    );

airflow backfill --start_date '2021-03-30 00:30:00' --end_date '2021-03-30 00:30:00' --task_regex '.*' incoming__cms_mysql__credit__daily_at_00h30



GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_chargebacks TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_chargebacks TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_chargebacks TO ROLE personal_role__tanithspinelli;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_chargebacks TO ROLE personal_role__ezraphilips;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_chargebacks TO ROLE personal_role__sebastianmaczka;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_chargebacks TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_chargebacks TO ROLE personal_role__barbarazacchino;
GRANT SELECT ON VIEW collab.travel_trust.money_out_psp_chargebacks TO ROLE personal_role__roshnidattani;

SELECT *
FROM collab.travel_trust.money_out_psp_chargebacks mopd;


ALTER SCHEMA collab.travel_trust RENAME TO collab.travel_trust_finance;

CREATE OR REPLACE TEMPORARY TABLE money_in
(
    transaction_id                            VARCHAR,
    transaction_tstamp                        TIMESTAMP,
    payment_service_provider                  VARCHAR,
    payment_service_provider_transaction_type VARCHAR,
    cashflow_direction                        VARCHAR,
    cashflow_tye                              VARCHAR,
    transaction_amount                        NUMBER,
    transaction_currency                      VARCHAR,
    captured                                  BOOLEAN,
    transaction_network_status                VARCHAR,
    failure_code                              VARCHAR,
    failure_message                           VARCHAR,
    balance_transaction                       VARCHAR,
    tb_payment_id                             NUMBER,
    tb_order_id                               NUMBER,
    tb_offer_id                               NUMBER,
    booking_id                                VARCHAR,
    payment_method_details                    OBJECT,
    payment_type                              VARCHAR,
    payment_card                              VARIANT,
    payment_card_brand                        VARCHAR,
    payment_sofort                            VARIANT,
    payment_bancontact                        VARIANT,
    payment_ideal                             VARIANT,
    orders_paymemt_classification             VARCHAR,
    orders_payment_planned_payment_id         NUMBER,
    record__o                                 VARIANT
);

self_describing_task --include 'dv/dwh/finance/stripe_cash_on_booking.py'  --method 'run' --start '2021-03-30 00:00:00' --end '2021-03-30 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.stripe.charges CLONE hygiene_snapshot_vault_mvp.stripe.charges;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_payment CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_payment;

SELECT *
FROM data_vault_mvp.dwh.booking_cancellation bc;

CREATE OR REPLACE TEMPORARY TABLE collab.travel_trust.refund AS
SELECT *
FROM collab.travel_trust.money_out_psp_refunds mopr;
SELECT GET_DDL('table', 'collab.travel_trust.refund');

CREATE OR REPLACE TEMPORARY TABLE refund
(
    transaction_id                            VARCHAR,
    transaction_tstamp                        TIMESTAMP,
    payment_service_provider                  VARCHAR,
    payment_service_provider_transaction_type VARCHAR,
    cashflow_direction                        VARCHAR,
    cashflow_tye                              VARCHAR,
    transaction_amount                        NUMBER,
    transaction_currency                      VARCHAR,
    transaction_charge_status                 VARCHAR,
    failure_balance_transaction               VARCHAR,
    failure_reason                            VARCHAR,
    balance_transaction                       VARCHAR,
    transaction_charge_id                     VARCHAR,
    tb_payment_id                             NUMBER,
    tb_order_id                               NUMBER,
    booking_id                                VARCHAR,
    payment_intent_id                         VARCHAR,
    reason                                    VARCHAR,
    receipt_number                            VARCHAR,
    refund_request_id                         NUMBER,
    initiator                                 VARCHAR,
    comments                                  VARCHAR,
    complaint_comment                         VARCHAR,
    cancellation_reason                       VARCHAR,
    record__o                                 VARIANT
);


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.stripe.refunds CLONE hygiene_snapshot_vault_mvp.stripe.refunds;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_psprefund CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_psprefund;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_refundrequestpsppayments CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_refundrequestpsppayments;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_refundrequest CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_refundrequest;

self_describing_task --include 'dv/dwh/finance/stripe_refunds.py'  --method 'run' --start '2021-03-30 00:00:00' --end '2021-03-30 00:00:00'

CREATE TEMPORARY TABLE collab.travel_trust.dispute
AS
SELECT *
FROM collab.travel_trust.money_out_psp_chargebacks mopc;

SELECT GET_DDL('table', 'collab.travel_trust.dispute');

CREATE OR REPLACE TEMPORARY TABLE dispute
(
    transaction_id                            VARCHAR,
    transaction_tstamp                        TIMESTAMP,
    payment_service_provider                  VARCHAR,
    payment_service_provider_transaction_type VARCHAR,
    cashflow_direction                        VARCHAR,
    cashflow_tye                              VARCHAR,
    transaction_amount                        NUMBER,
    transaction_currency                      VARCHAR,
    transaction_dispute_status                VARCHAR,
    balance_transaction                       VARCHAR,
    balance_transactions                      ARRAY,
    transaction_charge_id                     VARCHAR,
    tb_order_id                               NUMBER,
    evidence                                  OBJECT,
    evidence_details                          OBJECT,
    is_charge_refundable                      BOOLEAN,
    livemode                                  BOOLEAN,
    payment_intent                            VARCHAR,
    reason                                    VARCHAR,
    record__o                                 VARIANT
);
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.stripe.disputes CLONE hygiene_snapshot_vault_mvp.stripe.disputes;

self_describing_task --include 'dv/dwh/finance/stripe_chargebacks.py'  --method 'run' --start '2021-03-30 00:00:00' --end '2021-03-30 00:00:00'

self_describing_task --include 'se/finance/travel_trust/stripe_refund.py'  --method 'run' --start '2021-03-30 00:00:00' --end '2021-03-30 00:00:00'
self_describing_task --include 'se/finance/travel_trust/stripe_cash_on_booking.py'  --method 'run' --start '2021-03-30 00:00:00' --end '2021-03-30 00:00:00'

airflow backfill --start_date '2021-03-30 03:00:00' --end_date '2021-03-30 03:00:00' --task_regex '.*' dwh__transactional__se_credit__daily_at_03h00


SELECT *
FROM data_vault_mvp.dwh.se_credit
    QUALIFY COUNT(*) OVER (PARTITION BY se_credit.credit_id) > 1;
self_describing_task --include 'dv/dwh/transactional/se_credit.py'  --method 'run' --start '2021-03-30 00:00:00' --end '2021-03-30 00:00:00'


SELECT bs.record:bookingStatus::VARCHAR as status,
       count(*)
FROM raw_vault_mvp.cms_mongodb.booking_summary bs
WHERE bs.loaded_at::DATE = CURRENT_DATE
GROUP BY 1;

SELECT bs.record:bookingStatus::VARCHAR as status,
       count(*)
FROM raw_vault_mvp.cms_mongodb.booking_summary bs
WHERE bs.loaded_at::DATE = CURRENT_DATE-1
AND bs.loaded_at <= '2021-03-31 10:00:00'
GROUP BY 1
;


select * from se.data.se_sale_attributes where company_name = 'Borgo San Luigi';
select * from se.data.se_hotel_sale_offer
where sale_id IN(
'A15858',
'A15544',
'A15506',
'A15668',
'A15545',
'107770',
'37401',
'16763');
select * from se.data.se_cms_mari_link where offer_id = 16543;
select * from se.data.se_offer_attributes where se_offer_id = 'A16543';
select * from data_vault_mvp.cms_mysql_snapshots.offer_snapshot;


SELECT MIN(loaded_at) FROM raw_vault_mvp.travelbird_mysql.orders_orderevent oo; --2021-04-06 00:37:18.339610000