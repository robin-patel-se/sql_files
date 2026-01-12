SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.coupons_coupon_order_snapshot c ccos;

SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.coupons_couponconfig_snapshot c;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.order_payment_coupon AS (
    SELECT 'cpp_' || op.id       AS transaction_id,
           op.created            AS transaction_tstamp,
           'travelbird'          AS payment_service_provider,
           'order payments'      AS payment_service_provider_transaction_type,
           'money in'            AS cashflow_direction,
           'coupon payment'      AS cashflow_type,

           op.amount,
           ccs.code              AS currency,

           'TB-' || op.order_id  AS booking_id,
           op.order_id,
           op.balance,
           op.classification,
           op.source_content_type_id,
           op.id                 AS order_payment_id,
           op.state              AS payment_state,
           op.planned_payment_id,
           op.polymorphic_ctype_id,

           cc.code               AS coupon_code,
           cc.active = 1         AS coupon_is_active,

           c.id                  AS coupon_id,
           c.name                AS coupon_name,
           c.message             AS coupon_message,
           c.minimum_order_value AS coupon_minimum_order_value,
           c.discount_type       AS coupon_discount_type,
           c.discount_from       AS coupon_discount_from,
           c.discount            AS coupon_discount,
           c.max_usage           AS coupon_max_usage,
           c.valid_from          AS coupon_valid_from,
           c.valid_to            AS coupon_valid_to,
           c.can_be_combined     AS coupon_can_be_combined,
           c.valid_for_email, ---- NOTE: This column is considered PII
           c.coupon_type,
           c.created_at_dts      AS coupon_created_at_dts,
           c.created_by_id       AS coupon_created_by_id,
           c.updated_at_dts      AS coupon_updated_at_dts,
           op.classification     AS orders_paymemt_classification,
           c.is_credit           AS coupon_is_credit,
           c.se_credit_id        AS coupon_se_credit_id,
           sc.is_cash_credit     AS coupon_is_from_cash_credit
    FROM hygiene_snapshot_vault_mvp.travelbird_mysql.orders_payment op
             LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.currency_currency_snapshot ccs ON op.currency_id = ccs.id
             LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.coupons_coupon_snapshot cc ON op.source_id = cc.id
             LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.coupons_couponconfig c ON cc.config_id = c.id
             LEFT JOIN data_vault_mvp.dwh.se_credit sc ON TRY_TO_NUMBER(c.se_credit_id) = TRY_TO_NUMBER(sc.credit_id)
    WHERE op.source_content_type_id = 143 -- 143 is coupon payment
)
;

airflow backfill --start_date '2021-03-10 03:00:00' --end_date '2021-03-10 03:00:00' --task_regex '.*' dwh__transactional__tb_order_item_changelog__daily_at_03h00
airflow backfill --start_date '2021-03-10 07:00:00' --end_date '2021-03-10 07:00:00' --task_regex '.*' se_data_object_creation__daily_at_07h00

SELECT GET_DDL('table', 'scratch.robinpatel.order_payment_coupon');

CREATE OR REPLACE TRANSIENT TABLE order_payment_coupon
(
    transaction_id                            VARCHAR PRIMARY KEY NOT NULL,
    transaction_tstamp                        TIMESTAMP,
    payment_service_provider                  VARCHAR,
    payment_service_provider_transaction_type VARCHAR,
    cashflow_direction                        VARCHAR,
    cashflow_type                             VARCHAR,
    amount                                    NUMBER,
    currency                                  VARCHAR,
    booking_id                                VARCHAR,
    order_id                                  NUMBER,
    balance                                   NUMBER,
    classification                            VARCHAR,
    source_content_type_id                    NUMBER,
    order_payment_id                          NUMBER,
    payment_state                             VARCHAR,
    planned_payment_id                        NUMBER,
    polymorphic_ctype_id                      NUMBER,
    coupon_code                               VARCHAR,
    coupon_is_active                          BOOLEAN,
    coupon_id                                 NUMBER,
    coupon_name                               VARCHAR,
    coupon_message                            VARCHAR,
    coupon_minimum_order_value                NUMBER,
    coupon_discount_type                      VARCHAR,
    coupon_discount_from                      VARCHAR,
    coupon_discount                           NUMBER,
    coupon_max_usage                          NUMBER,
    coupon_valid_from                         DATE,
    coupon_valid_to                           DATE,
    coupon_can_be_combined                    NUMBER,
    valid_for_email                           VARCHAR,
    coupon_type                               VARCHAR,
    coupon_created_at_dts                     TIMESTAMP,
    coupon_created_by_id                      NUMBER,
    coupon_updated_at_dts                     TIMESTAMP,
    coupon_is_credit                          NUMBER,
    coupon_se_credit_id                       VARCHAR,
    coupon_is_from_cash_credit                BOOLEAN
);


self_describing_task --include 'dv/finance/travelbird_cashflow/order_payment_coupon.py'  --method 'run' --start '2021-04-06 00:00:00' --end '2021-04-06 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_mysql_snapshots.coupons_coupon_snapshot CLONE data_vault_mvp.travelbird_mysql_snapshots.coupons_coupon_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.coupons_couponconfig CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.coupons_couponconfig;

self_describing_task --include 'se/finance/travel_trust/tb_order_payment_coupon.py'  --method 'run' --start '2021-04-06 00:00:00' --end '2021-04-06 00:00:00'

SELECT topc.coupon_external_reference
FROM se.finance.tb_order_payment_coupon topc

SELECT topc.transaction_id,
       topc.transaction_tstamp,
       topc.payment_service_provider,
       topc.payment_service_provider_transaction_type,
       topc.cashflow_direction,
       topc.cashflow_type,
       topc.amount,
       topc.currency,
       topc.orders_paymemt_classification,
       topc.booking_id
FROM se.finance.tb_order_payment_coupon topc
WHERE se.finance.travel_trust_booking(REGEXP_REPLACE(topc.booking_id, 'TB-')::INT)
  AND topc.coupon_is_from_cash_credit IS DISTINCT FROM FALSE;



self_describing_task --include 'dv/finance/svb/manual_refund.py'  --method 'run' --start '2021-05-05 00:00:00' --end '2021-05-05 00:00:00'
self_describing_task --include 'dv/finance/travelbird_cashflow/order_payment_coupon.py'  --method 'run' --start '2021-05-05 00:00:00' --end '2021-05-05 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.coupons_couponconfig  clone hygiene_snapshot_vault_mvp.travelbird_mysql.coupons_couponconfig;

self_describing_task --include 'se/finance/travel_trust/svb_manual_refund.py'  --method 'run' --start '2021-05-05 00:00:00' --end '2021-05-05 00:00:00'
self_describing_task --include 'se/finance/travel_trust/svb_manual_refund.py'  --method 'run' --start '2021-05-05 00:00:00' --end '2021-05-05 00:00:00'
self_describing_task --include 'se/finance/travel_trust/travel_trust_money_in.py'  --method 'run' --start '2021-05-05 00:00:00' --end '2021-05-05 00:00:00'


self_describing_task --include 'dv/finance/aviate/transactions.py'  --method 'run' --start '2021-05-05 00:00:00' --end '2021-05-05 00:00:00'
self_describing_task --include 'se/finance/travel_trust/aviate_transactions.py'  --method 'run' --start '2021-05-05 00:00:00' --end '2021-05-05 00:00:00'