SELECT crs.id,
       crs.type,
       crs.source,
       crs.amount,
       crs.fee,
       crs.destination_platform_fee,
       crs.net,
       crs.currency,
       crs.created_utc,
       crs.available_on_utc,
       crs.description,
       crs.customer_facing_amount,
       crs.customer_facing_currency,
       crs.transfer,
       crs.transfer_date_utc,
       crs.transfer_group,
       crs.order_id_metadata,
       crs.payment_id_metadata,
       crs.session_key_metadata,
       crs.offer_id_metadata,
       crs.extract_metadata,
       COALESCE(crs.order_id_metadata::VARCHAR, ops.order_id::VARCHAR,
                REGEXP_SUBSTR(crs.description, '(218[0-9]+)\\)', 1, 1, '', 1)) AS order_id,
       REGEXP_SUBSTR(crs.description, 're_1.*|ch_1.*', 1, 1, 'e')              AS related_stripe_id,
       tb.se_sale_id || '-' || tb.reference_id                                 AS transaction_id
--        scs.charge_id,
--        ops.order_id
FROM raw_vault_mvp.finance_gsheets.cash_refunds_stripe crs
         LEFT JOIN data_vault_mvp.travelbird_cms.stripetb_charge_snapshot scs
                   ON REGEXP_SUBSTR(crs.description, 're_1.*|ch_1.*', 1, 1, 'e') = scs.charge_id
         LEFT JOIN data_vault_mvp.travelbird_cms.orders_payment_snapshot ops ON scs.payment_id = ops.id
         LEFT JOIN data_vault_mvp.dwh.tb_booking tb ON COALESCE(crs.order_id_metadata::VARCHAR, ops.order_id::VARCHAR,
                                                                REGEXP_SUBSTR(crs.description, '(218[0-9]+)\\)', 1, 1, '', 1)) =
                                                       tb.id;

SELECT *
FROM data_vault_mvp.dwh.tb_booking tb;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.finance_gsheets.cash_refunds_stripe CLONE raw_vault_mvp.finance_gsheets.cash_refunds_stripe;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.travelbird_cms.stripetb_charge_snapshot CLONE data_vault_mvp.travelbird_cms.stripetb_charge_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.travelbird_cms.orders_payment_snapshot CLONE data_vault_mvp.travelbird_cms.orders_payment_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;

CREATE TABLE finance_gsheets.cash_refunds_stripe
(
    id                       VARCHAR,
    type                     VARCHAR,
    source                   VARCHAR,
    amount                   DOUBLE,
    fee                      DOUBLE,
    destination_platform_fee DOUBLE,
    net                      DOUBLE,
    currency                 VARCHAR,
    created_utc              TIMESTAMPNTZ,
    available_on_utc         TIMESTAMPNTZ,
    description              VARCHAR,
    customer_facing_amount   DOUBLE,
    customer_facing_currency VARCHAR,
    transfer                 VARCHAR,
    transfer_date_utc        TIMESTAMPNTZ,
    transfer_group           VARCHAR,
    order_id_metadata        NUMBER,
    payment_id_metadata      NUMBER,
    session_key_metadata     VARCHAR,
    offer_id_metadata        NUMBER,
    extract_metadata         VARIANT,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
)
    CLUSTER BY (TO_DATE(schedule_tstamp));

SELECT
    --REGEXP_SUBSTR(description, '\\((.*)\\)', 1, 1, '', 1) AS REFERENCE_1,
    --REGEXP_SUBSTR(description, '\\(\\S+ (.*)\\)', 1, 1, '', 1) AS REFERENCE_2,
    COALESCE(order_id_metadata::VARCHAR, REGEXP_SUBSTR(description, '([0-9]+)\\)', 1, 1, '', 1)) AS order_id,
    cash_refunds_stripe.description,
    currency,
    SUM(CASE WHEN lower(type) IN ('refund', 'payment_refund') THEN amount ELSE 0 END)            AS refunded_amount,
    SUM(CASE WHEN lower(type) IN ('charge', 'payment') THEN amount ELSE 0 END)                   AS payment_amount
FROM raw_vault_mvp.finance_gsheets.cash_refunds_stripe
GROUP BY 1, 2, 3


SELECT *
FROM data_vault_mvp.travelbird_cms.stripetb_charge_snapshot scs;

SELECT
FROM collab.covid_pii.covid_master_list_catalogue cmlc;


self_describing_task --include 'staging/hygiene/finance_gsheets/cash_refunds_stripe'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM hygiene_vault_mvp_dev_robin.finance_gsheets.cash_refunds_stripe;
SELECT *
FROM raw_vault_mvp.finance_gsheets.cash_refunds_stripe;

self_describing_task --include 'staging/hygiene_snapshots/finance_gsheets/cash_refunds_stripe'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_stripe;