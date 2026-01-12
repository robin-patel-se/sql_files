SELECT crtm.dataset_name,
       crtm.dataset_source,
       crtm.schedule_interval,
       crtm.schedule_tstamp,
       crtm.run_tstamp,
       crtm.loaded_at,
       crtm.filename,
       crtm.file_row_number,
       crtm.cms_transaction_id,
       crtm.booking_id,
       crtm.include_flight,
       crtm.net_amount_paid_fx,
       crtm.net_amount_paid_gbp,
       crtm.non_flight_spls_cash_held,
       crtm.non_flight_vcc_held,
       crtm.flight_refunds_received_gbp,
       crtm.total_held_gbp,
       crtm.perc_held,
       crtm.flight_and_non_flight_components_held,
       crtm.refund_made,
       crtm.refund_type,
       crtm.amount,
       crtm.chargeback,
       crtm.currency,
       crtm.amount_inc_margin_adj,
       crtm.extract_metadata
FROM raw_vault_mvp.finance_gsheets.cash_refunds_to_members crtm;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.finance_gsheets.cash_refunds_to_members CLONE raw_vault_mvp.finance_gsheets.cash_refunds_to_members;

CREATE TABLE finance_gsheets.cash_refunds_to_members
(
    cms_transaction_id                    VARCHAR,
    booking_id                            VARCHAR,
    include_flight                        VARCHAR,
    net_amount_paid_fx                    VARCHAR,
    net_amount_paid_gbp                   VARCHAR,
    non_flight_spls_cash_held             VARCHAR,
    non_flight_vcc_held                   VARCHAR,
    flight_refunds_received_gbp           VARCHAR,
    total_held_gbp                        VARCHAR,
    perc_held                             VARCHAR,
    flight_and_non_flight_components_held VARCHAR,
    refund_made                           VARCHAR,
    refund_type                           VARCHAR,
    amount                                VARCHAR,
    chargeback                            VARCHAR,
    currency                              VARCHAR,
    amount_inc_margin_adj                 VARCHAR,
    extract_metadata                      VARIANT,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
)
    CLUSTER BY (TO_DATE(schedule_tstamp));

SELECT *
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order oo;
SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_status;
SELECT *
FROM data_vault_mvp.dwh.tb_booking tb;

self_describing_task --include 'staging/hygiene/finance_gsheets/cash_refunds_to_members'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM hygiene_vault_mvp_dev_robin.finance_gsheets.cash_refunds_to_members
WHERE unique_transaction_id IN (
    SELECT unique_transaction_id
    FROM hygiene_vault_mvp_dev_robin.finance_gsheets.cash_refunds_to_members
    GROUP BY 1
    HAVING count(*) > 1
);

self_describing_task --include 'staging/hygiene_snapshots/finance_gsheets/cash_refunds_to_members'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT * FROM  hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_to_members;