SELECT crarr.dataset_name,
       crarr.dataset_source,
       crarr.schedule_interval,
       crarr.schedule_tstamp,
       crarr.run_tstamp,
       crarr.loaded_at,
       crarr.filename,
       crarr.file_row_number,
       crarr.booking_system,
       crarr.product,
       crarr.travel_date,
       crarr.booking_date,
       crarr.booking_reference,
       crarr.external_reference,
       crarr.overall_booking_status,
       crarr.flight_booking_status,
       crarr.airline_name,
       crarr.supplier,
       crarr.booking_source,
       crarr.payment_identifier,
       crarr.flight_pnr,
       crarr.selling_country,
       crarr.buying_currency,
       crarr.buying_exchange_rate_to_gbp,
       crarr.cost_in_buying_currency,
       crarr.cost_in_gbp,
       crarr.member_refund_type,
       crarr.potential_in_resort_flag,
       crarr.in_resort_status,
       crarr.claimed_by,
       crarr.claimed_date,
       crarr.email_received_to_mailbox,
       crarr.refund_actioned_through_airline,
       crarr.airline_refund_type,
       crarr.refund_request_actioned_by,
       crarr.refund_requestd_date,
       crarr.actioned_in_system,
       crarr.system_updated_date,
       crarr.system_update_actioned_by,
       crarr.follow_up_action,
       crarr.comments_as_needed,
       crarr.extract_metadata
FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report crarr

SELECT external_reference,
       airline_name,
       supplier,
       overall_booking_status,
       flight_booking_status,
       cost_in_buying_currency,
       cost_in_gbp,
       member_refund_type,
       booking_system,
       ROW_NUMBER() OVER (
           PARTITION BY external_reference
           ORDER BY
               travel_date
           ) AS rank
FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report


CREATE TABLE finance_gsheets.cash_refunds_airline_refund_report
(
    dataset_name                    VARCHAR,
    dataset_source                  VARCHAR,
    schedule_interval               VARCHAR,
    schedule_tstamp                 TIMESTAMPNTZ,
    run_tstamp                      TIMESTAMPNTZ,
    loaded_at                       TIMESTAMPNTZ,
    filename                        VARCHAR,
    file_row_number                 NUMBER,
    booking_system                  VARCHAR,
    product                         VARCHAR,
    travel_date                     DATE,
    booking_date                    DATE,
    booking_reference               NUMBER,
    external_reference              VARCHAR,
    overall_booking_status          VARCHAR,
    flight_booking_status           VARCHAR,
    airline_name                    VARCHAR,
    supplier                        VARCHAR,
    booking_source                  VARCHAR,
    payment_identifier              VARCHAR,
    flight_pnr                      VARCHAR,
    selling_country                 VARCHAR,
    buying_currency                 VARCHAR,
    buying_exchange_rate_to_gbp     DOUBLE,
    cost_in_buying_currency         DOUBLE,
    cost_in_gbp                     DOUBLE,
    member_refund_type              VARCHAR,
    potential_in_resort_flag        BOOLEAN,
    in_resort_status                VARCHAR,
    claimed_by                      VARCHAR,
    claimed_date                    DATE,
    email_received_to_mailbox       BOOLEAN,
    refund_actioned_through_airline BOOLEAN,
    airline_refund_type             VARCHAR,
    refund_request_actioned_by      VARCHAR,
    refund_requestd_date            DATE,
    actioned_in_system              BOOLEAN,
    system_updated_date             DATE,
    system_update_actioned_by       VARCHAR,
    follow_up_action                VARCHAR,
    comments_as_needed              VARCHAR,
    extract_metadata                VARIANT,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
);
CREATE SCHEMA raw_vault_mvp_dev_robin.finance_gsheets;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report CLONE raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report;

self_describing_task --include 'staging/hygiene/finance_gsheets/cash_refunds_airline_refund_report'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report;


SELECT crarr.booking_reference,
       count(*)
FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report crarr
GROUP BY 1
HAVING COUNT(*) > 1;

SELECT *
FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report crarr
WHERE crarr.booking_reference IN (
    SELECT crarr.booking_reference
    FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report crarr
--     WHERE crarr.booking_reference != ''
    GROUP BY 1
    HAVING COUNT(*) > 1
);

self_describing_task --include 'staging/hygiene_snapshots/finance_gsheets/cash_refunds_airline_refund_report'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report carr
WHERE carr.unique_transaction_id IN
      (
          SELECT carr.unique_transaction_id
          FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report carr
          WHERE carr.unique_transaction_id != 'Unknown'
          GROUP BY 1
          HAVING COUNT(*) > 1
      )
;



SELECT * FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report ;
SELECT * FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report crarr;
SELECT * FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report carr;


SELECT * FROM data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hsos


