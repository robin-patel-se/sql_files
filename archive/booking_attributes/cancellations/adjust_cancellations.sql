CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation;


self_describing_task --include 'dv/dwh/transactional/se_booking_cancellation.py'  --method 'run' --start '2021-01-06 00:00:00' --end '2021-01-06 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation bc
WHERE bc.date_created::DATE != bc.last_updated::DATE;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.booking_cancellation bc
WHERE bc.booking_id = 'A2867756';

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mysql.booking_cancellation CLONE raw_vault_mvp.cms_mysql.booking_cancellation;

SELECT requested_by,
       SPLIT_PART(requested_by, '@', -1)
FROM raw_vault_mvp_dev_robin.cms_mysql.booking_cancellation bc
WHERE requested_by IS NOT NULL;

self_describing_task --include 'staging/hygiene/cms_mysql/booking_cancellation.py'  --method 'run' --start '2021-01-06 00:00:00' --end '2021-01-06 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/booking_cancellation.py'  --method 'run' --start '2021-01-06 00:00:00' --end '2021-01-06 00:00:00'



------------------------------------------------------------------------------------------------------------------------
-- update history for hygiene table.

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation_temp
(
    -- (lineage) metadata for the current job
    schedule_tstamp                                  TIMESTAMP,
    run_tstamp                                       TIMESTAMP,
    operation_id                                     VARCHAR,
    created_at                                       TIMESTAMP,
    updated_at                                       TIMESTAMP,

    -- (lineage) original metadata columns from previous step
    row_dataset_name                                 VARCHAR,
    row_dataset_source                               VARCHAR,
    row_loaded_at                                    TIMESTAMP,
    row_schedule_tstamp                              TIMESTAMP,
    row_run_tstamp                                   TIMESTAMP,
    row_filename                                     VARCHAR,
    row_file_row_number                              INT,
    row_extract_metadata                             VARIANT,

    -- hygiened columns
    booking_id                                       VARCHAR,
    booking_fee_gbp                                  DOUBLE,
    cc_fee_gbp                                       DOUBLE,
    hotel_good_will_gbp                              DOUBLE,
    se_good_will_gbp                                 DOUBLE,

    -- original columns that don't require any hygiene
    id                                               NUMBER,
    version                                          NUMBER,
    booking_id__o                                    NUMBER,
    reservation_id__o                                NUMBER,
    date_created                                     TIMESTAMP,
    last_updated                                     TIMESTAMP,
    fault                                            VARCHAR,
    reason                                           VARCHAR,
    booking_fee__o                                   FLOAT,
    cc_fee__o                                        FLOAT,
    hotel_good_will__o                               FLOAT,
    se_good_will__o                                  FLOAT,
    refund_channel                                   VARCHAR,
    refund_type                                      VARCHAR,
    who_pays                                         VARCHAR,
    cancel_with_provider                             BOOLEAN,
    status                                           VARCHAR,
    requested_by                                     VARCHAR, -- may contain PII
    requested_by_domain                              VARCHAR,
    payment_provider_refund_status                   VARCHAR,

    -- hygiene flags
    failed_some_validation                           INT,
    fails_validation__id__expected_nonnull           INT,
    fails_validation__date_created__expected_nonnull INT
);

INSERT INTO hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation_temp
SELECT bc.schedule_tstamp,
       bc.run_tstamp,
       bc.operation_id,
       bc.created_at,
       bc.updated_at,
       bc.row_dataset_name,
       bc.row_dataset_source,
       bc.row_loaded_at,
       bc.row_schedule_tstamp,
       bc.row_run_tstamp,
       bc.row_filename,
       bc.row_file_row_number,
       bc.row_extract_metadata,
       bc.booking_id,
       bc.booking_fee_gbp,
       bc.cc_fee_gbp,
       bc.hotel_good_will_gbp,
       bc.se_good_will_gbp,
       bc.id,
       bc.version,
       bc.booking_id__o,
       bc.reservation_id__o,
       bc.date_created,
       bc.last_updated,
       bc.fault,
       bc.reason,
       bc.booking_fee__o,
       bc.cc_fee__o,
       bc.hotel_good_will__o,
       bc.se_good_will__o,
       bc.refund_channel,
       bc.refund_type,
       bc.who_pays,
       bc.cancel_with_provider,
       bc.status,
       bc.requested_by,
       SPLIT_PART(requested_by, '@', -1) AS requested_by_domain,
       bc.payment_provider_refund_status,
       bc.failed_some_validation,
       bc.fails_validation__id__expected_nonnull,
       bc.fails_validation__date_created__expected_nonnull
FROM hygiene_vault_mvp.cms_mysql.booking_cancellation bc;

CREATE OR REPLACE TABLE hygiene_vault_mvp.cms_mysql.booking_cancellation CLONE hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation_temp;

------------------------------------------------------------------------------------------------------------------------
--update history of snapshot

CREATE TABLE IF NOT EXISTS hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation_temp
(

    -- (lineage) metadata for the current job
    schedule_tstamp                TIMESTAMP,
    run_tstamp                     TIMESTAMP,
    operation_id                   VARCHAR,
    created_at                     TIMESTAMP,
    updated_at                     TIMESTAMP,

    -- (lineage) original metadata of row itself
    row_dataset_name               VARCHAR,
    row_dataset_source             VARCHAR,
    row_loaded_at                  TIMESTAMP,
    row_schedule_tstamp            TIMESTAMP,
    row_run_tstamp                 TIMESTAMP,
    row_filename                   VARCHAR,
    row_file_row_number            INT,
    row_extract_metadata           VARIANT,

    -- deduped columns from hygiene step
    booking_id                     VARCHAR,
    booking_fee_gbp                DOUBLE,
    cc_fee_gbp                     DOUBLE,
    hotel_good_will_gbp            DOUBLE,
    se_good_will_gbp               DOUBLE,
    id                             NUMBER PRIMARY KEY NOT NULL,
    version                        NUMBER,
    booking_id__o                  NUMBER,
    reservation_id__o              NUMBER,
    date_created                   TIMESTAMP,
    last_updated                   TIMESTAMP,
    fault                          VARCHAR,
    reason                         VARCHAR,
    booking_fee__o                 FLOAT,
    cc_fee__o                      FLOAT,
    hotel_good_will__o             FLOAT,
    se_good_will__o                FLOAT,
    refund_channel                 VARCHAR,
    refund_type                    VARCHAR,
    who_pays                       VARCHAR,
    cancel_with_provider           BOOLEAN,
    status                         VARCHAR,
    requested_by                   VARCHAR,
    requested_by_domain            VARCHAR,
    payment_provider_refund_status VARCHAR
);

INSERT INTO hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation_temp
SELECT bc.schedule_tstamp,
       bc.run_tstamp,
       bc.operation_id,
       bc.created_at,
       bc.updated_at,
       bc.row_dataset_name,
       bc.row_dataset_source,
       bc.row_loaded_at,
       bc.row_schedule_tstamp,
       bc.row_run_tstamp,
       bc.row_filename,
       bc.row_file_row_number,
       bc.row_extract_metadata,
       bc.booking_id,
       bc.booking_fee_gbp,
       bc.cc_fee_gbp,
       bc.hotel_good_will_gbp,
       bc.se_good_will_gbp,
       bc.id,
       bc.version,
       bc.booking_id__o,
       bc.reservation_id__o,
       bc.date_created,
       bc.last_updated,
       bc.fault,
       bc.reason,
       bc.booking_fee__o,
       bc.cc_fee__o,
       bc.hotel_good_will__o,
       bc.se_good_will__o,
       bc.refund_channel,
       bc.refund_type,
       bc.who_pays,
       bc.cancel_with_provider,
       bc.status,
       bc.requested_by,
       SPLIT_PART(requested_by, '@', -1) AS requested_by_domain,
       bc.payment_provider_refund_status
FROM hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation bc;

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation CLONE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation_temp;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation bc;

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2021-01-06 00:00:00' --end '2021-01-06 00:00:00'




