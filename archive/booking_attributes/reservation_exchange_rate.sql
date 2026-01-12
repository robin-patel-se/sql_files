CREATE TABLE reservation_exchange_rate
(
    id                                       INT,
    version                                  bigint,
    date_created                             datetime,
    exchange_rate_last_updated_from_provider datetime,
    from_currency                            varchar,
    last_updated                             datetime,
    rate                                     decimal,
    to_currency                              varchar
);


dataset_task --include 'cms_mysql.reservation_exchange_rate' --operation ProductionIngestOperation --method 'run' --upstream --start '2021-01-06 00:30:00' --end '2021-01-06 00:30:00'

self_describing_task --include 'dv/cms_snapshots/cms_mysql_snapshot_bulk_wave3.py'  --method 'run' --start '2021-01-06 00:00:00' --end '2021-01-06 00:00:00'

SELECT COUNT(*)
FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.reservation_exchange_rate_snapshot;

SELECT sb.booking_id,
       sb.booking_completed_date,
       sb.currency,
       sb.sale_base_currency,
       rcs.*
FROM se.data.se_booking sb
         INNER JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.reservation_exchange_rate_snapshot rcs
                    ON sb.booking_id = 'A' || rcs.id
WHERE sb.booking_status = 'COMPLETE';


self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2021-01-06 00:00:00' --end '2021-01-06 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.amendment_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.amendment_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_reservation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_reservation_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.booking_cancellation CLONE data_vault_mvp.dwh.booking_cancellation;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.days_before_policy_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.days_before_policy_snapshot;


dataset_task --include 'cms_mysql.reservation' --operation ProductionIngestOperation --method 'run' --upstream --start '2020-12-17 00:30:00' --end '2020-12-17 00:30:00'

--need to backfill extract ingest to: 2020-12-17 13:27:13

self_describing_task --include 'staging/hygiene/cms_mysql/reservation.py'  --method 'run' --start '2021-01-07 00:00:00' --end '2021-01-07 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/reservation.py'  --method 'run' --start '2021-01-07 00:00:00' --end '2021-01-07 00:00:00';

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2021-01-07 00:00:00' --end '2021-01-07 00:00:00';


SELECT r.booking_id,
       r.completion_date,
       r.currency,
       sb.sale_base_currency,
       rers.to_currency,
       rers.from_currency,
       rers.rate


FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation r
         INNER JOIN se.data.se_booking sb ON r.booking_id = sb.booking_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.reservation_exchange_rate_snapshot rers
                    ON r.supplier_to_user_currency_exchange_rate_id = rers.id
WHERE r.currency != sb.sale_base_currency;

--dev
SELECT sb.booking_id,
       sb.booking_completed_date,
       sb.currency,
       sb.margin_gross_of_toms_cc,
       sb.margin_gross_of_toms_gbp,
       sb.sale_base_currency,
       sb.margin_gross_of_toms_sc,
       sb.cc_rate_to_gbp,
       sb.cc_rate_to_sc,
       sb.gbp_rate_to_sc
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
WHERE sb.booking_id IN ('A3002984',
                        'A3002468',
                        'A3001732',
                        'A2999323',
                        'A2997289',
                        'A2996870',
                        'A2996467',
                        'A2996190',
                        'A2995340',
                        'A2995159',
                        'A2994824',
                        'A2994220',
                        'A2989479',
                        'A2989039',
                        'A2988559',
                        'A2984419',
                        'A2978337',
                        'A2976130',
                        'A2975477',
                        'A2974791',
                        'A2974774',
                        'A2971736',
                        'A2970809',
                        'A2970111',
                        'A2965745',
                        'A2965538',
                        'A2963313',
                        'A2962524',
                        'A2959601',
                        'A2959052',
                        'A2958026',
                        'A2957597'
    );

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking sb;


--prod
SELECT sb.booking_id,
       sb.booking_completed_date,
       sb.currency,
       sb.margin_gross_of_toms_cc,
       sb.margin_gross_of_toms_gbp,
       sb.sale_base_currency,
       sb.margin_gross_of_toms_sc,
       sb.cc_rate_to_gbp,
       sb.cc_rate_to_sc,
       sb.gbp_rate_to_sc
FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.booking_id IN ('A3002984',
                        'A3002468',
                        'A3001732',
                        'A2999323',
                        'A2997289',
                        'A2996870',
                        'A2996467',
                        'A2996190',
                        'A2995340',
                        'A2995159',
                        'A2994824',
                        'A2994220',
                        'A2989479',
                        'A2989039',
                        'A2988559',
                        'A2984419',
                        'A2978337',
                        'A2976130',
                        'A2975477',
                        'A2974791',
                        'A2974774',
                        'A2971736',
                        'A2970809',
                        'A2970111',
                        'A2965745',
                        'A2965538',
                        'A2963313',
                        'A2962524',
                        'A2959601',
                        'A2959052',
                        'A2958026',
                        'A2957597'
    );


SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot bcs
WHERE bcs.booking_id = 'A1214759';

SELECT *
FROM se.data.se_booking sb
WHERE sb.booking_id = 'A1214759';
SELECT *
FROM se.data.master_se_booking_list msbl
WHERE msbl.booking_id = 'A1214759';


--dev summary
SELECT sb.booking_completed_date::DATE AS date,
       SUM(sb.margin_gross_of_toms_cc),
       SUM(sb.margin_gross_of_toms_gbp),
       SUM(sb.margin_gross_of_toms_sc)
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
WHERE sb.booking_completed_date::DATE IN ('2021-01-06', '2020-11-01')
GROUP BY 1;


--prod summary
SELECT sb.booking_completed_date::DATE AS date,
       SUM(sb.margin_gross_of_toms_cc),
       SUM(sb.margin_gross_of_toms_gbp),
       SUM(sb.margin_gross_of_toms_sc)
FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.booking_completed_date::DATE IN ('2021-01-06', '2020-11-01')
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--update historic hygiene tables

CREATE TABLE IF NOT EXISTS hygiene_vault_mvp_dev_robin.cms_mysql.reservation_temp
(

    -- (lineage) metadata for the current job
    schedule_tstamp                                                  TIMESTAMP,
    run_tstamp                                                       TIMESTAMP,
    operation_id                                                     VARCHAR,
    created_at                                                       TIMESTAMP,
    updated_at                                                       TIMESTAMP,

    -- (lineage) original metadata columns from previous step
    row_dataset_name                                                 VARCHAR,
    row_dataset_source                                               VARCHAR,
    row_loaded_at                                                    TIMESTAMP,
    row_schedule_tstamp                                              TIMESTAMP,
    row_run_tstamp                                                   TIMESTAMP,
    row_filename                                                     VARCHAR,
    row_file_row_number                                              INT,

    -- hygiened columns

    booking_id                                                       VARCHAR,
    booking_date                                                     DATE,
    check_in_date                                                    DATE,
    check_out_date                                                   DATE,
    sale_id                                                          VARCHAR,

    -- original columns that don't require any hygiene
    id                                                               NUMBER,
    version                                                          NUMBER,
    affiliate_user_id                                                NUMBER,
    agency                                                           NUMBER,
    booking_fee                                                      FLOAT,
    check_in                                                         TIMESTAMP,
    check_out                                                        TIMESTAMP,
    completion_date                                                  TIMESTAMP,
    credits                                                          FLOAT,
    currency                                                         VARCHAR,
    date_created                                                     TIMESTAMP,
    last_updated                                                     TIMESTAMP,
    passenger_first_name                                             VARCHAR,
    passenger_last_name                                              VARCHAR,
    passenger_phone_number                                           VARCHAR,
    payment_id                                                       NUMBER,
    sale_id__o                                                       NUMBER,
    status                                                           VARCHAR,
    surname                                                          VARCHAR,
    type                                                             VARCHAR,
    unique_transaction_reference                                     VARCHAR,
    user_id                                                          NUMBER,
    agent_id                                                         VARCHAR,
    passenger_address1                                               VARCHAR,
    passenger_address2                                               VARCHAR,
    passenger_city_name                                              VARCHAR,
    passenger_country_name                                           VARCHAR,
    passenger_postcode                                               VARCHAR,
    vcc_enabled                                                      BOOLEAN,
    cancellation_policy_id                                           BIGINT,
    supplier_to_user_currency_exchange_rate_id                       BIGINT,

    -- hygiene flags
    failed_some_validation                                           INT,
    fails_validation__id__expected_nonnull                           INT,
    fails_validation__unique_transaction_reference__expected_nonnull INT,
    fails_validation__status__expected_nonnull                       INT,
    fails_validation__type__expected_nonnull                         INT,
    fails_validation__user_id__expected_nonnull                      INT,
    fails_validation__sale_id__expected_nonnull                      INT
);

INSERT INTO hygiene_vault_mvp_dev_robin.cms_mysql.reservation_temp
SELECT r.schedule_tstamp,
       r.run_tstamp,
       r.operation_id,
       r.created_at,
       r.updated_at,
       r.row_dataset_name,
       r.row_dataset_source,
       r.row_loaded_at,
       r.row_schedule_tstamp,
       r.row_run_tstamp,
       r.row_filename,
       r.row_file_row_number,
       r.booking_id,
       r.booking_date,
       r.check_in_date,
       r.check_out_date,
       r.sale_id,
       r.id,
       r.version,
       r.affiliate_user_id,
       r.agency,
       r.booking_fee,
       r.check_in,
       r.check_out,
       r.completion_date,
       r.credits,
       r.currency,
       r.date_created,
       r.last_updated,
       r.passenger_first_name,
       r.passenger_last_name,
       r.passenger_phone_number,
       r.payment_id,
       r.sale_id__o,
       r.status,
       r.surname,
       r.type,
       r.unique_transaction_reference,
       r.user_id,
       r.agent_id,
       r.passenger_address1,
       r.passenger_address2,
       r.passenger_city_name,
       r.passenger_country_name,
       r.passenger_postcode,
       r.vcc_enabled,
       r.cancellation_policy_id,
       NULL AS supplier_to_user_currency_exchange_rate_id,
       r.failed_some_validation,
       r.fails_validation__id__expected_nonnull,
       r.fails_validation__unique_transaction_reference__expected_nonnull,
       r.fails_validation__status__expected_nonnull,
       r.fails_validation__type__expected_nonnull,
       r.fails_validation__user_id__expected_nonnull,
       r.fails_validation__sale_id__expected_nonnull
FROM hygiene_vault_mvp.cms_mysql.reservation r;

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_vault_mvp_dev_robin.cms_mysql.reservation_temp;

------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_temp
(

    -- (lineage) metadata for the current job
    schedule_tstamp                            TIMESTAMP,
    run_tstamp                                 TIMESTAMP,
    operation_id                               VARCHAR,
    created_at                                 TIMESTAMP,
    updated_at                                 TIMESTAMP,

    -- (lineage) original metadata of row itself
    row_dataset_name                           VARCHAR,
    row_dataset_source                         VARCHAR,
    row_loaded_at                              TIMESTAMP,
    row_schedule_tstamp                        TIMESTAMP,
    row_run_tstamp                             TIMESTAMP,
    row_filename                               VARCHAR,
    row_file_row_number                        INT,

    -- deduped columns from hygiene step

    booking_id                                 VARCHAR PRIMARY KEY NOT NULL,
    booking_date                               DATE,
    check_in_date                              DATE,
    check_out_date                             DATE,
    id                                         NUMBER,
    version                                    NUMBER,
    affiliate_user_id                          NUMBER,
    agency                                     NUMBER,
    booking_fee                                FLOAT,
    check_in                                   TIMESTAMP,
    check_out                                  TIMESTAMP,
    completion_date                            TIMESTAMP,
    credits                                    FLOAT,
    currency                                   VARCHAR,
    date_created                               TIMESTAMP,
    last_updated                               TIMESTAMP,
    passenger_first_name                       VARCHAR,
    passenger_last_name                        VARCHAR,
    passenger_phone_number                     VARCHAR,
    payment_id                                 NUMBER,
    sale_id                                    VARCHAR,
    sale_id__o                                 NUMBER,
    status                                     VARCHAR,
    surname                                    VARCHAR,
    type                                       VARCHAR,
    unique_transaction_reference               VARCHAR,
    user_id                                    NUMBER,
    agent_id                                   VARCHAR,
    passenger_address1                         VARCHAR,
    passenger_address2                         VARCHAR,
    passenger_city_name                        VARCHAR,
    passenger_country_name                     VARCHAR,
    passenger_postcode                         VARCHAR,
    vcc_enabled                                BOOLEAN,
    cancellation_policy_id                     BIGINT,
    supplier_to_user_currency_exchange_rate_id BIGINT

);

INSERT INTO hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_temp
SELECT r.schedule_tstamp,
       r.run_tstamp,
       r.operation_id,
       r.created_at,
       r.updated_at,
       r.row_dataset_name,
       r.row_dataset_source,
       r.row_loaded_at,
       r.row_schedule_tstamp,
       r.row_run_tstamp,
       r.row_filename,
       r.row_file_row_number,
       r.booking_id,
       r.booking_date,
       r.check_in_date,
       r.check_out_date,
       r.id,
       r.version,
       r.affiliate_user_id,
       r.agency,
       r.booking_fee,
       r.check_in,
       r.check_out,
       r.completion_date,
       r.credits,
       r.currency,
       r.date_created,
       r.last_updated,
       r.passenger_first_name,
       r.passenger_last_name,
       r.passenger_phone_number,
       r.payment_id,
       r.sale_id,
       r.sale_id__o,
       r.status,
       r.surname,
       r.type,
       r.unique_transaction_reference,
       r.user_id,
       r.agent_id,
       r.passenger_address1,
       r.passenger_address2,
       r.passenger_city_name,
       r.passenger_country_name,
       r.passenger_postcode,
       r.vcc_enabled,
       r.cancellation_policy_id,
       NULL AS supplier_to_user_currency_exchange_rate_id
FROM hygiene_snapshot_vault_mvp.cms_mysql.reservation r;

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_temp;
