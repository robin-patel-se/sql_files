SELECT *
FROM hygiene_vault_mvp_dev_robin.worldpay.transaction_summary;


SELECT bs.record__o['offerName']::VARCHAR,
       record__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs;

SELECT *
FROM data_vault_mvp.dwh.se_booking;


--rooms
--supplier name
--offername

SELECT *
FROM se.data.se_sale_attributes
WHERE se_sale_attributes.product_configuration = 'Hotel'
  AND se_sale_attributes.data_model = 'New Data Model'

SELECT *
FROM data_vault_mvp.dwh.se_booking sb


CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE raw_vault_mvp.cms_mongodb.booking_summary;

DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary;
self_describing_task --include 'staging/hygiene/cms_mongodb/booking_summary'  --method 'run' --start '2020-07-02 00:00:00' --end '2020-07-02 00:00:00'


SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary__step02__extract_data;
SELECT currency,
       sale_base_currency,
       commission_ex_vat_cc,
       commission_ex_vat_sc,
       rate_to_gbp,
       cc_rate_to_sc,
       gbp_rate_to_sc
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary__step03__apply_hygiene;


SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary__step04__apply_hygiene;
SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary__step05__apply_validation_rules

SELECT currency,
       sale_base_currency,
       commission_ex_vat_cc,
       commission_ex_vat_gbp,
       commission_ex_vat_sc,
       rate_to_gbp,
       cc_rate_to_sc,
       gbp_rate_to_sc
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary__step05__apply_validation_rules;

CREATE TRANSIENT TABLE cms_mongodb.booking_summary__step05__apply_validation_rules
(
    schedule_tstamp                                              VARCHAR(19),
    run_tstamp                                                   VARCHAR(19),
    operation_id                                                 VARCHAR(145),
    created_at                                                   TIMESTAMPNTZ,
    updated_at                                                   TIMESTAMPNTZ,
    row_dataset_name                                             VARCHAR,
    row_dataset_source                                           VARCHAR,
    row_loaded_at                                                TIMESTAMPNTZ,
    row_schedule_tstamp                                          TIMESTAMPNTZ,
    row_run_tstamp                                               TIMESTAMPNTZ,
    row_filename                                                 VARCHAR,
    row_file_row_number                                          NUMBER,

    no_nights                                                    NUMBER,
    rooms                                                        NUMBER,
    adult_guests                                                 NUMBER,
    child_guests                                                 NUMBER,
    infant_guests                                                NUMBER,
    rate_to_gbp                                                  NUMBER(38, 6),
    cc_rate_to_sc                                                DOUBLE,
    gbp_rate_to_sc                                               NUMBER(35, 11),
    last_updated                                                 TIMESTAMPNTZ,
    date_time_booked                                             TIMESTAMPNTZ,
    booking_date                                                 DATE,
    check_in_timestamp                                           TIMESTAMPNTZ,
    check_in_date                                                DATE,
    check_out_timestamp                                          TIMESTAMPNTZ,
    check_out_date                                               DATE,
    booking_lead_time_days                                       NUMBER(9),
    margin_gross_of_toms_cc                                      NUMBER(38, 6),
    gross_booking_value_cc                                       NUMBER(38, 6),
    vat_on_commission_cc                                         NUMBER(38, 6),
    booking_fee_net_rate_cc                                      NUMBER(38, 6),
    payment_surcharge_net_rate_cc                                NUMBER(38, 6),
    commission_ex_vat_cc                                         NUMBER(38, 6),
    insurance_commission_cc                                      NUMBER(38, 6),
    flight_commission_cc                                         NUMBER,
    margin_gross_of_toms_gbp                                     NUMBER(38, 12),
    gross_booking_value_gbp                                      NUMBER(38, 12),
    vat_on_commission_gbp                                        NUMBER(38, 12),
    booking_fee_net_rate_gbp                                     NUMBER(38, 12),
    payment_surcharge_net_rate_gbp                               NUMBER(38, 12),
    commission_ex_vat_gbp                                        NUMBER(38, 12),
    insurance_commission_gbp                                     NUMBER(38, 12),
    flight_commission_gbp                                        NUMBER(38, 6),
    margin_gross_of_toms_sc                                      DOUBLE,
    gross_booking_value_sc                                       DOUBLE,
    vat_on_commission_sc                                         DOUBLE,
    booking_fee_net_rate_sc                                      DOUBLE,
    payment_surcharge_net_rate_sc                                DOUBLE,
    commission_ex_vat_sc                                         DOUBLE,
    insurance_commission_sc                                      DOUBLE,
    flight_commission_sc                                         DOUBLE,
    is_new_model_booking                                         NUMBER(1),
    shiro_user_id                                                NUMBER,
    affiliate_user_id                                            NUMBER,
    booking_id                                                   VARCHAR,
    customer_id                                                  VARCHAR,
    currency                                                     VARCHAR,
    sale_base_currency                                           VARCHAR,
    territory                                                    VARCHAR,
    last_updated_v1                                              VARCHAR,
    last_updated_v2                                              VARCHAR,
    date_time_booked_v1                                          VARCHAR,
    date_time_booked_v2                                          VARCHAR,
    check_in_date_v1                                             VARCHAR,
    check_in_date_v2                                             VARCHAR,
    check_out_date_v1                                            VARCHAR,
    check_out_date_v2                                            VARCHAR,
    booking_type                                                 VARCHAR,
    no_nights__o                                                 VARCHAR,
    adult_guests__o                                              VARCHAR,
    child_guests__o                                              VARCHAR,
    infant_guests__o                                             VARCHAR,
    vat_on_commission_cc_100                                     VARCHAR,
    gross_booking_value_cc_100                                   VARCHAR,
    commission_ex_vat_cc_100                                     VARCHAR,
    booking_fee_net_rate_cc_100                                  VARCHAR,
    payment_surcharge_net_rate_cc_100                            VARCHAR,
    insurance_commission_cc_100                                  VARCHAR,
    flight_commission_cc_100                                     VARCHAR,
    rate_to_gbp_100000                                           VARCHAR,
    customer_email                                               VARCHAR,
    sale_type                                                    VARCHAR,
    booking_status                                               VARCHAR,
    affiliate                                                    VARCHAR,
    affiliate_domain                                             VARCHAR,
    booking_class                                                VARCHAR,
    affiliate_id                                                 VARCHAR,
    sale_id                                                      VARCHAR,
    offer_id                                                     VARCHAR,
    transaction_id                                               VARCHAR,
    bundle_id                                                    VARCHAR,
    unique_transaction_reference                                 VARCHAR,
    has_flights                                                  VARCHAR,
    supplier                                                     VARCHAR,
    offer_name                                                   VARCHAR,
    fails_validation__booking_id__expected_nonnull               NUMBER(1),
    fails_validation__customer_id__expected_nonnull              NUMBER(1),
    fails_validation_new_model_sale_type_expected_a              NUMBER(1),
    fails_validation__booking_date__expected_nonnull             NUMBER(1),
    fails_validation__margin_gross_of_toms_gbp__expected_nonzero NUMBER(1),
    failed_some_validation                                       NUMBER(1),
    record__o                                                    VARIANT
);

DROP TABLE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary;


SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary bs;

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary;

self_describing_task --include 'staging/hygiene_snapshots/cms_mongodb/booking_summary'  --method 'run' --start '2020-07-02 00:00:00' --end '2020-07-02 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary;

SELECT 'test' IS DISTINCT FROM NULL;

airflow backfill --start_date '2020-07-02 03:00:00' --end_date '2020-07-02 03:00:00' --task_regex '.*' single_customer_view__daily_at_03h00
airflow backfill --start_date '2020-07-03 03:00:00' --end_date '2020-07-05 03:00:00' --task_regex '.*' -m single_customer_view__daily_at_03h00

self_describing_task --include 'dv/dwh/transactional/se_booking'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- to update hygiene table.

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary_clone CLONE hygiene_vault_mvp.cms_mongodb.booking_summary;
DROP TABLE IF EXISTS hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary;
CREATE TABLE IF NOT EXISTS hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary
(

    -- (lineage) metadata for the current job
    schedule_tstamp                                              TIMESTAMP,
    run_tstamp                                                   TIMESTAMP,
    operation_id                                                 VARCHAR,
    created_at                                                   TIMESTAMP,
    updated_at                                                   TIMESTAMP,

    -- (lineage) original metadata columns from previous step
    row_dataset_name                                             VARCHAR,
    row_dataset_source                                           VARCHAR,
    row_loaded_at                                                TIMESTAMP,
    row_schedule_tstamp                                          TIMESTAMP,
    row_run_tstamp                                               TIMESTAMP,
    row_filename                                                 VARCHAR,
    row_file_row_number                                          INT,

    -- hygiened columns

    no_nights                                                    NUMBER,
    rooms                                                        NUMBER,
    adult_guests                                                 NUMBER,
    child_guests                                                 NUMBER,
    infant_guests                                                NUMBER,
    rate_to_gbp                                                  FLOAT,
    cc_rate_to_sc                                                FLOAT,
    gbp_rate_to_sc                                               FLOAT,

    last_updated                                                 TIMESTAMP,
    date_time_booked                                             TIMESTAMP,
    booking_date                                                 DATE,
    check_in_timestamp                                           TIMESTAMP,
    check_in_date                                                DATE,
    check_out_timestamp                                          TIMESTAMP,
    check_out_date                                               DATE,
    booking_lead_time_days                                       NUMBER,

    margin_gross_of_toms_cc                                      FLOAT,
    gross_booking_value_cc                                       FLOAT,
    vat_on_commission_cc                                         FLOAT,
    booking_fee_net_rate_cc                                      FLOAT,
    payment_surcharge_net_rate_cc                                FLOAT,
    commission_ex_vat_cc                                         FLOAT,
    insurance_commission_cc                                      FLOAT,
    flight_commission_cc                                         FLOAT,

    margin_gross_of_toms_gbp                                     FLOAT,
    gross_booking_value_gbp                                      FLOAT,
    vat_on_commission_gbp                                        FLOAT,
    booking_fee_net_rate_gbp                                     FLOAT,
    payment_surcharge_net_rate_gbp                               FLOAT,
    commission_ex_vat_gbp                                        FLOAT,
    insurance_commission_gbp                                     FLOAT,
    flight_commission_gbp                                        FLOAT,

    margin_gross_of_toms_sc                                      FLOAT,
    gross_booking_value_sc                                       FLOAT,
    vat_on_commission_sc                                         FLOAT,
    booking_fee_net_rate_sc                                      FLOAT,
    payment_surcharge_net_rate_sc                                FLOAT,
    commission_ex_vat_sc                                         FLOAT,
    insurance_commission_sc                                      FLOAT,
    flight_commission_sc                                         FLOAT,

    is_new_model_booking                                         NUMBER,
    affiliate_user_id                                            NUMBER,
    shiro_user_id                                                NUMBER,
    device_platform                                              VARCHAR,

    -- original columns (extracted from JSON)
    booking_id                                                   VARCHAR,
    customer_id                                                  VARCHAR,
    currency                                                     VARCHAR,
    sale_base_currency                                           VARCHAR,
    territory                                                    VARCHAR,
    last_updated_v1                                              VARCHAR,
    last_updated_v2                                              VARCHAR,
    date_time_booked_v1                                          VARCHAR,
    date_time_booked_v2                                          VARCHAR,
    check_in_date_v1                                             VARCHAR,
    check_in_date_v2                                             VARCHAR,
    check_out_date_v1                                            VARCHAR,
    check_out_date_v2                                            VARCHAR,
    booking_type                                                 VARCHAR,
    no_nights__o                                                 VARCHAR,
    rooms__o                                                     VARCHAR,
    adult_guests__o                                              VARCHAR,
    child_guests__o                                              VARCHAR,
    infant_guests__o                                             VARCHAR,
    vat_on_commission_cc_100                                     VARCHAR,
    gross_booking_value_cc_100                                   VARCHAR,
    commission_ex_vat_cc_100                                     VARCHAR,
    commission_ex_vat_sc_100                                     VARCHAR,
    booking_fee_net_rate_cc_100                                  VARCHAR,
    payment_surcharge_net_rate_cc_100                            VARCHAR,
    insurance_commission_cc_100                                  VARCHAR,
    flight_commission_cc_100                                     VARCHAR,
    rate_to_gbp_100000                                           VARCHAR,
    customer_email                                               VARCHAR,
    sale_type                                                    VARCHAR,
    booking_status                                               VARCHAR,
    affiliate                                                    VARCHAR,
    affiliate_domain                                             VARCHAR,
    booking_class                                                VARCHAR,
    affiliate_id                                                 VARCHAR,
    sale_id                                                      VARCHAR,
    offer_id                                                     VARCHAR,
    offer_name                                                   VARCHAR,
    transaction_id                                               VARCHAR,
    bundle_id                                                    VARCHAR,
    unique_transaction_reference                                 VARCHAR,
    has_flights                                                  VARCHAR,
    supplier                                                     VARCHAR,
    platform_name__o                                             VARCHAR,

    -- original columns that don't require any hygiene

    record__o                                                    VARIANT,

    -- hygiene flags
    failed_some_validation                                       INT,
    fails_validation__booking_id__expected_nonnull               INT,
    fails_validation__customer_id__expected_nonnull              INT,
    fails_validation_new_model_sale_type_expected_a              INT,
    fails_validation__booking_date__expected_nonnull             INT,
    fails_validation__margin_gross_of_toms_gbp__expected_nonzero INT

);

INSERT INTO hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary
SELECT bs.schedule_tstamp,
       bs.run_tstamp,
       bs.operation_id,
       bs.created_at,
       bs.updated_at,
       bs.row_dataset_name,
       bs.row_dataset_source,
       bs.row_loaded_at,
       bs.row_schedule_tstamp,
       bs.row_run_tstamp,
       bs.row_filename,
       bs.row_file_row_number,
       bs.no_nights,
       TRY_TO_NUMBER(record__o['rooms']::VARCHAR)              AS no_rooms,
       bs.adult_guests,
       bs.child_guests,
       bs.infant_guests,
       bs.rate_to_gbp,
       record__o['commissionExVatInSupplierCurrency']::VARCHAR /
       NULLIF(commission_ex_vat_cc_100, 0)                     AS cc_rate_to_sc,
       record__o['commissionExVatInSupplierCurrency']::VARCHAR /
       (NULLIF(commission_ex_vat_cc_100, 0) * rate_to_gbp)     AS gbp_rate_to_sc,
       bs.last_updated,
       bs.date_time_booked,
       bs.booking_date,
       bs.check_in_timestamp,
       bs.check_in_date,
       bs.check_out_timestamp,
       bs.check_out_date,
       bs.booking_lead_time_days,

       bs.gross_booking_value_cc,
       (
               booking_fee_net_rate_cc +
               payment_surcharge_net_rate_cc +
               commission_ex_vat_cc +
               insurance_commission_cc
           )                                                   AS margin_gross_of_toms_cc,
       bs.vat_on_commission_cc,
       bs.commission_ex_vat_cc,
       bs.booking_fee_net_rate_cc,
       bs.payment_surcharge_net_rate_cc,
       bs.insurance_commission_cc,
       bs.flight_commission_cc,

       bs.margin_gross_of_toms_gbp,
       bs.gross_booking_value_gbp,
       bs.vat_on_commission_gbp,
       bs.booking_fee_net_rate_gbp,
       bs.payment_surcharge_net_rate_gbp,
       bs.commission_ex_vat_gbp,
       bs.insurance_commission_gbp,
       bs.flight_commission_gbp,

       bs.gross_booking_value_cc * cc_rate_to_sc               AS gross_booking_value_sc,
       bs.vat_on_commission_cc * cc_rate_to_sc                 AS vat_on_commission_sc,
       bs.booking_fee_net_rate_cc * cc_rate_to_sc              AS booking_fee_net_rate_sc,
       bs.payment_surcharge_net_rate_cc * cc_rate_to_sc        AS payment_surcharge_net_rate_sc,
       bs.commission_ex_vat_cc * cc_rate_to_sc                 AS commission_ex_vat_sc,
       bs.insurance_commission_cc * cc_rate_to_sc              AS insurance_commission_sc,
       bs.flight_commission_cc * cc_rate_to_sc                 AS flight_commission_sc,
       (
               booking_fee_net_rate_sc +
               payment_surcharge_net_rate_sc +
               commission_ex_vat_sc +
               insurance_commission_sc
           )                                                   AS margin_gross_of_toms_sc,

       bs.is_new_model_booking,
       bs.affiliate_user_id,
       bs.shiro_user_id,
       CASE
           WHEN record__o['platformName']::VARCHAR = 'IOS_APP' THEN 'native app'
           WHEN record__o['platformName']::VARCHAR = 'WEB' THEN 'web'
           WHEN record__o['platformName']::VARCHAR = 'TABLET_WEB' THEN 'tablet web'
           WHEN record__o['platformName']::VARCHAR = 'MOBILE_WEB' THEN 'mobile web'
           WHEN record__o['platformName']::VARCHAR = 'MOBILE_WRAP_IOS' THEN 'mobile wrap ios'
           WHEN record__o['platformName']::VARCHAR = 'MOBILE_WRAP_ANDROID' THEN 'mobile wrap android'
           WHEN record__o['platformName']::VARCHAR = 'ANDROID_APP' THEN 'mobile wrap android'
           WHEN record__o['platformName']::VARCHAR = 'IOS_APP_V3' THEN 'native app'
           ELSE 'not specified'
           END                                                 AS device_platform,

       bs.booking_id,
       bs.customer_id,
       bs.currency,
       record__o['saleBaseCurrency']::VARCHAR                  AS sale_base_currency,
       bs.territory,
       bs.last_updated_v1,
       bs.last_updated_v2,
       bs.date_time_booked_v1,
       bs.date_time_booked_v2,
       bs.check_in_date_v1,
       bs.check_in_date_v2,
       bs.check_out_date_v1,
       bs.check_out_date_v2,
       bs.booking_type,
       bs.no_nights__o,
       bs.record__o['rooms']::VARCHAR                          AS rooms__o,
       bs.adult_guests__o,
       bs.child_guests__o,
       bs.infant_guests__o,
       bs.vat_on_commission_cc_100,
       bs.gross_booking_value_cc_100,
       bs.commission_ex_vat_cc_100,
       record__o['commissionExVatInSupplierCurrency']::VARCHAR AS commission_ex_vat_sc_100,
       bs.booking_fee_net_rate_cc_100,
       bs.payment_surcharge_net_rate_cc_100,
       bs.insurance_commission_cc_100,
       bs.flight_commission_cc_100,
       bs.rate_to_gbp_100000,
       bs.customer_email,
       bs.sale_type,
       bs.booking_status,
       bs.affiliate,
       bs.affiliate_domain,
       bs.booking_class,
       bs.affiliate_id,
       bs.sale_id,
       bs.offer_id,
       record__o['offerName']::VARCHAR                         AS offer_name,
       bs.transaction_id,
       bs.bundle_id,
       bs.unique_transaction_reference,
       bs.has_flights,
       record__o['supplier']::VARCHAR                          AS supplier,
       record__o['platformName']::VARCHAR                      AS platform_name__o,
       bs.record__o,
       bs.failed_some_validation,
       bs.fails_validation__booking_id__expected_nonnull,
       bs.fails_validation__customer_id__expected_nonnull,
       bs.fails_validation_new_model_sale_type_expected_a,
       bs.fails_validation__booking_date__expected_nonnull,
       bs.fails_validation__margin_gross_of_toms_gbp__expected_nonzero
FROM hygiene_vault_mvp.cms_mongodb.booking_summary_clone bs;

SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary bs;

------------------------------------------------------------------------------------------------------------------------
--to update hygiene snapshot table

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary_clone CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary
(

    -- (lineage) metadata for the current job
    schedule_tstamp                   TIMESTAMP,
    run_tstamp                        TIMESTAMP,
    operation_id                      VARCHAR,
    created_at                        TIMESTAMP,
    updated_at                        TIMESTAMP,

    -- (lineage) original metadata of row itself
    row_dataset_name                  VARCHAR,
    row_dataset_source                VARCHAR,
    row_loaded_at                     TIMESTAMP,
    row_schedule_tstamp               TIMESTAMP,
    row_run_tstamp                    TIMESTAMP,
    row_filename                      VARCHAR,
    row_file_row_number               INT,

    -- deduped columns from hygiene step

    no_nights                         NUMBER,
    rooms                             NUMBER,
    adult_guests                      NUMBER,
    child_guests                      NUMBER,
    infant_guests                     NUMBER,
    rate_to_gbp                       FLOAT,
    cc_rate_to_sc                     FLOAT,
    gbp_rate_to_sc                    FLOAT,

    last_updated                      TIMESTAMP,
    date_time_booked                  TIMESTAMP,
    booking_date                      DATE,
    check_in_timestamp                TIMESTAMP,
    check_in_date                     DATE,
    check_out_timestamp               TIMESTAMP,
    check_out_date                    DATE,
    booking_lead_time_days            NUMBER,

    margin_gross_of_toms_cc           FLOAT,
    gross_booking_value_cc            FLOAT,
    vat_on_commission_cc              FLOAT,
    booking_fee_net_rate_cc           FLOAT,
    payment_surcharge_net_rate_cc     FLOAT,
    commission_ex_vat_cc              FLOAT,
    insurance_commission_cc           FLOAT,
    flight_commission_cc              FLOAT,

    margin_gross_of_toms_gbp          FLOAT,
    gross_booking_value_gbp           FLOAT,
    vat_on_commission_gbp             FLOAT,
    booking_fee_net_rate_gbp          FLOAT,
    payment_surcharge_net_rate_gbp    FLOAT,
    commission_ex_vat_gbp             FLOAT,
    insurance_commission_gbp          FLOAT,
    flight_commission_gbp             FLOAT,

    margin_gross_of_toms_sc           FLOAT,
    gross_booking_value_sc            FLOAT,
    vat_on_commission_sc              FLOAT,
    booking_fee_net_rate_sc           FLOAT,
    payment_surcharge_net_rate_sc     FLOAT,
    commission_ex_vat_sc              FLOAT,
    insurance_commission_sc           FLOAT,
    flight_commission_sc              FLOAT,

    is_new_model_booking              NUMBER,
    affiliate_user_id                 NUMBER,
    shiro_user_id                     NUMBER,
    device_platform                   VARCHAR,

    booking_id                        VARCHAR NOT NULL,
    customer_id                       VARCHAR,
    currency                          VARCHAR,
    sale_base_currency                VARCHAR,
    territory                         VARCHAR,
    last_updated_v1                   VARCHAR,
    last_updated_v2                   VARCHAR,
    date_time_booked_v1               VARCHAR,
    date_time_booked_v2               VARCHAR,
    check_in_date_v1                  VARCHAR,
    check_in_date_v2                  VARCHAR,
    check_out_date_v1                 VARCHAR,
    check_out_date_v2                 VARCHAR,
    booking_type                      VARCHAR,
    no_nights__o                      VARCHAR,
    rooms__o                          VARCHAR,
    adult_guests__o                   VARCHAR,
    child_guests__o                   VARCHAR,
    infant_guests__o                  VARCHAR,
    vat_on_commission_cc_100          VARCHAR,
    gross_booking_value_cc_100        VARCHAR,
    commission_ex_vat_cc_100          VARCHAR,
    commission_ex_vat_sc_100          VARCHAR,
    booking_fee_net_rate_cc_100       VARCHAR,
    payment_surcharge_net_rate_cc_100 VARCHAR,
    insurance_commission_cc_100       VARCHAR,
    flight_commission_cc_100          VARCHAR,
    rate_to_gbp_100000                VARCHAR,
    customer_email                    VARCHAR,
    sale_type                         VARCHAR,
    booking_status                    VARCHAR,
    affiliate                         VARCHAR,
    affiliate_domain                  VARCHAR,
    booking_class                     VARCHAR,
    affiliate_id                      VARCHAR,
    sale_id                           VARCHAR,
    offer_id                          VARCHAR,
    offer_name                        VARCHAR,
    transaction_id                    VARCHAR,
    bundle_id                         VARCHAR,
    unique_transaction_reference      VARCHAR,
    has_flights                       VARCHAR,
    supplier                          VARCHAR,
    platform_name__o                  VARCHAR,
    record__o                         VARIANT

);

INSERT INTO hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary
SELECT bs.schedule_tstamp,
       bs.run_tstamp,
       bs.operation_id,
       bs.created_at,
       bs.updated_at,
       bs.row_dataset_name,
       bs.row_dataset_source,
       bs.row_loaded_at,
       bs.row_schedule_tstamp,
       bs.row_run_tstamp,
       bs.row_filename,
       bs.row_file_row_number,
       bs.no_nights,
       TRY_TO_NUMBER(record__o['rooms']::VARCHAR)              AS no_rooms,
       bs.adult_guests,
       bs.child_guests,
       bs.infant_guests,
       bs.rate_to_gbp,
       record__o['commissionExVatInSupplierCurrency']::VARCHAR /
       NULLIF(commission_ex_vat_cc_100, 0)                     AS cc_rate_to_sc,
       record__o['commissionExVatInSupplierCurrency']::VARCHAR /
       (NULLIF(commission_ex_vat_cc_100, 0) * rate_to_gbp)     AS gbp_rate_to_sc,
       bs.last_updated,
       bs.date_time_booked,
       bs.booking_date,
       bs.check_in_timestamp,
       bs.check_in_date,
       bs.check_out_timestamp,
       bs.check_out_date,
       bs.booking_lead_time_days,


       (
               booking_fee_net_rate_cc +
               payment_surcharge_net_rate_cc +
               commission_ex_vat_cc +
               insurance_commission_cc
           )                                                   AS margin_gross_of_toms_cc,
       bs.gross_booking_value_cc,
       bs.vat_on_commission_cc,
       bs.commission_ex_vat_cc,
       bs.booking_fee_net_rate_cc,
       bs.payment_surcharge_net_rate_cc,
       bs.insurance_commission_cc,
       bs.flight_commission_cc,

       bs.margin_gross_of_toms_gbp,
       bs.gross_booking_value_gbp,
       bs.vat_on_commission_gbp,
       bs.booking_fee_net_rate_gbp,
       bs.payment_surcharge_net_rate_gbp,
       bs.commission_ex_vat_gbp,
       bs.insurance_commission_gbp,
       bs.flight_commission_gbp,

       (
               bs.booking_fee_net_rate_cc * cc_rate_to_sc +
               bs.payment_surcharge_net_rate_cc * cc_rate_to_sc +
               bs.commission_ex_vat_cc * cc_rate_to_sc +
               bs.insurance_commission_cc * cc_rate_to_sc
           )                                                   AS margin_gross_of_toms_sc,
       bs.gross_booking_value_cc * cc_rate_to_sc               AS gross_booking_value_sc,
       bs.vat_on_commission_cc * cc_rate_to_sc                 AS vat_on_commission_sc,
       bs.booking_fee_net_rate_cc * cc_rate_to_sc              AS booking_fee_net_rate_sc,
       bs.payment_surcharge_net_rate_cc * cc_rate_to_sc        AS payment_surcharge_net_rate_sc,
       bs.commission_ex_vat_cc * cc_rate_to_sc                 AS commission_ex_vat_sc,
       bs.insurance_commission_cc * cc_rate_to_sc              AS insurance_commission_sc,
       bs.flight_commission_cc * cc_rate_to_sc                 AS flight_commission_sc,


       bs.is_new_model_booking,
       bs.affiliate_user_id,
       bs.shiro_user_id,
       CASE
           WHEN record__o['platformName']::VARCHAR = 'IOS_APP' THEN 'native app'
           WHEN record__o['platformName']::VARCHAR = 'WEB' THEN 'web'
           WHEN record__o['platformName']::VARCHAR = 'TABLET_WEB' THEN 'tablet web'
           WHEN record__o['platformName']::VARCHAR = 'MOBILE_WEB' THEN 'mobile web'
           WHEN record__o['platformName']::VARCHAR = 'MOBILE_WRAP_IOS' THEN 'mobile wrap ios'
           WHEN record__o['platformName']::VARCHAR = 'MOBILE_WRAP_ANDROID' THEN 'mobile wrap android'
           WHEN record__o['platformName']::VARCHAR = 'ANDROID_APP' THEN 'mobile wrap android'
           WHEN record__o['platformName']::VARCHAR = 'IOS_APP_V3' THEN 'native app'
           ELSE 'not specified'
           END                                                 AS device_platform,
       bs.booking_id,
       bs.customer_id,
       bs.currency,
       record__o['saleBaseCurrency']::VARCHAR                  AS sale_base_currency,
       bs.territory,
       bs.last_updated_v1,
       bs.last_updated_v2,
       bs.date_time_booked_v1,
       bs.date_time_booked_v2,
       bs.check_in_date_v1,
       bs.check_in_date_v2,
       bs.check_out_date_v1,
       bs.check_out_date_v2,
       bs.booking_type,
       bs.no_nights__o,
       record__o['rooms']::VARCHAR                             AS rooms__o,
       bs.adult_guests__o,
       bs.child_guests__o,
       bs.infant_guests__o,
       bs.vat_on_commission_cc_100,
       bs.gross_booking_value_cc_100,
       bs.commission_ex_vat_cc_100,
       record__o['commissionExVatInSupplierCurrency']::VARCHAR AS commission_ex_vat_sc_100,
       bs.booking_fee_net_rate_cc_100,
       bs.payment_surcharge_net_rate_cc_100,
       bs.insurance_commission_cc_100,
       bs.flight_commission_cc_100,
       bs.rate_to_gbp_100000,
       bs.customer_email,
       bs.sale_type,
       bs.booking_status,
       bs.affiliate,
       bs.affiliate_domain,
       bs.booking_class,
       bs.affiliate_id,
       bs.sale_id,
       bs.offer_id,
       record__o['offerName']::VARCHAR                         AS offer_name,
       bs.transaction_id,
       bs.bundle_id,
       bs.unique_transaction_reference,
       bs.has_flights,
       record__o['supplier']::VARCHAR                          AS supplier,
       record__o['platformName']::VARCHAR                      AS platform_name__o,
       bs.record__o
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary_clone bs;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary bs;

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;

SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary bs;
SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary bs;

SELECT count(*)
FROM data_vault_mvp.dwh.se_booking sb;
SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
WHERE sb.booking_status = 'COMPLETE';



SELECT se.data.posa_category_from_territory(sat.posa_territory)                           AS posa_territory,
       CASE WHEN class = 'com.flashsales.sale.WebRedirectSale' THEN 'WRD' ELSE sat.posu_country END AS posu_country,
       sa.view_date,
       COUNT(DISTINCT (sat.salesforce_opportunity_id))                                              AS global_sale_ids,
       COUNT(sa.se_sale_id)                                                                         AS sale_id
FROM se.data.se_sale_attributes sat
         INNER JOIN data_vault_mvp.dwh.sale_active sa ON sa.se_sale_id = sat.se_sale_id
WHERE sa.active
  AND sat.data_model = 'New Data Model'
GROUP BY 1, 2, 3


CREATE OR REPLACE TABLE hygiene_vault_mvp.cms_mongodb.booking_summary clone hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary clone hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary;