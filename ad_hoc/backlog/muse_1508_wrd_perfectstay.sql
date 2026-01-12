self_describing_task --include 'incoming/perfectstay/wrd_booking.json'  --method 'run' --start '2022-03-02 00:00:00' --end '2022-03-02 00:00:00'

dataset_task --include 'perfectstay.wrd_booking' --operation ExtractOperation --method 'run'  --start '2022-01-01 00:30:00' --end '2022-02-28 00:30:00'
dataset_task --include 'perfectstay.wrd_booking' --operation IngestOperation --method 'run'  --upstream --start '2022-01-01 00:30:00' --end '2022-02-26 00:30:00'
dataset_task --include 'perfectstay.wrd_booking' --operation HygieneOperation --method 'run'  --upstream --start '2021-01-01 00:30:00' --end '2022-02-26 00:30:00'

dataset_task --include 'perfectstay.wrd_booking' --operation IngestOperation --method 'run'  --upstream --start '2022-01-01 00:30:00' --end '2022-03-02 00:30:00'
dataset_task --include 'perfectstay.wrd_booking' --operation HygieneOperation --method 'run'  --start '2022-03-02 00:30:00' --end '2022-03-02 00:30:00'

SELECT *
FROM raw_vault_dev_robin.perfectstay.wrd_booking;
SELECT *
FROM hygiene_vault_dev_robin.perfectstay.wrd_booking;

SELECT *
FROM hygiene_vault.bedfinder.wrd_booking;

DROP TABLE raw_vault_dev_robin.perfectstay.wrd_booking;
DROP TABLE hygiene_vault_dev_robin.perfectstay.wrd_booking;
DROP TABLE latest_vault_dev_robin.perfectstay.wrd_booking;


dataset_task --include 'perfectstay.wrd_booking' --operation LatestRecordsOperation --method 'run' --start '2022-03-02 00:30:00' --end '2022-03-02 00:30:00'

MERGE INTO latest_vault_dev_robin.perfectstay.wrd_booking AS target
    USING latest_vault_dev_robin.perfectstay.wrd_booking__dedupe AS batch
    ON target.booking_id__o = batch.booking_id__o
    WHEN MATCHED
        AND target.row_loaded_at <= batch.row_loaded_at
            AND target.updated_at <= batch.updated_at
            AND batch.booking_status IS DISTINCT FROM 'CANCELLED'
        THEN UPDATE SET

        -- (lineage) metadata for the current job
        target.schedule_tstamp = batch.schedule_tstamp,
        target.run_tstamp = batch.run_tstamp,
        target.operation_id = batch.operation_id,
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        -- (lineage) original metadata of row itself
        target.row_dataset_name = batch.row_dataset_name,
        target.row_dataset_source = batch.row_dataset_source,
        target.row_loaded_at = batch.row_loaded_at,
        target.row_schedule_tstamp = batch.row_schedule_tstamp,
        target.row_run_tstamp = batch.row_run_tstamp,
        target.row_filename = batch.row_filename,
        target.row_file_row_number = batch.row_file_row_number,
        target.row_extract_metadata = batch.row_extract_metadata,

        -- transformed columns
        target.remote_filename = batch.remote_filename,
        target.remote_file_row_number = batch.remote_file_row_number,
        target.booking_id = batch.booking_id,
        target.last_updated_date_time = batch.last_updated_date_time,

        -- original columns
        target.booking_id__o = batch.booking_id__o,
        target.external_reference_id = batch.external_reference_id,
        target.booking_status = batch.booking_status,
        target.se_sale_id = batch.se_sale_id,
        target.customer_identifier = batch.customer_identifier,
        target.check_in_date = batch.check_in_date,
        target.check_out_date = batch.check_out_date,
        target.no_nights = batch.no_nights,
        target.rooms = batch.rooms,
        target.adult_guests = batch.adult_guests,
        target.child_guests = batch.child_guests,
        target.infant_guests = batch.infant_guests,
        target.booking_created_date_time = batch.booking_created_date_time,
        target.booking_completed_date_time = batch.booking_completed_date_time,
        target.gross_revenue_customer_currency = batch.gross_revenue_customer_currency,
        target.margin_gross_of_toms_customer_currency = batch.margin_gross_of_toms_customer_currency,
        target.rate_to_gbp_from_cc = batch.rate_to_gbp_from_cc,
        target.customer_currency = batch.customer_currency,
        target.rate_to_supplier_currency = batch.rate_to_supplier_currency,
        target.supplier_currency = batch.supplier_currency,
        target.territory = batch.territory,
        target.device_platform = batch.device_platform,
        target.travel_type = batch.travel_type,
        target.payment_type = batch.payment_type,
        target.product_type = batch.product_type,
        target.posu_country = batch.posu_country,
        target.posu_city = batch.posu_city,
        target.last_updated_date_time__o = batch.last_updated_date_time__o

    -- to handle new cancellations outside the batch of the booking creation
    WHEN MATCHED AND target.row_loaded_at <= batch.row_loaded_at
        AND target.updated_at <= batch.updated_at
        AND batch.booking_status IS NOT DISTINCT FROM 'CANCELLED'
        THEN UPDATE SET

        -- (lineage) metadata for the current job
        target.schedule_tstamp = batch.schedule_tstamp,
        target.run_tstamp = batch.run_tstamp,
        target.operation_id = batch.operation_id,
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        -- (lineage) original metadata of row itself
        target.row_dataset_name = batch.row_dataset_name,
        target.row_dataset_source = batch.row_dataset_source,
        target.row_loaded_at = batch.row_loaded_at,
        target.row_schedule_tstamp = batch.row_schedule_tstamp,
        target.row_run_tstamp = batch.row_run_tstamp,
        target.row_filename = batch.row_filename,
        target.row_file_row_number = batch.row_file_row_number,
        target.row_extract_metadata = batch.row_extract_metadata,

        -- original columns
        target.booking_status = batch.booking_status
    WHEN NOT MATCHED
        THEN INSERT VALUES (

                               -- (lineage) metadata for the current job
                               batch.row_schedule_tstamp,
                               batch.row_run_tstamp,
                               batch.operation_id,
                               CURRENT_TIMESTAMP()::TIMESTAMP,
                               CURRENT_TIMESTAMP()::TIMESTAMP,

                               -- (lineage) original metadata of row itself
                               batch.row_dataset_name,
                               batch.row_dataset_source,
                               batch.row_loaded_at,
                               batch.row_schedule_tstamp,
                               batch.row_run_tstamp,
                               batch.row_filename,
                               batch.row_file_row_number,
                               batch.row_extract_metadata,

                               -- transformed columns
                               batch.remote_filename,
                               batch.remote_file_row_number,
                               batch.booking_id,
                               batch.last_updated_date_time,

                               -- original columns
                               batch.booking_id__o,
                               batch.external_reference_id,
                               batch.booking_status,
                               batch.se_sale_id,
                               batch.customer_identifier,
                               batch.check_in_date,
                               batch.check_out_date,
                               batch.no_nights,
                               batch.rooms,
                               batch.adult_guests,
                               batch.child_guests,
                               batch.infant_guests,
                               batch.booking_created_date_time,
                               batch.booking_completed_date_time,
                               batch.gross_revenue_customer_currency,
                               batch.margin_gross_of_toms_customer_currency,
                               batch.rate_to_gbp_from_cc,
                               batch.customer_currency,
                               batch.rate_to_supplier_currency,
                               batch.supplier_currency,
                               batch.territory,
                               batch.device_platform,
                               batch.travel_type,
                               batch.payment_type,
                               batch.product_type,
                               batch.posu_country,
                               batch.posu_city,
                               batch.last_updated_date_time__o);


SELECT *
FROM latest_vault_dev_robin.perfectstay.wrd_booking;

CREATE OR REPLACE TRANSIENT TABLE latest_vault.bedfinder.wrd_booking;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.airline_holidays;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.airline_holidays.wrd_booking CLONE latest_vault.airline_holidays.wrd_booking;
CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.blue_bay_travel;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.blue_bay_travel.wrd_booking CLONE latest_vault.blue_bay_travel.wrd_booking;
CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.broadway_travel;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.broadway_travel.wrd_booking CLONE latest_vault.broadway_travel.wrd_booking;
CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.exoticca;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.exoticca.wrd_booking CLONE latest_vault.exoticca.wrd_booking;
CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.jetline_travel;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.jetline_travel.wrd_booking CLONE latest_vault.jetline_travel.wrd_booking;
CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.trading_gsheets;
CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.style_in_travel;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.style_in_travel.wrd_booking CLONE latest_vault.style_in_travel.wrd_booking;

CREATE SCHEMA latest_vault_dev_robin.bedfinder;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.bedfinder.wrd_booking CLONE latest_vault.bedfinder.wrd_booking;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.trading_gsheets.offline_margin CLONE latest_vault.trading_gsheets.offline_margin;

SELECT  * FROM data_vault_mvp_dev_robin.dwh.wrd_booking wbs01mnstw WHERE wrd_provider = 'PERFECTSTAY';

self_describing_task --include 'dv/dwh/wrd/wrd_booking.py'  --method 'run' --start '2022-03-02 00:00:00' --end '2022-03-02 00:00:00'

------------------------------------------------------------------------------------------------------------------------
--run perfect stay extract ingest from 8th November 2021