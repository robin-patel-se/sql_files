-- True up to get TB PLN bookings into cube 30/09/2019

USE DATABASE SCRATCH;
USE SCHEMA ROBINPATEL;
-- STEP 4

-- NOTE: remember to change `june` with the actual month when trueup happens
CREATE OR REPLACE TRANSIENT TABLE SCRATCH.ROBINPATEL.TRAVELBIRD_CATALOGUE_BOOKING_SUMMARY_SNAPSHOT_TRUE_UP_SEPTEMBER_PLN_BOOKINGS
    CLONE RAW_VAULT.TRAVELBIRD_CATALOGUE.BOOKING_SUMMARY;

-- STEP 5

-- NOTE: remember to change `june` with the actual month when trueup happens
SELECT
    'backup table'::varchar as tablename,
    date_booked,
    count(*) as nrows,
    count(distinct customer_id) as customers,
    sum(commission_ex_vat) as commission_ex_vat,
    sum(total_sell_rate) as total_sell_rate,
    sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
    schedule_tstamp, extracted_at
FROM SCRATCH.ROBINPATEL.TRAVELBIRD_CATALOGUE_BOOKING_SUMMARY_SNAPSHOT_TRUE_UP_SEPTEMBER_PLN_BOOKINGS
WHERE
    DATE_BOOKED >= '2019-06-26' AND DATE_BOOKED<= '2019-09-26'
GROUP BY schedule_tstamp, extracted_at, date_booked
ORDER BY schedule_tstamp asc
;

--  STEP 6 -- for comparison to archived table in step 5

SELECT
    'raw_vault table'::varchar as tablename,
    date_booked,
    count(*) as nrows,
    count(distinct customer_id) as customers,
    sum(commission_ex_vat) as commission_ex_vat,
    sum(total_sell_rate) as total_sell_rate,
    sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
    schedule_tstamp, extracted_at
FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.BOOKING_SUMMARY
WHERE
    DATE_BOOKED >= '2019-06-26' AND DATE_BOOKED<= '2019-09-26'
GROUP BY schedule_tstamp, extracted_at, date_booked
ORDER BY schedule_tstamp ASC
;


-- STEP 7  -- removing the old data

-- dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-04-02 00:00:00' --end-tstamp '2019-04-02 00:00:00' \


dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-06-27 00:00:00' --end-tstamp '2019-06-27 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-06-28 00:00:00' --end-tstamp '2019-06-28 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-06-29 00:00:00' --end-tstamp '2019-06-29 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-06-30 00:00:00' --end-tstamp '2019-06-30 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-01 00:00:00' --end-tstamp '2019-07-01 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-02 00:00:00' --end-tstamp '2019-07-02 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-03 00:00:00' --end-tstamp '2019-07-03 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-04 00:00:00' --end-tstamp '2019-07-04 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-05 00:00:00' --end-tstamp '2019-07-05 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-06 00:00:00' --end-tstamp '2019-07-06 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-07 00:00:00' --end-tstamp '2019-07-07 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-08 00:00:00' --end-tstamp '2019-07-08 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-09 00:00:00' --end-tstamp '2019-07-09 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-10 00:00:00' --end-tstamp '2019-07-10 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-11 00:00:00' --end-tstamp '2019-07-11 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-12 00:00:00' --end-tstamp '2019-07-12 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-13 00:00:00' --end-tstamp '2019-07-13 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-14 00:00:00' --end-tstamp '2019-07-14 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-15 00:00:00' --end-tstamp '2019-07-15 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-16 00:00:00' --end-tstamp '2019-07-16 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-17 00:00:00' --end-tstamp '2019-07-17 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-18 00:00:00' --end-tstamp '2019-07-18 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-19 00:00:00' --end-tstamp '2019-07-19 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-20 00:00:00' --end-tstamp '2019-07-20 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-21 00:00:00' --end-tstamp '2019-07-21 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-22 00:00:00' --end-tstamp '2019-07-22 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-23 00:00:00' --end-tstamp '2019-07-23 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-24 00:00:00' --end-tstamp '2019-07-24 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-24 09:00:00' --end-tstamp '2019-07-24 09:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-25 00:00:00' --end-tstamp '2019-07-25 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-26 00:00:00' --end-tstamp '2019-07-26 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-27 00:00:00' --end-tstamp '2019-07-27 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-28 00:00:00' --end-tstamp '2019-07-28 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-29 00:00:00' --end-tstamp '2019-07-29 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-30 00:00:00' --end-tstamp '2019-07-30 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-07-31 00:00:00' --end-tstamp '2019-07-31 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-01 00:00:00' --end-tstamp '2019-08-01 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-02 00:00:00' --end-tstamp '2019-08-02 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-03 00:00:00' --end-tstamp '2019-08-03 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-04 00:00:00' --end-tstamp '2019-08-04 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-05 00:00:00' --end-tstamp '2019-08-05 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-06 00:00:00' --end-tstamp '2019-08-06 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-07 00:00:00' --end-tstamp '2019-08-07 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-08 00:00:00' --end-tstamp '2019-08-08 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-09 00:00:00' --end-tstamp '2019-08-09 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-10 00:00:00' --end-tstamp '2019-08-10 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-11 00:00:00' --end-tstamp '2019-08-11 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-12 00:00:00' --end-tstamp '2019-08-12 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-13 00:00:00' --end-tstamp '2019-08-13 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-14 00:00:00' --end-tstamp '2019-08-14 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-15 00:00:00' --end-tstamp '2019-08-15 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-16 00:00:00' --end-tstamp '2019-08-16 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-17 00:00:00' --end-tstamp '2019-08-17 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-18 00:00:00' --end-tstamp '2019-08-18 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-19 00:00:00' --end-tstamp '2019-08-19 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-20 08:00:00' --end-tstamp '2019-08-20 08:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-21 00:00:00' --end-tstamp '2019-08-21 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-22 00:00:00' --end-tstamp '2019-08-22 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-23 00:00:00' --end-tstamp '2019-08-23 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-24 00:00:00' --end-tstamp '2019-08-24 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-25 00:00:00' --end-tstamp '2019-08-25 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-26 00:00:00' --end-tstamp '2019-08-26 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-27 00:00:00' --end-tstamp '2019-08-27 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-28 00:00:00' --end-tstamp '2019-08-28 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-29 00:00:00' --end-tstamp '2019-08-29 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-30 00:00:00' --end-tstamp '2019-08-30 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-31 00:00:00' --end-tstamp '2019-08-31 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-01 00:00:00' --end-tstamp '2019-09-01 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-02 00:00:00' --end-tstamp '2019-09-02 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-03 00:00:00' --end-tstamp '2019-09-03 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-04 00:00:00' --end-tstamp '2019-09-04 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-05 00:00:00' --end-tstamp '2019-09-05 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-06 00:00:00' --end-tstamp '2019-09-06 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-07 00:00:00' --end-tstamp '2019-09-07 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-08 00:00:00' --end-tstamp '2019-09-08 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-09 00:00:00' --end-tstamp '2019-09-09 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-10 00:00:00' --end-tstamp '2019-09-10 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-11 00:00:00' --end-tstamp '2019-09-11 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-12 00:00:00' --end-tstamp '2019-09-12 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-13 00:00:00' --end-tstamp '2019-09-13 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-14 00:00:00' --end-tstamp '2019-09-14 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-15 00:00:00' --end-tstamp '2019-09-15 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-16 00:00:00' --end-tstamp '2019-09-16 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-17 00:00:00' --end-tstamp '2019-09-17 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-18 00:00:00' --end-tstamp '2019-09-18 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-19 00:00:00' --end-tstamp '2019-09-19 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-20 00:00:00' --end-tstamp '2019-09-20 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-21 00:00:00' --end-tstamp '2019-09-21 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-22 00:00:00' --end-tstamp '2019-09-22 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-23 00:00:00' --end-tstamp '2019-09-23 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-24 00:00:00' --end-tstamp '2019-09-24 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-25 00:00:00' --end-tstamp '2019-09-25 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-26 00:00:00' --end-tstamp '2019-09-26 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-27 00:00:00' --end-tstamp '2019-09-27 00:00:00'

-- STEP 8 - wipe the dag historic run

-- airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-04-02 00:00:00' --end_date '2019-04-02 00:00:00' \

airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-06-27 00:00:00' --end_date '2019-06-27 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-06-28 00:00:00' --end_date '2019-06-28 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-06-29 00:00:00' --end_date '2019-06-29 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-06-30 00:00:00' --end_date '2019-06-30 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-01 00:00:00' --end_date '2019-07-01 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-02 00:00:00' --end_date '2019-07-02 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-03 00:00:00' --end_date '2019-07-03 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-04 00:00:00' --end_date '2019-07-04 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-05 00:00:00' --end_date '2019-07-05 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-06 00:00:00' --end_date '2019-07-06 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-07 00:00:00' --end_date '2019-07-07 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-08 00:00:00' --end_date '2019-07-08 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-09 00:00:00' --end_date '2019-07-09 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-10 00:00:00' --end_date '2019-07-10 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-11 00:00:00' --end_date '2019-07-11 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-12 00:00:00' --end_date '2019-07-12 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-13 00:00:00' --end_date '2019-07-13 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-14 00:00:00' --end_date '2019-07-14 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-15 00:00:00' --end_date '2019-07-15 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-16 00:00:00' --end_date '2019-07-16 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-17 00:00:00' --end_date '2019-07-17 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-18 00:00:00' --end_date '2019-07-18 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-19 00:00:00' --end_date '2019-07-19 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-20 00:00:00' --end_date '2019-07-20 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-21 00:00:00' --end_date '2019-07-21 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-22 00:00:00' --end_date '2019-07-22 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-23 00:00:00' --end_date '2019-07-23 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-24 00:00:00' --end_date '2019-07-24 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-24 09:00:00' --end_date '2019-07-24 09:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-25 00:00:00' --end_date '2019-07-25 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-26 00:00:00' --end_date '2019-07-26 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-27 00:00:00' --end_date '2019-07-27 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-28 00:00:00' --end_date '2019-07-28 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-29 00:00:00' --end_date '2019-07-29 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-30 00:00:00' --end_date '2019-07-30 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-07-31 00:00:00' --end_date '2019-07-31 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-01 00:00:00' --end_date '2019-08-01 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-02 00:00:00' --end_date '2019-08-02 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-03 00:00:00' --end_date '2019-08-03 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-04 00:00:00' --end_date '2019-08-04 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-05 00:00:00' --end_date '2019-08-05 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-06 00:00:00' --end_date '2019-08-06 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-07 00:00:00' --end_date '2019-08-07 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-08 00:00:00' --end_date '2019-08-08 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-09 00:00:00' --end_date '2019-08-09 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-10 00:00:00' --end_date '2019-08-10 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-11 00:00:00' --end_date '2019-08-11 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-12 00:00:00' --end_date '2019-08-12 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-13 00:00:00' --end_date '2019-08-13 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-14 00:00:00' --end_date '2019-08-14 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-15 00:00:00' --end_date '2019-08-15 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-16 00:00:00' --end_date '2019-08-16 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-17 00:00:00' --end_date '2019-08-17 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-18 00:00:00' --end_date '2019-08-18 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-19 00:00:00' --end_date '2019-08-19 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-20 08:00:00' --end_date '2019-08-20 08:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-21 00:00:00' --end_date '2019-08-21 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-22 00:00:00' --end_date '2019-08-22 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-23 00:00:00' --end_date '2019-08-23 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-24 00:00:00' --end_date '2019-08-24 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-25 00:00:00' --end_date '2019-08-25 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-26 00:00:00' --end_date '2019-08-26 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-27 00:00:00' --end_date '2019-08-27 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-28 00:00:00' --end_date '2019-08-28 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-29 00:00:00' --end_date '2019-08-29 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-30 00:00:00' --end_date '2019-08-30 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-31 00:00:00' --end_date '2019-08-31 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-01 00:00:00' --end_date '2019-09-01 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-02 00:00:00' --end_date '2019-09-02 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-03 00:00:00' --end_date '2019-09-03 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-04 00:00:00' --end_date '2019-09-04 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-05 00:00:00' --end_date '2019-09-05 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-06 00:00:00' --end_date '2019-09-06 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-07 00:00:00' --end_date '2019-09-07 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-08 00:00:00' --end_date '2019-09-08 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-09 00:00:00' --end_date '2019-09-09 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-10 00:00:00' --end_date '2019-09-10 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-11 00:00:00' --end_date '2019-09-11 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-12 00:00:00' --end_date '2019-09-12 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-13 00:00:00' --end_date '2019-09-13 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-14 00:00:00' --end_date '2019-09-14 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-15 00:00:00' --end_date '2019-09-15 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-16 00:00:00' --end_date '2019-09-16 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-17 00:00:00' --end_date '2019-09-17 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-18 00:00:00' --end_date '2019-09-18 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-19 00:00:00' --end_date '2019-09-19 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-20 00:00:00' --end_date '2019-09-20 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-21 00:00:00' --end_date '2019-09-21 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-22 00:00:00' --end_date '2019-09-22 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-23 00:00:00' --end_date '2019-09-23 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-24 00:00:00' --end_date '2019-09-24 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-25 00:00:00' --end_date '2019-09-25 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-26 00:00:00' --end_date '2019-09-26 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-27 00:00:00' --end_date '2019-09-27 00:00:00'

-- airflow backfill --start_date '2019-04-02 00:00:00' --end_date '2019-04-02 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \

airflow backfill --start_date '2019-06-27 00:00:00' --end_date '2019-06-27 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-06-28 00:00:00' --end_date '2019-06-28 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-06-29 00:00:00' --end_date '2019-06-29 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-06-30 00:00:00' --end_date '2019-06-30 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-01 00:00:00' --end_date '2019-07-01 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-02 00:00:00' --end_date '2019-07-02 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-03 00:00:00' --end_date '2019-07-03 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-04 00:00:00' --end_date '2019-07-04 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-05 00:00:00' --end_date '2019-07-05 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-06 00:00:00' --end_date '2019-07-06 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-07 00:00:00' --end_date '2019-07-07 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-08 00:00:00' --end_date '2019-07-08 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-09 00:00:00' --end_date '2019-07-09 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-10 00:00:00' --end_date '2019-07-10 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-11 00:00:00' --end_date '2019-07-11 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-12 00:00:00' --end_date '2019-07-12 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-13 00:00:00' --end_date '2019-07-13 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-14 00:00:00' --end_date '2019-07-14 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-15 00:00:00' --end_date '2019-07-15 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-16 00:00:00' --end_date '2019-07-16 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-17 00:00:00' --end_date '2019-07-17 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-18 00:00:00' --end_date '2019-07-18 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-19 00:00:00' --end_date '2019-07-19 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-20 00:00:00' --end_date '2019-07-20 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-21 00:00:00' --end_date '2019-07-21 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-22 00:00:00' --end_date '2019-07-22 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-23 00:00:00' --end_date '2019-07-23 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-24 00:00:00' --end_date '2019-07-24 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-24 09:00:00' --end_date '2019-07-24 09:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-25 00:00:00' --end_date '2019-07-25 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-26 00:00:00' --end_date '2019-07-26 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-27 00:00:00' --end_date '2019-07-27 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-28 00:00:00' --end_date '2019-07-28 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-29 00:00:00' --end_date '2019-07-29 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-30 00:00:00' --end_date '2019-07-30 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-07-31 00:00:00' --end_date '2019-07-31 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-01 00:00:00' --end_date '2019-08-01 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-02 00:00:00' --end_date '2019-08-02 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-03 00:00:00' --end_date '2019-08-03 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-04 00:00:00' --end_date '2019-08-04 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-05 00:00:00' --end_date '2019-08-05 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-06 00:00:00' --end_date '2019-08-06 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-07 00:00:00' --end_date '2019-08-07 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-08 00:00:00' --end_date '2019-08-08 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-09 00:00:00' --end_date '2019-08-09 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-10 00:00:00' --end_date '2019-08-10 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-11 00:00:00' --end_date '2019-08-11 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-12 00:00:00' --end_date '2019-08-12 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-13 00:00:00' --end_date '2019-08-13 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-14 00:00:00' --end_date '2019-08-14 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-15 00:00:00' --end_date '2019-08-15 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-16 00:00:00' --end_date '2019-08-16 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-17 00:00:00' --end_date '2019-08-17 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-18 00:00:00' --end_date '2019-08-18 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-19 00:00:00' --end_date '2019-08-19 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-20 08:00:00' --end_date '2019-08-20 08:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-21 00:00:00' --end_date '2019-08-21 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-22 00:00:00' --end_date '2019-08-22 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-23 00:00:00' --end_date '2019-08-23 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-24 00:00:00' --end_date '2019-08-24 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-25 00:00:00' --end_date '2019-08-25 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-26 00:00:00' --end_date '2019-08-26 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-27 00:00:00' --end_date '2019-08-27 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-28 00:00:00' --end_date '2019-08-28 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-29 00:00:00' --end_date '2019-08-29 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-30 00:00:00' --end_date '2019-08-30 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-31 00:00:00' --end_date '2019-08-31 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-01 00:00:00' --end_date '2019-09-01 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-02 00:00:00' --end_date '2019-09-02 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-03 00:00:00' --end_date '2019-09-03 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-04 00:00:00' --end_date '2019-09-04 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-05 00:00:00' --end_date '2019-09-05 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-06 00:00:00' --end_date '2019-09-06 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-07 00:00:00' --end_date '2019-09-07 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-08 00:00:00' --end_date '2019-09-08 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-09 00:00:00' --end_date '2019-09-09 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-10 00:00:00' --end_date '2019-09-10 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-11 00:00:00' --end_date '2019-09-11 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-12 00:00:00' --end_date '2019-09-12 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-13 00:00:00' --end_date '2019-09-13 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-14 00:00:00' --end_date '2019-09-14 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-15 00:00:00' --end_date '2019-09-15 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-16 00:00:00' --end_date '2019-09-16 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-17 00:00:00' --end_date '2019-09-17 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-18 00:00:00' --end_date '2019-09-18 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-19 00:00:00' --end_date '2019-09-19 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-20 00:00:00' --end_date '2019-09-20 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-21 00:00:00' --end_date '2019-09-21 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-22 00:00:00' --end_date '2019-09-22 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-23 00:00:00' --end_date '2019-09-23 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-24 00:00:00' --end_date '2019-09-24 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-25 00:00:00' --end_date '2019-09-25 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-26 00:00:00' --end_date '2019-09-26 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-27 00:00:00' --end_date '2019-09-27 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly


-- Step 9

-- Change the manifest on Prod so that ignore_file_tstamp is active (as the files were modified recently, the current
-- implementation on master will not pick them up). To do that, add to query object to
-- biapp/manifests/travelbird_catalogue/booking_summary.json the following
-- "ignore_file_tstamp": true,


-- STEP 12

select a.schedule_tstamp,
    a.DATE_BOOKED,
    a.nrows AS nrows_backup,
    a.total_sell_rate_in_currency as total_sell_rate_in_currency_backup,
    a.commission_ex_vat as commission_ex_vat_backup,
    b.nrows AS nrows_rv,
    b.total_sell_rate_in_currency as total_sell_rate_in_currency_rv,
    b.commission_ex_vat as commission_ex_vat_rv,
    a.nrows - b.nrows as diff_nrows_backup_vs_rv,
    a.customers - b.customers as diff_customers_backup_vs_rv,
    a.total_sell_rate - b.total_sell_rate as diff_total_sell_rate_backup_vs_rv,
    a.total_sell_rate_in_currency - b.total_sell_rate_in_currency as diff_total_sell_rate_in_currency_backup_vs_rv,
    a.commission_ex_vat - b.commission_ex_vat as diff_commission_ex_vat_backup_vs_rv --relevant for fix to derived exchange rate

from (
    select
        date_booked,
        'backup'::varchar as tablename,
        count(*) as nrows,
        count(distinct customer_id) as customers,
        sum(total_sell_rate) as total_sell_rate,
        sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
        sum(commission_ex_vat) as commission_ex_vat,
        schedule_tstamp, extracted_at
    from SCRATCH.ROBINPATEL.TRAVELBIRD_CATALOGUE_BOOKING_SUMMARY_SNAPSHOT_TRUE_UP_SEPTEMBER_PLN_BOOKINGS
    where DATE_BOOKED >= '2019-06-26' AND DATE_BOOKED<= '2019-09-26'
    group by schedule_tstamp, extracted_at, date_booked
) as a --backup
left join (
    select
        'raw_vault'::varchar as tablename,
        count(*) as nrows,
        count(distinct customer_id) as customers,
        sum(total_sell_rate) as total_sell_rate, --relevant for derived exchange rate
        sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
        sum(commission_ex_vat) as commission_ex_vat,
        schedule_tstamp, extracted_at
    from raw_vault.travelbird_catalogue.booking_summary
     where DATE_BOOKED >= '2019-06-26' AND DATE_BOOKED<= '2019-09-26'
    group by schedule_tstamp, extracted_at, date_booked
) as b -- live
on a.schedule_tstamp = b.schedule_tstamp
order by date_booked
;

-- Sale ids in bookings were changed. Run both in comparison.

SELECT SALE_ID, TRANSACTION_ID, CURRENCY
from SCRATCH.ROBINPATEL.TRAVELBIRD_CATALOGUE_BOOKING_SUMMARY_SNAPSHOT_TRUE_UP_SEPTEMBER_PLN_BOOKINGS
     where DATE_BOOKED >= '2019-06-26' AND DATE_BOOKED<= '2019-09-26'
order by TRANSACTION_ID;

SELECT SALE_ID, TRANSACTION_ID, CURRENCY
from raw_vault.travelbird_catalogue.booking_summary
     where DATE_BOOKED >= '2019-06-26' AND DATE_BOOKED<= '2019-09-26'
order by TRANSACTION_ID;

-- STEP 15

airflow clear --no_confirm --dag_regex 'Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2' --start_date '2019-06-26 02:00:00' --end_date '2019-06-26 02:00:00'
airflow backfill --start_date '2019-06-26 02:00:00' --end_date '2019-06-26 02:00:00'  --local  Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2