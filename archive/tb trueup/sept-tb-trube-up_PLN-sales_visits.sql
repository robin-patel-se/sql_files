-- True up to get TB PLN SPVs into cube 30/09/2019

USE DATABASE SCRATCH;
USE SCHEMA ROBINPATEL;
-- STEP 4

-- NOTE: remember to change `june` with the actual month when trueup happens

CREATE OR REPLACE TRANSIENT TABLE SCRATCH.ROBINPATEL.TRAVELBIRD_CATALOGUE_SALE_VISITS_BY_STATE_AND_DATE_SNAPSHOT_TRUE_UP_SEPTEMBER_PLN_SALES_VISITS
    CLONE RAW_VAULT.TRAVELBIRD_CATALOGUE.SALE_VISITS_BY_STATE_AND_DATE;

-- STEP 5

-- NOTE: remember to change `june` with the actual month when trueup happens
SELECT
       'backup table'::varchar as tablename,
       DATE,
       COUNT(*) AS nrows,
       COUNT(distinct TERRITORY) AS territories,
       COUNT(distinct SALE_ID) AS sales,
       SUM(USER_VISITS) AS visits,
       SCHEDULE_TSTAMP,
       EXTRACTED_AT
FROM TRAVELBIRD_CATALOGUE_SALE_VISITS_BY_STATE_AND_DATE_SNAPSHOT_TRUE_UP_SEPTEMBER_PLN_SALES_VISITS
WHERE DATE >= '2019-06-26' AND DATE <= '2019-09-26'
GROUP BY DATE, EXTRACTED_AT, SCHEDULE_TSTAMP
ORDER BY DATE
;

--  STEP 6 -- for comparison to archived table in step 5

SELECT
       'raw_vault table'::varchar as tablename,
       DATE,
       COUNT(*) AS nrows,
       COUNT(distinct TERRITORY) AS territories,
       COUNT(distinct SALE_ID) AS sales,
       SUM(USER_VISITS) AS visits,
       SCHEDULE_TSTAMP,
       EXTRACTED_AT
FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.SALE_VISITS_BY_STATE_AND_DATE
WHERE DATE >= '2019-06-26' AND DATE <= '2019-09-26'
GROUP BY DATE, EXTRACTED_AT, SCHEDULE_TSTAMP
ORDER BY DATE
;

-- STEP 7  -- removing the old data

-- dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-04-02 00:00:00' --end-tstamp '2019-04-02 00:00:00' \


dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-06-27 00:00:00' --end-tstamp '2019-06-27 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-06-28 00:00:00' --end-tstamp '2019-06-28 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-06-29 00:00:00' --end-tstamp '2019-06-29 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-06-30 00:00:00' --end-tstamp '2019-06-30 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-01 00:00:00' --end-tstamp '2019-07-01 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-02 00:00:00' --end-tstamp '2019-07-02 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-03 00:00:00' --end-tstamp '2019-07-03 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-04 00:00:00' --end-tstamp '2019-07-04 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-05 00:00:00' --end-tstamp '2019-07-05 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-06 00:00:00' --end-tstamp '2019-07-06 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-07 00:00:00' --end-tstamp '2019-07-07 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-08 00:00:00' --end-tstamp '2019-07-08 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-09 00:00:00' --end-tstamp '2019-07-09 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-10 00:00:00' --end-tstamp '2019-07-10 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-11 00:00:00' --end-tstamp '2019-07-11 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-12 00:00:00' --end-tstamp '2019-07-12 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-13 00:00:00' --end-tstamp '2019-07-13 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-14 00:00:00' --end-tstamp '2019-07-14 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-15 00:00:00' --end-tstamp '2019-07-15 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-16 00:00:00' --end-tstamp '2019-07-16 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-17 00:00:00' --end-tstamp '2019-07-17 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-18 00:00:00' --end-tstamp '2019-07-18 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-19 00:00:00' --end-tstamp '2019-07-19 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-20 00:00:00' --end-tstamp '2019-07-20 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-21 00:00:00' --end-tstamp '2019-07-21 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-22 00:00:00' --end-tstamp '2019-07-22 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-23 00:00:00' --end-tstamp '2019-07-23 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-24 00:00:00' --end-tstamp '2019-07-24 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-24 09:00:00' --end-tstamp '2019-07-24 09:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-25 00:00:00' --end-tstamp '2019-07-25 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-26 00:00:00' --end-tstamp '2019-07-26 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-27 00:00:00' --end-tstamp '2019-07-27 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-28 00:00:00' --end-tstamp '2019-07-28 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-29 00:00:00' --end-tstamp '2019-07-29 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-30 00:00:00' --end-tstamp '2019-07-30 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-07-31 00:00:00' --end-tstamp '2019-07-31 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-01 00:00:00' --end-tstamp '2019-08-01 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-02 00:00:00' --end-tstamp '2019-08-02 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-03 00:00:00' --end-tstamp '2019-08-03 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-04 00:00:00' --end-tstamp '2019-08-04 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-05 00:00:00' --end-tstamp '2019-08-05 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-06 00:00:00' --end-tstamp '2019-08-06 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-07 00:00:00' --end-tstamp '2019-08-07 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-08 00:00:00' --end-tstamp '2019-08-08 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-09 00:00:00' --end-tstamp '2019-08-09 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-10 00:00:00' --end-tstamp '2019-08-10 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-11 00:00:00' --end-tstamp '2019-08-11 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-12 00:00:00' --end-tstamp '2019-08-12 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-13 00:00:00' --end-tstamp '2019-08-13 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-14 00:00:00' --end-tstamp '2019-08-14 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-15 00:00:00' --end-tstamp '2019-08-15 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-16 00:00:00' --end-tstamp '2019-08-16 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-17 00:00:00' --end-tstamp '2019-08-17 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-18 00:00:00' --end-tstamp '2019-08-18 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-19 00:00:00' --end-tstamp '2019-08-19 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-20 08:00:00' --end-tstamp '2019-08-20 08:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-21 00:00:00' --end-tstamp '2019-08-21 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-22 00:00:00' --end-tstamp '2019-08-22 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-23 00:00:00' --end-tstamp '2019-08-23 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-24 00:00:00' --end-tstamp '2019-08-24 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-25 00:00:00' --end-tstamp '2019-08-25 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-26 00:00:00' --end-tstamp '2019-08-26 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-27 00:00:00' --end-tstamp '2019-08-27 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-28 00:00:00' --end-tstamp '2019-08-28 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-29 00:00:00' --end-tstamp '2019-08-29 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-30 00:00:00' --end-tstamp '2019-08-30 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-08-31 00:00:00' --end-tstamp '2019-08-31 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-01 00:00:00' --end-tstamp '2019-09-01 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-02 00:00:00' --end-tstamp '2019-09-02 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-03 00:00:00' --end-tstamp '2019-09-03 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-04 00:00:00' --end-tstamp '2019-09-04 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-05 00:00:00' --end-tstamp '2019-09-05 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-06 00:00:00' --end-tstamp '2019-09-06 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-07 00:00:00' --end-tstamp '2019-09-07 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-08 00:00:00' --end-tstamp '2019-09-08 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-09 00:00:00' --end-tstamp '2019-09-09 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-10 00:00:00' --end-tstamp '2019-09-10 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-11 00:00:00' --end-tstamp '2019-09-11 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-12 00:00:00' --end-tstamp '2019-09-12 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-13 00:00:00' --end-tstamp '2019-09-13 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-14 00:00:00' --end-tstamp '2019-09-14 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-15 00:00:00' --end-tstamp '2019-09-15 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-16 00:00:00' --end-tstamp '2019-09-16 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-17 00:00:00' --end-tstamp '2019-09-17 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-18 00:00:00' --end-tstamp '2019-09-18 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-19 00:00:00' --end-tstamp '2019-09-19 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-20 00:00:00' --end-tstamp '2019-09-20 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-21 00:00:00' --end-tstamp '2019-09-21 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-22 00:00:00' --end-tstamp '2019-09-22 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-23 00:00:00' --end-tstamp '2019-09-23 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-24 00:00:00' --end-tstamp '2019-09-24 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-25 00:00:00' --end-tstamp '2019-09-25 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-26 00:00:00' --end-tstamp '2019-09-26 00:00:00' \
&& dataset_task --include travelbird_catalogue.sale_visits_by_state_and_date --run-retracts --retract-extracts --start-tstamp '2019-09-27 00:00:00' --end-tstamp '2019-09-27 00:00:00'

-- STEP 8 - wipe the dag historic run

-- airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-04-02 00:00:00' --end_date '2019-04-02 00:00:00' \

airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-06-27 00:00:00' --end_date '2019-06-27 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-06-28 00:00:00' --end_date '2019-06-28 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-06-29 00:00:00' --end_date '2019-06-29 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-06-30 00:00:00' --end_date '2019-06-30 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-01 00:00:00' --end_date '2019-07-01 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-02 00:00:00' --end_date '2019-07-02 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-03 00:00:00' --end_date '2019-07-03 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-04 00:00:00' --end_date '2019-07-04 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-05 00:00:00' --end_date '2019-07-05 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-06 00:00:00' --end_date '2019-07-06 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-07 00:00:00' --end_date '2019-07-07 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-08 00:00:00' --end_date '2019-07-08 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-09 00:00:00' --end_date '2019-07-09 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-10 00:00:00' --end_date '2019-07-10 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-11 00:00:00' --end_date '2019-07-11 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-12 00:00:00' --end_date '2019-07-12 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-13 00:00:00' --end_date '2019-07-13 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-14 00:00:00' --end_date '2019-07-14 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-15 00:00:00' --end_date '2019-07-15 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-16 00:00:00' --end_date '2019-07-16 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-17 00:00:00' --end_date '2019-07-17 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-18 00:00:00' --end_date '2019-07-18 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-19 00:00:00' --end_date '2019-07-19 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-20 00:00:00' --end_date '2019-07-20 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-21 00:00:00' --end_date '2019-07-21 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-22 00:00:00' --end_date '2019-07-22 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-23 00:00:00' --end_date '2019-07-23 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-24 00:00:00' --end_date '2019-07-24 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-24 09:00:00' --end_date '2019-07-24 09:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-25 00:00:00' --end_date '2019-07-25 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-26 00:00:00' --end_date '2019-07-26 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-27 00:00:00' --end_date '2019-07-27 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-28 00:00:00' --end_date '2019-07-28 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-29 00:00:00' --end_date '2019-07-29 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-30 00:00:00' --end_date '2019-07-30 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-07-31 00:00:00' --end_date '2019-07-31 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-01 00:00:00' --end_date '2019-08-01 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-02 00:00:00' --end_date '2019-08-02 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-03 00:00:00' --end_date '2019-08-03 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-04 00:00:00' --end_date '2019-08-04 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-05 00:00:00' --end_date '2019-08-05 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-06 00:00:00' --end_date '2019-08-06 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-07 00:00:00' --end_date '2019-08-07 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-08 00:00:00' --end_date '2019-08-08 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-09 00:00:00' --end_date '2019-08-09 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-10 00:00:00' --end_date '2019-08-10 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-11 00:00:00' --end_date '2019-08-11 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-12 00:00:00' --end_date '2019-08-12 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-13 00:00:00' --end_date '2019-08-13 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-14 00:00:00' --end_date '2019-08-14 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-15 00:00:00' --end_date '2019-08-15 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-16 00:00:00' --end_date '2019-08-16 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-17 00:00:00' --end_date '2019-08-17 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-18 00:00:00' --end_date '2019-08-18 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-19 00:00:00' --end_date '2019-08-19 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-20 08:00:00' --end_date '2019-08-20 08:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-21 00:00:00' --end_date '2019-08-21 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-22 00:00:00' --end_date '2019-08-22 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-23 00:00:00' --end_date '2019-08-23 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-24 00:00:00' --end_date '2019-08-24 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-25 00:00:00' --end_date '2019-08-25 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-26 00:00:00' --end_date '2019-08-26 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-27 00:00:00' --end_date '2019-08-27 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-28 00:00:00' --end_date '2019-08-28 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-29 00:00:00' --end_date '2019-08-29 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-30 00:00:00' --end_date '2019-08-30 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-08-31 00:00:00' --end_date '2019-08-31 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-01 00:00:00' --end_date '2019-09-01 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-02 00:00:00' --end_date '2019-09-02 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-03 00:00:00' --end_date '2019-09-03 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-04 00:00:00' --end_date '2019-09-04 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-05 00:00:00' --end_date '2019-09-05 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-06 00:00:00' --end_date '2019-09-06 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-07 00:00:00' --end_date '2019-09-07 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-08 00:00:00' --end_date '2019-09-08 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-09 00:00:00' --end_date '2019-09-09 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-10 00:00:00' --end_date '2019-09-10 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-11 00:00:00' --end_date '2019-09-11 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-12 00:00:00' --end_date '2019-09-12 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-13 00:00:00' --end_date '2019-09-13 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-14 00:00:00' --end_date '2019-09-14 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-15 00:00:00' --end_date '2019-09-15 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-16 00:00:00' --end_date '2019-09-16 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-17 00:00:00' --end_date '2019-09-17 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-18 00:00:00' --end_date '2019-09-18 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-19 00:00:00' --end_date '2019-09-19 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-20 00:00:00' --end_date '2019-09-20 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-21 00:00:00' --end_date '2019-09-21 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-22 00:00:00' --end_date '2019-09-22 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-23 00:00:00' --end_date '2019-09-23 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-24 00:00:00' --end_date '2019-09-24 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-25 00:00:00' --end_date '2019-09-25 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-26 00:00:00' --end_date '2019-09-26 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly' --start_date '2019-09-27 00:00:00' --end_date '2019-09-27 00:00:00'

-- airflow backfill --start_date '2019-04-02 00:00:00' --end_date '2019-04-02 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \

airflow backfill --start_date '2019-06-27 00:00:00' --end_date '2019-06-27 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-06-28 00:00:00' --end_date '2019-06-28 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-06-29 00:00:00' --end_date '2019-06-29 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-06-30 00:00:00' --end_date '2019-06-30 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-01 00:00:00' --end_date '2019-07-01 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-02 00:00:00' --end_date '2019-07-02 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-03 00:00:00' --end_date '2019-07-03 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-04 00:00:00' --end_date '2019-07-04 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-05 00:00:00' --end_date '2019-07-05 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-06 00:00:00' --end_date '2019-07-06 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-07 00:00:00' --end_date '2019-07-07 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-08 00:00:00' --end_date '2019-07-08 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-09 00:00:00' --end_date '2019-07-09 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-10 00:00:00' --end_date '2019-07-10 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-11 00:00:00' --end_date '2019-07-11 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-12 00:00:00' --end_date '2019-07-12 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-13 00:00:00' --end_date '2019-07-13 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-14 00:00:00' --end_date '2019-07-14 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-15 00:00:00' --end_date '2019-07-15 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-16 00:00:00' --end_date '2019-07-16 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-17 00:00:00' --end_date '2019-07-17 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-18 00:00:00' --end_date '2019-07-18 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-19 00:00:00' --end_date '2019-07-19 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-20 00:00:00' --end_date '2019-07-20 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-21 00:00:00' --end_date '2019-07-21 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-22 00:00:00' --end_date '2019-07-22 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-23 00:00:00' --end_date '2019-07-23 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-24 00:00:00' --end_date '2019-07-24 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-24 09:00:00' --end_date '2019-07-24 09:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-25 00:00:00' --end_date '2019-07-25 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-26 00:00:00' --end_date '2019-07-26 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-27 00:00:00' --end_date '2019-07-27 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-28 00:00:00' --end_date '2019-07-28 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-29 00:00:00' --end_date '2019-07-29 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-30 00:00:00' --end_date '2019-07-30 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-07-31 00:00:00' --end_date '2019-07-31 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-01 00:00:00' --end_date '2019-08-01 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-02 00:00:00' --end_date '2019-08-02 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-03 00:00:00' --end_date '2019-08-03 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-04 00:00:00' --end_date '2019-08-04 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-05 00:00:00' --end_date '2019-08-05 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-06 00:00:00' --end_date '2019-08-06 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-07 00:00:00' --end_date '2019-08-07 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-08 00:00:00' --end_date '2019-08-08 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-09 00:00:00' --end_date '2019-08-09 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-10 00:00:00' --end_date '2019-08-10 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-11 00:00:00' --end_date '2019-08-11 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-12 00:00:00' --end_date '2019-08-12 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-13 00:00:00' --end_date '2019-08-13 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-14 00:00:00' --end_date '2019-08-14 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-15 00:00:00' --end_date '2019-08-15 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-16 00:00:00' --end_date '2019-08-16 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-17 00:00:00' --end_date '2019-08-17 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-18 00:00:00' --end_date '2019-08-18 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-19 00:00:00' --end_date '2019-08-19 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-20 08:00:00' --end_date '2019-08-20 08:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-21 00:00:00' --end_date '2019-08-21 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-22 00:00:00' --end_date '2019-08-22 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-23 00:00:00' --end_date '2019-08-23 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-24 00:00:00' --end_date '2019-08-24 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-25 00:00:00' --end_date '2019-08-25 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-26 00:00:00' --end_date '2019-08-26 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-27 00:00:00' --end_date '2019-08-27 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-28 00:00:00' --end_date '2019-08-28 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-29 00:00:00' --end_date '2019-08-29 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-30 00:00:00' --end_date '2019-08-30 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-08-31 00:00:00' --end_date '2019-08-31 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-01 00:00:00' --end_date '2019-09-01 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-02 00:00:00' --end_date '2019-09-02 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-03 00:00:00' --end_date '2019-09-03 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-04 00:00:00' --end_date '2019-09-04 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-05 00:00:00' --end_date '2019-09-05 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-06 00:00:00' --end_date '2019-09-06 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-07 00:00:00' --end_date '2019-09-07 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-08 00:00:00' --end_date '2019-09-08 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-09 00:00:00' --end_date '2019-09-09 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-10 00:00:00' --end_date '2019-09-10 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-11 00:00:00' --end_date '2019-09-11 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-12 00:00:00' --end_date '2019-09-12 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-13 00:00:00' --end_date '2019-09-13 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-14 00:00:00' --end_date '2019-09-14 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-15 00:00:00' --end_date '2019-09-15 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-16 00:00:00' --end_date '2019-09-16 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-17 00:00:00' --end_date '2019-09-17 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-18 00:00:00' --end_date '2019-09-18 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-19 00:00:00' --end_date '2019-09-19 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-20 00:00:00' --end_date '2019-09-20 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-21 00:00:00' --end_date '2019-09-21 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-22 00:00:00' --end_date '2019-09-22 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-23 00:00:00' --end_date '2019-09-23 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-24 00:00:00' --end_date '2019-09-24 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-25 00:00:00' --end_date '2019-09-25 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-26 00:00:00' --end_date '2019-09-26 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly \
&& airflow backfill --start_date '2019-09-27 00:00:00' --end_date '2019-09-27 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__sale_visits_by_state_and_date__hourly

-- Step 9

-- Change the manifest on Prod so that ignore_file_tstamp is active (as the files were modified recently, the current
-- implementation on master will not pick them up). To do that, add to query object to
-- biapp/manifests/travelbird_catalogue/sale_visits_by_state_and_date.json the following
-- "ignore_file_tstamp": true,

-- STEP 12

WITH live AS (SELECT
       'backup table'::varchar as tablename,
       DATE,
       COUNT(*) AS nrows,
       COUNT(distinct TERRITORY) AS territories,
       COUNT(distinct SALE_ID) AS sales,
       SUM(USER_VISITS) AS visits,
       SCHEDULE_TSTAMP,
       EXTRACTED_AT
FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.SALE_VISITS_BY_STATE_AND_DATE
WHERE DATE >= '2019-06-26' AND DATE <= '2019-09-26'
GROUP BY DATE, EXTRACTED_AT, SCHEDULE_TSTAMP
ORDER BY DATE),
backup AS ( --backup of sale visits prior to new ingest
    SELECT
       'backup table'::varchar as tablename,
       DATE,
       COUNT(*) AS nrows,
       COUNT(distinct TERRITORY) AS territories,
       COUNT(distinct SALE_ID) AS sales,
       SUM(USER_VISITS) AS visits,
       SCHEDULE_TSTAMP,
       EXTRACTED_AT
FROM TRAVELBIRD_CATALOGUE_SALE_VISITS_BY_STATE_AND_DATE_SNAPSHOT_TRUE_UP_SEPTEMBER_PLN_SALES_VISITS
WHERE DATE >= '2019-06-26' AND DATE <= '2019-09-26'
GROUP BY DATE, EXTRACTED_AT, SCHEDULE_TSTAMP
ORDER BY DATE
)
SELECT l.DATE,
       l.nrows                       as live_rows,
       b.nrows                       as bkup_rows,
       l.territories                 as live_territories,
       b.territories                 as bkup_territories,
       l.sales                       as live_sales,
       b.sales                       as bkup_sales,
       l.visits                      as live_visits,
       b.visits                      as bkup_visits,
       l.EXTRACTED_AT                as live_extracted_at,
       b.EXTRACTED_AT                as bkup_extracted_at,
       l.nrows - b.nrows             as diff_nrows_live_vs_backup,
       l.territories - b.territories as diff_territories_live_vs_backup,
       l.sales - b.sales             as diff_sales_live_vs_backup,
       l.visits - b.visits           as diff_visits_live_vs_backup

FROM live l
         LEFT JOIN backup b ON l.DATE = b.DATE

;

SELECT DATE, SALE_ID, sum(USER_VISITS)
FROM TRAVELBIRD_CATALOGUE_SALE_VISITS_BY_STATE_AND_DATE_SNAPSHOT_TRUE_UP_SEPTEMBER_PLN_SALES_VISITS
WHERE DATE >= '2019-06-26' AND DATE <= '2019-09-26'
group by 1,2
    order by DATE, SALE_ID;

SELECT DATE, SALE_ID, sum(USER_VISITS)
FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.SALE_VISITS_BY_STATE_AND_DATE
WHERE DATE >= '2019-06-26' AND DATE <= '2019-09-26'
group by 1,2
    order by DATE, SALE_ID;



SELECT DATE, SALE_ID, COUNT(*)
FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.SALE_VISITS_BY_STATE_AND_DATE
WHERE DATE >= '2019-06-26' AND DATE <= '2019-09-26'
-- AND SALE_ID='A2483'
group by 1,2
ORDER BY 3 DESC;

SELECT DISTINCT DATE, EXTRACTED_AT, SCHEDULE_TSTAMP FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.SALE_VISITS_BY_STATE_AND_DATE
WHERE DATE = '2019-07-23';







-- found some inconsistencies with the spvs

SELECT sum(USER_VISITS) FROM TRAVELBIRD_CATALOGUE_SALE_VISITS_BY_STATE_AND_DATE_SNAPSHOT_TRUE_UP_SEPTEMBER_PLN_SALES_VISITS WHERE SALE_ID='8010';
SELECT sum(USER_VISITS) FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.SALE_VISITS_BY_STATE_AND_DATE WHERE SALE_ID='8010';

SELECT * FROM TRAVELBIRD_CATALOGUE_SALE_VISITS_BY_STATE_AND_DATE_SNAPSHOT_TRUE_UP_SEPTEMBER_PLN_SALES_VISITS WHERE SALE_ID='8010';

SELECT * FROM RAW_VAULT.CMS_MYSQL.SALE WHERE ID=8010;

SELECT * FROM RAW_VAULT.CMS_MYSQL.BASE_SALE;



-- STEP 15

airflow clear --no_confirm --dag_regex 'Export_v0_1__chiasma__travelbird_catalogue_spvs__preexport__daily_at_2 ' --start_date '2019-06-26 02:00:00' --end_date '2019-06-26 02:00:00'
airflow backfill --start_date '2019-06-26 02:00:00' --end_date '2019-06-26 02:00:00'  --local  Export_v0_1__chiasma__travelbird_catalogue_spvs__preexport__daily_at_2


