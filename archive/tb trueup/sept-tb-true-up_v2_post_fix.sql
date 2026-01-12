-- TB issue with their booking summary identified, TB fixed the issue, repulled all summaries for bookings since 16th August (this is when they started taking deposit payments)
-- Created archive bucket in s3 - "booking_summary_true_up-september"
-- Copied existing week 35 26th aug to 1st sep files into archive bucket
-- uploaded newly downloaded booking summary files from TB CMS (09/09/2019 at 10.30AM approx) into main bucket (to replace existing)
use role ACCOUNTADMIN;
-- STEP 4

-- NOTE: remember to change `june` with the actual month when trueup happens
create or replace transient table adhoc.public.travelbird_catalogue_booking_summary_snapshot_true_up_september_post_fix
    clone raw_vault.travelbird_catalogue.booking_summary;

-- STEP 5

-- NOTE: remember to change `june` with the actual month when trueup happens
select
    'raw_vault table'::varchar as tablename,
    date_booked,
    count(*) as nrows,
    sum(commission_ex_vat) as commission_ex_vat,
    sum(total_sell_rate) as total_sell_rate,  --relevant for derived exchange rate
    sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
    schedule_tstamp, extracted_at
from adhoc.public.travelbird_catalogue_booking_summary_snapshot_true_up_september_post_fix
where
    -- this where clause gives us all the schedule tstamps that fetched files booking_summary_20190401_0300.csv through to booking_summary_20190531_0200.csv (as the job is configured on each schedule tstamp we look for the day before)
    DATE_BOOKED >= '2019-08-16 00:00:00'
and DATE_BOOKED <= '2019-09-10 00:00:00'
group by schedule_tstamp, extracted_at, date_booked
order by schedule_tstamp asc
;

--  STEP 6 -- for comparison to archived table in step 5

select
    'raw_vault table'::varchar as tablename,
    date_booked,
    count(*) as nrows,
    sum(commission_ex_vat) as commission_ex_vat,
    sum(total_sell_rate) as total_sell_rate,  --relevant for derived exchange rate
    sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
    schedule_tstamp, extracted_at
from raw_vault.travelbird_catalogue.booking_summary
where
    -- this where clause gives us all the schedule tstamps that fetched files booking_summary_20190401_0300.csv through to booking_summary_20190531_0200.csv (as the job is configured on each schedule tstamp we look for the day before)
    DATE_BOOKED >= '2019-08-16 00:00:00'
and DATE_BOOKED <= '2019-09-10 00:00:00'
group by schedule_tstamp, extracted_at, date_booked
order by schedule_tstamp asc
;

-- STEP 7

-- dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-04-02 00:00:00' --end-tstamp '2019-04-02 00:00:00' \

dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-17 00:00:00' --end-tstamp '2019-08-17 00:00:00' \
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
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-11 00:00:00' --end-tstamp '2019-09-11 00:00:00'

-- STEP 8

-- airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-04-02 00:00:00' --end_date '2019-04-02 00:00:00' \

airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-17 00:00:00' --end_date '2019-08-17 00:00:00' \
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
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-11 00:00:00' --end_date '2019-09-11 00:00:00'



-- airflow backfill --start_date '2019-04-02 00:00:00' --end_date '2019-04-02 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \

airflow backfill --start_date '2019-08-17 00:00:00' --end_date '2019-08-17 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
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
&& airflow backfill --start_date '2019-09-11 00:00:00' --end_date '2019-09-11 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly

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
    a.total_sell_rate - b.total_sell_rate as diff_total_sell_rate_backup_vs_rv,
    a.total_sell_rate_in_currency - b.total_sell_rate_in_currency as diff_total_sell_rate_in_currency_backup_vs_rv,
    a.commission_ex_vat - b.commission_ex_vat as diff_commission_ex_vat_backup_vs_rv --relevant for fix to derived exchange rate

from (
    select
        date_booked,
        'backup'::varchar as tablename,
        count(*) as nrows,
        sum(total_sell_rate) as total_sell_rate,
        sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
        sum(commission_ex_vat) as commission_ex_vat,
        schedule_tstamp, extracted_at
    from adhoc.public.travelbird_catalogue_booking_summary_snapshot_true_up_september_post_fix
    where DATE_BOOKED >= '2019-08-16 00:00:00' and DATE_BOOKED <= '2019-09-10 00:00:00'
    group by schedule_tstamp, extracted_at, date_booked
) as a --backup
left join (
    select
        'raw_vault'::varchar as tablename,
        count(*) as nrows,
        sum(total_sell_rate) as total_sell_rate, --relevant for derived exchange rate
        sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
        sum(commission_ex_vat) as commission_ex_vat,
        schedule_tstamp, extracted_at
    from raw_vault.travelbird_catalogue.booking_summary
     where DATE_BOOKED >= '2019-08-16 00:00:00' and DATE_BOOKED <= '2019-09-10 00:00:00'
    group by schedule_tstamp, extracted_at, date_booked
) as b -- live
on a.schedule_tstamp = b.schedule_tstamp
order by date_booked
;

-- STEP 15

airflow clear --no_confirm --dag_regex 'Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2' --start_date '2019-08-16 02:00:00' --end_date '2019-08-16 02:00:00'
airflow backfill --start_date '2019-08-16 02:00:00' --end_date '2019-08-16 02:00:00'  --local  Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2

