-- Created archive bucket in s3 - "booking_summary_true_up-september"
-- Copied existing week 35 26th aug to 1st sep files into archive bucket
-- uploaded newly downloaded booking summary files from TB CMS (09/09/2019 at 10.30AM approx) into main bucket (to replace existing)


-- STEP 4

-- NOTE: remember to change `september` with the actual month when trueup happens
create or replace transient table adhoc.public.travelbird_catalogue_booking_summary_snapshot_true_up_september
    clone raw_vault.travelbird_catalogue.booking_summary;

-- STEP 5

-- NOTE: remember to change `september` with the actual month when trueup happens
select
    'raw_vault table'::varchar as tablename,
    date_booked,
    count(*) as nrows,
    sum(commission_ex_vat) as commission_ex_vat,
    sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
    schedule_tstamp, extracted_at
from adhoc.public.travelbird_catalogue_booking_summary_snapshot_true_up_september
where
    -- this where clause gives us all the schedule tstamps that fetched files booking_summary_20190401_0300.csv through to booking_summary_20190531_0200.csv (as the job is configured on each schedule tstamp we look for the day before)
    schedule_tstamp >= '2019-08-27 00:00:00'
and schedule_tstamp <= '2019-09-02 00:00:00'
group by schedule_tstamp, extracted_at, date_booked
order by schedule_tstamp asc
;

--  STEP 6 -- for comparison to archived table in step 5

select
    'raw_vault table'::varchar as tablename,
    date_booked,
    count(*) as nrows,
    sum(commission_ex_vat) as commission_ex_vat,
    sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
    schedule_tstamp, extracted_at
from raw_vault.travelbird_catalogue.booking_summary
where
    -- this where clause gives us all the schedule tstamps that fetched files booking_summary_20190401_0300.csv through to booking_summary_20190531_0200.csv (as the job is configured on each schedule tstamp we look for the day before)
    schedule_tstamp >= '2019-08-27 00:00:00'
and schedule_tstamp <= '2019-09-02 00:00:00'
group by schedule_tstamp, extracted_at, date_booked
order by schedule_tstamp asc
;

-- STEP 7

dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-27 00:00:00' --end-tstamp '2019-08-27 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-28 00:00:00' --end-tstamp '2019-08-28 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-29 00:00:00' --end-tstamp '2019-08-29 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-30 00:00:00' --end-tstamp '2019-08-30 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-08-31 00:00:00' --end-tstamp '2019-08-31 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-01 00:00:00' --end-tstamp '2019-09-01 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-02 00:00:00' --end-tstamp '2019-09-02 00:00:00'


-- STEP 8

airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-27 00:00:00' --end_date '2019-08-27 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-28 00:00:00' --end_date '2019-08-28 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-29 00:00:00' --end_date '2019-08-29 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-30 00:00:00' --end_date '2019-08-30 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-08-31 00:00:00' --end_date '2019-08-31 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-01 00:00:00' --end_date '2019-09-01 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-02 00:00:00' --end_date '2019-09-02 00:00:00' \

airflow backfill --start_date '2019-08-27 00:00:00' --end_date '2019-08-27 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-28 00:00:00' --end_date '2019-08-28 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-29 00:00:00' --end_date '2019-08-29 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-30 00:00:00' --end_date '2019-08-30 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-08-31 00:00:00' --end_date '2019-08-31 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-01 00:00:00' --end_date '2019-09-01 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-02 00:00:00' --end_date '2019-09-02 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \

-- alter session set use_cached_result=FALSE;

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
    a.total_sell_rate_in_currency - b.total_sell_rate_in_currency as diff_total_sell_rate_in_currency_backup_vs_rv,
    a.commission_ex_vat - b.commission_ex_vat as diff_commission_ex_vat_backup_vs_rv

from (
    select
        date_booked,
        'backup'::varchar as tablename,
        count(*) as nrows,
        sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
        sum(commission_ex_vat) as commission_ex_vat,
        schedule_tstamp, extracted_at
    from adhoc.public.travelbird_catalogue_booking_summary_snapshot_true_up_september
    where schedule_tstamp >= '2019-08-27 00:00:00' and schedule_tstamp <= '2019-09-02 00:00:00'
    group by schedule_tstamp, extracted_at, date_booked
) as a
left join (
    select
        'raw_vault'::varchar as tablename,
        count(*) as nrows,
        sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
        sum(commission_ex_vat) as commission_ex_vat,
        schedule_tstamp, extracted_at
    from raw_vault.travelbird_catalogue.booking_summary
    where schedule_tstamp >= '2019-08-27 00:00:00' and schedule_tstamp <= '2019-09-02 00:00:00'
    group by schedule_tstamp, extracted_at, date_booked
) as b
on a.schedule_tstamp = b.schedule_tstamp
order by date_booked
;

-- STEP 15

-- Create one of these for each of the extracts you want to push to Chiasma

airflow clear --no_confirm --dag_regex 'Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2' --start_date '2019-08-27 02:00:00' --end_date '2019-08-27 02:00:00'
airflow clear --no_confirm --dag_regex 'Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2' --start_date '2019-08-28 02:00:00' --end_date '2019-08-28 02:00:00'
airflow clear --no_confirm --dag_regex 'Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2' --start_date '2019-08-29 02:00:00' --end_date '2019-08-29 02:00:00'
airflow clear --no_confirm --dag_regex 'Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2' --start_date '2019-08-30 02:00:00' --end_date '2019-08-30 02:00:00'
airflow clear --no_confirm --dag_regex 'Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2' --start_date '2019-08-31 02:00:00' --end_date '2019-08-31 02:00:00'
airflow clear --no_confirm --dag_regex 'Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2' --start_date '2019-09-01 02:00:00' --end_date '2019-09-01 02:00:00'
airflow clear --no_confirm --dag_regex 'Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2' --start_date '2019-09-02 02:00:00' --end_date '2019-09-02 02:00:00'

airflow backfill --start_date '2019-08-27 02:00:00' --end_date '2019-08-27 02:00:00'  --local  Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2

