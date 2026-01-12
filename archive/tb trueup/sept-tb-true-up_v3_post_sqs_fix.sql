-- Following true up of repairing issues with currency conversion for deposit bookings we also identified that for a few edge cases bookings/customer information wasn't being sent via sqs messages to our transactional database
-- this resulted in the transformation query not being able to resolve customer id's via an email address because they didn't exist in our database. This resulted in them not being pushed into the cube and therefore not appearing.
-- Another true up necessary to repopulate data to chiasma to inform of the customer that made booking
use role ACCOUNTADMIN;
-- STEP 4

-- NOTE: remember to change `june` with the actual month when trueup happens
create or replace transient table adhoc.public.travelbird_catalogue_booking_summary_snapshot_true_up_september_post_sqs_fix
    clone raw_vault.travelbird_catalogue.booking_summary;

-- STEP 5

-- NOTE: remember to change `june` with the actual month when trueup happens
select
    'backup table'::varchar as tablename,
    date_booked,
    count(*) as nrows,
    count(distinct customer_id) as customers,
    sum(commission_ex_vat) as commission_ex_vat,
    sum(total_sell_rate) as total_sell_rate,
    sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
    schedule_tstamp, extracted_at
from adhoc.public.travelbird_catalogue_booking_summary_snapshot_true_up_september_post_sqs_fix
where
    DATE_BOOKED IN ('2019-09-02', '2019-09-11', '2019-09-17')
group by schedule_tstamp, extracted_at, date_booked
order by schedule_tstamp asc
;

--  STEP 6 -- for comparison to archived table in step 5

select
    'raw_vault table'::varchar as tablename,
    date_booked,
    count(*) as nrows,
    count(distinct customer_id) as customers,
    sum(commission_ex_vat) as commission_ex_vat,
    sum(total_sell_rate) as total_sell_rate,
    sum(total_sell_rate_in_currency) as total_sell_rate_in_currency,
    schedule_tstamp, extracted_at
from raw_vault.travelbird_catalogue.booking_summary
where
    DATE_BOOKED IN ('2019-09-02', '2019-09-11', '2019-09-17')
group by schedule_tstamp, extracted_at, date_booked
order by schedule_tstamp asc
;

-- STEP 7

-- dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-04-02 00:00:00' --end-tstamp '2019-04-02 00:00:00' \

dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-03 00:00:00' --end-tstamp '2019-09-03 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-12 00:00:00' --end-tstamp '2019-09-12 00:00:00' \
&& dataset_task --include travelbird_catalogue.booking_summary --run-retracts --retract-extracts --start-tstamp '2019-09-18 00:00:00' --end-tstamp '2019-09-18 00:00:00'

-- STEP 8

-- airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-04-02 00:00:00' --end_date '2019-04-02 00:00:00' \

airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-03 00:00:00' --end_date '2019-09-03 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-12 00:00:00' --end_date '2019-09-12 00:00:00' \
&& airflow clear --no_confirm --dag_regex 'ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly' --start_date '2019-09-18 00:00:00' --end_date '2019-09-18 00:00:00'


-- airflow backfill --start_date '2019-04-02 00:00:00' --end_date '2019-04-02 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \

airflow backfill --start_date '2019-09-03 00:00:00' --end_date '2019-09-03 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-12 00:00:00' --end_date '2019-09-12 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly \
&& airflow backfill --start_date '2019-09-18 00:00:00' --end_date '2019-09-18 00:00:00' --local ExtractIngestDag_v0_1__travelbird_catalogue__booking_summary__hourly


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
    from adhoc.public.travelbird_catalogue_booking_summary_snapshot_true_up_september_post_sqs_fix
    where DATE_BOOKED IN ('2019-09-02', '2019-09-11', '2019-09-17')
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
     where DATE_BOOKED IN ('2019-09-02', '2019-09-11', '2019-09-17')
    group by schedule_tstamp, extracted_at, date_booked
) as b -- live
on a.schedule_tstamp = b.schedule_tstamp
order by date_booked
;

-- STEP 15

airflow clear --no_confirm --dag_regex 'Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2' --start_date '2019-09-02 02:00:00' --end_date '2019-09-02 02:00:00'
airflow backfill --start_date '2019-09-02 02:00:00' --end_date '2019-09-02 02:00:00'  --local  Export_v0_1__chiasma__travelbird_catalogue_bookings__preexport__daily_at_2

