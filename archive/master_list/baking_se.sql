CREATE OR REPLACE TABLE scratch.robinpatel.salesforce_rebooking_requests AS (
    WITH combine_sales_force
             AS (
            --two gsheets, Jan paid for add on to connect to sf
            -- he was going to separate. Short story, SF data comes in two files
            SELECT *
            FROM raw_vault_mvp.sfsc.rebooking_request_cases_pkg
            UNION
            SELECT *
            FROM raw_vault_mvp.sfsc.rebooking_request_cases_ho
        )
         -- Jan owns logic on how we categorise SF data
         --persist this level table as output so can be queried to debug.
    SELECT transaction_id,
           booking_id,
           booking_lookup_check_in_date, -- some records used to come in without :SS portion
           booking_lookup_check_out_date,
           booking_lookup_store_id,
           booking_lookup_supplier_territory,
           case_number::INT                               AS case_number,
           case_origin,
           case_owner_full_name,
           contact_reason,
           opportunity_sale_id,
           LOWER(status)                                  AS status,
           --this is how we dedupe on a rank to deem case attributed to a booking
           CASE
               WHEN LOWER(status) = 'hold' THEN 1
               WHEN LOWER(status) = 'pending' THEN 1
               WHEN LOWER(status) = 'open' THEN 1
               WHEN LOWER(status) = 'new' THEN 2
               WHEN LOWER(status) = 'solved' THEN 3
               WHEN LOWER(status) = 'closed' THEN 4
               ELSE 99
               END                                        AS status_rank,
           LOWER(subject)                                 AS subject,
           CASE
               WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NOT NULL
                   THEN 'Member asked for rebooking with date'
               WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NULL
                   THEN 'Member asked for rebooking without date'
               WHEN LOWER(subject) LIKE '%refund%' THEN 'Member asked for refund'
               WHEN LOWER(subject) LIKE '%storn%' THEN 'Member asked for refund'
               WHEN LOWER(subject) LIKE '%cxl%' THEN 'Member asked for refund'
               WHEN LOWER(subject) LIKE '%cancel%' THEN 'Member asked for refund'
               ELSE 'Unknown' END                         AS status_se,
           "VIEW",
           TRY_CAST(postponed_booking_request AS BOOLEAN) AS postponed_booking_request,
           requested_rebooking_date,
           LOWER(last_modified_by_full_name)              AS last_modified_by_full_name,
           LOWER(overbooking_rebooking_stage)             AS overbooking_rebooking_stage,
           LOWER(reason)                                  AS reason,
           case_id,
           date_time_opened,
           case_name::INT                                 AS case_name,
           last_modified_date,
           last_modified_by_case_overview,
           priority_type,
           covid19_member_resolution_cs,
           case_overview_id,
           case_thread_id,
           priority,
           -- CS sometimes put booking id in transaction id field
           COALESCE(transaction_id, booking_id)           AS unique_transaction_id,
           -- hygiene flags
           CASE
               WHEN unique_transaction_id IS NULL
                   THEN 1
               END                                        AS fails_validation__unique_transaction_id__expected_nonnull,
           CASE
               WHEN fails_validation__unique_transaction_id__expected_nonnull = 1
                   THEN 1
               END                                        AS failed_some_validation,
           ROW_NUMBER() OVER ( --create a rank to filter later
               PARTITION BY unique_transaction_id
               ORDER BY
                   status_rank ASC, --choose case based on ranking
                   case_number DESC, --if still dupes choose highest case number
                   case_name DESC --if still dupes choose highest case overview name
               )                                          AS rank
    FROM combine_sales_force
    WHERE lower(last_modified_by_full_name) NOT IN ('dylan hone', 'kate donaghy', 'jessica ho') --these guys clean data
      AND NOT ( --marta cleans data but not always so only exclude marta when she meets these criteria
            lower(last_modified_by_full_name) = 'marta lagut'
            AND case_name IS NULL
            AND lower(status) = 'solved'
        )
      AND lower(status) != 'closed' --not used anymore, sometimes close a case if its a duplicate
)


CREATE TABLE robinpatel.salesforce_rebooking_requests
(


    transaction_id                                            VARCHAR,
    booking_id                                                VARCHAR,
    booking_lookup_check_in_date                              TIMESTAMP,
    booking_lookup_check_out_date                             TIMESTAMP,
    booking_lookup_store_id                                   VARCHAR,
    booking_lookup_supplier_territory                         VARCHAR,
    case_number                                               NUMBER,
    case_origin                                               VARCHAR,
    case_owner_full_name                                      VARCHAR,
    contact_reason                                            VARCHAR,
    opportunity_sale_id                                       VARCHAR,
    status                                                    VARCHAR,
    status_rank                                               NUMBER,
    subject                                                   VARCHAR,
    status_se                                                 VARCHAR,
    "VIEW"                                                    VARCHAR,
    postponed_booking_request                                 BOOLEAN,
    requested_rebooking_date                                  DATE,
    last_modified_by_full_name                                VARCHAR,
    overbooking_rebooking_stage                               VARCHAR,
    reason                                                    VARCHAR,
    case_id                                                   VARCHAR,
    date_time_opened                                          TIMESTAMP,
    case_name                                                 NUMBER,
    last_modified_date                                        DATE,
    last_modified_by_case_overview                            VARCHAR,
    priority_type                                             VARCHAR,
    covid19_member_resolution_cs                              VARCHAR,
    case_overview_id                                          VARCHAR,
    case_thread_id                                            VARCHAR,
    priority                                                  VARCHAR,
    unique_transaction_id                                     VARCHAR,
    fails_validation__unique_transaction_id__expected_nonnull NUMBER,
    failed_some_validation                                    NUMBER,
    rank                                                      NUMBER
);

SELECT salesforce_rebooking_requests.case_number,
       count(*)
FROM scratch.robinpatel.salesforce_rebooking_requests
GROUP BY 1
HAVING COUNT(*) > 1;


SELECT case_number,
       case_name,
       count(*)
FROM raw_vault_mvp.sfsc.rebooking_request_cases_pkg
GROUP BY 1, 2
HAVING count(*) > 1;


CREATE OR REPLACE SCHEMA raw_vault_mvp_dev_robin.sfsc;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfsc.rebooking_request_cases_ho CLONE raw_vault_mvp.sfsc.rebooking_request_cases_ho;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfsc.rebooking_request_cases_pkg CLONE raw_vault_mvp.sfsc.rebooking_request_cases_pkg;

self_describing_task --include 'dv/dwh/master_booking_list/salesforce_rebooking_request'  --method 'run' --start '2020-06-21 00:00:00' --end '2020-06-21 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.salesforce_rebooking_requests sbr
WHERE unique_transaction_id IS NULL;

SELECT sbr.unique_transaction_id,
       sbr.subject,
       REGEXP_SUBSTR(sbr.subject, '.*(\\\s|_)(\\\d+\\\-\\\d+\\\-\\\d+)', 1, 1, 'e')
FROM data_vault_mvp_dev_robin.dwh.salesforce_rebooking_requests sbr
WHERE unique_transaction_id IS NULL;

SELECT 'fw: re: re: re: buchungsnummer:	106389-882497-52233536' REGEXP '.*(A?\\\d{6,7}\-\\\d{6,7}\-\\\d{7,8})'

SELECT *
FROM hygiene_snapshot_vault_mvp.sfsc.rebooking_request_cases;

SELECT rrcp.case_id,
       rrcp.case_name,
       rrcp.case_overview_id
FROM raw_vault_mvp.sfsc.rebooking_request_cases_pkg rrcp


HAVING COUNT(*) > 1;

self_describing_task --include 'staging/hygiene/sfsc/rebooking_requests'  --method 'run' --start '2020-06-21 00:00:00' --end '2020-06-21 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfsc/rebooking_requests'  --method 'run' --start '2020-06-21 00:00:00' --end '2020-06-21 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfsc.rebooking_requests;
SELECT *
FROM hygiene_vault_mvp_dev_robin.sfsc.rebooking_requests;

------------------------------------------------------------------------------------------------------------------------

airflow backfill --start_date '2020-03-25 00:00:00' --end_date '2020-03-25 00:00:00' --task_regex '.*' incoming__cms_mysql__amendment__daily
airflow backfill --start_date '2020-03-26 00:00:00' --end_date '2020-06-23 00:00:00' --task_regex '.*' -m incoming__cms_mysql__amendment__daily
--min date created is '2020-03-26'

SELECT *
FROM raw_vault_mvp_dev_robin.cms_mysql.amendment;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.amendment CLONE raw_vault_mvp.cms_mysql.amendment;

SELECT *
FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.amendment_snapshot "AS";

self_describing_task --include 'dv/cms_snapshots/cms_mysql_snapshot_bulk_wave2'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.amendment_snapshot "AS"
WHERE "AS".product_reservation_id IS NOT NULL;

SELECT *
FROM raw_vault_mvp.cms_mysql.product_reservation pr;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.product_reservation CLONE raw_vault_mvp.cms_mysql.product_reservation;
SELECT *
FROM raw_vault_mvp_dev_robin.cms_mysql.product_reservation pr;
SELECT *
FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.product_reservation_snapshot;
self_describing_task --include 'dv/cms_snapshots/cms_mysql_snapshot_bulk_wave2'  --method 'run' --start '2020-01-01 00:00:00' --end '2020-01-01 00:00:00'



------------------------------------------------------------------------------------------------------------------------

SELECT
    -- a booking can have multiple adjustments but we needs the dates associated with
    -- the most recent one
    -- TODO: if we need this elsewhere too we should define it in a `dv` module
    -- or change `cms_mysql_snapshot_bulk_wave2.booking_adjustment` to deduplicate on `booking_id` instead of `id`
    booking_id,
    check_in_date::DATE  AS adjusted_check_in_date,
    check_out_date::DATE AS adjusted_check_out_date,
    stay_by_date         AS voucher_stay_by_date
FROM data_vault_mvp.cms_mysql_snapshots.booking_adjustment_snapshot
    QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY date_created DESC) = 1;

self_describing_task --include 'dv/dwh/transactional/se_booking'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;

SELECT booking_status, count(*)
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
GROUP BY 1;
SELECT booking_status, count(*)
FROM data_vault_mvp.dwh.se_booking sb
GROUP BY 1;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.amendment_snapshot;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb;

SELECT *
FROM hygiene_vault_mvp_dev_robin.worldpay.transaction_summary;

SELECT *
FROM data_vault_mvp.dwh.se_booking sb;
SELECT *
FROM se.data_pii.se_booking_summary_extended sbse;



SELECT transactionid
FROM se.data_pii.se_booking_summary_extended sbse
MINUS
SELECT sb.transaction_id
FROM data_vault_mvp.dwh.se_booking sb;

SELECT *
FROM se.data_pii.se_booking_summary_extended sbse
WHERE sbse.transactionid IN ('A11037-12415-1387583',
                             'A10646-12118-1387596',
                             'A10640-12116-1387595')


CREATE OR REPLACE VIEW se_dev_robin.data.se_credit_model AS
SELECT *
FROM se.data.se_credit_model scm;

CREATE OR REPLACE VIEW se_dev_robin.data_pii.se_booking_summary_extended AS
SELECT *
FROM se.data_pii.se_booking_summary_extended sbse;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.rebooking_requests CLONE hygiene_snapshot_vault_mvp.sfsc.rebooking_requests;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.sfsc.rebooking_requests CLONE hygiene_vault_mvp.sfsc.rebooking_requests;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.booking_cancellation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot;

self_describing_task --include 'dv/dwh/master_booking_list/master_se_booking_list'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

DROP SCHEMA data_vault_mvp_dev_robin.cms_mysql_snapshots;
CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.cms_mysql_snapshots CLONE data_vault_mvp.cms_mysql_snapshots;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list__model_data;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list__model_data
WHERE master_se_booking_list__model_data.dwh_transaction_id = 'A8741-10045-1140152';

SELECT *
FROM collab.covid_pii.covid_master_list_ho_packages cmlhp
WHERE cmlhp.transactionid = 'A8741-10045-1140152';


SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfsc.rebooking_requests rr
WHERE rr.transaction_id IN (
    SELECT rr.transaction_id
    FROM hygiene_snapshot_vault_mvp_dev_robin.sfsc.rebooking_requests rr
    GROUP BY 1
    HAVING COUNT(*) > 1
);



SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list msbl
WHERE msbl.transaction_id IN (
    SELECT transaction_id
    FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list msbl
    GROUP BY 1
    HAVING COUNT(*) > 1
);
SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list msbl
WHERE msbl.booking_id = 'A1259551';

SELECT *
FROM collab.covid_pii.covid_master_list_ho_packages cmlhp
WHERE cmlhp.transaction_id IN (
    SELECT transactionid,
           count(*)
    FROM collab.covid_pii.covid_master_list_ho_packages c
    GROUP BY 1
    HAVING COUNT(*) > 1
);

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.sfsc.rebooking_requests CLONE hygiene_vault_mvp.sfsc.rebooking_requests;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.rebooking_requests CLONE hygiene_snapshot_vault_mvp.sfsc.rebooking_requests;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.worldpay.transaction_summary CLONE hygiene_snapshot_vault_mvp.worldpay.transaction_summary;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.ratepay.clearing CLONE hygiene_snapshot_vault_mvp.ratepay.clearing;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.manual_refunds CLONE hygiene_snapshot_vault_mvp.finance_gsheets.manual_refunds;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report CLONE hygiene_snapshot_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_status CLONE hygiene_snapshot_vault_mvp.finance_gsheets.cash_refunds_airline_refund_status;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.chargebacks_se CLONE hygiene_snapshot_vault_mvp.finance_gsheets.chargebacks_se;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_to_members CLONE hygiene_snapshot_vault_mvp.finance_gsheets.cash_refunds_to_members;


airflow backfill --start_date '2020-07-01 03:00:00' --end_date '2020-07-01 03:00:00' --task_regex '.*' dwh__master_se_booking_list__daily_at_03h00

SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list msbl
GROUP BY 1
HAVING count(*) > 1;

SELECT *
FROM collab.covid_pii.covid_master_list_ho_packages cmlhp
WHERE cmlhp.transactionid IN (
    SELECT cmlhp.transactionid
    FROM collab.covid_pii.covid_master_list_ho_packages cmlhp
    MINUS
    SELECT msbl.transaction_id
    FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list msbl
);

SELECT COUNT(*)
FROM collab.covid_pii.covid_master_list_ho_packages cmlhp; --1105746

SELECT COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list msbl; --1105725


SELECT *
FROM data_vault_mvp.dwh.master_se_booking_list msbl
WHERE msbl.booking_id IN (
    SELECT msbl.booking_id
    FROM data_vault_mvp.dwh.master_se_booking_list msbl
    GROUP BY 1
    HAVING count(*) > 1
)

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report CLONE raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report;
self_describing_task --include 'staging/hygiene/finance_gsheets/cash_refunds_airline_refund_report.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/finance_gsheets/cash_refunds_airline_refund_report.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT *
FROM hygiene_snapshot_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report crarr
WHERE crarr.booking_id IN (
    SELECT booking_id
    FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report crarr
    WHERE flight_rank = 1
    GROUP BY 1
    HAVING count(*) > 1
);

SELECT CASE
           WHEN booking_system = 'Travelbird' THEN 'TB-' || booking_reference
           WHEN LEFT(external_reference, 1) = 'A' THEN 'A' || REGEXP_SUBSTR(external_reference, '-.*-(.*)', 1, 1, 'e')
           ELSE REGEXP_SUBSTR(external_reference, '-.*-(.*)', 1, 1, 'e') END AS booking_id,
       external_reference                                                    AS transaction_id,
       crarr.booking_reference,
       crarr.external_reference,
       crarr.booking_system,
       crarr.file_row_number
FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report crarr
WHERE booking_id IN (
    SELECT booking_id
    FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report crarr
    WHERE flight_rank = 1
    GROUP BY 1
    HAVING count(*) > 1
)
