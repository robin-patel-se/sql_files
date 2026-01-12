self_describing_task --include 'staging/hygiene/sfsc/rebooking_requests'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
/
USERS/
robin/
myrepos/
one-DATA-pipeline/
biapp/
task_catalogue/
staging/
hygiene/
sfsc/
rebooking_requests.py

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfsc.rebooking_request_cases_ho CLONE raw_vault_mvp.sfsc.rebooking_request_cases_ho;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfsc.rebooking_request_cases_pkg CLONE raw_vault_mvp.sfsc.rebooking_request_cases_pkg

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.sfsc.rebooking_requests__model_data AS (
    SELECT

        -- (lineage) original metadata columns from previous step
        dataset_name                                                               AS row_dataset_name,
        dataset_source                                                             AS row_dataset_source,
        loaded_at                                                                  AS row_loaded_at,
        schedule_tstamp                                                            AS row_schedule_tstamp,
        run_tstamp                                                                 AS row_run_tstamp,
        filename                                                                   AS row_filename,
        file_row_number                                                            AS row_file_row_number,

        transaction_id,
        booking_id,
        booking_lookup_check_in_date, -- some records used to come in without :SS portion
        booking_lookup_check_out_date,
        booking_lookup_store_id,
        booking_lookup_supplier_territory,
        case_number::INT                                                           AS case_number,
        case_origin,
        case_owner_full_name,
        contact_reason,
        opportunity_sale_id,
        LOWER(status)                                                              AS status,
        --this is how we dedupe on a rank to deem case attributed to a booking
        CASE
            WHEN LOWER(status) = 'hold' THEN 1
            WHEN LOWER(status) = 'pending' THEN 1
            WHEN LOWER(status) = 'open' THEN 1
            WHEN LOWER(status) = 'new' THEN 2
            WHEN LOWER(status) = 'solved' THEN 3
            WHEN LOWER(status) = 'closed' THEN 4
            ELSE 99
            END                                                                    AS status_rank,
        LOWER(subject)                                                             AS subject,
        CASE
            WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NOT NULL
                THEN 'Member asked for rebooking with date'
            WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NULL
                THEN 'Member asked for rebooking without date'
            WHEN LOWER(subject) LIKE '%refund%' THEN 'Member asked for refund'
            WHEN LOWER(subject) LIKE '%storn%' THEN 'Member asked for refund'
            WHEN LOWER(subject) LIKE '%cxl%' THEN 'Member asked for refund'
            WHEN LOWER(subject) LIKE '%cancel%' THEN 'Member asked for refund'
            ELSE 'Unknown' END                                                     AS status_se,
        "VIEW",
        TRY_CAST(postponed_booking_request AS BOOLEAN)                             AS postponed_booking_request,
        requested_rebooking_date,
        LOWER(last_modified_by_full_name)                                          AS last_modified_by_full_name,
        LOWER(overbooking_rebooking_stage)                                         AS overbooking_rebooking_stage,
        LOWER(reason)                                                              AS reason,
        case_id,
        date_time_opened,
        case_name::INT                                                             AS case_name,
        last_modified_date,
        last_modified_by_case_overview,
        priority_type,
        covid19_member_resolution_cs,
        case_overview_id,
        case_thread_id,
        priority,
        -- CS sometimes put booking id in transaction id field
        COALESCE(transaction_id, booking_id)                                       AS unique_transaction_id,
        REGEXP_SUBSTR(transaction_id, '-.*-(.*)', 1, 1, 'e')                       AS booking_id_from_trans_id,
        ROW_NUMBER() OVER (
            PARTITION BY unique_transaction_id
            ORDER BY
                status_rank ASC, --choose case based on ranking
                case_number DESC, --if still dupes choose highest case number
                case_name DESC --if still dupes choose highest case overview name
            )                                                                      AS rank,

        -- hygiene flags
        IFF(unique_transaction_id IS NULL, 1, NULL)                                 AS fails_validation__unique_transaction_id__expected_nonnull,
        IFF(fails_validation__unique_transaction_id__expected_nonnull = 1, 1, NULL) AS failed_some_validation

    FROM hygiene_vault_mvp_dev_robin.sfsc.rebooking_requests__union_two_datasets

    WHERE
      --remove users that clean the data.
            LOWER(last_modified_by_full_name) NOT IN ('dylan hone', 'kate donaghy', 'jessica ho')
      AND NOT (
        --marta cleans data but also processes rebooking requests, so remove her only when she meets these criteria
            LOWER(last_modified_by_full_name) = 'marta lagut'
            AND case_name IS NULL
            AND lower(status) = 'solved'
        )
      AND LOWER(status) != 'closed' --not used anymore, sometimes close a case if its a duplicate
)
;

DROP TABLE hygiene_vault_mvp_dev_robin.sfsc.rebooking_requests;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.rebooking_requests;

airflow backfill --start_date '2020-04-07 04:00:00' --end_date '2020-04-07 04:00:00' --task_regex '.*' hygiene_snapshots__sfsc__rebooking_requests__daily_at_04h00
airflow backfill --start_date '2020-04-08 04:00:00' --end_date '2020-04-08 04:00:00' --task_regex '.*' hygiene_snapshots__sfsc__rebooking_requests__daily_at_04h00



airflow backfill --start_date '2020-06-22 09:30:00' --end_date '2020-06-22 09:30:00' --task_regex '.*' hygiene_snapshots__worldpay__transaction_summary_snapshot__daily_at_09h30