dataset_task --include 'marketing_gsheets.tv_spend_data' --operation ExtractOperation --method 'run'  --start '2022-06-12 00:00:00' --end '2022-06-13 00:00:00'
dataset_task --include 'marketing_gsheets.tv_spend_data' --operation IngestOperation --method 'run'  --start '2022-06-12 00:00:00' --end '2022-06-13 00:00:00'
dataset_task --include 'marketing_gsheets.tv_spend_data' --operation HygieneOperation --method 'run'  --start '2022-06-12 00:00:00' --end '2022-06-13 00:00:00'
dataset_task --include 'marketing_gsheets.tv_spend_data' --operation LatestRecordsOperation --method 'run'  --start '2022-06-12 00:00:00' --end '2022-06-13 00:00:00'

SELECT
    date,
    TRY_TO_DATE(date, 'dd-mon-yy')
FROM raw_vault_dev_robin.marketing_gsheets.tv_spend_data tsd;

DROP TABLE hygiene_vault_dev_robin.marketing_gsheets.tv_spend_data;
DROP TABLE latest_vault_dev_robin.marketing_gsheets.tv_spend_data;



SELECT
    -- (lineage) metadata for the current job
    '2022-06-12 00:00:00'                                                                 AS schedule_tstamp,
    '2022-06-15 08:42:48'                                                                 AS run_tstamp,
    'HygieneOperator__incoming__marketing_gsheets__tv_spend_data__20220612T000000__daily' AS operation_id,
    CURRENT_TIMESTAMP()::TIMESTAMP                                                        AS created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP                                                        AS updated_at,

    -- (lineage) original metadata of row itself
    row_dataset_name,
    row_dataset_source,
    row_loaded_at,
    row_schedule_tstamp,
    row_run_tstamp,
    row_filename,
    row_file_row_number,
    row_extract_metadata,


    -- transformed columns
    TRY_TO_DATE(date, 'dd-mon-yy')::DATE                                                  AS date,
    TRY_TO_DATE(month, 'dd-mon-yy')::DATE                                                 AS month,
    TRY_TO_DATE(weekbegin, 'dd-mon-yy')::DATE                                             AS week_begin,

    -- original columns
    key_dt,
    status,
    date__o,
    month__o,
    week_begin__o,
    territory,
    rollup,
    ppc_brand_leads,
    direct_leads,
    app_leads,
    total_gross_leads,
    tvs_reg,
    tvs_response_rev_gbp,
    media_spend,
    media_spend_gbp,
    impacts,
    ppc_brand_spend,
    tv_lead_value,
    mult_effect_sign_ups,
    mult_effect_revenue_existing_users,
    revenue_from_new_sign_ups,
    base_ppc_cost,
    net_brand_ppc_spend,
    base_ppc_sign_up,
    base_organic,
    base_app_sign_ups,
    net_core_sign_ups,
    net_app_sign_ups,
    net_sign_ups,
    free_sign_ups,
    total_atl_spend,
    cpl,
    response_rate,
    roas,
    other_spend,
    other_impacts,
    other_sign_ups,
    flag,

    -- validation columns
    IFF(key_dt IS NULL, 1, NULL)                                                          AS fails_validation__key_dt__expected_nonnull,
    IFF(fails_validation__key_dt__expected_nonnull = 1, 1, NULL)                          AS failed_some_validation

FROM hygiene_vault_dev_robin.marketing_gsheets.tv_spend_data__apply_data_types