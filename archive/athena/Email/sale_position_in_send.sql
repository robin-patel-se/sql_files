CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.sfmc.athena_send_log CLONE raw_vault_mvp.sfmc.athena_send_log;

SELECT *
FROM raw_vault_mvp_dev_robin.sfmc.athena_send_log;

--hygiene step

self_describing_task --include 'staging/hygiene/sfmc/athena_send_log.py'  --method 'run' --start '2021-07-18 00:00:00' --end '2021-07-18 00:00:00'

--adjust hygiene history
USE WAREHOUSE pipe_2xlarge;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.sfmc.athena_send_log_20210719 CLONE hygiene_vault_mvp.sfmc.athena_send_log;

DROP TABLE hygiene_vault_mvp_dev_robin.sfmc.athena_send_log;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.sfmc.athena_send_log
(

    -- (lineage) metadata for the current job
    schedule_tstamp                                         TIMESTAMP,
    run_tstamp                                              TIMESTAMP,
    operation_id                                            VARCHAR,
    created_at                                              TIMESTAMP,
    updated_at                                              TIMESTAMP,

    -- (lineage) original metadata of row itself
    row_dataset_name                                        VARCHAR,
    row_dataset_source                                      VARCHAR,
    row_loaded_at                                           TIMESTAMP,
    row_schedule_tstamp                                     TIMESTAMP,
    row_run_tstamp                                          TIMESTAMP,
    row_filename                                            VARCHAR,
    row_file_row_number                                     INT,

    -- hygiene columns
    deal_position_in_send                                   INT,
    deal_position_within_top_10                             BOOLEAN,

    -- original columns that don't require any hygiene
    log_date                                                TIMESTAMP,
    territory_id                                            INT,
    job_id                                                  INT,
    campaign_name                                           VARCHAR,
    subscriber_key                                          BIGINT,
    source_table                                            VARCHAR,
    campaign_type                                           VARCHAR,
    load_date                                               TIMESTAMP,
    section_campaign_filters                                VARCHAR,
    section                                                 INT,
    position_in_section                                     INT,
    deal_id                                                 VARCHAR,
    content_source                                          VARCHAR,

    -- hygiene flags
    failed_some_validation                                  INT,
    fails_validation__log_date__expected_nonnull            INT,
    fails_validation__territory_id__expected_nonnull        INT,
    fails_validation__job_id__expected_nonnull              INT,
    fails_validation__campaign_name__expected_nonnull       INT,
    fails_validation__subscriber_key__expected_nonnull      INT,
    fails_validation__source_table__expected_nonnull        INT,
    fails_validation__campaign_type__expected_nonnull       INT,
    fails_validation__load_date__expected_nonnull           INT,
    fails_validation__position_in_section__expected_nonnull INT
);

INSERT INTO hygiene_vault_mvp_dev_robin.sfmc.athena_send_log
SELECT asl.schedule_tstamp,
       asl.run_tstamp,
       asl.operation_id,
       asl.created_at,
       asl.updated_at,
       asl.row_dataset_name,
       asl.row_dataset_source,
       asl.row_loaded_at,
       asl.row_schedule_tstamp,
       asl.row_run_tstamp,
       asl.row_filename,
       asl.row_file_row_number,
       ROW_NUMBER() OVER (PARTITION BY asl.job_id, asl.subscriber_key ORDER BY asl.section, asl.position_in_section) AS deal_position_in_send,
       IFF(deal_position_in_send <= 10, TRUE, FALSE) AS deal_position_within_top_10,
       asl.log_date,
       asl.territory_id,
       asl.job_id,
       asl.campaign_name,
       asl.subscriber_key,
       asl.source_table,
       asl.campaign_type,
       asl.load_date,
       asl.section_campaign_filters,
       asl.section,
       asl.position_in_section,
       asl.deal_id,
       asl.content_source,
       asl.failed_some_validation,
       asl.fails_validation__log_date__expected_nonnull,
       asl.fails_validation__territory_id__expected_nonnull,
       asl.fails_validation__job_id__expected_nonnull,
       asl.fails_validation__campaign_name__expected_nonnull,
       asl.fails_validation__subscriber_key__expected_nonnull,
       asl.fails_validation__source_table__expected_nonnull,
       asl.fails_validation__campaign_type__expected_nonnull,
       asl.fails_validation__load_date__expected_nonnull,
       asl.fails_validation__position_in_section__expected_nonnull
FROM hygiene_vault_mvp_dev_robin.sfmc.athena_send_log_20210719 asl;

SELECT *
FROM hygiene_vault_mvp_dev_robin.sfmc.athena_send_log asl;


------------------------------------------------------------------------------------------------------------------------

DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.athena_send_log;

self_describing_task --include 'staging/hygiene_snapshots/sfmc/athena_send_log.py'  --method 'run' --start '2021-07-18 00:00:00' --end '2021-07-18 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.athena_send_log_20210719 CLONE hygiene_snapshot_vault_mvp.sfmc.athena_send_log;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.athena_send_log
(

    -- (lineage) metadata for the current job
    schedule_tstamp          TIMESTAMP,
    run_tstamp               TIMESTAMP,
    operation_id             VARCHAR,
    created_at               TIMESTAMP,
    updated_at               TIMESTAMP,

    -- (lineage) original metadata of row itself
    row_dataset_name         VARCHAR,
    row_dataset_source       VARCHAR,
    row_loaded_at            TIMESTAMP,
    row_schedule_tstamp      TIMESTAMP,
    row_run_tstamp           TIMESTAMP,
    row_filename             VARCHAR,
    row_file_row_number      INT,

    -- hygiene columns
    deal_position_in_send    INT,
    deal_position_within_top_10 BOOLEAN,

    -- original columns that don't require any hygiene
    log_date                 TIMESTAMP,
    territory_id             INT,
    job_id                   INT,
    campaign_name            VARCHAR,
    subscriber_key           BIGINT,
    source_table             VARCHAR,
    campaign_type            VARCHAR,
    load_date                TIMESTAMP,
    section_campaign_filters VARCHAR,
    section                  INT,
    position_in_section      INT,
    deal_id                  VARCHAR,
    content_source           VARCHAR,

    PRIMARY KEY (
                 log_date,
                 territory_id,
                 job_id,
                 campaign_name,
                 subscriber_key,
                 deal_id
        )
);

INSERT INTO hygiene_snapshot_vault_mvp_dev_robin.sfmc.athena_send_log
SELECT asl.schedule_tstamp,
       asl.run_tstamp,
       asl.operation_id,
       asl.created_at,
       asl.updated_at,
       asl.row_dataset_name,
       asl.row_dataset_source,
       asl.row_loaded_at,
       asl.row_schedule_tstamp,
       asl.row_run_tstamp,
       asl.row_filename,
       asl.row_file_row_number,
       ROW_NUMBER() OVER (PARTITION BY asl.job_id, asl.subscriber_key ORDER BY asl.section, asl.position_in_section) AS deal_position_in_send,
       IFF(deal_position_in_send <= 10, TRUE, FALSE) AS deal_position_within_top_10,
       asl.log_date,
       asl.territory_id,
       asl.job_id,
       asl.campaign_name,
       asl.subscriber_key,
       asl.source_table,
       asl.campaign_type,
       asl.load_date,
       asl.section_campaign_filters,
       asl.section,
       asl.position_in_section,
       asl.deal_id,
       asl.content_source
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.athena_send_log_20210719 asl;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.athena_send_log asl;

------------------------------------------------------------------------------------------------------------------------
--athena sales_in_send

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_list;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_sources;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.athena_sales_in_send CLONE data_vault_mvp.dwh.athena_sales_in_send;

self_describing_task --include 'dv/dwh/athena/sales_in_send.py'  --method 'run' --start '2021-07-18 00:00:00' --end '2021-07-18 00:00:00'

------------------------------------------------------------------------------------------------------------------------
--athena email reporting

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred CLONE hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.crm_email_segments CLONE data_vault_mvp.dwh.crm_email_segments;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_calendar CLONE data_vault_mvp.dwh.se_calendar;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting CLONE data_vault_mvp.dwh.athena_email_reporting;


self_describing_task --include 'dv/dwh/athena/email_reporting.py'  --method 'run' --start '2021-07-15 00:00:00' --end '2021-07-15 00:00:00';


SELECT *
FROM hygiene_snapshot_vault_mvp.sfmc.athena_send_log asl;
SELECT *
FROM hygiene_vault_mvp_dev_robin.sfmc.athena_send_log asl;

SELECT module_touch_basic_attributes.updated_at
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
WHERE module_touch_basic_attributes.touch_start_tstamp >= CURRENT_DATE - 1;

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM se.data.user_segmentation