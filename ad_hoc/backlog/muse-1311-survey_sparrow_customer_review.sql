SELECT ubr.*,
       ua.email,
       us.margin_segment
FROM data_vault_mvp.dwh.user_booking_review ubr
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON ubr.shiro_user_id = ua.shiro_user_id
    INNER JOIN data_vault_mvp.dwh.user_segmentation us ON ubr.shiro_user_id = us.shiro_user_id AND us.date = CURRENT_DATE - 1;


--standard review data
SELECT nr.custom_properties[1]:value_string::VARCHAR AS booking_id,
       nr.response_created_at,
       nr.response_created_at::DATE,
       nr.score                                      AS customer_score,
       se.data.review_type(nr.score)                 AS review_type,
       nr.contact_properties,
--        nr.answers[1]:answer_txt::VARCHAR             AS answer_text,
       nr.record
FROM latest_vault.survey_sparrow.nps_responses nr;

--flatten additional questions
WITH flatten_questions AS (
    SELECT nr.record,
           nr.custom_properties[1]:value_string::VARCHAR AS booking_id,
           elements.key                                  AS question_id,
           elements.value                                AS question_context,
           TRIM(elements.value:question::VARCHAR)        AS follow_up_question,
           elements.value:answer::VARCHAR                AS follow_up_answer
    FROM latest_vault.survey_sparrow.nps_responses nr,
         LATERAL FLATTEN(INPUT => nr.record, OUTER => TRUE) elements
    WHERE elements.key LIKE 'question_%' -- filter flatten based on question keys
      AND TRY_TO_NUMBER(elements.value:answer::VARCHAR) IS NULL --filter out questions that result in the customer score
)
SELECT *
FROM flatten_questions fq
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fq.booking_id ORDER BY fq.question_id) = 1
;



SELECT IFF(nr.record:customProperties[1]:value_string::VARCHAR LIKE '%-%'
           , 'TB-' || SPLIT_PART(nr.record:customProperties[1]:value_string::VARCHAR, '-', -1)
           , nr.record:customProperties[1]:value_string::VARCHAR) AS booking_id,
       nr.response_created_at,
       nr.response_created_at::DATE,
       nr.score                                                   AS customer_score,
       se.data.review_type(nr.score)                              AS review_type,
       nr.contact_properties,
--        nr.answers[1]:answer_txt::VARCHAR             AS answer_text,
       nr.record
FROM latest_vault.survey_sparrow.nps_responses nr;

dataset_task --include 'survey_sparrow.nps_responses' --operation LatestRecordsOperation --method 'run' --upstream --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- post deployment steps

-- update hygiene vault
-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.survey_sparrow.nps_responses CLONE hygiene_vault.survey_sparrow.nps_responses;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.survey_sparrow.nps_responses_20211104 CLONE hygiene_vault.survey_sparrow.nps_responses;

CREATE OR REPLACE TABLE hygiene_vault_dev_robin.survey_sparrow.nps_responses
(

    -- (lineage) metadata for the current job
    schedule_tstamp                               TIMESTAMP NOT NULL,
    run_tstamp                                    TIMESTAMP NOT NULL,
    operation_id                                  VARCHAR   NOT NULL,
    created_at                                    TIMESTAMP NOT NULL,
    updated_at                                    TIMESTAMP NOT NULL,

    -- (lineage) original metadata of row itself
    row_dataset_name                              VARCHAR   NOT NULL,
    row_dataset_source                            VARCHAR   NOT NULL,
    row_loaded_at                                 TIMESTAMP NOT NULL,
    row_schedule_tstamp                           TIMESTAMP NOT NULL,
    row_run_tstamp                                TIMESTAMP NOT NULL,
    row_filename                                  VARCHAR   NOT NULL,
    row_file_row_number                           INT       NOT NULL,
    row_extract_metadata                          VARIANT,


    -- transformed columns
    account_id                                    INT,
    answers                                       ARRAY,
    browser                                       VARCHAR,
    browser_language                              VARCHAR,
    contact_id                                    INT,
    contact_name                                  VARCHAR,
    contact_properties                            ARRAY,
    custom_properties                             VARIANT,
    custom_properties_second_value                VARCHAR,
    booking_id                                    VARCHAR,
    device_type                                   VARCHAR,
    feedback                                      VARCHAR,
    id                                            VARCHAR,
    ip                                            VARCHAR,
    language                                      VARCHAR,
    locked                                        BOOLEAN,
    nps_channel_id                                INT,
    nps_schedule_history_id                       INT,
    nps_submission_id                             INT,
    nps_trigger_contact_id                        INT,
    nps_trigger_id                                VARCHAR,
    nps_trigger_recent_id                         INT,
    os                                            VARCHAR,
    overall_nps_score                             INT,
    respondent_type                               VARCHAR,
    response_created_at                           TIMESTAMP,
    response_deleted_at                           TIMESTAMP,
    response_email_sent                           BOOLEAN,
    response_updated_at                           TIMESTAMP,
    score                                         INT,
    sentiment                                     VARCHAR,
    state                                         VARCHAR,
    state_token                                   VARCHAR,
    survey_id                                     INT,
    tags                                          ARRAY,
    time_taken                                    NUMBER(13, 2),
    time_zone                                     VARCHAR,

    -- original columns
    record                                        VARIANT,

    -- validation columns
    failed_some_validation                        INT,
    fails_validation__id__expected_nonnull        INT,
    fails_validation__survey_id__expected_nonnull INT
);

INSERT INTO hygiene_vault_dev_robin.survey_sparrow.nps_responses
SELECT n.schedule_tstamp,
       n.run_tstamp,
       n.operation_id,
       n.created_at,
       n.updated_at,
       n.row_dataset_name,
       n.row_dataset_source,
       n.row_loaded_at,
       n.row_schedule_tstamp,
       n.row_run_tstamp,
       n.row_filename,
       n.row_file_row_number,
       n.row_extract_metadata,
       n.account_id,
       n.answers,
       n.browser,
       n.browser_language,
       n.contact_id,
       n.contact_name,
       n.contact_properties,
       n.custom_properties,
       n.record['customProperties'][1]['value_string']::VARCHAR                                                                                     AS custom_properties_second_value,
       IFF(custom_properties_second_value LIKE '%-%', 'TB-' || SPLIT_PART(custom_properties_second_value, '-', -1), custom_properties_second_value) AS booking_id,
       n.device_type,
       n.feedback,
       n.id,
       n.ip,
       n.language,
       n.locked,
       n.nps_channel_id,
       n.nps_schedule_history_id,
       n.nps_submission_id,
       n.nps_trigger_contact_id,
       n.nps_trigger_id,
       n.nps_trigger_recent_id,
       n.os,
       n.overall_nps_score,
       n.respondent_type,
       n.response_created_at,
       n.response_deleted_at,
       n.response_email_sent,
       n.response_updated_at,
       n.score,
       n.sentiment,
       n.state,
       n.state_token,
       n.survey_id,
       n.tags,
       n.time_taken,
       n.time_zone,
       n.record,
       n.failed_some_validation,
       n.fails_validation__id__expected_nonnull,
       n.fails_validation__survey_id__expected_nonnull
FROM hygiene_vault_dev_robin.survey_sparrow.nps_responses_20211104 n;

------------------------------------------------------------------------------------------------------------------------
-- update latest vault
-- CREATE OR REPLACE TRANSIENT TABLE  latest_vault_dev_robin.survey_sparrow.nps_responses CLONE  latest_vault.survey_sparrow.nps_responses;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.survey_sparrow.nps_responses_20211104 CLONE latest_vault_dev_robin.survey_sparrow.nps_responses;

CREATE OR REPLACE TABLE latest_vault_dev_robin.survey_sparrow.nps_responses
(
    -- (lineage) metadata for the current job
    schedule_tstamp                TIMESTAMP NOT NULL,
    run_tstamp                     TIMESTAMP NOT NULL,
    operation_id                   VARCHAR   NOT NULL,
    created_at                     TIMESTAMP NOT NULL,
    updated_at                     TIMESTAMP NOT NULL,

    -- (lineage) original metadata of row itself
    row_dataset_name               VARCHAR   NOT NULL,
    row_dataset_source             VARCHAR   NOT NULL,
    row_loaded_at                  TIMESTAMP NOT NULL,
    row_schedule_tstamp            TIMESTAMP NOT NULL,
    row_run_tstamp                 TIMESTAMP NOT NULL,
    row_filename                   VARCHAR   NOT NULL,
    row_file_row_number            INT       NOT NULL,
    row_extract_metadata           VARIANT,

    -- transformed columns
    account_id                     INT,
    answers                        ARRAY,
    browser                        VARCHAR,
    browser_language               VARCHAR,
    contact_id                     INT,
    contact_name                   VARCHAR,
    contact_properties             ARRAY,
    custom_properties              VARIANT,
    custom_properties_second_value VARCHAR,
    booking_id                     VARCHAR,
    device_type                    VARCHAR,
    feedback                       VARCHAR,
    id                             VARCHAR,
    ip                             VARCHAR,
    language                       VARCHAR,
    locked                         BOOLEAN,
    nps_channel_id                 INT,
    nps_schedule_history_id        INT,
    nps_submission_id              INT,
    nps_trigger_contact_id         INT,
    nps_trigger_id                 VARCHAR,
    nps_trigger_recent_id          INT,
    os                             VARCHAR,
    overall_nps_score              INT,
    respondent_type                VARCHAR,
    response_created_at            TIMESTAMP,
    response_deleted_at            TIMESTAMP,
    response_email_sent            BOOLEAN,
    response_updated_at            TIMESTAMP,
    score                          INT,
    sentiment                      VARCHAR,
    state                          VARCHAR,
    state_token                    VARCHAR,
    survey_id                      INT,
    tags                           ARRAY,
    time_taken                     NUMBER(13, 2),
    time_zone                      VARCHAR,

    -- original columns
    record                         VARIANT,
    CONSTRAINT pk_1 PRIMARY KEY (id, survey_id)
);

INSERT INTO latest_vault_dev_robin.survey_sparrow.nps_responses
SELECT n.schedule_tstamp,
       n.run_tstamp,
       n.operation_id,
       n.created_at,
       n.updated_at,
       n.row_dataset_name,
       n.row_dataset_source,
       n.row_loaded_at,
       n.row_schedule_tstamp,
       n.row_run_tstamp,
       n.row_filename,
       n.row_file_row_number,
       n.row_extract_metadata,
       n.account_id,
       n.answers,
       n.browser,
       n.browser_language,
       n.contact_id,
       n.contact_name,
       n.contact_properties,
       n.custom_properties,
       n.record['customProperties'][1]['value_string']::VARCHAR                                                                                     AS custom_properties_second_value,
       IFF(custom_properties_second_value LIKE '%-%', 'TB-' || SPLIT_PART(custom_properties_second_value, '-', -1), custom_properties_second_value) AS booking_id,
       n.device_type,
       n.feedback,
       n.id,
       n.ip,
       n.language,
       n.locked,
       n.nps_channel_id,
       n.nps_schedule_history_id,
       n.nps_submission_id,
       n.nps_trigger_contact_id,
       n.nps_trigger_id,
       n.nps_trigger_recent_id,
       n.os,
       n.overall_nps_score,
       n.respondent_type,
       n.response_created_at,
       n.response_deleted_at,
       n.response_email_sent,
       n.response_updated_at,
       n.score,
       n.sentiment,
       n.state,
       n.state_token,
       n.survey_id,
       n.tags,
       n.time_taken,
       n.time_zone,
       n.record
FROM latest_vault_dev_robin.survey_sparrow.nps_responses_20211104 n;



SELECT *
FROM latest_vault_dev_robin.survey_sparrow.nps_responses nr;
SELECT *
FROM latest_vault.survey_sparrow.nps_responses nr;
SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp.sfmc.net_promoter_score nps;

SELECT DISTINCT booking_id
FROM latest_vault_dev_robin.survey_sparrow.nps_responses
MINUS
SELECT DISTINCT booking_id
FROM hygiene_snapshot_vault_mvp.sfmc.net_promoter_score nps;

------------------------------------------------------------------------------------------------------------------------

SELECT column1 AS transaction_id,
       transaction_id REGEXP '.*-[A-Z][A-Z][A-Z][A-Z]?-.*'
FROM
VALUES ('A4417-SED-21880563'),
       ('A3301-2354-261973'),
       ('99873-851623-49383018'),
       ('A6121-SEUK-21878892');


CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.sfmc.net_promoter_score CLONE raw_vault_mvp.sfmc.net_promoter_score;
self_describing_task --include 'staging/hygiene/sfmc/net_promoter_score.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfmc/net_promoter_score.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'


CASE
    WHEN LEFT({c}, 1) = 'A' THEN 'A' || SPLIT_PART({c}::VARCHAR, '-', -1)
    WHEN {c} REGEXP '.*-[A-Z][A-Z][a-Z]-.*' THEN 'TB-' || SPLIT_PART({c}::VARCHAR, '-', -1)
    ELSE SPLIT_PART({c}::VARCHAR, '-', -1)
END;

------------------------------------------------------------------------------------------------------------------------
--post deployment update sfmc data
-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.sfmc.net_promoter_score CLONE hygiene_vault_mvp.sfmc.net_promoter_score;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.sfmc.net_promoter_score_20211104 CLONE hygiene_vault_mvp_dev_robin.sfmc.net_promoter_score;

UPDATE hygiene_vault_mvp_dev_robin.sfmc.net_promoter_score target
SET target.booking_id = CASE
                            WHEN target.transaction_id REGEXP '.*-[A-Z][A-Z][A-Z][A-Z]?-.*' THEN 'TB-' || SPLIT_PART(target.transaction_id::VARCHAR, '-', -1)
                            WHEN LEFT(target.transaction_id, 1) = 'A' THEN 'A' || SPLIT_PART(target.transaction_id::VARCHAR, '-', -1)
                            ELSE SPLIT_PART(transaction_id::VARCHAR, '-', -1) END
-- check update script worked
SELECT *
FROM hygiene_vault_mvp_dev_robin.sfmc.net_promoter_score
WHERE transaction_id REGEXP '.*-[A-Z][A-Z][A-Z][A-Z]?-.*';

-- CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.net_promoter_score CLONE hygiene_snapshot_vault_mvp.sfmc.net_promoter_score;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.net_promoter_score_20211104 CLONE hygiene_snapshot_vault_mvp_dev_robin.sfmc.net_promoter_score;


UPDATE hygiene_snapshot_vault_mvp_dev_robin.sfmc.net_promoter_score target
SET target.booking_id = CASE
                            WHEN target.transaction_id REGEXP '.*-[A-Z][A-Z][A-Z][A-Z]?-.*' THEN 'TB-' || SPLIT_PART(target.transaction_id::VARCHAR, '-', -1)
                            WHEN LEFT(target.transaction_id, 1) = 'A' THEN 'A' || SPLIT_PART(target.transaction_id::VARCHAR, '-', -1)
                            ELSE SPLIT_PART(transaction_id::VARCHAR, '-', -1) END

-- check update script worked
SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.net_promoter_score
WHERE transaction_id REGEXP '.*-[A-Z][A-Z][A-Z][A-Z]?-.*';

------------------------------------------------------------------------------------------------------------------------
self_describing_task --include 'dv/dwh/reviews/user_booking_review.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_booking_review
    QUALIFY COUNT(*) OVER (PARTITION BY booking_id) > 1;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_booking_review
WHERE user_booking_review.booking_id = 'TB-21908651';

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.reviews_npsscore CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.reviews_npsscore;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_booking_review
    QUALIFY COUNT(*) OVER (PARTITION BY user_booking_review.booking_id) > 1;