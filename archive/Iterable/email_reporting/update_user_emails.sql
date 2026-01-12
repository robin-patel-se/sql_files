CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_emails CLONE data_vault_mvp.dwh.user_emails;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.email_click_event AS
SELECT *
FROM data_vault_mvp.dwh.email_click_event;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.email_open_event AS
SELECT *
FROM data_vault_mvp.dwh.email_open_event;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.email_send_event AS
SELECT *
FROM data_vault_mvp.dwh.email_send_event;

self_describing_task --include 'dv/dwh/email/user_emails.py'  --method 'run' --start '2021-12-02 00:00:00' --end '2021-12-02 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_emails
WHERE date = CURRENT_DATE - 2;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.email_list_test AS
SELECT cjl.email_id,
       cjl.send_id,
       cjl.campaign_id,
       cjl.scheduled_date,
       cjl.scheduled_tstmap,
       cjl.email_name,
       cjl.mapped_crm_date,
       cjl.mapped_territory,
       cjl.mapped_objective,
       cjl.mapped_platform,
       cjl.mapped_campaign,
       cjl.sent_date,
       cjl.sent_tstamp,
       cjl.is_email_name_remapped,
       cjl.client_id,
       cjl.from_name,
       cjl.from_email,
       cjl.subject,
       cjl.triggered_send_external_key,
       cjl.send_definition_external_key,
       cjl.job_status,
       cjl.preview_url,
       cjl.is_multipart,
       cjl.additional,
       cjl.campaign_created_at,
       cjl.campaign_updated_at,
       cjl.ended_at,
       cjl.template_id,
       cjl.message_medium,
       cjl.created_by_user_id,
       cjl.updated_by_user_id,
       cjl.campaign_state,
       cjl.list_ids,
       cjl.suppression_list_ids,
       cjl.send_size,
       cjl.labels,
       cjl.type,
       cjl.crm_platform
FROM se.data.crm_jobs_list cjl;

SELECT GET_DDL('table', 'scratch.robinpatel.email_list_test');

CREATE OR REPLACE TRANSIENT TABLE email_list_test
(
    email_id                     VARCHAR,
    send_id                      NUMBER,
    campaign_id                  NUMBER,
    scheduled_date               DATE,
    scheduled_tstmap             TIMESTAMP_NTZ,
    email_name                   VARCHAR,
    mapped_crm_date              VARCHAR,
    mapped_territory             VARCHAR,
    mapped_objective             VARCHAR,
    mapped_platform              VARCHAR,
    mapped_campaign              VARCHAR,
    sent_date                    DATE,
    sent_tstamp                  TIMESTAMP_NTZ,
    is_email_name_remapped       BOOLEAN,
    client_id                    NUMBER,
    from_name                    VARCHAR,
    from_email                   VARCHAR,
    subject                      VARCHAR,
    triggered_send_external_key  VARCHAR,
    send_definition_external_key VARCHAR,
    job_status                   VARCHAR,
    preview_url                  VARCHAR,
    is_multipart                 VARCHAR,
    additional                   VARCHAR,
    campaign_created_at          TIMESTAMP_NTZ,
    campaign_updated_at          TIMESTAMP_NTZ,
    ended_at                     TIMESTAMP_NTZ,
    template_id                  NUMBER,
    message_medium               VARCHAR,
    created_by_user_id           VARCHAR,
    updated_by_user_id           VARCHAR,
    campaign_state               VARCHAR,
    list_ids                     ARRAY,
    suppression_list_ids         ARRAY,
    send_size                    NUMBER,
    labels                       ARRAY,
    type                         VARCHAR,
    crm_platform                 VARCHAR
);