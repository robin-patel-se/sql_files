CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_emails CLONE se.data.user_emails;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_emails_20201208 CLONE se.data.user_emails;
-- ALTER TABLE data_vault_mvp_dev_robin.dwh.user_emails RENAME COLUMN user_id TO shiro_user_id; --cannot rename due to clustering

CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.dwh.user_emails
(

    -- (lineage) metadata for the current job
    schedule_tstamp TIMESTAMP,
    run_tstamp      TIMESTAMP,
    operation_id    VARCHAR,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,

    shiro_user_id   INT,
    date            DATE,
    unique_sends    NUMBER,
    sends           NUMBER,
    unique_opens    NUMBER,
    opens           NUMBER,
    unique_clicks   NUMBER,
    clicks          NUMBER
)
    CLUSTER BY (date, shiro_user_id);

USE WAREHOUSE pipe_xlarge;
INSERT INTO data_vault_mvp_dev_robin.dwh.user_emails
SELECT ue.schedule_tstamp,
       ue.run_tstamp,
       ue.operation_id,
       ue.created_at,
       ue.updated_at,
       ue.user_id,
       ue.date,
       ue.unique_sends,
       ue.sends,
       ue.unique_opens,
       ue.opens,
       ue.unique_clicks,
       ue.clicks
FROM se.data.user_emails ue;

DROP TABLE se.data.user_emails;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends clone hygiene_snapshot_vault_mvp.sfmc.events_sends;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred clone hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_clicks clone hygiene_snapshot_vault_mvp.sfmc.events_clicks;

self_describing_task --include 'dv/dwh/email/sfmc_user_emails.py'  --method 'run' --start '2020-12-08 00:00:00' --end '2020-12-08 00:00:00'

airflow backfill --start_date '2020-12-08 04:00:00' --end_date '2020-12-08 04:00:00' --task_regex '.*' dwh__cms_mari_link__daily_at_04h00

self_describing_task --include 'se/data/dwh/user_emails.py'  --method 'run' --start '2020-12-08 00:00:00' --end '2020-12-08 00:00:00'

SELECT MIN(date) FROM data_vault_mvp.dwh.user_activity ua
