CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

self_describing_task --include 'dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py'  --method 'run' --start '2022-03-15 00:00:00' --end '2022-03-15 00:00:00'



USE WAREHOUSE pipe_xlarge;
DELETE
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution AS target USING (
    SELECT mta.touch_id,
           mta.updated_at
    FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
        QUALIFY ROW_NUMBER() OVER (PARTITION BY mta.touch_id, mta.attribution_model ORDER BY mta.updated_at DESC) != 1
) AS batch
WHERE target.touch_id = batch.touch_id
  AND target.updated_at = batch.updated_at
;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
QUALIFY COUNT(*) OVER (PARTITION BY touch_id, attribution_model) > 1;
--confirm is 0


--run attribution for today

USE WAREHOUSE pipe_xlarge;
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution
QUALIFY COUNT(*) OVER (PARTITION BY touch_id, attribution_model) > 1;
-- confirm is still 0



------------------------------------------------------------------------------------------------------------------------
-- add warehouse to assertions
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_calendar CLONE data_vault_mvp.dwh.se_calendar;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_activity CLONE data_vault_mvp.dwh.user_activity;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_emails CLONE data_vault_mvp.dwh.user_emails;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_segmentation CLONE data_vault_mvp.dwh.user_segmentation;

self_describing_task --include 'dv/bi/email/daily_customer_email_activity.py'  --method 'run' --start '2022-03-15 00:00:00' --end '2022-03-15 00:00:00'