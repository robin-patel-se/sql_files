USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_email_clicks
CLONE data_vault_mvp.dwh.user_email_clicks;


------------------------------------------------------------------------------------------------------------------------

-- testing downstream

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_email_clicks
-- CLONE data_vault_mvp.dwh.user_email_clicks;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_flattened_email_send_log
CLONE data_vault_mvp.dwh.iterable_flattened_email_send_log;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.email_sale_insertion_metrics
CLONE data_vault_mvp.dwh.email_sale_insertion_metrics;


data_vault_mvp_dev_robin.dwh.email_sale_insertion_metrics;


USE ROLE pipelinerunner;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage', 'data_vault_mvp.dwh.user_email_clicks');

SELECT * FROM scratch.robinpatel.table_usage;

USE ROLE personal_role__robinpatel;


USE ROLE pipelinerunner;
USE WAREHOUSE pipe_xlarge;
CALL data_vault_mvp.dwh.table_reference_in_view('scratch.robinpatel.table_reference_in_view', 'data_vault_mvp.dwh.user_email_clicks', 'collab, data_vault_mvp, se, dbt');
SELECT * FROM scratch.robinpatel.table_reference_in_view;

USE ROLE personal_role__robinpatel;

------------------------------------------------------------------------------------------------------------------------

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/email_sale_insertion/email_sale_insertion_metrics.py'  --method 'run' --start '2025-07-06 00:00:00' --end '2025-07-06 00:00:00'

SELECT COUNT(*) FROM data_vault_mvp_dev_robin.dwh.email_sale_insertion_metrics WHERE send_date >= current_date - 10;
SELECT COUNT(*) FROM data_vault_mvp.dwh.email_sale_insertion_metrics WHERE send_date >= current_date - 10;

2,716,646,719

USE WAREHOUSE pipe_xlarge;