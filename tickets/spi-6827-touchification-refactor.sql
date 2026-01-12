USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker
CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker
CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/03_touchification/03_touchification.py'  --method 'run' --start '2024-12-30 00:00:00' --end '2024-12-30 00:00:00'

