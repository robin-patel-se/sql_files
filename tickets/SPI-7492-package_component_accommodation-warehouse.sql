USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.travelbird__configs_data
CLONE data_vault_mvp.dwh.travelbird__configs_data;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/sale/packages/package_component_accommodation.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'