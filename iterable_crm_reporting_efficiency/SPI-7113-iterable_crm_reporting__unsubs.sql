USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iterable;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_unsubscribe
CLONE latest_vault.iterable.email_unsubscribe;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__unsubs
CLONE data_vault_mvp.dwh.iterable_crm_reporting__unsubs;


self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__unsubs.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__unsubs