USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
CLONE hygiene_vault_mvp.snowplow.event_stream;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.external_booking
CLONE latest_vault.cms_mysql.external_booking;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations
CLONE data_vault_mvp.single_customer_view_stg.module_identity_associations;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.02_identity_stitching.01_module_identity_associations.py' \
    --method 'run' \
    --start '2025-01-13 00:00:00' \
    --end '2025-01-13 00:00:00'



