USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale
AS SELECT * FROM data_vault_mvp.dwh.dim_sale;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.bi;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.dim_sale_territory
CLONE data_vault_mvp.bi.dim_sale_territory;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS SELECT * FROM data_vault_mvp.dwh.fact_booking;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.generic_targets
AS SELECT * FROM data_vault_mvp.dwh.generic_targets;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.global_sale_attributes
CLONE data_vault_mvp.dwh.global_sale_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active_snapshot
CLONE data_vault_mvp.dwh.sale_active_snapshot;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS SELECT * FROM data_vault_mvp.dwh.se_calendar;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_company_attributes
CLONE data_vault_mvp.dwh.se_company_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
CLONE data_vault_mvp.dwh.se_sale;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.deal_count
CLONE data_vault_mvp.bi.deal_count;

/*
self_describing_task \
    --include 'biapp.task_catalogue.dv.bi.tableau.deal_count_model.deal_count.py' \
    --method 'run' \
    --start '2025-01-30 00:00:00' \
    --end '2025-01-30 00:00:00'

*/

