USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
CLONE data_vault_mvp.dwh.user_attributes;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS SELECT * FROM data_vault_mvp.dwh.fact_booking;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale
AS SELECT * FROM data_vault_mvp.dwh.dim_sale;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.event_grain
CLONE data_vault_mvp.bi.event_grain;

self_describing_task \
    --include 'biapp.task_catalogue.dv.bi.tableau.demand_model.event_grain.py' \
    --method 'run' \
    --start '2024-10-18 00:00:00' \
    --end '2024-10-18 00:00:00'


------------------------------------------------------------------------------------------------------------------------

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
CLONE data_vault_mvp.dwh.user_attributes;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS SELECT * FROM data_vault_mvp.dwh.fact_booking;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale
AS SELECT * FROM data_vault_mvp.dwh.dim_sale;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_grain
CLONE data_vault_mvp.bi.session_grain;

self_describing_task \
    --include 'biapp.task_catalogue.dv.bi.tableau.demand_model.session_grain.py' \
    --method 'run' \
    --start '2024-10-18 00:00:00' \
    --end '2024-10-18 00:00:00'


------------------------------------------------------------------------------------------------------------------------

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_activity
CLONE data_vault_mvp.dwh.user_activity;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
CLONE data_vault_mvp.dwh.user_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_emails
CLONE data_vault_mvp.dwh.user_emails;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.weekly_active_users
CLONE data_vault_mvp.bi.weekly_active_users;

self_describing_task \
    --include 'biapp.task_catalogue.dv.bi.tableau.demand_model.weekly_active_users.py' \
    --method 'run' \
    --start '2024-10-18 00:00:00' \
    --end '2024-10-18 00:00:00'