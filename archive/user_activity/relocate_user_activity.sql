CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes clone data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes


self_describing_task --include 'dv/dwh/user_attributes/user_activity.py'  --method 'run' --start '2020-12-08 00:00:00' --end '2020-12-08 00:00:00'

CREATE OR REPLACE TABLE data_vault_mvp.dwh.user_activity CLONE se.data.user_activity;

self_describing_task --include 'se/data/se_user_activity.py'  --method 'run' --start '2020-12-08 00:00:00' --end '2020-12-08 00:00:00'