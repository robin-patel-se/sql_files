CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities CLONE data_vault_mvp.dwh.user_recent_activities;

self_describing_task --include 'dv/dwh/iterable/user_profile_activity.py'  --method 'run' --start '2021-10-19 00:00:00' --end '2021-10-19 00:00:00'

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity;

select count(*) FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa;