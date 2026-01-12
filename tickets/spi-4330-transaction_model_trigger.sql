self_describing_task --include 'biapp/task_catalogue/staging/triggers/tableau/transaction_model.py'  --method 'run' --start '2023-10-23 00:00:00' --end '2023-10-23 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_snapshot CLONE data_vault_mvp.dwh.se_sale_snapshot;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/channel_manager_changelog.py'  --method 'run' --start '2023-10-23 00:00:00' --end '2023-10-23 00:00:00'