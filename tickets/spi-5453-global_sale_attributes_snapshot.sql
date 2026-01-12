self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/global_sale_attributes_snapshot.py'  --method 'run' --start '2024-07-18 00:00:00' --end '2024-07-18 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.global_sale_attributes CLONE data_vault_mvp.dwh.global_sale_attributes;

self_describing_task --include 'biapp/task_catalogue/se/data_pii/dwh/global_sale_attributes_snapshot.py'  --method 'run' --start '2024-07-18 00:00:00' --end '2024-07-18 00:00:00'
self_describing_task --include 'biapp/task_catalogue/se/data/dwh/global_sale_attributes_snapshot.py'  --method 'run' --start '2024-07-18 00:00:00' --end '2024-07-18 00:00:00'


SELECT * FROM se.data.global_sale_attributes_snapshot gsa;