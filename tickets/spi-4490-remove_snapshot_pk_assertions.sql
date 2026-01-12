CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer CLONE data_vault_mvp.dwh.tb_offer;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.posu_categorisation CLONE latest_vault.fpa_gsheets.posu_categorisation;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/tb_offer_snapshot.py'  --method 'run' --start '2023-11-23 00:00:00' --end '2023-11-23 00:00:00'

dwh__transactional__sale_snapshot__daily_at_03h00

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/tb_booking_snapshot.py'  --method 'run' --start '2023-11-23 00:00:00' --end '2023-11-23 00:00:00'

dwh__transactional__tb_booking_snapshot__daily_at_03h00

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_voucher CLONE data_vault_mvp.dwh.se_voucher;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/se_voucher_snapshot.py'  --method 'run' --start '2023-11-23 00:00:00' --end '2023-11-23 00:00:00'

dwh__transactional__se_voucher_snapshot__every7days

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags CLONE data_vault_mvp.dwh.se_sale_tags;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/se_sale_tags_snapshot.py'  --method 'run' --start '2023-11-23 00:00:00' --end '2023-11-23 00:00:00'

dwh__transactional__se_sale_tags_snapshot__daily_at_03h00

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.posu_categorisation CLONE latest_vault.fpa_gsheets.posu_categorisation;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/se_sale_snapshot.py'  --method 'run' --start '2023-11-23 00:00:00' --end '2023-11-23 00:00:00'

dwh__transactional__sale_snapshot__daily_at_03h00

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit CLONE data_vault_mvp.dwh.se_credit;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/se_credit_snapshot.py'  --method 'run' --start '2023-11-23 00:00:00' --end '2023-11-23 00:00:00'

dwh__transactional__se_credit_snapshot__every7days

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/se_booking_snapshot.py'  --method 'run' --start '2023-11-23 00:00:00' --end '2023-11-23 00:00:00'

dwh__transactional__se_booking_snapshot__daily_at_03h00

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.global_sale_attributes CLONE data_vault_mvp.dwh.global_sale_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_territory_active_snapshot CLONE data_vault_mvp.dwh.sale_territory_active_snapshot;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/global_sale_active_snapshot.py'  --method 'run' --start '2023-11-23 00:00:00' --end '2023-11-23 00:00:00'

dwh__global_sale_active_snapshot__daily_at_03h00
