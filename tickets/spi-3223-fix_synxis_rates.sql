CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.cms_allocation_link CLONE data_vault_mvp.dwh.cms_allocation_link;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_offer CLONE data_vault_mvp.dwh.se_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel_sale_offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel_sale_offer;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.ari.hotel_inventory_synxis CLONE latest_vault.ari.hotel_inventory_synxis;
CREATE SCHEMA latest_vault_dev_robin.ari;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.ari.hotel_rates_synxis CLONE latest_vault.ari.hotel_rates_synxis;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.ari.hotel_inventory_siteminder CLONE latest_vault.ari.hotel_inventory_siteminder;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.rates CLONE data_vault_mvp.fx.rates;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/allocation_and_rates/synxis/synxis_room_rates.py'  --method 'run' --start '2022-11-14 00:00:00' --end '2022-11-14 00:00:00'


self_describing_task --include 'biapp/task_catalogue/dv/dwh/allocation_and_rates/synxis/synxis_room_type_rooms_and_rates.py'  --method 'run' --start '2022-11-14 00:00:00' --end '2022-11-14 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.ari.hotel_rates_siteminder CLONE latest_vault.ari.hotel_rates_siteminder;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/allocation_and_rates/siteminder/siteminder_room_rates.py'  --method 'run' --start '2022-11-14 00:00:00' --end '2022-11-14 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/allocation_and_rates/siteminder/siteminder_room_type_rooms_and_rates.py'  --method 'run' --start '2022-11-14 00:00:00' --end '2022-11-14 00:00:00'

