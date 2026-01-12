CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_sale;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_flight_config;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.opportunity CLONE hygiene_snapshot_vault_mvp.sfsc.opportunity;


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.in_house_package_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.in_house_package_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.location_info_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.location_info_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.city_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.city_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_sale_translation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_sale_translation_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.territory_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_translation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_translation_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.offer_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.offer_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.allocation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.allocation_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_territory_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_affiliate_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_affiliate_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_sale_affiliate_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_sale_affiliate_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.web_redirect_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.web_redirect_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.company_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_company_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.ihp_sale_company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.ihp_sale_company_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.web_redirect_company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.web_redirect_company_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.supplier_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.supplier_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.contractor_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.contractor_snapshot;

self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/dv/dwh/transactional/se_sale.py


SELECT * FROM data_vault_mvp_dev_robin.dwh.se_sale ss WHERE ss.sale_active AND ss.product_configuration != 'Catalogue'

self_describing_task --include 'dv/dwh/transactional/tb_offer.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

self_describing_task --include 'se/data/dim_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/se_sale_attributes.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/tb_offer.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/se/data/dim_sale.py

SELECT * FROM se_dev_robin.data.dim_sale ds WHERE ds.tech_platform = 'TRAVELBIRD' AND ds.sale_active