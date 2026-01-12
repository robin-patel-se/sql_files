SELECT *
FROM data_vault_mvp.dwh.se_sale_tags sst
	use role personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
	CLONE data_vault_mvp.dwh.se_sale
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.tags
	CLONE latest_vault.cms_mysql.tags
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.tag_links
	CLONE latest_vault.cms_mysql.tag_links
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags
	CLONE data_vault_mvp.dwh.se_sale_tags
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.transactional.se_sale_tags.py' \
    --method 'run' \
    --start '2024-12-05 00:00:00' \
    --end '2024-12-05 00:00:00'



SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale_tags