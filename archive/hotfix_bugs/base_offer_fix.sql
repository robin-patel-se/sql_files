CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.base_offer clone raw_vault_mvp.cms_mysql.base_offer;

self_describing_task --include 'dv/dwh/transactional/se_offer'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/dv/dwh/transactional/se_offer.py

DROP TABLE data_vault_mvp_dev_robin.dwh.se_offer;