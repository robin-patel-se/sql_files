SELECT *
FROM data_vault_mvp.dwh.se_sale;


SELECT *
FROM hygiene_vault_mvp.cms_mysql.base_sale;

SELECT min(last_updated)
FROM raw_vault_mvp.cms_mysql.base_sale bs;

SELECT *
FROM hygiene_vault_mvp.cms_mysql.sale s
         self_describing_task --include 'staging/hygiene/snowplow/events'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
         airflow backfill --start_date '2017-10-19 00:30:00' --end_date '2017-10-19 00:30:00' --task_regex '.*' incoming__cms_mysql__base_sale__daily_at_00h30
DROP TABLE raw_vault_mvp_dev_robin.cms_mysql.base_sale;

--run on development
SELECT min(last_updated)
FROM raw_vault_mvp.cms_mysql.base_sale bs;
--2017-10-19 12:27:16.000000000

--run on production
ALTER TABLE raw_vault_mvp_dev_robin.cms_mysql.base_sale
    ADD COLUMN supplier_id INT;

ALTER TABLE raw_vault_mvp_dev_robin.cms_mysql.base_sale
    ADD COLUMN offer_order VARCHAR;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.base_sale_bkup CLONE raw_vault_mvp_dev_robin.cms_mysql.base_sale;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.base_sale CLONE raw_vault_mvp.cms_mysql.base_sale;

SELECT *
FROM raw_vault_mvp_dev_robin.cms_mysql.base_sale bs
WHERE bs.supplier_id IS NULL;

UPDATE raw_vault_mvp_dev_robin.cms_mysql.base_sale target
SET target.supplier_id = batch.supplier_id,
    target.offer_order = batch.offer_order
FROM collab.muse_data_modelling.base_sale_supplier_id batch
WHERE target.id = batch.id
  AND target.last_updated = batch.last_updated;

SELECT *
FROM raw_vault_mvp_dev_robin.cms_mysql.base_sale
WHERE base_sale.supplier_id IS NOT NULL;

SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.cms_mysql.base_sale; --2020-02-26 14:13:15.993836000

self_describing_task --include 'staging/hygiene/cms_mysql/base_sale'  --method 'run' --start '2020-02-26 00:00:00' --end '2020-02-26 00:00:00'

ALTER TABLE hygiene_vault_mvp_dev_robin.cms_mysql.base_sale
    ADD COLUMN supplier_id INT;
ALTER TABLE hygiene_vault_mvp_dev_robin.cms_mysql.base_sale
    ADD COLUMN offer_order VARCHAR;


SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mysql.base_sale;
ALTER TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale
    ADD COLUMN supplier_id INT;

self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/base_sale'  --method 'run' --start '2020-02-26 00:00:00' --end '2020-02-26 00:00:00'
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale;

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale;

self_describing_task --include 'dv/dwh/transactional/se_sale'  --method 'run' --start '2020-02-26 00:00:00' --end '2020-02-26 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale
WHERE se_sale.data_model = 'New Data Model'
  AND se_sale.supplier_id IS NOT NULL;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale bs;
SELECT *
FROM raw_vault_mvp_dev_robin.cms_mysql.base_sale bs;


CREATE OR REPLACE TABLE collab.muse_data_modelling.base_sale_supplier_id
(
    id           INT,
    supplier_id  INT,
    offer_order  VARCHAR,
    last_updated TIMESTAMP
);

USE SCHEMA collab.muse_data_modelling;

PUT file:///Users/robin/sqls/base_sale_supplier_id/base_sale.csv @%base_sale_supplier_id;

COPY INTO collab.muse_data_modelling.base_sale_supplier_id
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
        );

SELECT *
FROM collab.muse_data_modelling.base_sale_supplier_id
WHERE base_sale_supplier_id.supplier_id IS NOT NULL;

UPDATE raw_vault_mvp_dev_robin.cms_mysql.base_sale bs
SET bs.supplier_id = bss.supplier_id,
    bs.offer_order = bss.offer_order
FROM collab.muse_data_modelling.base_sale_supplier_id bss
WHERE bs.id = bss.id;

UPDATE hygiene_vault_mvp_dev_robin.cms_mysql.base_sale bs
SET bs.supplier_id = bss.supplier_id,
    bs.offer_order = bss.offer_order
FROM collab.muse_data_modelling.base_sale_supplier_id bss
WHERE bs.id = bss.id;

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_sale;

ALTER TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale
    ADD COLUMN supplier_id INT;

ALTER TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale
    ADD COLUMN offer_order VARCHAR;

UPDATE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale bs
SET bs.supplier_id = bss.supplier_id,
    bs.offer_order = bss.offer_order
FROM collab.muse_data_modelling.base_sale_supplier_id bss
WHERE bs.id = bss.id;
--   AND bs.last_updated = bss.last_updated;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale bs where bs.offer_order IS NOT NULL;

SELECT * FROM data_vault_mvp.cms_mysql_snapshots.supplier_snapshot ss;


SELECT * FROM data_vault_mvp_dev_robin.dwh.se_sale ss WHERE ss.data_model = 'Old Data Model' AND ss.supplier_name IS NOT NULL;

SELECT * FROM se.data.se_sale_attributes ssa;

airflow backfill --start_date '2020-06-22 00:00:00' --end_date '2020-06-30 00:00:00' --task_regex '.*' -m customer_model_full_uk_de__every7days