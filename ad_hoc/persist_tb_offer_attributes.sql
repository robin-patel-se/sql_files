CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer;

DROP TABLE data_vault_mvp_dev_robin.dwh.tb_offer;

self_describing_task --include 'dv/dwh/transactional/tb_offer'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_offer;

self_describing_task --include 'se/data/dim_sale'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM se_dev_robin.data.dim_sale ds
WHERE ds.tech_platform = 'TRAVELBIRD';

SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_offer t;

SELECT MIN(updated_at) FROM hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer oo --2020-02-28 09:59:56.568000000

self_describing_task --include 'dv/dwh/transactional/tb_offer'  --method 'run' --start '2020-02-27 00:00:00' --end '2020-02-27 00:00:00'

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_offer_bkup clone data_vault_mvp.dwh.tb_offer;

SELECT * FROM se.
