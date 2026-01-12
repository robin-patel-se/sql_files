CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale AS
SELECT *
FROM data_vault_mvp.dwh.dim_sale;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.fact_sale_metrics CLONE data_vault_mvp.bi.fact_sale_metrics;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.global_sale_attributes CLONE data_vault_mvp.dwh.global_sale_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active CLONE data_vault_mvp.dwh.sale_active;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_territory;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_translation CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_translation;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_company_attributes CLONE data_vault_mvp.dwh.se_company_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags CLONE data_vault_mvp.dwh.se_sale_tags;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer CLONE data_vault_mvp.dwh.tb_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.territory;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.dim_sale CLONE data_vault_mvp.bi.dim_sale;

SELECT *
FROM data_vault_mvp.bi.dim_sale ds
WHERE tech_platform = 'TRAVELBIRD';

self_describing_task --include 'dv/bi/tableau/deal_model/dim_sale.py'  --method 'run' --start '2022-03-27 00:00:00' --end '2022-03-27 00:00:00'

SELECT current_contractor_name,
       COUNT(*)
FROM data_vault_mvp_dev_robin.bi.dim_sale ds
WHERE tech_platform = 'TRAVELBIRD'
GROUP BY 1;

SELECT * FROM data_vault_mvp.dwh.se_sale ss WHERE ss.se_sale_id= 'A46790';


SELECT * FROM collab.finance_netsuite.