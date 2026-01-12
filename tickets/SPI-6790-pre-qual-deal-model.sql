USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.bi
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.fact_sale_metrics
	CLONE data_vault_mvp.bi.fact_sale_metrics
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.global_sale_attributes
	CLONE data_vault_mvp.dwh.global_sale_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active_snapshot
	CLONE data_vault_mvp.dwh.sale_active_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.reactivated_sale_active
	CLONE data_vault_mvp.dwh.reactivated_sale_active
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_territory
	CLONE latest_vault.cms_mysql.sale_territory
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_translation
	CLONE latest_vault.cms_mysql.sale_translation
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_company_attributes
	CLONE data_vault_mvp.dwh.se_company_attributes
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
-- CLONE data_vault_mvp.dwh.se_sale;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags
	CLONE data_vault_mvp.dwh.se_sale_tags
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer
	CLONE data_vault_mvp.dwh.tb_offer
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory
	CLONE latest_vault.cms_mysql.territory
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.dim_sale_territory
	CLONE data_vault_mvp.bi.dim_sale_territory
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.tableau.deal_model.dim_sale_territory.py' \
    --method 'run' \
    --start '2025-09-25 00:00:00' \
    --end '2025-09-25 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.bi.dim_sale_territory
WHERE dim_sale_territory.pre_qualification_uk IS NOT NULL

-- master version
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.dim_sale_territory_master CLONE data_vault_mvp_dev_robin.bi.dim_sale_territory;

WITH
	prod AS (
		SELECT
			se_sale_id,
			posa_territory,
			HASH(* exclude(sale_name, schedule_tstamp, run_tstamp, created_at, updated_at, operation_id)) AS prod_hash
		FROM data_vault_mvp_dev_robin.bi.dim_sale_territory_master
	),
	dev AS (
		SELECT
			se_sale_id,
			posa_territory,
			HASH(* exclude(sale_name, schedule_tstamp, run_tstamp, created_at, updated_at, operation_id, pre_qualification_dach,
						   pre_qualification_row, pre_qualification_uk)) AS dev_hash
		FROM data_vault_mvp_dev_robin.bi.dim_sale_territory
	)
SELECT
	COALESCE(prod.se_sale_id, dev.se_sale_id)         AS se_sale_id,
	COALESCE(prod.posa_territory, dev.posa_territory) AS se_sale_id,
	prod_hash,
	dev_hash,
FROM prod
FULL OUTER JOIN dev
	ON prod.se_sale_id = dev.se_sale_id
	AND prod.posa_territory = dev.posa_territory
WHERE prod_hash != dev_hash
;

-- prod
SELECT * EXCLUDE (schedule_tstamp, run_tstamp, created_at, updated_at, operation_id)
FROM data_vault_mvp.bi.dim_sale_territory
WHERE se_sale_id = 'TVL3169'
  AND posa_territory = 'PL'
;

--dev
SELECT *
	EXCLUDE (schedule_tstamp, run_tstamp, created_at, updated_at, operation_id, pre_qualification_dach,pre_qualification_row, pre_qualification_uk)
FROM data_vault_mvp_dev_robin.bi.dim_sale_territory
WHERE se_sale_id = 'TVL3169'
  AND posa_territory = 'PL'
;

