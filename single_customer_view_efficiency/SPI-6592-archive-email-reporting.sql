module=/biapp/task_catalogue/dv/bi/tableau/deal_model/fact_sale_metrics.py make clones


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review
	CLONE data_vault_mvp.dwh.user_booking_review
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active_snapshot
	CLONE data_vault_mvp.dwh.sale_active_snapshot
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.bi
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.sale_date_spvs
	CLONE data_vault_mvp.bi.sale_date_spvs
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_territory
	CLONE latest_vault.cms_mysql.sale_territory
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory
	CLONE latest_vault.cms_mysql.territory
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.fpa_gsheets
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.constant_currency
	CLONE latest_vault.fpa_gsheets.constant_currency
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.promotion
	CLONE latest_vault.fpa_gsheets.promotion
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking
	CLONE data_vault_mvp.dwh.se_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.wrd_booking
	CLONE data_vault_mvp.dwh.wrd_booking
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags_snapshot
	CLONE data_vault_mvp.dwh.se_sale_tags_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking
	CLONE data_vault_mvp.dwh.tb_booking
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.fact_sale_metrics
	CLONE data_vault_mvp.bi.fact_sale_metrics
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.tableau.deal_model.fact_sale_metrics.py' \
    --method 'run' \
    --start '2025-07-07 00:00:00' \
    --end '2025-07-07 00:00:00';



------------------------------------------------------------------------------------------------------------------------



USE ROLE pipelinerunner
;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
									'se.data.email_reporting, data_vault_mvp.dwh.email_reporting')
;

SELECT *
FROM scratch.robinpatel.table_usage
;

USE ROLE personal_role__robinpatel
;


USE ROLE pipelinerunner
;

USE WAREHOUSE pipe_xlarge
;

CALL data_vault_mvp.dwh.table_reference_in_view('scratch.robinpatel.table_reference_in_view', 'se.data.email_reporting',
												'collab, data_vault_mvp, se, dbt')
;

SELECT *
FROM scratch.robinpatel.table_reference_in_view
;

USE ROLE personal_role__robinpatel
;


/*
(estimate) = 0.00002075
(estimate) = 0.00001186
(estimate) = 0.00001659
(estimate) = 0.00001761
(estimate) = 1.62593058
(estimate) = 0.29677478
(estimate) = 1.01268599
(estimate) = 0.00856173
(estimate) = 0.01271037
(estimate) = 0.02631392
(estimate) = 0.01326472
(estimate) = 0.20716518
(estimate) = 0.06532050
(estimate) = 0.00025850
(estimate) = 0.00005109
(estimate) = 0.00001416
(estimate) = 0.00060807
(estimate) = 0.04851610
(estimate) = 0.00041791
(estimate) = 0.00002260
(estimate) = 0.00001921
(estimate) = 0.00002159
(estimate) = 0.00002694
(estimate) = 0.00002211
(estimate) = 0.00002034
(estimate) = 0.00002134
(estimate) = 0.00002551

creds 3.31884005
$ 6.538114899
annual $ 2386.411938
*/

SELECT *
FROM data_vault_mvp_dev_robin.bi.fact_sale_metrics
;
-- dev
SELECT
	fact_sale_metrics.date,
	COUNT(DISTINCT fact_sale_metrics.se_sale_id)
-- 	COUNT(*)
FROM data_vault_mvp_dev_robin.bi.fact_sale_metrics
WHERE fact_sale_metrics.date >= '2024-01-01'
GROUP BY 1
;

-- prod
SELECT
	fact_sale_metrics.date,
	COUNT(DISTINCT fact_sale_metrics.se_sale_id)
-- 	COUNT(*)
FROM data_vault_mvp.bi.fact_sale_metrics
WHERE fact_sale_metrics.date >= '2024-01-01'
GROUP BY 1
;


-- dev
SELECT
	fact_sale_metrics.date,
	SUM(fact_sale_metrics.spvs)
-- 	COUNT(*)
FROM data_vault_mvp_dev_robin.bi.fact_sale_metrics
WHERE fact_sale_metrics.date >= '2024-01-01'
GROUP BY 1
;

-- prod
SELECT
	fact_sale_metrics.date,
	SUM(fact_sale_metrics.spvs)
FROM data_vault_mvp.bi.fact_sale_metrics
WHERE fact_sale_metrics.date >= '2024-01-01'
GROUP BY 1
;


-- dev margin
SELECT
	fact_sale_metrics.date,
	SUM(fact_sale_metrics.margin_constant_currency)
FROM data_vault_mvp_dev_robin.bi.fact_sale_metrics
WHERE fact_sale_metrics.date >= '2024-01-01'
GROUP BY 1
;

-- prod margin
SELECT
	fact_sale_metrics.date,
	SUM(fact_sale_metrics.margin_constant_currency)
FROM data_vault_mvp.bi.fact_sale_metrics
WHERE fact_sale_metrics.date >= '2024-01-01'
GROUP BY 1
;


-- dev trx
SELECT
	fact_sale_metrics.date,
	SUM(fact_sale_metrics.trx)
FROM data_vault_mvp_dev_robin.bi.fact_sale_metrics
WHERE fact_sale_metrics.date >= '2024-01-01'
GROUP BY 1
;

-- prod trx
SELECT
	fact_sale_metrics.date,
	SUM(fact_sale_metrics.trx)
FROM data_vault_mvp.bi.fact_sale_metrics
WHERE fact_sale_metrics.date >= '2024-01-01'
GROUP BY 1
;

-- dev sessions
SELECT
	fact_sale_metrics.date,
	SUM(fact_sale_metrics.sessions)
FROM data_vault_mvp_dev_robin.bi.fact_sale_metrics
WHERE fact_sale_metrics.date >= '2024-01-01'
GROUP BY 1
;

-- prod sessions
SELECT
	fact_sale_metrics.date,
	SUM(fact_sale_metrics.sessions)
FROM data_vault_mvp.bi.fact_sale_metrics
WHERE fact_sale_metrics.date >= '2024-01-01'
GROUP BY 1
;


-- dev gross_revenue
SELECT
	fact_sale_metrics.date,
	SUM(fact_sale_metrics.gross_revenue)
FROM data_vault_mvp_dev_robin.bi.fact_sale_metrics
WHERE fact_sale_metrics.date >= '2024-01-01'
GROUP BY 1
;

-- prod gross_revenue
SELECT
	fact_sale_metrics.date,
	SUM(fact_sale_metrics.gross_revenue)
FROM data_vault_mvp.bi.fact_sale_metrics
WHERE fact_sale_metrics.date >= '2024-01-01'
GROUP BY 1
;