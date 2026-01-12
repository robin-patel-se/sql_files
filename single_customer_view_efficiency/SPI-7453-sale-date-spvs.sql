/*
module=/biapp/task_catalogue/dv/bi/scv/sale_date_spvs.py make clones
module=/biapp/task_catalogue/dv/bi/tableau/deal_model/spv_thresholds.py make clones
*/

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.chiasma_sale_active_spvs
	CLONE data_vault_mvp.dwh.chiasma_sale_active_spvs
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.sale_date_spvs
	CLONE data_vault_mvp.bi.sale_date_spvs
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.scv.sale_date_spvs.py' \
    --method 'run' \
    --start '2025-07-07 00:00:00' \
    --end '2025-07-07 00:00:00'



SELECT *
FROM data_vault_mvp.bi.sale_date_spvs


------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.random_dates AS
SELECT
	DATEADD(DAY, UNIFORM(0, 365 * 2, RANDOM()), CURRENT_DATE() - INTERVAL '2 year') AS random_date
FROM
	TABLE (GENERATOR(ROWCOUNT => 50))
;


-- prod
SELECT
	sds.spv_date,
	SUM(sds.spvs),
	SUM(sds.member_spvs),
	SUM(sds.non_member_spvs),
	SUM(sds.sessions)
FROM data_vault_mvp.bi.sale_date_spvs sds
	INNER JOIN scratch.robinpatel.random_dates rd ON sds.spv_date = rd.random_date
GROUP BY 1
ORDER BY 1
;

-- dev
SELECT
	sds.spv_date,
	SUM(sds.spvs),
	SUM(sds.member_spvs),
	SUM(sds.non_member_spvs),
	SUM(sds.sessions)
FROM data_vault_mvp_dev_robin.bi.sale_date_spvs sds
	INNER JOIN scratch.robinpatel.random_dates rd ON sds.spv_date = rd.random_date
GROUP BY 1
ORDER BY 1
;


-- prod
SELECT
	sds.spv_date,
	SUM(sds.spvs),
	SUM(sds.member_spvs),
	SUM(sds.non_member_spvs)
FROM data_vault_mvp.bi.sale_date_spvs sds
WHERE sds.spv_date >= CURRENT_DATE - 100
GROUP BY 1
ORDER BY 1
;

-- dev
SELECT
	sds.spv_date,
	SUM(sds.spvs),
	SUM(sds.member_spvs),
	SUM(sds.non_member_spvs)
FROM data_vault_mvp_dev_robin.bi.sale_date_spvs sds
WHERE sds.spv_date >= CURRENT_DATE - 100
GROUP BY 1
ORDER BY 1
;


DELETE
FROM data_vault_mvp_dev_robin.bi.sale_date_spvs
WHERE sale_date_spvs.spv_date = CURRENT_DATE - 60
;

self_describing_task --include 'sale_date_spvs.py'  --method 'run' --start '2025-01-01 00:00:00' --end '2025-01-01 00:00:00'

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active_snapshot
	CLONE data_vault_mvp.dwh.sale_active_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
	CLONE data_vault_mvp.dwh.se_sale
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.bi
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.sale_date_spvs
	CLONE data_vault_mvp.bi.sale_date_spvs
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.spv_thresholds
	CLONE data_vault_mvp.bi.spv_thresholds
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.tableau.deal_model.spv_thresholds.py' \
    --method 'run' \
    --start '2025-07-07 00:00:00' \
    --end '2025-07-07 00:00:00'

-- prod

SELECT
	st.global_sale_start_date,
	SUM(st.spvs)
FROM data_vault_mvp.bi.spv_thresholds st
WHERE st.global_sale_start_date BETWEEN CURRENT_DATE - 100 AND CURRENT_DATE
GROUP BY 1
;

-- dev
SELECT
	st.global_sale_start_date,
	SUM(st.spvs)
FROM data_vault_mvp_dev_robin.bi.spv_thresholds st
WHERE st.global_sale_start_date BETWEEN CURRENT_DATE - 100 AND CURRENT_DATE
GROUP BY 1
;


SELECT
	COUNT(*)
FROM data_vault_mvp.bi.sale_date_spvs sds


-- sale_date_spvs before fix
	0.00014954
3.01154641
0.00672711
0.05709727
0.02196691
0.00064832
0.05319185
0.00015739
0.00001312
0.00005740
0.00517488
0.00004817
0.00004808
0.00069590
0.00002070
0.00002419
0.00002525
0.00002487
0.00002792
0.00002623

-- sale_date_spvs after fix
0.00003121
0.00130889
0.00002344
0.01299330
0.01445095
0.02917123
0.00958116
0.00026362
0.00029022
0.00022550
0.00033899
0.00001306
0.00022870
0.00011774
0.00006237
0.00003712
0.00005603
0.00003625
0.00002324
0.00003706
0.00002427
0.00003282
0.00002462
0.00002926
0.00003135


ALTER TABLE data_vault_mvp.bi.trx_union_bookings
	RENAME TO data_vault_mvp_dev_donald.bi.trx_union_bookings_spi_7455_delete_on_20250715
;

CREATE TABLE data_vault_mvp.bi.trx_union_bookings
AS
SELECT *
FROM data_vault_mvp_dev_donald.bi.trx_union_bookings_spi_7455_delete_on_20250715
;

USE ROLE pipelinerunner
;

CREATE OR REPLACE TABLE data_vault_mvp.bi.sale_date_spvs_permanent_table
(
	-- metadata fields for the current job
	schedule_tstamp TIMESTAMP,
	run_tstamp      TIMESTAMP,
	operation_id    VARCHAR,
	created_at      TIMESTAMP,
	updated_at      TIMESTAMP,
	-- data fields
	se_sale_id      VARCHAR NOT NULL,
	spv_date        DATE    NOT NULL,
	posa_territory  VARCHAR NOT NULL,
	member_spvs     INTEGER,
	non_member_spvs INTEGER,
	spvs            INTEGER,
	sessions        INTEGER,
	data_source     VARCHAR,
	CONSTRAINT pk_sale_date_spvs
		PRIMARY KEY (se_sale_id, spv_date, posa_territory)
)
;

INSERT INTO data_vault_mvp.bi.sale_date_spvs_permanent_table
SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	se_sale_id,
	spv_date,
	posa_territory,
	member_spvs,
	non_member_spvs,
	spvs,
	sessions,
	data_source
FROM data_vault_mvp.bi.sale_date_spvs sds
;

ALTER TABLE data_vault_mvp.bi.sale_date_spvs
	RENAME TO data_vault_mvp.bi.sale_date_spvs_spi_7453_delete_on_20250808
;

ALTER TABLE data_vault_mvp.bi.sale_date_spvs_permanent_table
	RENAME TO data_vault_mvp.bi.sale_date_spvs
;

SELECT GET_DDL('table', 'data_vault_mvp.bi.sale_date_spvs')
;

SELECT
	sds.spv_date,
	SUM(sds.spvs)
FROM data_vault_mvp.bi.sale_date_spvs sds
WHERE sds.spv_date >= CURRENT_DATE - 10
GROUP BY 1
;

SELECT
	sts.event_tstamp::DATE AS date,
	COUNT(*)               AS spvs
FROM se.data.scv_touched_spvs sts
	INNER JOIN se.data.scv_touch_basic_attributes stba
			   ON sts.touch_id = stba.touch_id
			          AND stba.touch_start_tstamp >= CURRENT_DATE - 30
-- 				   AND stba.touch_se_brand = 'SE Brand'
WHERE sts.event_tstamp >= CURRENT_DATE - 10
GROUP BY 1