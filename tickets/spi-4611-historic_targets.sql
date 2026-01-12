SELECT *
FROM latest_vault.fpa_gsheets.generic_targets gt
WHERE gt.dimension_4 = 'Scandi'
;

USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault.fpa_gsheets.generic_targets_20240115 CLONE latest_vault.fpa_gsheets.generic_targets
;

USE ROLE personal_role__robinpatel
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.generic_targets CLONE latest_vault.fpa_gsheets.generic_targets
;


UPDATE latest_vault.fpa_gsheets.generic_targets target
SET target.dimension_1 = 2
WHERE target.dimension_4 = 'Scandi'
  AND target_name IN (
					  'cluster_sub_region_target',
					  'margin_v2'
	)
;

SELECT *
FROM data_vault_mvp.dwh.generic_targets gt
WHERE gt.dimension_4 = 'Scandi'
  AND gt.target_name IN (
						 'cluster_sub_region_target',
						 'margin_v2'
	)
;

SELECT *
FROM data_vault_mvp.bi.targets t
WHERE t.dimension_4 = 'Scandi'
;

SELECT *
FROM se.data.generic_targets gt
WHERE gt.dimension_4 = 'Scandi' AND source = 'SE Targets Input'
;


CREATE OR REPLACE TRANSIENT TABLE scratch.jackbackler.generic_targets CLONE latest_vault.fpa_gsheets.generic_targets
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.generic_targets CLONE latest_vault.fpa_gsheets.generic_targets
;

SELECT *
FROM se.bi.targets
WHERE dimension_4 = 'Scandi'


UPDATE scratch.jackbackler.generic_targets target
SET target.dimension_1 = 2
WHERE target.dimension_4 = 'Scandi'
  AND target_name IN (
					  'cluster_sub_region_target',
					  'margin_v2'
	)
;


-- create a local copy of the se targets input data
CREATE OR REPLACE TRANSIENT TABLE scratch.jackbackler.generic_targets AS
SELECT *
FROM se.data.generic_targets gt
WHERE gt.source = 'SE Targets Input'
;

-- update the records
UPDATE scratch.jackbackler.generic_targets target
SET target.dimension_1 = 2
WHERE target.dimension_4 = 'Scandi'
  AND target_name IN (
					  'cluster_sub_region_target',
					  'margin_v2'
	)
;

-- check the output
SELECT *
FROM scratch.jackbackler.generic_targets target
;


SELECT *
FROM se.data.fact_booking fb

SELECT *
FROM se.data.dim_sale ds
WHERE ds.posu_country = 'Sweden';


SELECT *
FROM latest_vault.fpa_gsheets.posu_categorisation pc
;

SELECT *
FROM se.bi.targets
WHERE dimension_4 = 'Scandi'
AND TARGETS.target_date >= current_date;

