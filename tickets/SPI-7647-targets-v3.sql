SELECT *
FROM latest_vault_dev_robin.fpa_gsheets.se_targets_input_v3
;


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.fpa_gsheets
;

CREATE OR REPLACE VIEW latest_vault_dev_robin.fpa_gsheets.generic_targets
AS
SELECT *
FROM latest_vault.fpa_gsheets.generic_targets
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.se_targets_input_v2
	CLONE latest_vault.fpa_gsheets.se_targets_input_v2
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.transactional.generic_targets.py' \
    --method 'run' \
    --start '2025-08-12 00:00:00' \
    --end '2025-08-12 00:00:00'

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.generic_targets
;
-- 1805424


SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.generic_targets
;
-- 1805422

-- two sample rows correctly appended