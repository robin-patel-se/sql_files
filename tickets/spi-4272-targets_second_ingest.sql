dataset_task --include 'fpa_gsheets.se_targets_input_v2' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-05 00:30:00' --end '2023-10-05 00:30:00'

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.generic_targets CLONE latest_vault.fpa_gsheets.generic_targets
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/generic_targets.py'  --method 'run' --start '2023-10-05 00:00:00' --end '2023-10-05 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.generic_targets
WHERE source = 'SE Targets Input v2'
;


self_describing_task --include 'biapp/task_catalogue/se/data/dwh/generic_targets.py'  --method 'run' --start '2023-10-05 00:00:00' --end '2023-10-05 00:00:00'

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale AS
SELECT *
FROM data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;

self_describing_task --include 'biapp/task_catalogue/dv/bi/tableau/target_model/targets.py'  --method 'run' --start '2023-10-05 00:00:00' --end '2023-10-05 00:00:00'

USE ROLE pipelinerunner;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage', 'latest_vault.fpa_gsheets.generic_targets');

SELECT * FROM scratch.robinpatel.table_usage;

SELECT * FROM snowflake.account_usage.query_history qh WHERE LOWER(qh.query_text) LIKE '%latest_vault.fpa_gsheets.generic_targets%'
AND qh.role_name = 'PERSONAL_ROLE__TABLEAU'


USE ROLE pipelinerunner;

CALL data_vault_mvp.dwh.table_reference_in_view('scratch.robinpatel.table_reference_in_view', 'latest_vault.fpa_gsheets.generic_targets', 'collab, data_vault_mvp, se');

SELECT * FROM scratch.robinpatel.table_reference_in_view;