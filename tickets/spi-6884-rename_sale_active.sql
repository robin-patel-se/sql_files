USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
CLONE data_vault_mvp.dwh.se_sale;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer
CLONE data_vault_mvp.dwh.tb_offer;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active
CLONE data_vault_mvp.dwh.sale_active;

ALTER TABLE data_vault_mvp_dev_robin.dwh.sale_active RENAME TO data_vault_mvp_dev_robin.dwh.sale_active_snapshot;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.transactional.sale_active_snapshot.py' \
    --method 'run' \
    --start '2024-12-31 00:00:00' \
    --end '2024-12-31 00:00:00'


------------------------------------------------------------------------------------------------------------------------
-- post deps
USE ROLE pipelinerunner;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.sale_active_20250107 CLONE data_vault_mvp.dwh.sale_active;

ALTER TABLE data_vault_mvp.dwh.sale_active RENAME TO data_vault_mvp.dwh.sale_active_snapshot;



------------------------------------------------------------------------------------------------------------------------


USE ROLE pipelinerunner
;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
									'data_vault_mvp.dwh.sale_active, se.data.sale_active')
;

SELECT *
FROM scratch.robinpatel.table_usage
;

USE ROLE pipelinerunner
;

USE WAREHOUSE pipe_xlarge
;

CALL data_vault_mvp.dwh.table_reference_in_view('scratch.robinpatel.table_reference_in_view',
												'data_vault_mvp.dwh.sale_active, se.data.sale_active',
												'collab, data_vault_mvp, se, data_science')
;

SELECT *
FROM scratch.robinpatel.table_reference_in_view
;
