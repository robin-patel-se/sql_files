USE ROLE pipelinerunner
;

ALTER TABLE data_vault_mvp.dwh.iterable_crm_reporting
	RENAME TO data_vault_mvp.dwh.iterable_crm_reporting__legacy
;


USE ROLE pipelinerunner
;

ALTER TABLE data_vault_mvp.dwh.iterable_crm_reporting
	RENAME TO data_vault_mvp.dwh.iterable_crm_reporting__legacy

ALTER TABLE data_vault_mvp.dwh.iterable_crm_reporting__migration
	RENAME TO data_vault_mvp.dwh.iterable_crm_reporting;
;

USE ROLE personal_role__robinpatel;

SELECT * FROM data_vault_mvp.dwh.iterable_crm_reporting;

SELECT NULL != 'MARI'