SELECT *
FROM collab.finance_netsuite.employees e
;

CREATE OR REPLACE TRANSIENT TABLE collab.finance_netsuite.employees_20240126 CLONE collab.finance_netsuite.employees
;

CREATE OR REPLACE TRANSIENT TABLE collab.finance_netsuite.employees CLONE collab.finance_netsuite.employees_20240126;

USE ROLE pipelinerunner
;

TRUNCATE collab.finance_netsuite.employees;

USE SCHEMA collab.finance_netsuite
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/employees/20240126_extract.csv' @%employees
;

COPY INTO collab.finance_netsuite.employees
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;