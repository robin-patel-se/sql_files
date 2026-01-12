SELECT *
FROM collab.finance_netsuite.system_notes
;

CREATE OR REPLACE TRANSIENT TABLE collab.finance_netsuite.system_notes_20240126 CLONE collab.finance_netsuite.system_notes
;

USE ROLE pipelinerunner
;

USE SCHEMA collab.finance_netsuite
;


PUT 'file:///Users/robin.patel/Documents/netsuite_export/system_notes/01-2023.csv' @%system_notes
;

COPY INTO collab.finance_netsuite.system_notes
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name = 'Jan 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/system_notes/02-2023.csv' @%system_notes
;

COPY INTO collab.finance_netsuite.system_notes
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name = 'Feb 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/system_notes/03-2023.csv' @%system_notes
;

COPY INTO collab.finance_netsuite.system_notes
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name = 'Mar 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/system_notes/04-2023.csv' @%system_notes
;

COPY INTO collab.finance_netsuite.system_notes
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name = 'Apr 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/system_notes/05-2023.csv' @%system_notes
;

COPY INTO collab.finance_netsuite.system_notes
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name = 'May 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/system_notes/06-2023.csv' @%system_notes
;

COPY INTO collab.finance_netsuite.system_notes
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name = 'Jun 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/system_notes/07-2023.csv' @%system_notes
;

COPY INTO collab.finance_netsuite.system_notes
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name = 'Jul 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/system_notes/08-2023.csv' @%system_notes
;

COPY INTO collab.finance_netsuite.system_notes
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name = 'Aug 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/system_notes/09-2023.csv' @%system_notes
;

COPY INTO collab.finance_netsuite.system_notes
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name = 'Sep 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/system_notes/10-2023.csv' @%system_notes
;

COPY INTO collab.finance_netsuite.system_notes
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name = 'Oct 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/system_notes/11-2023.csv' @%system_notes
;

COPY INTO collab.finance_netsuite.system_notes
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name = 'Nov 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/system_notes/12-2023.csv' @%system_notes
;

COPY INTO collab.finance_netsuite.system_notes
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name = 'Dec 2023'
;


SELECT
	accounting_periods_name,
	COUNT(*)
FROM collab.finance_netsuite.system_notes
WHERE accounting_periods_name LIKE '%2023'
GROUP BY 1;


