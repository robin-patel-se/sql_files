SELECT *
FROM collab.finance_netsuite.transaction_lines
;

CREATE OR REPLACE TRANSIENT TABLE collab.finance_netsuite.transaction_lines_20240126 CLONE collab.finance_netsuite.transaction_lines
;

CREATE OR REPLACE TRANSIENT TABLE collab.finance_netsuite.transaction_lines_test CLONE collab.finance_netsuite.transaction_lines
;

------------------------------------------------------------------------------------------------------------------------

USE SCHEMA collab.finance_netsuite
;

USE ROLE pipelinerunner
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/transaction_lines/01-2023.csv' @%transaction_lines
;

COPY INTO collab.finance_netsuite.transaction_lines
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/transaction_lines/02-2023.csv' @%transaction_lines
;

SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name = 'Jan 2023'
;


COPY INTO collab.finance_netsuite.transaction_lines
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/transaction_lines/03-2023.csv' @%transaction_lines
;

SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name = 'Feb 2023'
;


COPY INTO collab.finance_netsuite.transaction_lines
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/transaction_lines/04-2023.csv' @%transaction_lines
;

SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name = 'Mar 2023'
;


COPY INTO collab.finance_netsuite.transaction_lines
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;

SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name = 'Apr 2023'
;


PUT 'file:///Users/robin.patel/Documents/netsuite_export/transaction_lines/05-2023.csv' @%transaction_lines
;

COPY INTO collab.finance_netsuite.transaction_lines
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name = 'May 2023'
;


PUT 'file:///Users/robin.patel/Documents/netsuite_export/transaction_lines/06-2023.csv' @%transaction_lines
;

COPY INTO collab.finance_netsuite.transaction_lines
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name = 'Jun 2023'
;


PUT 'file:///Users/robin.patel/Documents/netsuite_export/transaction_lines/07-2023.csv' @%transaction_lines
;

COPY INTO collab.finance_netsuite.transaction_lines
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name = 'Jul 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/transaction_lines/08-2023.csv' @%transaction_lines
;

COPY INTO collab.finance_netsuite.transaction_lines
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name = 'Aug 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/transaction_lines/09-2023.csv' @%transaction_lines
;

COPY INTO collab.finance_netsuite.transaction_lines
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name = 'Sep 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/transaction_lines/10-2023.csv' @%transaction_lines
;

COPY INTO collab.finance_netsuite.transaction_lines
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name = 'Oct 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/transaction_lines/11-2023.csv' @%transaction_lines
;

COPY INTO collab.finance_netsuite.transaction_lines
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name = 'Nov 2023'
;

PUT 'file:///Users/robin.patel/Documents/netsuite_export/transaction_lines/12-2023.csv' @%transaction_lines
;

COPY INTO collab.finance_netsuite.transaction_lines
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


ALTER TABLE collab.finance_netsuite.transaction_lines
	ALTER COLUMN transaction_lines_amount NUMBER(14, 4)
;

SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name = 'Dec 2023'
;


------------------------------------------------------------------------------------------------------------------------

SELECT
	accounting_periods_name,
	COUNT(*)
FROM collab.finance_netsuite.transaction_lines
WHERE accounting_periods_name LIKE '%2023'
GROUP BY 1
;


SELECT *
FROM collab.finance_netsuite.transaction_lines
WHERE transaction_lines.transaction_lines_amount = 0
;


GRANT USAGE ON SCHEMA collab.finance_netsuite TO ROLE personal_role__gerrykerins
;

GRANT SELECT ON ALL VIEWS IN SCHEMA collab.finance_netsuite TO ROLE personal_role__gerrykerins
;

USE ROLE personal_role__gerrykerins
;

SELECT *
FROM collab.finance_netsuite.netsuite_audit_transactions
WHERE accounting_periods_name = 'Jan 2023'
;