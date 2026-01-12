USE WAREHOUSE pipe_large
;

USE SCHEMA scratch.robinpatel
;


CREATE OR REPLACE TABLE scratch.robinpatel.epsilon_transactions_pixel
(
	pxl_conversion_tran_id VARCHAR,
	order_time             VARCHAR,
	order_date             DATE,
	col4                   VARCHAR,
	col5                   VARCHAR
)
;


USE SCHEMA scratch.robinpatel
;
-- /Users/robin.patel/myrepos/sql_files/ad_hoc/epsilon-conversions-variance/Secret-Escapes-UK-Pixel-Only-Orders.csv
PUT 'file:///Users/robin.patel/myrepos/sql_files/ad_hoc/epsilon-conversions-variance/Secret-Escapes-UK-Pixel-Only-Orders.csv' @%epsilon_transactions_pixel
;

COPY INTO scratch.robinpatel.epsilon_transactions_pixel
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;

ALTER TABLE scratch.robinpatel.epsilon_transactions_pixel DROP COLUMN col4;
ALTER TABLE scratch.robinpatel.epsilon_transactions_pixel DROP COLUMN col5;

SELECT *
FROM scratch.robinpatel.epsilon_transactions_pixel
;



------------------------------------------------------------------------------------------------------------------------
