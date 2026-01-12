CREATE OR REPLACE TABLE scratch.robinpatel.cookie_opt_out
(
	user_id VARCHAR
)
;


USE SCHEMA scratch.robinpatel
;

PUT 'file:///Users/robin.patel/myrepos/sql_files/pay_button_error/Users affected by Stripe iframe error.csv' @%cookie_opt_out
;

COPY INTO scratch.robinpatel.cookie_opt_out
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;

SELECT *
FROM scratch.robinpatel.cookie_opt_out
;
;

SELECT
	coo.user_id,
	sua.original_affiliate_territory,
	COUNT(DISTINCT fcb.booking_id),
	SUM(fcb.margin_gross_of_toms_gbp_constant_currency)
FROM se.data.fact_complete_booking fcb
	INNER JOIN scratch.robinpatel.cookie_opt_out coo ON fcb.shiro_user_id = coo.user_id
	INNER JOIN se.data.se_user_attributes sua ON coo.user_id = sua.shiro_user_id
WHERE fcb.booking_completed_date >= '2024-07-09'
GROUP BY 1, 2

SELECT
	sua.shiro_user_id,
	sua.current_affiliate_territory
FROM scratch.robinpatel.cookie_opt_out coo
	INNER JOIN se.data.se_user_attributes sua ON coo.user_id = sua.shiro_user_id;

SELECT * FROM se.data.se_user_attributes sua WHERE sua.shiro_user_id = '11649723'