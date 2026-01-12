CREATE SCHEMA collab.weather_analysis
;

USE ROLE accountadmin
;

GRANT USAGE ON SCHEMA collab.weather_analysis TO ROLE personal_role__dharmitabhanderi
;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA collab.weather_analysis TO ROLE personal_role__dharmitabhanderi
;

GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA collab.weather_analysis TO ROLE personal_role__robinpatel
;

CREATE OR REPLACE TABLE collab.weather_analysis.weather_data
(
	datetime VARCHAR,
	day_name VARCHAR,
	had_rain VARCHAR
)
;


USE SCHEMA collab.weather_analysis
;

PUT 'file:///Users/robin.patel/myrepos/sql_files/weather_data/weather-data.csv' @%weather_data
;

COPY INTO collab.weather_analysis.weather_data
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;

CREATE OR REPLACE TRANSIENT TABLE collab.weather_analysis.rain_data AS
SELECT
	TO_DATE(datetime, 'dd/MM/yyyy')     AS date,
	day_name,
	IFF(had_rain = 'rain', TRUE, FALSE) AS had_rain
FROM collab.weather_analysis.weather_data
;

SELECT *
FROM collab.weather_analysis.rain_data
;

DROP TABLE collab.weather_analysis.weather_data
;

SELECT *
FROM collab.weather_analysis.rain_data
;

SELECT
	fsm.date,
	SUM(fsm.trx)                      AS total_bookings,
	SUM(IFF(rd.had_rain, fsm.trx, 0)) AS rain_day_bookings,
	SUM(IFF(rd.had_rain = FALSE, fsm.trx, 0)) AS no_rain_day_bookings
FROM se.bi.fact_sale_metrics fsm
	INNER JOIN collab.weather_analysis.rain_data rd ON fsm.date = rd.date
WHERE fsm.posa_territory = 'UK'
GROUP BY 1;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction CLONE data_vault_mvp.dwh.iterable__user_profile_transaction;

SELECT * FROM data_vault_mvp.dwh.iterable__user_profile_transaction iupt WHERE iupt.shiro_user_id = 72703050;

SELECT * FROm se.data.fact_complete_booking fcb WHERE fcb.shiro_user_id = 72703050;

USE ROLE ACCOUNTADMIN;
ALTER USER datasciencerunner SET MINS_TO_UNLOCK = 0;