SELECT *
FROM unload_vault_mvp.sfsc.top_up_emails__20240403t050000__daily_at_05h00
;

-- on the 4th of April this account (hotel id) was included in the outgoing top up job

SELECT *
FROM unload_vault_mvp.sfsc.top_up_emails__20240403t050000__daily_at_05h00
WHERE account__c = '001w000001DVHGb'
;

-- Showed top ups for these rooms
/*

 001w000001DVHGb

ROOM__C	DATE__C
PSZ - Penta Standard Zimmer	2024-06-14
PSZ - Penta Standard Zimmer	2024-04-27
PSZ - Penta Standard Zimmer	2024-04-11
PSZ - Penta Standard Zimmer	2024-06-15
PSZ - Penta Standard Zimmer	2024-07-06
PSZ - Penta Standard Zimmer	2024-06-07
PSZ - Penta Standard Zimmer	2024-06-28
PSZ - Penta Standard Zimmer	2024-04-12
PSZ - Penta Standard Zimmer	2024-06-08
PSZ - Penta Standard Zimmer	2024-06-13
PSZ - Penta Standard Zimmer	2024-04-19

*/

-- limiting snapshot of availability data to date and room we knew a top up occurred
SELECT
	mhras.view_date,
	mhras.hotel_code,
	mhras.hotel_name,
	mhras.room_type_code,
	mhras.room_type_name,
	mhras.inventory_date,
	mhras.no_available_rooms
FROM data_vault_mvp.dwh.mari_hotel_room_availability_snapshot mhras
WHERE mhras.hotel_code = '001w000001DVHGb'
  AND mhras.inventory_date = '2024-06-14'
  AND mhras.room_type_code = 'PSZ'
;

-- 166 rows to run forecasting on

-- we want to forecast on multiple series https://docs.snowflake.com/en/user-guide/snowflake-cortex/ml-functions/forecasting#forecast-on-multiple-series
-- series we want to forecast on include:
-- hotel code
-- room type code
-- room type name
-- inventory date

-- to do this we want to create a view with this historic data
CREATE OR REPLACE VIEW scratch.robinpatel.hotel_room_availability_snapshot_data AS
(
SELECT
	[mhras.hotel_code, mhras.room_type_code, mhras.room_type_name, mhras.inventory_date] AS series_key, -- combination key for series
	mhras.view_date::TIMESTAMP                                                           AS view_date,  -- needs to be set as a timestamp
-- 	mhras.hotel_code,
-- 	mhras.hotel_name,
-- 	mhras.room_type_code,
-- 	mhras.room_type_name,
-- 	mhras.inventory_date,
	mhras.no_available_rooms
FROM data_vault_mvp.dwh.mari_hotel_room_availability_snapshot mhras
WHERE mhras.hotel_code = '001w000001DVHGb'
  AND mhras.inventory_date = '2024-06-14'
  AND mhras.room_type_code = 'PSZ'
-- need to remove instances where the minimum time series is less than 12 for the model to train
-- timestamp is view date
QUALIFY COUNT(*) OVER (PARTITION BY series_key) > 12
	)
;

-- train the model using a select to minus 10 days to see if the model can predict 10 days
CREATE

OR

REPLACE
SNOWFLAKE.ML.FORECAST scratch.robinpatel.hotel_room_availability_forecast(
	INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT * FROM scratch.robinpatel.hotel_room_availability_snapshot_data WHERE view_date < CURRENT_DATE - 10'),
	SERIES_COLNAME => 'series_key',
	TIMESTAMP_COLNAME => 'view_date',
	TARGET_COLNAME => 'no_available_rooms'
);


-- call model asking for a forecast of 20 days

CALL scratch.robinpatel.hotel_room_availability_forecast!FORECAST(SERIES_VALUE => [
	'001w000001DVHGb',
	'PSZ',
	'Penta Standard Zimmer',
	'2024-06-14'
	], FORECASTING_PERIODS => 10)
;

-- make a table from the forecast data
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.hotel_room_availability_forecast_data AS
SELECT *
FROM TABLE (RESULT_SCAN(-1))
;

SELECT *
FROM scratch.robinpatel.hotel_room_availability_forecast_data
;

-- check how forcast and actual align
SELECT
	COALESCE(a.view_date, f.ts) AS view_date,
	a.no_available_rooms,
	FLOOR(f.forecast)           AS forecast_no_available_rooms,
	f.lower_bound,
	f.upper_bound
FROM scratch.robinpatel.hotel_room_availability_snapshot_data a
	FULL OUTER JOIN scratch.robinpatel.hotel_room_availability_forecast_data f ON f.ts = a.view_date
WHERE a.view_date >= '2024-01-01'
;

-- has broadly shown rough estimation of rooms depleting to 1
-- happy with this because current process will create a top up when it reaches 0 and therefore prompt hotelliers to top up

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- reprocessing on all data to see if we can go through and check other hotels

CREATE OR REPLACE VIEW scratch.robinpatel.hotel_room_availability_snapshot_data AS
(
SELECT
	[mhras.hotel_code, mhras.room_type_code, mhras.room_type_name, mhras.inventory_date] AS series_key, -- combination key for series
	mhras.view_date::TIMESTAMP                                                           AS view_date,  -- needs to be set as a timestamp
	mhras.no_available_rooms
FROM data_vault_mvp.dwh.mari_hotel_room_availability_snapshot mhras
WHERE mhras.view_date >= '2024-01-01'
-- need to remove instances where the minimum time series is less than 12 for the model to train
-- timestamp is view date
QUALIFY COUNT(*) OVER (PARTITION BY series_key) > 12
	)
;

SELECT *
FROM scratch.robinpatel.hotel_room_availability_snapshot_data
;

-- found issue where if there's too many gaps in historic data the model cannot train, so limiting to snapshots
-- from this year.
USE WAREHOUSE pipe_xlarge
;
-- train the model using a select to minus 10 days to see if the model can predict 10 days for all hotels to interrogate more data
CREATE

OR

REPLACE
SNOWFLAKE.ML.FORECAST scratch.robinpatel.hotel_room_availability_forecast(
	INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT * FROM scratch.robinpatel.hotel_room_availability_snapshot_data WHERE view_date < CURRENT_DATE - 20'),
	SERIES_COLNAME => 'series_key',
	TIMESTAMP_COLNAME => 'view_date',
	TARGET_COLNAME => 'no_available_rooms'
);
-- this took 1 h 18 m 49 s 91 ms to train

CALL scratch.robinpatel.hotel_room_availability_forecast!FORECAST(FORECASTING_PERIODS => 25)
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.hotel_room_availability_forecast_data AS
SELECT *
FROM TABLE (RESULT_SCAN(-1))
;

SELECT
	series,
	ts,
	IFF(forecast < 0, 0, ROUND(forecast)) AS adjusted_forecast,
	forecast,
	lower_bound,
	upper_bound
FROM scratch.robinpatel.hotel_room_availability_forecast_data
;

-- look for other

SELECT
-- 	external_id__c,
-- 	opportunity__c,
account__c,
room__c,
date__c,
-- 	total_inventory__c,
-- 	rooms_booked__c,
-- 	close_out__c,
still_available__c,
-- 	schedule_tstamp,
-- 	run_tstamp,
-- 	operation_id,
-- 	created_at,
-- 	updated_at
FROM unload_vault_mvp.sfsc.top_up_emails__20240403t050000__daily_at_05h00
;


SELECT
	series,
	series[0]::VARCHAR                    AS hotel_code,
	series[1]::VARCHAR                    AS room_code,
	series[2]::VARCHAR                    AS room_name,
	series[3]::DATE                       AS inventory_date,
	ts,
	IFF(forecast < 0, 0, ROUND(forecast)) AS adjusted_forecast,
	forecast,
	lower_bound,
	upper_bound
FROM scratch.robinpatel.hotel_room_availability_forecast_data f
WHERE hotel_code = '001w000001DVHGb'
  AND room_code = 'PSZ'
  AND inventory_date = '2024-06-14'
;

SELECT
	series_key,
	series_key[0]::VARCHAR AS hotel_code,
	series_key[1]::VARCHAR AS room_code,
	series_key[2]::VARCHAR AS room_name,
	series_key[3]::DATE    AS inventory_date,
	view_date,
	no_available_rooms
FROM scratch.robinpatel.hotel_room_availability_snapshot_data hrasd
WHERE hrasd.series_key[0]::VARCHAR = '001w000001DVHGb'
  AND series_key[1]::VARCHAR = 'PSZ'
  AND series_key[3]::DATE = '2024-06-14'


-- looking at the model trained on all data the forecast is a lot different to when it was trained on a specific use case
-- going to try train on just the hotel and include 2023 data to see if this is different

USE WAREHOUSE pipe_xlarge
;

CREATE OR REPLACE VIEW scratch.robinpatel.hotel_room_availability_snapshot_data AS
(
SELECT
	[mhras.hotel_code, mhras.room_type_code, mhras.room_type_name, mhras.inventory_date] AS series_key, -- combination key for series
	mhras.view_date::TIMESTAMP                                                           AS view_date,  -- needs to be set as a timestamp
	mhras.no_available_rooms
FROM data_vault_mvp.dwh.mari_hotel_room_availability_snapshot mhras
WHERE mhras.view_date >= '2023-01-01'
  AND mhras.hotel_code = '001w000001DVHGb'
  AND mhras.inventory_date = '2024-06-14'
  AND mhras.room_type_code = 'PSZ'
-- need to remove instances where the minimum time series is less than 12 for the model to train
-- timestamp is view date
QUALIFY COUNT(*) OVER (PARTITION BY series_key) > 12
	)
;

CREATE

OR

REPLACE
SNOWFLAKE.ML.FORECAST scratch.robinpatel.hotel_room_availability_forecast_v2(
	INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT * FROM scratch.robinpatel.hotel_room_availability_snapshot_data WHERE view_date < CURRENT_DATE - 10'),
	SERIES_COLNAME => 'series_key',
	TIMESTAMP_COLNAME => 'view_date',
	TARGET_COLNAME => 'no_available_rooms'
);


CALL scratch.robinpatel.hotel_room_availability_forecast_v2!FORECAST(FORECASTING_PERIODS => 20)
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.hotel_room_availability_forecast_data_one_hotel AS
SELECT *
FROM TABLE (RESULT_SCAN(-1))
;

SELECT
	series,
	series[0]::VARCHAR                    AS hotel_code,
	series[1]::VARCHAR                    AS room_code,
	series[2]::VARCHAR                    AS room_name,
	series[3]::DATE                       AS inventory_date,
	ts,
	IFF(forecast < 0, 0, ROUND(forecast)) AS adjusted_forecast,
	forecast,
	lower_bound,
	upper_bound
FROM scratch.robinpatel.hotel_room_availability_forecast_data_one_hotel f
WHERE hotel_code = '001w000001DVHGb'
  AND room_code = 'PSZ'
  AND inventory_date = '2024-06-14'
;

SELECT
	series_key,
	series_key[0]::VARCHAR AS hotel_code,
	series_key[1]::VARCHAR AS room_code,
	series_key[2]::VARCHAR AS room_name,
	series_key[3]::DATE    AS inventory_date,
	view_date,
	no_available_rooms
FROM scratch.robinpatel.hotel_room_availability_snapshot_data hrasd
WHERE hrasd.series_key[0]::VARCHAR = '001w000001DVHGb'
  AND hrasd.series_key[1]::VARCHAR = 'PSZ'
  AND hrasd.series_key[3]::DATE = '2024-06-14'

-- looks like more historical data is necessary to train the model more accurately as forecast looks closer to original models

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_4xlarge
;

-- 2023 data is too large so filtering to just UK posu

CREATE OR REPLACE VIEW scratch.robinpatel.hotel_room_availability_snapshot_data AS
(
WITH
	posu_cluster_region AS (
		SELECT DISTINCT
			ssa.hotel_code
		FROM se.data.se_sale_attributes ssa
		WHERE ssa.posu_cluster_region = 'UK'
	)
SELECT
	[mhras.hotel_code, mhras.room_type_code, mhras.room_type_name, mhras.inventory_date] AS series_key, -- combination key for series
	mhras.view_date::TIMESTAMP                                                           AS view_date,  -- needs to be set as a timestamp
	mhras.no_available_rooms
FROM data_vault_mvp.dwh.mari_hotel_room_availability_snapshot mhras
	--filter for UK posu
	INNER JOIN posu_cluster_region pcr ON mhras.hotel_code = pcr.hotel_code
WHERE mhras.view_date >= '2023-01-01'
-- need to remove instances where the minimum time series is less than 12 for the model to train
-- timestamp is view date
QUALIFY COUNT(*) OVER (PARTITION BY series_key) > 12
	)
;

CREATE

OR

REPLACE
SNOWFLAKE.ML.FORECAST scratch.robinpatel.hotel_room_availability_forecast_v3(
	INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT * FROM scratch.robinpatel.hotel_room_availability_snapshot_data WHERE view_date < CURRENT_DATE - 10'),
	SERIES_COLNAME => 'series_key',
	TIMESTAMP_COLNAME => 'view_date',
	TARGET_COLNAME => 'no_available_rooms'
);


CALL scratch.robinpatel.hotel_room_availability_forecast_v3!FORECAST(FORECASTING_PERIODS => 20)
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.hotel_room_availability_forecast_data AS
SELECT *
FROM TABLE (RESULT_SCAN(-1))
;

SELECT * FROM scratch.robinpatel.hotel_room_availability_snapshot_data;

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM collab.muse.warehouse_metering wm
;



-- to do this we want to create a view with this historic data
CREATE OR REPLACE VIEW scratch.robinpatel.warehouse_metering_historic_data AS
(
WITH
	warehouses AS (
		SELECT DISTINCT
			warehouse_name
		FROM collab.muse.warehouse_metering wm
	),
	explode AS (
		SELECT
			sc.date_value,
			w.warehouse_name
		FROM se.data.se_calendar sc
			CROSS JOIN warehouses w
		WHERE sc.date_value BETWEEN '2023-04-01' AND CURRENT_DATE
	),
	warehouse_data AS (
		SELECT
			DATE_TRUNC(DAY, wm.start_time)::TIMESTAMP AS view_date,
			wm.warehouse_name,
			SUM(wm.credits_used)                      AS credits_used
		FROM collab.muse.warehouse_metering wm
		GROUP BY 1, 2
	)
SELECT
	date_value::TIMESTAMP     AS date_value,
	e.warehouse_name,
-- 	view_date,
-- 	wd.warehouse_name,
	COALESCE(credits_used, 0) AS credits_used
FROM explode e
	LEFT JOIN warehouse_data wd ON e.date_value = wd.view_date AND e.warehouse_name = wd.warehouse_name
	)
;

-- train the model using a select to minus 10 days to see if the model can predict 10 days
CREATE

OR

REPLACE
SNOWFLAKE.ML.FORECAST scratch.robinpatel.warehouse_cost_forecast(
	INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT * FROM scratch.robinpatel.warehouse_metering_historic_data WHERE date_value < CURRENT_DATE - 10'),
	SERIES_COLNAME => 'warehouse_name',
	TIMESTAMP_COLNAME => 'date_value',
	TARGET_COLNAME => 'credits_used'
);



CALL scratch.robinpatel.warehouse_cost_forecast!FORECAST(FORECASTING_PERIODS => 20)
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.warehouse_cost_forecast_data AS
SELECT *
FROM TABLE (RESULT_SCAN(-1))
;

SELECT *
FROM scratch.robinpatel.warehouse_cost_forecast_data
;


-- check how forcast and actual align
SELECT
	a.warehouse_name,
	COALESCE(a.date_value, f.ts) AS view_date,
	a.credits_used,
	f.forecast,
	f.lower_bound,
	f.upper_bound
FROM scratch.robinpatel.warehouse_metering_historic_data a
	FULL OUTER JOIN scratch.robinpatel.warehouse_cost_forecast_data f ON f.ts = a.date_value
	AND a.warehouse_name = f.series::VARCHAR
WHERE a.date_value >= '2024-03-01'
AND a.warehouse_name = 'PIPE_MEDIUM'
;


SELECT
	a.warehouse_name,
	COALESCE(a.date_value, f.ts) AS view_date,
	a.credits_used,
	f.forecast,
	f.lower_bound,
	f.upper_bound
FROM scratch.robinpatel.warehouse_metering_historic_data a
	FULL OUTER JOIN scratch.robinpatel.warehouse_cost_forecast_data f ON f.ts = a.date_value
	AND a.warehouse_name = f.series::VARCHAR
WHERE a.date_value >= '2024-03-01'
AND a.warehouse_name = 'ANALYST_LARGE'
;
