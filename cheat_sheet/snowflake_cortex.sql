CREATE OR REPLACE VIEW v1 AS
SELECT DISTINCT
	(TO_TIMESTAMP_NTZ(date)) AS ntz_date,
	close,
	symbol
FROM test
WHERE symbol IN ('HTH', 'GOOG')
;

CREATE

OR

REPLACE
SNOWFLAKE.ML.FORECAST model1(INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'v1'),
                                    TIMESTAMP_COLNAME => 'NTZ_DATE',
                                    TARGET_COLNAME => 'CLOSE',
                                    SERIES_COLNAME => 'SYMBOL'
                                    );

SHOW SNOWFLAKE.ML.FORECAST
;

CALL model1!FORECAST(FORECASTING_PERIODS => 3)
;


CREATE OR REPLACE VIEW scratch.robinpatel.bookings_by_territory AS
SELECT
	(TO_TIMESTAMP_NTZ(fcb.booking_completed_date))                   AS booking_completed_date,
-- 	COUNT(DISTINCT fcb.booking_id)                      AS bookings,
	COALESCE(SUM(fcb.margin_gross_of_toms_gbp_constant_currency), 0) AS margin
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_timestamp >= CURRENT_DATE - 720
  AND fcb.booking_completed_timestamp < CURRENT_DATE - 5
  AND fcb.margin_gross_of_toms_gbp_constant_currency IS NOT NULL
  AND fcb.territory = 'DE'
GROUP BY 1
;

SELECT *
FROM scratch.robinpatel.bookings_by_territory
;

USE DATABASE scratch
;

USE SCHEMA scratch.robinpatel
;

-- CREATE OR REPLACE
-- SNOWFLAKE.ML.FORECAST model1(INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'scratch.robinpatel.bookings_by_territory'),
--                                     TIMESTAMP_COLNAME => 'BOOKING_COMPLETED_DATE',
--                                     TARGET_COLNAME => 'MARGIN'
--                                     );


CREATE

OR

REPLACE
SNOWFLAKE.ML.FORECAST scratch.robinpatel.model1(INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'scratch.robinpatel.bookings_by_territory'),
                                    TIMESTAMP_COLNAME => 'BOOKING_COMPLETED_DATE',
                                    TARGET_COLNAME => 'MARGIN'
                                    );

SHOW SNOWFLAKE.ML.FORECAST
;

CALL scratch.robinpatel.model1!FORECAST(FORECASTING_PERIODS => 10)
;



SELECT
	(TO_TIMESTAMP_NTZ(fcb.booking_completed_date))                   AS booking_completed_date,
-- 	COUNT(DISTINCT fcb.booking_id)                      AS bookings,
	COALESCE(SUM(fcb.margin_gross_of_toms_gbp_constant_currency), 0) AS margin
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_timestamp >= CURRENT_DATE - 5
  AND fcb.margin_gross_of_toms_gbp_constant_currency IS NOT NULL
  AND fcb.territory = 'DE'
GROUP BY 1
;

SELECT CURRENT_DATE - 720


SELECT *
FROM TABLE (RESULT_SCAN(LAST_QUERY_ID()))


------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW scratch.robinpatel.bookings_by_territory AS
SELECT
	(TO_TIMESTAMP_NTZ(fcb.booking_completed_date))                   AS booking_completed_date,
	fcb.territory,
	COUNT(DISTINCT fcb.booking_id)                      AS bookings
-- 	COALESCE(SUM(fcb.margin_gross_of_toms_gbp_constant_currency), 0) AS margin
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_timestamp >= CURRENT_DATE - 720
  AND fcb.booking_completed_timestamp < CURRENT_DATE - 5
  AND fcb.margin_gross_of_toms_gbp_constant_currency IS NOT NULL
  AND fcb.territory IN ('DE', 'UK')
GROUP BY 1, 2
;

SELECT *
FROM scratch.robinpatel.bookings_by_territory
;

USE DATABASE scratch
;

USE SCHEMA scratch.robinpatel
;

-- CREATE OR REPLACE
-- SNOWFLAKE.ML.FORECAST model1(INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'scratch.robinpatel.bookings_by_territory'),
--                                     TIMESTAMP_COLNAME => 'BOOKING_COMPLETED_DATE',
--                                     TARGET_COLNAME => 'MARGIN'
--                                     );


CREATE

OR

REPLACE
SNOWFLAKE.ML.FORECAST scratch.robinpatel.model1(INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'scratch.robinpatel.bookings_by_territory'),
                                    TIMESTAMP_COLNAME => 'BOOKING_COMPLETED_DATE',
                                    TARGET_COLNAME => 'BOOKINGS',
									SERIES_COLNAME => 'TERRITORY'
                                    );

SHOW SNOWFLAKE.ML.FORECAST
;

CALL scratch.robinpatel.model1!FORECAST(FORECASTING_PERIODS => 10)
;



SELECT
	(TO_TIMESTAMP_NTZ(fcb.booking_completed_date))                   AS booking_completed_date,
	fcb.territory,
	COUNT(DISTINCT fcb.booking_id)                      AS bookings
-- 	COALESCE(SUM(fcb.margin_gross_of_toms_gbp_constant_currency), 0) AS margin
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_timestamp >= CURRENT_DATE - 5
  AND fcb.margin_gross_of_toms_gbp_constant_currency IS NOT NULL
  AND fcb.territory IN ('DE', 'UK')
GROUP BY 1, 2
;

SELECT CURRENT_DATE - 720


SELECT *
FROM TABLE (RESULT_SCAN(LAST_QUERY_ID()))