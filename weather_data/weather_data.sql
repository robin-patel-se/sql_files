SELECT *
FROM global_weather__climate_data_for_bi.standard_tile.climatology_day cd
;

SELECT *
FROM global_weather__climate_data_for_bi.standard_tile.forecast_day fd
;

SELECT *
FROM global_weather__climate_data_for_bi.standard_tile.history_day hd
;


SELECT
	*
FROM global_weather__climate_data_for_bi.standard_tile.forecast_day fd
WHERE country = 'GB'
;