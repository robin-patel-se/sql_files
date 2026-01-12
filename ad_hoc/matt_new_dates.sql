/*
 matt
 Wants a CM report to show how many new dates they've added per deal per month
 */


--
-- WITH dim_sale AS (
--     SELECT DISTINCT
--     salesforce_opportunity_id,
--     posa_territory
--         FROM se.data.dim_sale
-- ), avails AS (SELECT hocvs.view_date,
--                      ds.posa_territory,
--                      hocvs.salesforce_opportunity_id,
--                      hocvs.room_name,
--                      hocvs.rate_plan_name,
--                      hocvs.calendar_date,
--                      1 AS date_available
--               FROM se.data.harmonised_offer_calendar_view_snapshot hocvs
--                        INNER JOIN dim_sale ds ON ds.salesforce_opportunity_id = hocvs.salesforce_opportunity_id
--               WHERE ds.posa_territory IN ('DE', 'CH', 'AT')
--                 AND per_night_rate_eur <= 169
--                 AND offer_available_in_calendar)
-- SELECT DATE_TRUNC('month',view_date) AS view_month,
--        posa_territory,
--        salesforce_opportunity_id,
--        room_name,
--        rate_plan_name,
--        calendar_date,
--        COUNT(*)
-- FROM avails
-- WHERE salesforce_opportunity_id = '006Tg000000sCLx'
-- GROUP BY ALL


WITH
	dach_opps AS (
		-- currently limited to opportunities that are available in DACH
		SELECT DISTINCT
			ds.salesforce_opportunity_id
		FROM se.data.dim_sale ds
		WHERE ds.posa_territory IN ('DE', 'CH', 'AT')
	)
		,
	avail_data AS (
		SELECT
			SHA2(COALESCE(hocvs.salesforce_opportunity_id, '')
				|| COALESCE(hocvs.room_name, '')
				|| COALESCE(hocvs.rate_plan_name, '')
				|| COALESCE(hocvs.calendar_date, '1970-01-01')
			) AS id,
			hocvs.view_date,
			hocvs.salesforce_opportunity_id,
			hocvs.room_name,
			hocvs.rate_plan_name,
			hocvs.calendar_date
		FROM se.data.harmonised_offer_calendar_view_snapshot hocvs
			INNER JOIN dach_opps dop ON hocvs.salesforce_opportunity_id = dop.salesforce_opportunity_id
		WHERE per_night_rate_eur <= 169
		  AND offer_available_in_calendar
		  AND hocvs.salesforce_opportunity_id = '0066900001I2D7a' -- TODO REMOVE
		  AND hocvs.room_name = 'Club Room'                       -- TODO REMOVE
		  AND hocvs.calendar_date = '2024-08-14'                  -- TODO REMOVE
		  AND hocvs.rate_plan_name = 'Club Room BB Lounge'        -- TODO REMOVE
		  AND hocvs.view_date <> '2024-07-31'                     -- TODO REMOVE
		  AND hocvs.view_date <> '2024-08-01' -- TODO REMOVE
	),
	calendar AS (
		SELECT
			sc.date_value,
			date_value = LAST_DAY(sc.date_value, MONTH) AS is_last_day_of_month
		FROM se.data.se_calendar sc
		WHERE sc.date_value BETWEEN '2024-01-01' AND CURRENT_DATE
	)
SELECT
	c.date_value,
	c.is_last_day_of_month,
	IFF(c.is_last_day_of_month AND ad.view_date IS NOT NULL, 1, NULL) AS available_last_day_of_month,
-- 	SUM(available_last_day_of_month) OVER (PARTITION BY ) ad.view_date, ad.salesforce_opportunity_id,
	ad.id,
	ad.salesforce_opportunity_id,
	ad.room_name,
	ad.rate_plan_name,
	ad.calendar_date
FROM calendar c
	LEFT JOIN avail_data ad ON c.date_value = ad.view_date

;



WITH
	dach_opps AS (
		-- currently limited to opportunities that are available in DACH
		SELECT DISTINCT
			ds.salesforce_opportunity_id
		FROM se.data.dim_sale ds
		WHERE ds.posa_territory IN ('DE', 'CH', 'AT')
	)
		,
	avail_data AS (
		SELECT
			-- create a common id field for self join on historic dates
			SHA2(COALESCE(hocvs.salesforce_opportunity_id, '')
				|| COALESCE(hocvs.room_name, '')
				|| COALESCE(hocvs.rate_plan_name, '')
				|| COALESCE(hocvs.calendar_date, '1970-01-01')
			)                                                  AS id,
			hocvs.view_date,
			hocvs.salesforce_opportunity_id,
			hocvs.room_name,
			hocvs.rate_plan_name,
			hocvs.calendar_date,
			hocvs.view_date = LAST_DAY(hocvs.view_date, MONTH) AS is_last_date_of_month
		FROM se.data.harmonised_offer_calendar_view_snapshot hocvs
			INNER JOIN dach_opps dop ON hocvs.salesforce_opportunity_id = dop.salesforce_opportunity_id
		WHERE per_night_rate_eur <= 169
		  AND offer_available_in_calendar
		  AND hocvs.salesforce_opportunity_id = '0066900001I2D7a' -- TODO REMOVE
-- 		  AND hocvs.room_name = 'Club Room'                       -- TODO REMOVE
-- 		  AND hocvs.calendar_date = '2024-08-14'                  -- TODO REMOVE
-- 		  AND hocvs.rate_plan_name = 'Club Room BB Lounge'        -- TODO REMOVE
-- 		  AND hocvs.view_date <> '2024-07-31'                     -- TODO REMOVE
-- 		  AND hocvs.view_date <> '2024-08-01' -- TODO REMOVE
	),
	modelling AS (
		SELECT
			ad1.id,
			ad1.view_date,
			ad1.salesforce_opportunity_id,
			ad1.room_name,
			ad1.rate_plan_name,
			ad1.calendar_date,
			ad1.is_last_date_of_month,
			IFF(ad2.id IS NOT NULL, TRUE, FALSE) AS had_avail_end_of_last_month
		FROM avail_data ad1
			-- self join on last date of month
			LEFT JOIN avail_data ad2 ON ad1.id = ad2.id
			AND LAST_DAY(DATEADD(MONTH, -1, ad1.view_date), MONTH) = ad2.view_date
			AND ad2.is_last_date_of_month
	)
SELECT
	DATE_TRUNC(MONTH, m.view_date)                                       AS month,
	m.salesforce_opportunity_id,
	COUNT(DISTINCT IFF(m.had_avail_end_of_last_month = FALSE, id, NULL)) AS added_dates
FROM modelling m
GROUP BY 1, 2;
