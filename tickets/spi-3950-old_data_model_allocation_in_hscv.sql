-- Code from Gianni
-- https://se-tech.slack.com/archives/DQB436T27/p1686570671313339
SELECT
	s.id                                                                        AS saleid,
	o.id                                                                        AS offerid,
	ot.name                                                                     AS offername,
	LISTAGG(DISTINCT d.code, ', ')                                              AS airportcode,
	s.base_currency                                                             AS currency,
	a.id                                                                        AS allocationid,
	a.start_date                                                                AS allocationstart,
	a.end_date                                                                  AS allocationend,
	COUNT(DISTINCT ai.id)                                                       AS numberofrooms,
	COUNT(DISTINCT CASE WHEN ai.state = 'AVAILABLE' THEN ai.id ELSE NULL END)   AS available,
	COUNT(DISTINCT CASE WHEN ai.state = 'BOOKED' THEN ai.id ELSE NULL END)      AS booked,
	COUNT(DISTINCT CASE WHEN ai.state = 'LOCKED' THEN ai.id ELSE NULL END)      AS locked,
	COUNT(DISTINCT CASE WHEN ai.state = 'BLACKED_OUT' THEN ai.id ELSE NULL END) AS blackout,
	MIN(a.rate)                                                                 AS rate,
	''                                                                          AS rackrate,
	'-'                                                                         AS singlerate,
	''                                                                          AS childrate,
	''                                                                          AS infantrate,
	''                                                                          AS minnumberofnights
FROM latest_vault.cms_mysql.sale s
	INNER JOIN latest_vault.cms_mysql.offer o ON o.sale_id = s.id
	INNER JOIN latest_vault.cms_mysql.allocation a ON a.offer_id = o.id
	LEFT JOIN  latest_vault.cms_mysql.allocation_items ais ON ais.allocation_id = a.id
	LEFT JOIN  latest_vault.cms_mysql.allocation_item ai ON ai.id = ais.allocation_item_id
	LEFT JOIN  latest_vault.cms_mysql.departure d ON d.id = a.departure_id
	INNER JOIN latest_vault.cms_mysql.offer_translation ot ON ot.offer_id = o.id
WHERE o.active = TRUE
  --and (
  --      (s.start > :startDate and s.start <= :endDate)
  --      or (s.start < :startDate and s.end > :endDate)
  --      or (s.end > :startDate and s.end <= :endDate)
  --  )
  AND ot.locale = 'en_GB'
  AND (s.with_shared_allocations = TRUE AND s.type = 'PACKAGE')
  AND s.id = '114674'
GROUP BY saleid,
		 offerid,
		 offername,
		 currency,
		 allocationid,
		 allocationstart,
		 allocationend,
		 rate,
		 rackrate,
		 singlerate,
		 childrate,
		 infantrate,
		 minnumberofnights
;

------------------------------------------------------------------------------------------------------------------------

SELECT
	s.id                           AS saleid,
	o.id                           AS offerid,
	s.base_currency                AS currency,
	a.id                           AS allocationid,
	a.start_date                   AS allocationstart,
	a.end_date                     AS allocationend,
	s.with_shared_allocations,
	s.type,
	LISTAGG(DISTINCT d.code, ', ') AS airportcode,
	MIN(a.rate)                    AS rate
FROM latest_vault.cms_mysql.sale s
	INNER JOIN latest_vault.cms_mysql.offer o ON o.sale_id = s.id
	INNER JOIN latest_vault.cms_mysql.allocation a ON a.offer_id = o.id
	LEFT JOIN  latest_vault.cms_mysql.departure d ON d.id = a.departure_id
WHERE o.active = TRUE
  AND (s.with_shared_allocations = TRUE AND s.type = 'PACKAGE')
--   AND s.id = '114674'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
;


SELECT *
FROM data_vault_mvp.dwh.odm_base_allocation_and_rates obaar
WHERE obaar.se_sale_id = '114674'
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.allocation CLONE latest_vault.cms_mysql.allocation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.departure CLONE latest_vault.cms_mysql.departure
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.offer CLONE latest_vault.cms_mysql.offer
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale CLONE latest_vault.cms_mysql.sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.rates CLONE data_vault_mvp.fx.rates
;

CREATE SCHEMA hygiene_snapshot_vault_mvp_dev_robin.cms_reports
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_reports.sale_allocations CLONE hygiene_snapshot_vault_mvp.cms_reports.sale_allocations
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.odm_base_allocation_and_rates__model_data AS (
	SELECT
		sa.saleid                                                            AS se_sale_id,
		sa.offerid                                                           AS offer_id,
		sa.allocationid                                                      AS allocation_id,
		sa.allocationstart::DATE                                             AS allocation_start_date,
		sa.allocationend::DATE                                               AS allocation_end_date,
		DATEDIFF(DAYS, allocation_start_date, allocation_end_date)           AS allocation_duration_days,
		sa.minnumberofnights                                                 AS min_number_of_nights,
		DATEADD(DAY, min_number_of_nights - 1, allocation_start_date)        AS offer_required_allocation_end_date,
		sa.numberofrooms                                                     AS inventory_total,
		sa.available                                                         AS inventory_available,
		sa.booked                                                            AS inventory_reserved,
		sa.locked                                                            AS inventory_locked,
		sa.locked + sa.booked                                                AS total_reserved_rooms,
		sa.blackout                                                          AS inventory_force_blacked_out,

		sa.airportcode                                                       AS airport_code,
		--rate currency, the currency the rate is loaded in
		sa.currency,
		sa.rate                                                              AS rate_rc,
		sa.rackrate                                                          AS rack_rate_rc,
		sa.singlerate                                                        AS single_rate_rc,
		sa.childrate                                                         AS child_rate_rc,
		sa.infantrate                                                        AS infant_rate_rc,

		-- rates converted to gbp:
		IFF(currency = 'GBP', rate_rc, rate_rc * gbpr.fx_rate)               AS rate_gbp,
		IFF(currency = 'GBP', rack_rate_rc, rack_rate_rc * gbpr.fx_rate)     AS rack_rate_gbp,
		IFF(currency = 'GBP', single_rate_rc, single_rate_rc * gbpr.fx_rate) AS single_rate_gbp,
		IFF(currency = 'GBP', child_rate_rc, child_rate_rc * gbpr.fx_rate)   AS child_rate_gbp,
		IFF(currency = 'GBP', infant_rate_rc, infant_rate_rc * gbpr.fx_rate) AS infant_rate_gbp,
		IFF(currency = 'GBP', 1, gbpr.fx_rate)                               AS rc_to_gbp,

		--rates converted to eur:
		IFF(currency = 'EUR', rate, rate * eurr.fx_rate)                     AS rate_eur,
		IFF(currency = 'EUR', rack_rate_rc, rack_rate_rc * eurr.fx_rate)     AS rack_rate_eur,
		IFF(currency = 'EUR', single_rate_rc, single_rate_rc * eurr.fx_rate) AS single_rate_eur,
		IFF(currency = 'EUR', child_rate_rc, child_rate_rc * eurr.fx_rate)   AS child_rate_eur,
		IFF(currency = 'EUR', infant_rate_rc, infant_rate_rc * eurr.fx_rate) AS infant_rate_eur,
		IFF(currency = 'EUR', 1, eurr.fx_rate)                               AS rc_to_eur,

		(rack_rate_rc - rate_rc) / NULLIF(rack_rate_rc, 0)                   AS discount_percentage,

		-- for per night calculations, rate and rack_rate only
		rate_rc / allocation_duration_days                                   AS rate_per_night_rc,
		rack_rate_rc / allocation_duration_days                              AS rack_rate_per_night_rc,
		rate_gbp / allocation_duration_days                                  AS rate_per_night_gbp,
		rack_rate_gbp / allocation_duration_days                             AS rack_rate_per_night_gbp,
		rate_eur / allocation_duration_days                                  AS rate_per_night_eur,
		rack_rate_eur / allocation_duration_days                             AS rack_rate_per_night_eur,

		(rack_rate_per_night_rc - rate_per_night_rc) /
		NULLIF(rack_rate_per_night_rc, 0)                                    AS discount_percentage_per_night

	FROM hygiene_snapshot_vault_mvp.cms_reports.sale_allocations sa
		LEFT JOIN data_vault_mvp.fx.rates gbpr
				  ON sa.currency = gbpr.source_currency
					  AND gbpr.target_currency = 'GBP'
					  AND gbpr.fx_date = CURRENT_DATE
		LEFT JOIN data_vault_mvp.fx.rates eurr
				  ON sa.currency = eurr.source_currency
					  AND eurr.target_currency = 'EUR'
					  AND eurr.fx_date = CURRENT_DATE
;


SELECT
	rate_gbp
FROM data_vault_mvp.dwh.odm_base_allocation_and_rates obaar
WHERE obaar.se_sale_id = '114674'
;

SELECT
	rate_gbp
FROM data_vault_mvp_dev_robin.dwh.odm_base_allocation_and_rates obaar
WHERE obaar.se_sale_id = '114674' AND obaar.rate_gbp IS NOT NULL
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.odm_base_allocation_and_rates obaar
WHERE obaar.se_sale_id = '114674'
  AND obaar.allocation_start_date = '2023-06-24'
  AND obaar.allocation_end_date = '2023-07-03'
;

SELECT *
FROM data_vault_mvp.dwh.odm_base_allocation_and_rates obaar
WHERE obaar.se_sale_id = '114674'
  AND obaar.allocation_start_date = '2023-06-24'
  AND obaar.allocation_end_date = '2023-07-03'
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.odm_base_allocation_and_rates__step01__model_old_data_model_shared_allocation_rates a
WHERE a.saleid = 114674
  AND a.allocationstart = '2023-06-24'
  AND a.allocationend = '2023-07-03'
-- 	  AND a.allocationstart = CURRENT_DATE
;


SELECT
	obaar.allocation_start_date,
	obaar.allocation_end_date,
	obaar.rate_gbp
FROM data_vault_mvp_dev_robin.dwh.odm_base_allocation_and_rates obaar
	INNER JOIN se.data.dim_sale ds ON obaar.se_sale_id = ds.se_sale_id
WHERE ds.salesforce_opportunity_id = '0066900001cYFpl'
  AND obaar.rate_gbp IS NOT NULL
;


SELECT *
FROM latest_vault.cms_mysql.allocation a
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/allocation_and_rates/old_data_model/odm_base_allocation_and_rates.py'  --method 'run' --start '2023-06-12 00:00:00' --end '2023-06-12 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_offer CLONE data_vault_mvp.dwh.se_offer
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/allocation_and_rates/old_data_model/odm_offer_allocation_and_rates.py'  --method 'run' --start '2023-06-12 00:00:00' --end '2023-06-12 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/allocation_and_rates/old_data_model/odm_sale_allocation_and_rates.py'  --method 'run' --start '2023-06-12 00:00:00' --end '2023-06-12 00:00:00'

SELECT *
FROM data_vault_mvp.dwh.odm_sale_allocation_and_rates odmr
WHERE odmr.se_sale_id = '114674'
  AND odmr.lead_rate_gbp IS NOT NULL
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.odm_sale_allocation_and_rates odmr
WHERE odmr.se_sale_id = '114674'
  AND odmr.lead_rate_gbp IS NOT NULL
;



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.incoming_price_comparison CLONE data_vault_mvp.dwh.incoming_price_comparison
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.mari_offer_calendar_view CLONE data_vault_mvp.dwh.mari_offer_calendar_view
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.ratedock_offer_calendar_view CLONE data_vault_mvp.dwh.ratedock_offer_calendar_view
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.siteminder_offer_calendar_view CLONE data_vault_mvp.dwh.siteminder_offer_calendar_view
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.synxis_offer_calendar_view CLONE data_vault_mvp.dwh.synxis_offer_calendar_view
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/allocation_and_rates/harmonised_offer_calendar_view.py'  --method 'run' --start '2023-06-12 00:00:00' --end '2023-06-12 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.hotel_sale_offer CLONE latest_vault.cms_mysql.hotel_sale_offer
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.odm_sale_allocation_and_rates CLONE data_vault_mvp.dwh.odm_sale_allocation_and_rates
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.travelbird_offer_allocation_and_rates CLONE data_vault_mvp.dwh.travelbird_offer_allocation_and_rates
;



self_describing_task --include 'biapp/task_catalogue/dv/dwh/allocation_and_rates/harmonised_sale_calendar_view.py'  --method 'run' --start '2023-06-12 00:00:00' --end '2023-06-12 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.harmonised_offer_calendar_view hocv
WHERE hocv.salesforce_opportunity_id = '0066900001cYFpl' AND hocv.total_rate_gbp IS NOT NULL
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view hscv
WHERE hscv.se_sale_id = '114674' AND hscv.available_lead_rate_gbp IS NOT NULL
;

SELECT *
FROM data_vault_mvp.dwh.harmonised_sale_calendar_view hscv
WHERE hscv.se_sale_id = '114674' AND hscv.available_lead_rate_gbp IS NOT NULL
;

--back up for comparison
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view_20230613 CLONE data_vault_mvp.dwh.harmonised_sale_calendar_view
;

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view hscv
; -- 28955461
SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view_20230613 hscv
; -- 28955461

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view hscv
WHERE hscv.available_lead_rate_gbp IS NOT NULL
; -- 6038747
SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view_20230613 hscv
WHERE hscv.available_lead_rate_gbp IS NOT NULL
; -- 5476232

-- dev
SELECT
    ds.product_configuration,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view hscv
INNER JOIN se.data.dim_sale ds ON hscv.se_sale_id = ds.se_sale_id
WHERE hscv.available_lead_rate_gbp IS NOT NULL
GROUP BY 1
; -- 6038747

-- prod
SELECT
    ds.product_configuration,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view_20230613 hscv
INNER JOIN se.data.dim_sale ds ON hscv.se_sale_id = ds.se_sale_id
WHERE hscv.available_lead_rate_gbp IS NOT NULL
GROUP BY 1
; -- 5476232


SELECT
    ds.product_configuration,
	COUNT(*)
FROM data_vault_mvp.dwh.harmonised_sale_calendar_view hscv
INNER JOIN se.data.dim_sale ds ON hscv.se_sale_id = ds.se_sale_id
WHERE hscv.available_lead_rate_gbp IS NOT NULL
GROUP BY 1;

SELECT
    *
FROM data_vault_mvp.dwh.harmonised_sale_calendar_view hscv
INNER JOIN se.data.dim_sale ds ON hscv.se_sale_id = ds.se_sale_id
WHERE hscv.available_lead_rate_gbp IS NOT NULL
AND ds.salesforce_opportunity_id = '0066900001cYFpl';


SELECT
    *
FROM data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view_20230613 hscv
INNER JOIN se.data.dim_sale ds ON hscv.se_sale_id = ds.se_sale_id
WHERE hscv.available_lead_rate_gbp IS NOT NULL
AND ds.salesforce_opportunity_id = '0066900001cYFpl';
