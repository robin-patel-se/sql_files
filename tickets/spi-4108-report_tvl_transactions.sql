WITH
	incoming_data AS (
		SELECT
			fsm.date,
			ds.posa_territory,
			ds.product_type,
			ds.travel_type,
			fsm.member_spvs,
			fsm.trx,
			fsm.gross_revenue,
			fsm.gross_revenue_gbp_constant_currency,
			fsm.margin,
			fsm.margin_constant_currency
		FROM se.bi.fact_sale_metrics AS fsm
			INNER JOIN se.bi.dim_sale_territory AS ds
					   ON ds.se_sale_id = fsm.se_sale_id AND fsm.posa_territory = ds.posa_territory
		WHERE fsm.date BETWEEN '2018-12-29' AND (CURRENT_DATE - 1) AND
			  fsm.posa_territory IN
			  ('UK', 'Conde Nast UK', 'Guardian - UK', 'DE', 'CH', 'AT', 'BE', 'TB-BE_FR', 'TB-BE_NL', 'CZ', 'HU', 'FR',
			   'ES',
			   'US', 'HK', 'ID', 'MY', 'SG', 'IT', 'NL', 'TB-NL', 'DK', 'NO', 'SE', 'PL')
	),
	input_tvl AS (
		SELECT
			DATE_TRUNC('month', date)                    AS month_commencing,
			CASE
				WHEN ind.posa_territory IN ('UK', 'Conde Nast UK', 'Guardian - UK') THEN 'UK'
				WHEN ind.posa_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
				WHEN ind.posa_territory IN ('BE', 'TB-BE_FR', 'TB-BE_NL') THEN 'Belgium'
				-- when ind.posa_territory in ('CZ', 'HU') then 'CEE'
				-- when ind.posa_territory in ('FR', 'ES', 'US', 'HK', 'ID', 'MY', 'SG') then 'Other'
				WHEN ind.posa_territory IN ('IT') THEN 'Italy'
				WHEN ind.posa_territory IN ('NL', 'TB-NL') THEN 'Netherlands'
				WHEN ind.posa_territory IN ('DK', 'NO', 'SE') THEN 'Scandi'
				WHEN ind.posa_territory IN ('PL') THEN 'TVL'
				ELSE 'Not_identified'
			END                                          AS territory_plus_entities,
			CASE
				WHEN ind.product_type IN ('Day Experience', 'WRD', 'Package') THEN 'Package'
				WHEN ind.product_type IN ('Hotel') AND ind.travel_type IN ('International') THEN 'International_hotel'
				WHEN ind.product_type IN ('Hotel') AND ind.travel_type IN ('Domestic') THEN 'Domestic_hotel'
				ELSE 'unknown'
			END                                          AS product_type,
			ind.posa_territory,
			SUM(ind.member_spvs)                         AS spvs,
			SUM(ind.trx)                                 AS transactions,
			SUM(ind.gross_revenue)                       AS gross_revenue_local_currency,
			SUM(ind.gross_revenue_gbp_constant_currency) AS gross_revenue_constant_currency,
			SUM(ind.margin)                              AS net_revenue_local_currency,
			SUM(ind.margin_constant_currency)            AS net_revenue_constant_currency
		FROM incoming_data ind
		WHERE ind.date BETWEEN '2018-12-29' AND (CURRENT_DATE - 1) AND
			  ind.posa_territory IN
			  ('UK', 'Conde Nast UK', 'Guardian - UK', 'DE', 'CH', 'AT', 'BE', 'TB-BE_FR', 'TB-BE_NL', 'CZ', 'HU', 'FR',
			   'ES',
			   'US', 'HK', 'ID', 'MY', 'SG', 'IT', 'NL', 'TB-NL', 'DK', 'NO', 'SE', 'PL')
		GROUP BY 1, 2, 3, 4
	),
	aggregation AS (
		SELECT
			itvl.posa_territory,
			SUM(itvl.net_revenue_constant_currency)
		FROM input_tvl itvl
		WHERE itvl.month_commencing = '2023-08-01'
		GROUP BY 1
	)
SELECT *
FROM aggregation
;

-- matched fact sale metrics data with group rev report
-- group rev report for tvl shows: £589,124
-- query on fact sale metrics with territory PL = £589,123

-- next reconcile with transaction model
WITH
	incoming_data AS (
		SELECT *
		FROM se.data.fact_complete_booking fcb
		WHERE DATE_TRUNC(MONTH, fcb.booking_completed_date) = '2023-08-01'
		  AND fcb.territory = 'PL'
	)
SELECT
	ind.territory,
	SUM(ind.margin_gross_of_toms_gbp_constant_currency)
FROM incoming_data ind
GROUP BY 1
;

-- fact booking - £589,123
-- using above query to get a booking level break down of margin

SELECT
	fcb.booking_id,
	fcb.booking_completed_date,
	fcb.booking_status,
	fcb.booking_status_type,
	fcb.margin_gross_of_toms_gbp_constant_currency
FROM se.data.fact_complete_booking fcb
WHERE DATE_TRUNC(MONTH, fcb.booking_completed_date) = '2023-08-01'
  AND fcb.territory = 'PL'

------------------------------------------------------------------------------------------------------------------------

-- Radoslaw has come back with the following:

/*Hi Robin,
we have 2 types of differences, excel prepared by our analytics team is attached.
1.
5 reservations listed by you are canceled, so shouldn't be included in the summary:
BOOKING_ID
TL-22227576
TL-22229471
TL-22233625
TL-22240666
TL-22240992

2.
Fx rate - we have small differences on each reservation probably caused by different fx rate.
In the attached excel we compare all reservations from you (column F) and from our database (column g), where we calculate PLNs to GBPs using 5,45 fx rate. We can see small differences on every reservation so it looks like your fx rate fluctuates and is not constant.

Could you please have a look?
thanks,
Radek*/

-- 1. Investigating the status of these bookings

SELECT *
FROM se.data.tb_booking tb
WHERE tb.order_id IN (
					  22227576,
					  22229471,
					  22233625,
					  22240666,
					  22240992
	)
;

-- all now cancelled and cancelled yday so we can ignore these

-- 2. Investigating currency rate


SELECT
	fcb.booking_id,
	fcb.booking_completed_date,
	fcb.booking_status,
	fcb.booking_status_type,
	fcb.currency,
	fcb.margin_gross_of_toms_cc,
	fcb.margin_gross_of_toms_gbp,
	fcb.margin_gross_of_toms_gbp_constant_currency,
	fcb.margin_gross_of_toms_gbp_constant_currency - fcb.margin_gross_of_toms_gbp AS diff
FROM se.data.fact_complete_booking fcb
WHERE DATE_TRUNC(MONTH, fcb.booking_completed_date) = '2023-08-01'
  AND fcb.territory = 'PL'
ORDER BY diff DESC
;

-- Diff between margin gbp and margin gbp constant currency isn't very large

-- diff from TVL bookings is different to what we are seeing in dwh
-- https://docs.google.com/spreadsheets/d/1XE7_gSI9UixbrNRG4tJpTR_l8yRYe9jt/edit#gid=999698474
-- choosing one of the bookings with the biggest diff


SELECT *
FROM se.data.tb_order_item_changelog toic
WHERE toic.order_id = 22226865
;

SELECT
	tb.sold_price_currency,
	tb.sold_price_total_cc,
	tb.cost_price_total_cc,
	tb.margin_cc,
	tb.margin_cc / 5.45 AS margin_gbp,
	tb.sold_price_total_gbp,
	tb.cost_price_total_gbp,
	tb.margin_gbp
FROM se.data.tb_booking tb
WHERE tb.order_id = 22226865
;


SELECT
	toi.sold_price_currency,
	toi.sold_price_incl_vat,
	toi.cost_price_excl_vat_sold_currency,
	sold_price_incl_vat - cost_price_excl_vat_sold_currency                            AS margin,
	margin / 5.45                                                                      AS margin_gbp_calc,
	toi.sold_price_incl_vat / NULLIF(toi.sold_price_incl_vat_gbp, 0)                   AS gbp_rate,
	toi.sold_price_incl_vat_gbp,
	toi.cost_price_excl_vat_gbp,
	toi.sold_price_incl_vat_gbp - cost_price_excl_vat_gbp                              AS margin_gbp,
	toi.sold_price_incl_vat_gbp_constant_currency,
	toi.cost_price_excl_vat_gbp_constant_currency,
	toi.sold_price_incl_vat_gbp_constant_currency -
	toi.cost_price_excl_vat_gbp_constant_currency                                      AS margin_gbp_constant_currency,
	toi.sold_price_incl_vat / NULLIF(toi.sold_price_incl_vat_gbp_constant_currency, 0) AS constant_currency_gbp_rate
FROM se.data.tb_order_item toi
WHERE toi.order_id = 22226865
;

SELECT *
FROM data_vault_mvp.fx.tb_rates
WHERE usage_date = CURRENT_DATE AND source_currency = 'GBP' AND target_currency = 'PLN'
;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog CLONE data_vault_mvp.dwh.tb_order_item_changelog
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_20230823 CLONE data_vault_mvp_dev_robin.dwh.tb_order_item
;
-- original data
SELECT
	DATE_TRUNC(MONTH, toi.event_created_tstamp)                             AS month,
	SUM(toi.sold_price_incl_vat_gbp_constant_currency)                      AS total_sold_price_constant_currency,
	SUM(toi.cost_price_excl_vat_gbp_constant_currency)                      AS total_cost_price_constant_currency,
	total_sold_price_constant_currency - total_cost_price_constant_currency AS margin
FROM data_vault_mvp_dev_robin.dwh.tb_order_item_20230823 toi
GROUP BY 1
;
-- adjusted data
SELECT
	DATE_TRUNC(MONTH, toi.event_created_tstamp)                             AS month,
	SUM(toi.sold_price_incl_vat_gbp_constant_currency)                      AS total_sold_price_constant_currency,
	SUM(toi.cost_price_excl_vat_gbp_constant_currency)                      AS total_cost_price_constant_currency,
	total_sold_price_constant_currency - total_cost_price_constant_currency AS margin
FROM data_vault_mvp_dev_robin.dwh.tb_order_item toi
GROUP BY 1
;

-- prod
SELECT
	toi.sold_price_currency,
	toi.sold_price_incl_vat,
	toi.sold_price_incl_vat / 5.45                AS sold_price_calc,
	toi.sold_price_incl_vat_gbp_constant_currency,
	toi.cost_price_currency,
	toi.cost_price_excl_vat,
	toi.cost_price_excl_vat / 5.45                AS cost_price_calc,
	toi.cost_price_excl_vat_gbp_constant_currency,
	toi.sold_price_incl_vat_gbp_constant_currency -
	toi.cost_price_excl_vat_gbp_constant_currency AS margin_gbp_constant_currency
FROM data_vault_mvp_dev_robin.dwh.tb_order_item_20230823 toi
WHERE toi.order_id = 22226147
;

SELECT *
FROM se.data.tb_booking tb
WHERE tb.order_id = '22226147'

-- dev
SELECT
	toi.sold_price_currency,
	toi.sold_price_incl_vat,
	toi.sold_price_incl_vat / 5.45                AS sold_price_calc,
	toi.sold_price_incl_vat_gbp_constant_currency,
	toi.cost_price_currency,
	toi.cost_price_excl_vat,
	toi.cost_price_excl_vat / 5.45                AS cost_price_calc,
	toi.cost_price_excl_vat_gbp_constant_currency,
	toi.sold_price_incl_vat_gbp_constant_currency -
	toi.cost_price_excl_vat_gbp_constant_currency AS margin_gbp_constant_currency
FROM data_vault_mvp_dev_robin.dwh.tb_order_item toi
WHERE toi.order_id = 22226147
;

SELECT
	tb.sold_price_currency,
	tb.sold_price_total_cc,
	tb.sold_price_total_cc / 5.45 AS sold_price_total_gbp_calc,
	tb.sold_price_total_gbp,
	tb.sold_price_total_gbp_constant_currency,
	tb.cost_price_total_cc,
	tb.cost_price_total_cc / 5.45 AS cost_price_total_gbp_calc,
	tb.cost_price_total_gbp,
	tb.cost_price_total_gbp_constant_currency,
	tb.margin_gbp_constant_currency
FROM data_vault_mvp.dwh.tb_booking tb
WHERE tb.order_id = 22226147
;

------------------------------------------------------------------------------------------------------------------------
--running tb booking to check
CREATE SCHEMA data_vault_mvp_dev_robin.cms_mysql_snapshots
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.cms_mysql_snapshots.external_booking_snapshot AS
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.external_booking_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.external_booking CLONE latest_vault.cms_mysql.external_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.mari_reservation_information CLONE data_vault_mvp.dwh.mari_reservation_information
;
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item CLONE data_vault_mvp.dwh.tb_order_item;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog CLONE data_vault_mvp.dwh.tb_order_item_changelog
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_order CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderevent CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderevent
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderproperty CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderproperty
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderproperty CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderproperty
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_person CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_person
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.tb_rates CLONE data_vault_mvp.fx.tb_rates
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/tb_order_item.py'  --method 'run' --start '2023-08-23 00:00:00' --end '2023-08-23 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2023-08-23 00:00:00' --end '2023-08-23 00:00:00'

-- tb booking prod

SELECT
	DATE_TRUNC(MONTH, tb.created_at_dts) AS month,
	SUM(tb.margin_gbp_constant_currency)
FROM data_vault_mvp.dwh.tb_booking tb
WHERE UPPER(tb.payment_status) IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE') OR
	  (YEAR(tb.created_at_dts) = '2019'
		  AND tb.cancellation_date >= '2020-03-01'
		  AND UPPER(tb.payment_status) = 'CANCELLED')
GROUP BY 1
;

-- tb booking dev
SELECT
	DATE_TRUNC(MONTH, tb.created_at_dts) AS month,
	SUM(tb.margin_gbp_constant_currency)
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
WHERE UPPER(tb.payment_status) IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE') OR
	  (YEAR(tb.created_at_dts) = '2019'
		  AND tb.cancellation_date >= '2020-03-01'
		  AND UPPER(tb.payment_status) = 'CANCELLED')
GROUP BY 1
;


-- tb booking prod territory
SELECT
	DATE_TRUNC(MONTH, tb.created_at_dts) AS month,
	tb.territory,
	SUM(tb.margin_gbp_constant_currency)
FROM data_vault_mvp.dwh.tb_booking tb
WHERE UPPER(tb.payment_status) IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE') OR
	  (YEAR(tb.created_at_dts) = '2019'
		  AND tb.cancellation_date >= '2020-03-01'
		  AND UPPER(tb.payment_status) = 'CANCELLED')
GROUP BY 1, 2
;

-- tb booking dev territory
SELECT
	DATE_TRUNC(MONTH, tb.created_at_dts) AS month,
	tb.territory,
	SUM(tb.margin_gbp_constant_currency)
FROM collab.muse.tb_booking_robin_dev tb
WHERE UPPER(tb.payment_status) IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE') OR
	  (YEAR(tb.created_at_dts) = '2019'
		  AND tb.cancellation_date >= '2020-03-01'
		  AND UPPER(tb.payment_status) = 'CANCELLED')
GROUP BY 1, 2
;


SELECT
	dev_tb.booking_id,
	dev_tb.territory,
	dev_tb.sold_price_currency,
	dev_tb.margin_gbp_constant_currency  AS dev_margin_gbp_constant_currency,
	prod_tb.margin_gbp_constant_currency AS prod_margin_gbp_constant_currency
FROM collab.muse.tb_booking_robin_dev dev_tb
	LEFT JOIN data_vault_mvp.dwh.tb_booking prod_tb ON dev_tb.booking_id = prod_tb.booking_id
-- filter for 'live' bookings
WHERE UPPER(dev_tb.payment_status) IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE') OR
	  (YEAR(dev_tb.created_at_dts) = '2019'
		  AND dev_tb.cancellation_date >= '2020-03-01'
		  AND UPPER(dev_tb.payment_status) = 'CANCELLED')
		  AND DATE_TRUNC(MONTH, dev_tb.created_at_dts) = '2023-07-01'
ORDER BY dev_margin_gbp_constant_currency - prod_margin_gbp_constant_currency
;

-- booking order item comp
SELECT
	prod.sold_price_currency,
	prod.sold_price_incl_vat,
	prod.sold_price_incl_vat_gbp_constant_currency,
	dev.sold_price_incl_vat_gbp_constant_currency,

	prod.cost_price_currency,
	prod.cost_price_excl_vat,
	prod.cost_price_excl_vat_gbp_constant_currency,
	dev.cost_price_excl_vat_gbp_constant_currency,
	prod.sold_price_incl_vat_gbp_constant_currency -
	prod.cost_price_excl_vat_gbp_constant_currency AS margin_gbp_constant_currency
FROM data_vault_mvp.dwh.tb_order_item prod
	LEFT JOIN data_vault_mvp_dev_robin.dwh.tb_order_item dev ON prod.order_item_id = dev.order_item_id
WHERE prod.order_id = 21916716
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_order_item dev
WHERE dev.cost_price_excl_vat_gbp_constant_currency IS NULL
;


CREATE OR REPLACE VIEW collab.muse.tb_booking_robin_dev AS
SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_booking
;

GRANT SELECT ON TABLE collab.muse.tb_booking_robin_dev TO ROLE personal_role__gianniraftis
;

------------------------------------------------------------------------------------------------------------------------

WITH
	input_data AS (
		SELECT
			dev_tb.booking_id,
			dev_tb.created_at_dts,
			dev_tb.territory,
			ds.posu_country,
			ds.posu_cluster_sub_region,
			dev_tb.sold_price_currency,
			dev_tb.sold_price_total_gbp_constant_currency,
			prod_tb.sold_price_total_gbp_constant_currency,
			dev_tb.cost_price_total_gbp_constant_currency,
			prod_tb.cost_price_total_gbp_constant_currency,
			dev_tb.margin_gbp_constant_currency  AS dev_margin_gbp_constant_currency,
			prod_tb.margin_gbp_constant_currency AS prod_margin_gbp_constant_currency
		FROM collab.muse.tb_booking_robin_dev dev_tb
			LEFT JOIN data_vault_mvp.dwh.tb_booking prod_tb ON dev_tb.booking_id = prod_tb.booking_id
			LEFT JOIN data_vault_mvp.dwh.dim_sale ds ON dev_tb.se_sale_id = ds.se_sale_id
-- filter for 'live' bookings
		WHERE (UPPER(dev_tb.payment_status) IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE') OR
			   (YEAR(dev_tb.created_at_dts) = '2019'
				   AND dev_tb.cancellation_date >= '2020-03-01'
				   AND UPPER(dev_tb.payment_status) = 'CANCELLED')
				   AND DATE_TRUNC(MONTH, dev_tb.created_at_dts) = '2023-07-01')
		  AND dev_tb.territory IS DISTINCT FROM 'PL'

	)
SELECT
	DATE_TRUNC(MONTH, ind.created_at_dts)      AS month,
	ind.posu_cluster_sub_region,
	SUM(ind.dev_margin_gbp_constant_currency)  AS dev_margin_gbp_constant_currency,
	SUM(ind.prod_margin_gbp_constant_currency) AS prod_margin_gbp_constant_currency
FROM input_data ind
-- ORDER BY dev_margin_gbp_constant_currency - prod_margin_gbp_constant_currency
GROUP BY 1, 2
;
------------------------------------------------------------------------------------------------------------------------


SELECT
	COUNT(DISTINCT dim_sale.salesforce_opportunity_id)
FROM se.data.dim_sale
WHERE sale_active AND tech_platform = 'TRAVELBIRD'
;

-- request from Christie:
-- Add new view that is global sale id by month by posa - with only net in month cancellations

WITH
	input_data AS (
		SELECT
			dev_tb.booking_id,
			dev_tb.created_at_dts,
			dev_tb.territory,
			fb.booking_status_type,
			fb.cancellation_date,
			ds.salesforce_opportunity_id,
			ds.posu_country,
			ds.posu_cluster_sub_region,
			dev_tb.sold_price_currency,
			dev_tb.sold_price_total_gbp_constant_currency,
			prod_tb.sold_price_total_gbp_constant_currency,
			dev_tb.cost_price_total_gbp_constant_currency,
			prod_tb.cost_price_total_gbp_constant_currency,
			dev_tb.margin_gbp_constant_currency  AS dev_margin_gbp_constant_currency,
			prod_tb.margin_gbp_constant_currency AS prod_margin_gbp_constant_currency
		FROM collab.muse.tb_booking_robin_dev dev_tb
			LEFT JOIN data_vault_mvp.dwh.tb_booking prod_tb ON dev_tb.booking_id = prod_tb.booking_id
			LEFT JOIN data_vault_mvp.dwh.dim_sale ds ON dev_tb.se_sale_id = ds.se_sale_id
			LEFT JOIN data_vault_mvp.dwh.fact_booking fb ON dev_tb.booking_id = fb.booking_id
			-- filter for 'live' bookings or bookings that are cancelled but cancelled outside of the month of booking
		WHERE (fb.booking_status_type = 'live'
			OR (
						   fb.booking_status_type = 'cancelled'
					   AND
						   DATE_TRUNC(MONTH, fb.booking_completed_date) < DATE_TRUNC(MONTH, fb.cancellation_date)
				   ))
		  AND dev_tb.territory IS DISTINCT FROM 'PL'

	)
SELECT
	DATE_TRUNC(MONTH, ind.created_at_dts)      AS month,
	ind.salesforce_opportunity_id,
	ind.territory,
	SUM(ind.dev_margin_gbp_constant_currency)  AS dev_margin_gbp_constant_currency,
	SUM(ind.prod_margin_gbp_constant_currency) AS prod_margin_gbp_constant_currency
FROM input_data ind
GROUP BY 1, 2, 3
;


SELECT *
FROM data_vault_mvp.dwh.tb_order_item toi
;

SELECT *
FROM data_vault_mvp.dwh.tb_booking tb
;

------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.tb_order_item_20231002 CLONE data_vault_mvp.dwh.tb_order_item
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.tb_booking_20231002 CLONE data_vault_mvp.dwh.tb_booking
;


-- new
SELECT
	YEAR(order_item_created_tstamp)                                                               AS year,
	COUNT(*)                                                                                      AS order_items,
	SUM(tboi.cost_price_excl_vat_gbp_constant_currency)                                           AS sum_cost_price_excl_vat_gbp_constant_currency,
	SUM(tboi.sold_price_incl_vat_gbp_constant_currency)                                           AS sum_sold_price_incl_vat_gbp_constant_currency,
	sum_sold_price_incl_vat_gbp_constant_currency - sum_cost_price_excl_vat_gbp_constant_currency AS margin
FROM data_vault_mvp.dwh.tb_order_item tboi
GROUP BY 1
;


-- old
SELECT
	YEAR(order_item_created_tstamp)                                                               AS year,
	COUNT(*)                                                                                      AS order_items,
	SUM(tboi.cost_price_excl_vat_gbp_constant_currency)                                           AS sum_cost_price_excl_vat_gbp_constant_currency,
	SUM(tboi.sold_price_incl_vat_gbp_constant_currency)                                           AS sum_sold_price_incl_vat_gbp_constant_currency,
	sum_sold_price_incl_vat_gbp_constant_currency - sum_cost_price_excl_vat_gbp_constant_currency AS margin
FROM data_vault_mvp.dwh.tb_order_item_20231002 tboi
GROUP BY 1
;

-- new
SELECT
	YEAR(tb.created_at_dts) AS year,
	SUM(tb.sold_price_total_gbp_constant_currency),
	SUM(tb.cost_price_total_gbp_constant_currency),
	SUM(tb.margin_gbp_constant_currency)
FROM data_vault_mvp.dwh.tb_booking tb
WHERE tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE')
  AND tb.territory IS DISTINCT FROM 'PL'
GROUP BY 1
;

-- old
SELECT
	YEAR(tb.created_at_dts) AS year,
	SUM(tb.sold_price_total_gbp_constant_currency),
	SUM(tb.cost_price_total_gbp_constant_currency),
	SUM(tb.margin_gbp_constant_currency)
FROM data_vault_mvp.dwh.tb_booking_20231002 tb
WHERE tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE')
  AND tb.territory IS DISTINCT FROM 'PL'
GROUP BY 1
;
;


-- new
SELECT
	YEAR(tb.created_at_dts) AS year,
	SUM(tb.sold_price_total_gbp_constant_currency),
	SUM(tb.cost_price_total_gbp_constant_currency),
	SUM(tb.margin_gbp_constant_currency)
FROM data_vault_mvp.dwh.tb_booking tb
WHERE tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE')
  AND tb.territory = 'PL'
GROUP BY 1
;

-- old
SELECT
	YEAR(tb.created_at_dts) AS year,
	SUM(tb.sold_price_total_gbp_constant_currency),
	SUM(tb.cost_price_total_gbp_constant_currency),
	SUM(tb.margin_gbp_constant_currency)
FROM data_vault_mvp.dwh.tb_booking_20231002 tb
WHERE tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE')
  AND tb.territory = 'PL'
GROUP BY 1
;
;

SELECT *
FROM data_vault_mvp.bi.event_grain eg
;

USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.event_grain_20231002 CLONE data_vault_mvp.bi.event_grain
;

DROP TABLE data_vault_mvp.bi.event_grain;

SELECT * FROM SE.DATA.TB_BOOKING;

SELECT * FROM data_vault_mvp.dwh.booking_cancellation;
