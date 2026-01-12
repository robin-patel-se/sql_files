SELECT *
FROM data_vault_mvp.finance.netsuite_bill_line nbl
WHERE nbl.booking_id = 'A19911166'
;


SELECT *
FROM data_vault_mvp.finance.netsuite_billing_harmonised_components nbhc
WHERE nbhc.booking_id = 'A19911166'



SELECT *
FROM data_vault_mvp.finance.netsuite_tnt_billing_harmonised_components_base ntbhcb
WHERE ntbhcb.booking_id = 'A19911166'


SELECT *
FROM data_vault_mvp.finance.netsuite_billing_harmonised_components_derived nbhcd
WHERE nbhcd.booking_id = 'A19911166'
;


SELECT *
FROM se.data.tb_order_item toi
WHERE toi.booking_id = 'TB-22450674'
;

SELECT *
FROM data_vault_mvp.finance.netsuite_billing_harmonised_components nbhc
WHERE nbhc.booking_id = 'TB-22450674'
;


SELECT *
FROM data_vault_mvp.finance.netsuite_tnt_billing_harmonised_components_base ntbhcb
WHERE ntbhcb.booking_id = 'TB-22450674'
;


SELECT *
FROM data_vault_mvp.dwh.flightservice__order_orderchange foo
WHERE
;


SELECT *
FROM se.data.tb_order_item toi
WHERE toi.booking_id = 'TB-22450674'
;


SELECT *
FROM data_vault_mvp.dwh.flightservice__order_orderchange foo
WHERE foo.client_order_reference LIKE '%22450674%'



SELECT *
FROM data_vault_mvp.finance.netsuite_tnt_billing_harmonised_components ntbhc
WHERE ntbhc.booking_id = 'TB-22450674'

SELECT *
FROM data_vault_mvp.finance.netsuite_tnt_billing_harmonised_components_base ntbhcb
WHERE ntbhcb.booking_id = 'TB-22450674'
;



WITH
	vcc AS (
		SELECT
			oitems.orderitembase_id AS order_item_id,
			suvc.external_reference AS vcc_reference
		FROM latest_vault.travelbird_mysql.integrations_bookingtransaction bt
			INNER JOIN latest_vault.travelbird_mysql.integrations_suvc suvc ON suvc.booking_transaction_id = bt.id
			INNER JOIN latest_vault.travelbird_mysql.orders_orderitemintegration ointegration
					   ON ointegration.booking_transaction_id = bt.id
			INNER JOIN latest_vault.travelbird_mysql.orders_orderitemintegration_order_items oitems
					   ON oitems.orderitemintegration_id = ointegration.id
		WHERE suvc.external_reference IS NOT NULL
	)
SELECT
	b.product_type,
	b.booking_id,
	tbi.partner_id::varchar                                                   AS vendor_id,
	tbi.order_item_id || '-' || b.booking_id                                  AS order_item_id,
	tbi.order_item_type,
	tbi.event_created_tstamp::date                                            AS component_created_dt,
	tbi.event_created_tstamp                                                  AS component_created_tstamp,
	CASE
		WHEN tbi.order_item_type IN ('CAR', 'CAR_EXTRA') THEN tbi.commission_sold_price_incl_vat_in_cost_currency
		ELSE tbi.cost_price_excl_vat
	END                                                                       AS amount,
	tbi.cost_price_currency                                                   AS currency,
	CASE
		WHEN tbi.order_item_type IN ('CAR', 'CAR_EXTRA') THEN tbi.commission_sold_price_incl_vat_gbp
		ELSE tbi.cost_price_excl_vat_gbp
	END                                                                       AS amount_gbp,
	COALESCE(tbi.start_date, b.check_in_date)                                 AS amort_start_dt,
	COALESCE(tbi.end_date, b.check_out_date, tbi.start_date, b.check_in_date) AS amort_end_dt,
	tbi.supplier_reference                                                    AS supplier_reference,
	b.company_region,
	b.territory,
	b.check_in_date                                                           AS booking_itinerary_start_date,
	b.check_out_date                                                          AS booking_itinerary_end_date,
	vcc.vcc_reference
FROM data_vault_mvp.finance.netsuite_tnt_booking b
	INNER JOIN data_vault_mvp.dwh.tb_order_item_changelog tbi ON tbi.booking_id = b.booking_id
	LEFT JOIN  vcc ON tbi.order_item_id = vcc.order_item_id
WHERE b.product_type = 'Catalogue'
  AND tbi.cost_price_excl_vat >= 0 --negative values are anomalies
  AND tbi.order_item_type NOT IN (
								  'INCLUDED_SERVICE',
								  'ROUNDING',
								  'ABSTRACT_INVOICE_ITEM',
								  'TOUR_INCLUDED_SERVICE',
								  'CORRECTION',
								  'BOOKING_FEE'
	)
  AND NOT se.finance.is_flight_component(tbi.order_item_type)
  AND b.booking_id = 'TB-22450674'
;



WITH
	flight AS (
		SELECT
			supplier_name,
			supplier_id,
			order_id,
			created_at_dts,
			new_departure_datetime,
			new_return_datetime,
			supplier_reference,
			client_order_reference,
			supplier_amount,
			supplier_currency,
			supplier_amount_gbp,
			component_status
		FROM data_vault_mvp.dwh.flightservice__order_orderchange
		--Remove duplicate amount rows
		QUALIFY ROW_NUMBER() OVER (
			PARTITION BY
				order_id,
				supplier_name,
				supplier_id,
				created_at_dts::DATE,
				supplier_amount,
				supplier_currency,
				supplier_amount_gbp,
				component_status
			ORDER BY created_at_dts DESC) = 1
	)
SELECT
	b.product_type,
	b.booking_id,
	COALESCE(flightservice_lookup.vendor_id, 'UNKNOWN')                                                                                                           AS vendor_id,
	flight.order_id || '-' || b.booking_id                                                                                                                        AS order_item_id,
	'FLIGHT'                                                                                                                                                      AS order_item_type,
	flight.created_at_dts::date                                                                                                                                   AS component_created_dt,
	flight.created_at_dts                                                                                                                                         AS component_created_tstamp,
	SUM(flight.supplier_amount)
		OVER (PARTITION BY b.booking_id, flight.order_id, flight.supplier_id ORDER BY flight.created_at_dts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS amount,
	flight.supplier_currency                                                                                                                                      AS currency,
	SUM(flight.supplier_amount_gbp)
		OVER (PARTITION BY b.booking_id, flight.order_id, flight.supplier_id ORDER BY flight.created_at_dts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS amount_gbp,
	flight.new_departure_datetime::date                                                                                                                           AS amort_start_dt,
	flight.new_return_datetime::date                                                                                                                              AS amort_end_dt,
	flight.supplier_reference                                                                                                                                     AS supplier_reference,
	b.company_region,
	b.territory,
	b.check_in_date                                                                                                                                               AS booking_itinerary_start_date,
	b.check_out_date                                                                                                                                              AS booking_itinerary_end_date,
	PARSE_JSON(iev.response_data)['VNettTransactionID']                                                                                                           AS vcc_reference
FROM data_vault_mvp.finance.netsuite_tnt_booking b
	INNER JOIN flight ON
	LEFT(flight.client_order_reference, 1) <> 'A'
		AND 'TB-' || SPLIT_PART(flight.client_order_reference, '-', -1) = b.booking_id
	LEFT JOIN  latest_vault.finance_gsheets.netsuite_flight_supplier_lookup flightservice_lookup ON
	flight.supplier_name = flightservice_lookup.supplier_name
	LEFT JOIN  latest_vault.flightservice_mysql.integrations_bookingtransaction ibt ON
	flight.client_order_reference = ibt.client_reference
		--Avoid duplicate joins
		AND flight.supplier_reference = TRY_PARSE_JSON(ibt.external_reference)['system']::varchar
	LEFT JOIN  latest_vault.flightservice_mysql.integrations_enettvan iev ON ibt.id = iev.booking_transaction_id
WHERE b.product_type = 'Catalogue'
  AND flight.component_status <> 'ERROR'
  AND b.booking_id = 'TB-22450674'
QUALIFY amount > 0 -- negative values are anomolies
;

SELECT *
FROM data_vault_mvp.finance.netsuite_tnt_bill_line bl
WHERE bl.booking_id = 'TB-22450674'
;


SELECT *
FROM data_vault_mvp.finance.netsuite_tnt_invoice_line il
WHERE il.booking_id = 'TB-22450674'
;


SELECT *
FROM se.data.se_booking sb
WHERE DATE_TRUNC(MONTH, sb.booking_completed_date) = '2024-06-01'
  AND sb.sale_dimension = 'HotelPlus'
;


SELECT
	components.product_type,
	components.booking_id,
	NULL                                                     AS vendor_id,
	components.order_item_id || '-' || 'INTERCOMPANY_FLIGHT' AS order_item_id,
	'INTERCOMPANY_FLIGHT'                                    AS order_item_type,
	components.component_created_dt,
	components.amount,
	components.currency,
	components.amount_gbp,
	components.amort_start_dt,
	components.amort_end_dt,
	components.supplier_reference,
	components.company_region,
	components.territory,
	components.booking_itinerary_start_date,
	components.booking_itinerary_end_date,
	NULL                                                     AS vcc_reference,
	se.finance.is_flight_component(components.order_item_type),
	components.order_item_type
FROM data_vault_mvp.finance.netsuite_tnt_billing_harmonised_components_base components
WHERE se.finance.is_flight_component(components.order_item_type)
  AND components.product_type IN ('Catalogue', 'HotelPlus')
  AND components.booking_id = 'TB-22450674'
;

SELECT *
FROM se.data.tb_booking tb
WHERE tb.created_at_dts::DATE = '2024-05-01'
;



SELECT
	components.product_type,
	components.booking_id,
	NULL                                                     AS vendor_id,
	components.order_item_id || '-' || 'INTERCOMPANY_FLIGHT' AS order_item_id,
	'INTERCOMPANY_FLIGHT'                                    AS order_item_type,
	components.component_created_dt,
	components.amount,
	components.currency,
	components.amount_gbp,
	components.amort_start_dt,
	components.amort_end_dt,
	components.supplier_reference,
	components.company_region,
	components.territory,
	components.booking_itinerary_start_date,
	components.booking_itinerary_end_date,
	NULL                                                     AS vcc_reference,
	se.finance.is_flight_component(components.order_item_type),
	components.order_item_type
FROM data_vault_mvp.finance.netsuite_tnt_billing_harmonised_components_base components
WHERE se.finance.is_flight_component(components.order_item_type)
  AND components.product_type IN ('Catalogue', 'HotelPlus')
  AND components.booking_id = 'TB-22398139'
;

SELECT *
FROM data_vault_mvp.finance.netsuite_bill_line nbl
WHERE nbl.booking_id = 'TB-22398139'


------------------------------------------------------------------------------------------------------------------------

WITH
	de_iata_scope AS (
		SELECT DISTINCT
			bl.booking_id,
			bl.vendor_external_id,
			'DE IATA BOOKING' AS de_iata_flag
		FROM se.finance.netsuite_tnt_bill_line bl
		WHERE bl.netsuite_record_type = 'BILL'
		  AND bl.vendor_external_id IN ('1822943', '1823208', '1823256', '1823206', '1823266')
	)
SELECT
	dis.de_iata_flag,
	book.transaction_id,
	TO_VARCHAR(book.booking_created_dt, 'dd/mm/yyyy') AS booking_created_dt,
	bl.product_type,
	bl.booking_id,
	TO_VARCHAR(bl.component_created_dt, 'dd/mm/yyyy') AS component_created_dt,
	TO_VARCHAR(bl.row_created_at, 'dd/mm/yyyy')       AS row_created_at,
	bl.amount                                         AS bill_amount,
	bl.currency,
	TO_VARCHAR(bl.amort_start_dt, 'dd/mm/yyyy')       AS amort_start_dt,
	TO_VARCHAR(bl.amort_end_dt, 'dd/mm/yyyy')         AS amort_end_dt,
	bl.supplier_reference,
	bl.netsuite_record_type,
	bl.company_region,
	bl.territory,
	bl.vendor_external_id,
	CASE
		WHEN (dis.de_iata_flag = 'DE IATA BOOKING' AND
			  bl.vendor_external_id IN ('1822943', '1823208', '1823256', '1823206', '1823266') AND
			  bl.product_type = 'Catalogue') THEN 'Flight - Catalogue GmbH w/o amortisation'
		WHEN (dis.de_iata_flag = 'DE IATA BOOKING' AND
			  bl.vendor_external_id IN ('1822943', '1823208', '1823256', '1823206', '1823266') AND
			  bl.product_type = 'HotelPlus') THEN 'Hotel Plus Flight Cost GmbH w/o amortisation'
		ELSE bl.ns_item_name
	END                                               AS ns_item_name,
	CASE
		WHEN (dis.de_iata_flag = 'DE IATA BOOKING' AND
			  bl.vendor_external_id IN ('1822943', '1823208', '1823256', '1823206', '1823266') AND
			  bl.product_type = 'Catalogue') THEN '2255'
		WHEN (dis.de_iata_flag = 'DE IATA BOOKING' AND
			  bl.vendor_external_id IN ('1822943', '1823208', '1823256', '1823206', '1823266') AND
			  bl.product_type = 'HotelPlus') THEN '2342'
		ELSE bl.ns_item_internal_id
	END                                               AS ns_item_internal_id,
	bl.bill_external_id,
	bl.cost_centre,
	bl.budget_holder,
	bl.vcc_reference
FROM se.finance.netsuite_tnt_bill_line bl
	INNER JOIN se.finance.netsuite_tnt_booking book ON bl.booking_id = book.booking_id
	LEFT JOIN  de_iata_scope dis ON dis.booking_id = bl.booking_id
WHERE netsuite_record_type = 'BILL'

  AND bl.booking_id = 'TB-22450674'
ORDER BY book.booking_id, component_created_dt, order_item_type
;

------------------------------------------------------------------------------------------------------------------------


WITH
	de_iata_scope AS (
		SELECT DISTINCT
			bl.booking_id,
			bl.vendor_external_id,
			'DE IATA BOOKING' AS de_iata_flag
		FROM se.finance.netsuite_bill_line bl
		WHERE bl.netsuite_record_type = 'BILL'
		  AND bl.vendor_external_id IN ('1822943', '1823208', '1823256', '1823206', '1823266')
	)
SELECT
	de_iata_flag,
	book.transaction_id,
	TO_VARCHAR(book.booking_created_dt, 'dd/mm/yyyy') AS booking_created_dt,
	il.product_type,
	il.booking_id,
	TO_VARCHAR(il.component_created_dt, 'dd/mm/yyyy') AS component_created_dt,
	TO_VARCHAR(il.row_created_at, 'dd/mm/yyyy')       AS row_created_at,
	il.amount                                         AS invoice_amount,
	il.currency,
	TO_VARCHAR(il.revenue_start_dt, 'dd/mm/yyyy')     AS revenue_start_dt,
	TO_VARCHAR(il.revenue_end_dt, 'dd/mm/yyyy')       AS revenue_end_dt,
	il.supplier_reference,
	il.netsuite_record_type,
	il.company_region,
	il.territory,
	il.vat,
	il.vendor_external_id,
	il.tax_code,
	il.customer_external_id,
	il.ns_item_name,
	il.ns_item_internal_id,
	il.invoice_external_id,
	il.cost_centre,
	il.budget_holder,
	il.promo_code_amount,
	il.discount_item,
	il.vcc_reference
FROM se.finance.netsuite_tnt_invoice_line il
	INNER JOIN se.finance.netsuite_tnt_booking book ON il.booking_id = book.booking_id
	LEFT JOIN  de_iata_scope dis ON dis.booking_id = il.booking_id
WHERE netsuite_record_type = 'INVOICE' AND il.booking_id = 'TB-22450674'
ORDER BY il.booking_id, component_created_dt, order_item_type
;