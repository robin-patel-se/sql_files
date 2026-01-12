SELECT
	transaction_id,
	booking_status,
	booking_status_type,
	booking_completed_date,
	check_in_date,
	check_out_date,
	no_nights,
	customer_name,
	email,
	territory,
	sale_dimension_type,
	product_configuration,
	tech_platform,
	sale_closest_airport_code,
	departure_airport_code,
	flight_carrier,
	flight_supplier_reference,
	company_name,
	supplier_name,
	posu_country,
	posu_division,
	posu_city,
	se_sale_id,
	adult_guests,
	child_guests,
	infant_guests,
	currency,
	customer_total_price_gbp,
	customer_total_price_cc,
	commission_ex_vat_gbp,
	flight_buy_rate_gbp,
	margin_segment
FROM collab.covid_pii.dflo_view_booking_summary
WHERE check_in_date >= '2023-11-01'
  AND check_in_date < '2023-11-30'
;

SELECT GET_DDL('table', 'collab.covid_pii.dflo_view_booking_summary')
;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW dflo_view_booking_summary
			(
			 se_sale_id,
			 transaction_id,
			 booking_status,
			 booking_status_type,
			 adult_guests,
			 child_guests,
			 infant_guests,
			 customer_name,
			 first_name,
			 last_name,
			 email,
			 membership_account_status,
			 booking_completed_date,
			 original_check_in_date,
			 original_check_out_date,
			 check_in_date,
			 check_out_date,
			 no_nights,
			 rooms,
			 territory,
			 currency,
			 customer_total_price_gbp,
			 customer_total_price_cc,
			 commission_ex_vat_gbp,
			 company_name,
			 supplier_name,
			 posu_country,
			 posu_division,
			 posu_city,
			 product_configuration,
			 has_flights,
			 sale_dimension_type,
			 flight_buy_rate_gbp,
			 flight_carrier,
			 flight_supplier_reference,
			 sale_closest_airport_code,
			 departure_airport_code,
			 margin_segment,
			 booking_id,
			 voucher_stay_by_date,
			 sf_case_thread_id,
			 sf_case_number,
			 sf_case_id,
			 sf_case_name,
			 sf_case_overview_id,
			 sf_priority_type,
			 sf_priority,
			 sf_status,
			 sf_contact_reason,
			 sf_case_owner_full_name,
			 sf_view,
			 weekly_summary_receiver_emails,
			 customer_support_email,
			 se_api_url,
			 shiro_user_id,
			 tech_platform
				)
AS
WITH
	tb_airline_details AS (
		SELECT
			toi.order_id,
			LISTAGG(COALESCE(se.data.airline_name_from_iata_code(toi.flight_validating_airline_id),
							 toi.flight_validating_airline_id), ', ') AS flight_carrier
		FROM data_vault_mvp.dwh.tb_order_item toi
		GROUP BY 1
	),
	customer_support_email_order_item AS (
		-- obtain the customer support email at order item level from tracy tables
		SELECT
			toi.booking_id,
			toi.order_item_id,
			toi.order_item_type,
			toi.partner_name,
			pp.email                      AS partner_default_email,
			ppe.email                     AS partner_customer_service_email,
			ppe.email_type,
			COALESCE(ppe.email, pp.email) AS customer_support_email
		FROM data_vault_mvp.dwh.tb_order_item toi
			INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order oo ON toi.order_id = oo.id
			INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.partners_partner pp ON toi.partner_id = pp.id
			LEFT JOIN  latest_vault.travelbird_mysql.partners_partneremail ppe
					   ON pp.id = ppe.partner_id AND ppe.email_type = 'Customer Service'
		WHERE oo.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
		  AND toi.order_item_type IN
			  (
			   'ACCOMMODATION',
			   'LEISURE',
			   'TRANSFER',
			   'TOUR'
				  )
	),
	distinct_partner_details AS (
		-- remove duplicates amongst an order
		SELECT DISTINCT
			cseoi.booking_id,
			REPLACE(cseoi.order_item_type, '_', ' ') AS order_item_type,
			cseoi.partner_name,
			cseoi.customer_support_email
		FROM customer_support_email_order_item cseoi
	),
	aggregate_tb_partner_emails AS (
		-- aggregate up to order level with new line breaks
		SELECT
			dpd.booking_id,
			LISTAGG(INITCAP(dpd.order_item_type) || ': ' || dpd.partner_name || ' - Email: ' ||
					dpd.customer_support_email, ' | ') AS customer_support_email
		FROM distinct_partner_details dpd
		GROUP BY 1
	),
	stack AS (
		SELECT
			sb.sale_id                         AS se_sale_id,
			sb.transaction_id,
			sb.booking_status,
			CASE
				WHEN sb.booking_status = 'COMPLETE' THEN 'live'
				WHEN (YEAR(sb.booking_completed_date) = '2019'
					AND sb.cancellation_date >= '2020-03-01'
					AND sb.booking_status IN ('CANCELLED', 'REFUNDED')) THEN 'live'
				WHEN sb.booking_status IN ('CANCELLED', 'REFUNDED') THEN 'cancelled'
				WHEN sb.booking_status = 'ABANDONED' THEN 'abandoned'
				ELSE 'other'
			END                                AS booking_status_type,
			sb.adult_guests,
			sb.child_guests,
			sb.infant_guests,
			sb.booking_completed_date,
			sb.original_check_in_date,
			sb.original_check_out_date,
			sb.check_in_date,
			sb.check_out_date,
			sb.no_nights,
			sb.rooms,
			sb.territory,
			sb.currency,
			sb.customer_total_price_gbp,
			sb.customer_total_price_cc,
			sb.commission_ex_vat_gbp,
			sb.supplier_name,
			sb.has_flights,
			CASE
				WHEN LOWER(sb.sale_dimension) = 'hotel' THEN 'Hotel'
				WHEN LOWER(sb.sale_dimension) = 'hotelplus' AND LOWER(sb.has_flights) = FALSE THEN 'Hotel'
				WHEN LOWER(sb.sale_dimension) = 'ihp - static' AND LOWER(sb.supplier_name) NOT LIKE ('secret escapes%')
					THEN 'Third Party Package'
				WHEN LOWER(sb.sale_dimension) LIKE 'ihp%' THEN 'IHP - Packages'
				ELSE sb.sale_dimension
			END                                AS sale_dimension_type,
			sb.flight_buy_rate_gbp,
			sb.flight_carrier,
			sb.flight_supplier_reference,
			sb.sale_closest_airport_code,
			sb.departure_airport_code,
			sb.booking_id,
			sb.voucher_stay_by_date,
			sb.shiro_user_id,
			bs.customer_email,
			bs.record__o['firstName']::VARCHAR AS first_name,
			bs.record__o['lastName']::VARCHAR  AS last_name,
			'SECRET_ESCAPES'                   AS tech_platform
		FROM data_vault_mvp.dwh.se_booking sb
			LEFT JOIN hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs ON sb.transaction_id = bs.transaction_id
		WHERE sb.se_brand = 'SE Brand'
		  AND sb.booking_status IN ('COMPLETE', 'REFUNDED')
		UNION ALL
		SELECT
			tb.se_sale_id,
			tb.transaction_id,
			tb.payment_status,
			CASE
				WHEN tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE') THEN 'live'
				WHEN (YEAR(tb.created_at_dts) = '2019'
					AND tb.cancellation_date >= '2020-03-01'
					AND tb.payment_status = 'CANCELLED') THEN 'live'
				WHEN tb.payment_status = 'CANCELLED' THEN 'cancelled'
				WHEN tb.payment_status = 'FINISHED' THEN 'abandoned'
				ELSE 'other'
			END                                            AS booking_status_type,
			tb.adult_guests,
			tb.child_guests,
			tb.infant_guests,
			tb.created_at_dts                              AS booking_completed_date,
			tb.order_creation_holiday_start_date           AS original_check_in_date,
			tb.order_creation_holiday_end_date             AS original_check_out_date,
			tb.holiday_start_date                          AS check_in_date,
			tb.holiday_end_date                            AS check_out_date,
			tb.no_nights,
			tb.rooms,
			tb.territory,
			tb.sold_price_currency                         AS currency,
			tb.sold_price_total_gbp,
			tb.sold_price_total_cc,
			COALESCE(tb.sold_price_total_gbp, 0)
				- COALESCE(tb.cost_price_total_gbp, 0)
				- COALESCE(tb.booking_fee_incl_vat_gbp, 0) AS commission_ex_vat_gbp,
			tb.order_partners                              AS supplier_name,
			tb.booking_includes_flight                     AS has_flights,
			'Catalogue'                                    AS sale_dimension_type,
			tb.flight_cost_price_gbp                       AS flight_buy_rate_gbp,
			tad.flight_carrier                             AS flight_carrier,
			tb.order_flight_reservation_numbers            AS flight_supplier_reference,
			tb.flight_outbound_arrival_airport,
			tb.flight_outbound_departure_airport,
			tb.booking_id,
			NULL                                           AS voucher_stay_by_date,
			tb.shiro_user_id,
			op.email,
			op.first_name,
			op.last_name,
			'TRAVELBIRD'                                   AS tech_platform
		FROM data_vault_mvp.dwh.tb_booking tb
			LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_person op ON tb.customer_id = op.id
			LEFT JOIN tb_airline_details tad ON tb.order_id = tad.order_id
		WHERE tb.se_brand IS DISTINCT FROM 'Travelist'
		  AND tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
	)

SELECT
	s.se_sale_id,
	s.transaction_id,
	s.booking_status,
	s.booking_status_type,
	s.adult_guests,
	s.child_guests,
	s.infant_guests,
	COALESCE(sua.first_name, s.first_name) || ' ' || COALESCE(sua.surname, s.last_name) AS customer_name,
	COALESCE(sua.first_name, s.first_name)                                              AS first_name,
	COALESCE(sua.surname, s.last_name)                                                  AS last_name,
	COALESCE(sua.email, s.customer_email)                                               AS email,                  -- NOTE: This column is considered PII
	sua.membership_account_status,
	s.booking_completed_date,
	s.original_check_in_date,
	s.original_check_out_date,
	s.check_in_date,
	s.check_out_date,
	s.no_nights,
	s.rooms,
	s.territory,
	s.currency,
	s.customer_total_price_gbp,
	s.customer_total_price_cc,
	s.commission_ex_vat_gbp,
	ss.company_name,
	s.supplier_name,
	ss.posu_country,
	ss.posu_division,
	ss.posu_city,
	ss.product_configuration,
	s.has_flights,
	COALESCE(s.sale_dimension_type, ss.product_configuration)                           AS sale_dimension_type,
	s.flight_buy_rate_gbp,
	s.flight_carrier,
	s.flight_supplier_reference,
	s.sale_closest_airport_code,
	s.departure_airport_code,
	us.margin_segment,
	s.booking_id,
	s.voucher_stay_by_date,
	rrc.case_thread_id                                                                  AS sf_case_thread_id,
	rrc.case_number                                                                     AS sf_case_number,
	rrc.case_id                                                                         AS sf_case_id,
	rrc.case_name                                                                       AS sf_case_name,
	rrc.case_overview_id                                                                AS sf_case_overview_id,
	rrc.priority_type                                                                   AS sf_priority_type,
	rrc.priority                                                                        AS sf_priority,
	rrc.status                                                                          AS sf_status,
	rrc.contact_reason                                                                  AS sf_contact_reason,
	rrc.case_owner_full_name                                                            AS sf_case_owner_full_name,
	rrc.view                                                                            AS sf_view,
	c.weekly_summary_receiver_emails,-- NOTE: This column is considered PII
	COALESCE(c.customer_support_email, atpe.customer_support_email)                     AS customer_support_email, -- NOTE: This column is considered PII
	ss.se_api_url,
	s.shiro_user_id,
	s.tech_platform
FROM stack s
	LEFT JOIN data_vault_mvp.dwh.user_attributes sua ON s.shiro_user_id = sua.shiro_user_id
	LEFT JOIN data_vault_mvp.dwh.se_sale ss ON s.se_sale_id = ss.se_sale_id
	LEFT JOIN data_vault_mvp.dwh.user_segmentation us
			  ON s.shiro_user_id = us.shiro_user_id AND us.date = CURRENT_DATE - 1
	LEFT JOIN /*latest_vault.sfsc.rebooking_request_cases*/ data_vault_mvp.dwh.sfsc__rebooking_request_cases rrc
			  ON s.transaction_id = rrc.transaction_id
	LEFT JOIN latest_vault.cms_mysql.company c ON c.id::VARCHAR = ss.company_id
	LEFT JOIN aggregate_tb_partner_emails atpe ON s.booking_id = atpe.booking_id
WHERE tech_platform = 'TRAVELBIRD'
;

USE WAREHOUSE pipe_medium
;


SELECT
	tb.se_sale_id,
	tb.transaction_id,
	tb.payment_status,
	CASE
		WHEN tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE') THEN 'live'
		WHEN (YEAR(tb.created_at_dts) = '2019'
			AND tb.cancellation_date >= '2020-03-01'
			AND tb.payment_status = 'CANCELLED') THEN 'live'
		WHEN tb.payment_status = 'CANCELLED' THEN 'cancelled'
		WHEN tb.payment_status = 'FINISHED' THEN 'abandoned'
		ELSE 'other'
	END                                            AS booking_status_type,
	tb.adult_guests,
	tb.child_guests,
	tb.infant_guests,
	tb.created_at_dts                              AS booking_completed_date,
	tb.order_creation_holiday_start_date           AS original_check_in_date,
	tb.order_creation_holiday_end_date             AS original_check_out_date,
	tb.holiday_start_date                          AS check_in_date,
	tb.holiday_end_date                            AS check_out_date,
	tb.no_nights,
	tb.rooms,
	tb.territory,
	tb.sold_price_currency                         AS currency,
	tb.sold_price_total_gbp,
	tb.sold_price_total_cc,
	COALESCE(tb.sold_price_total_gbp, 0)
		- COALESCE(tb.cost_price_total_gbp, 0)
		- COALESCE(tb.booking_fee_incl_vat_gbp, 0) AS commission_ex_vat_gbp,
	tb.order_partners                              AS supplier_name,
	tb.booking_includes_flight                     AS has_flights,
	'Catalogue'                                    AS sale_dimension_type,
	tb.flight_cost_price_gbp                       AS flight_buy_rate_gbp,
	tb.order_flight_reservation_numbers            AS flight_supplier_reference,
	tb.flight_outbound_arrival_airport,
	tb.flight_outbound_departure_airport,
	tb.booking_id,
	NULL                                           AS voucher_stay_by_date,
	tb.shiro_user_id,
	op.email,
	op.first_name,
	op.last_name,
	'TRAVELBIRD'                                   AS tech_platform
FROM data_vault_mvp.dwh.tb_booking tb
	LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_person op ON tb.customer_id = op.id
WHERE tb.se_brand IS DISTINCT FROM 'Travelist'
  AND tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')


WITH
	order_item AS (
		SELECT
			toi.order_id,
			toi.order_item_type,
			toi.partner_name
		FROM data_vault_mvp.dwh.tb_order_item toi
	)
SELECT
	oi.order_id,
	LISTAGG(oi.partner_name, ', ') WITHIN GROUP (ORDER BY
		CASE oi.order_item_type
			WHEN 'ACCOMMODATION' THEN 1
			WHEN 'TOUR' THEN 2
			ELSE 3
		END
		) AS order_partners
FROM order_item oi
GROUP BY 1
;


SELECT DISTINCT
	toi.order_item_type
FROM data_vault_mvp.dwh.tb_order_item toi


------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.accounts_userorder CLONE latest_vault.travelbird_mysql.accounts_userorder
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.external_booking CLONE latest_vault.cms_mysql.external_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.mari_reservation_information CLONE data_vault_mvp.dwh.mari_reservation_information
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item CLONE data_vault_mvp.dwh.tb_order_item
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog CLONE data_vault_mvp.dwh.tb_order_item_changelog
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_order CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderevent CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderevent
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderproperty CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderproperty
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.offers_offer CLONE latest_vault.travelbird_mysql.offers_offer
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderproperty CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderproperty
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_person CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_person
;

-- CREATE OR REPLACE TRANSIENT TABLE se_dev_robin.data.posa_territory_from_tb_site_id CLONE se.data.posa_territory_from_tb_site_id;
-- CREATE OR REPLACE TRANSIENT TABLE se_dev_robin.data.se_brand CLONE se.data.se_brand;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.tb_rates CLONE data_vault_mvp.fx.tb_rates
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2023-11-14 00:00:00' --end '2023-11-14 00:00:00'


SELECT
	transaction_id,
	supplier_name
FROM collab.covid_pii.dflo_view_booking_summary
WHERE tech_platform = 'TRAVELBIRD'
  AND transaction_id LIKE '%SEUK%'
;


SELECT
	transaction_id,
	order_partners
FROM data_vault_mvp.dwh.tb_booking
WHERE tb_booking.transaction_id IN (
									'A53709-SEUK-22229051',
									'A60975-SEUK-22281130',
									'A57934-SEUK-22258039',
									'A10341-SEUK-22101916',
									'A47664-SEUK-21958367',
									'A30650-SEUK-22152302',
									'A27972-SEUK-22113138'
	)
;

SELECT
	transaction_id,
	order_partners
FROM data_vault_mvp_dev_robin.dwh.tb_booking
WHERE tb_booking.transaction_id IN (
									'A53709-SEUK-22229051',
									'A60975-SEUK-22281130',
									'A57934-SEUK-22258039',
									'A10341-SEUK-22101916',
									'A47664-SEUK-21958367',
									'A30650-SEUK-22152302',
									'A27972-SEUK-22113138'
	)
;


WITH
	order_item AS (
		SELECT
			toi.order_id,
			toi.order_item_id,
			toi.order_item_type,
			toi.partner_name
		FROM data_vault_mvp.dwh.tb_order_item toi
		WHERE toi.order_id IN (
							   22101916,
							   22113138,
							   22152302,
							   21958367,
							   22229051,
							   22258039,
							   22281130
			)
	),
	distinct_partners AS (
		SELECT DISTINCT
			oi.order_id,
			oi.partner_name,
			CASE oi.order_item_type
				WHEN 'ACCOMMODATION' THEN 1
				WHEN 'TOUR' THEN 2
				ELSE 3
			END AS partner_order
		FROM order_item oi
	)
SELECT
	order_id,
	LISTAGG(partner_name, ', ') WITHIN GROUP ( ORDER BY partner_order )
FROM distinct_partners
GROUP BY 1
;


WITH
	distinct_partners_order AS (
		-- force and ordering of partner name in listagg so that tour and accommodation come first
		SELECT DISTINCT
			oi.order_id,
			oi.partner_name,
			oi.order_item_type,
			CASE oi.order_item_type
				WHEN 'ACCOMMODATION' THEN 1
				WHEN 'TOUR' THEN 2
				ELSE 3
			END AS partner_order
		FROM data_vault_mvp_dev_robin.dwh.tb_order_item oi
		WHERE oi.order_id = 22101916
	),
	remove_duplicate_partners AS (
		-- remove duplicate providers across different order item types prioritising partner order
		SELECT *
		FROM distinct_partners_order dpo
		QUALIFY ROW_NUMBER() OVER (PARTITION BY dpo.order_id, dpo.partner_name ORDER BY dpo.partner_order) = 1
	)
SELECT
	rdp.order_id,
	LISTAGG(rdp.partner_name, ', ') WITHIN GROUP ( ORDER BY rdp.partner_order) AS order_partners
FROM remove_duplicate_partners rdp
GROUP BY 1
;


SELECT
	transaction_id,
	booking_status,
	booking_status_type,
	booking_completed_date,
	check_in_date,
	check_out_date,
	no_nights,
	customer_name,
	email,
	territory,
	sale_dimension_type,
	product_configuration,
	tech_platform,
	sale_closest_airport_code,
	departure_airport_code,
	flight_carrier,
	flight_supplier_reference,
	company_name,
	supplier_name,
	posu_country,
	posu_division,
	posu_city,
	se_sale_id,
	adult_guests,
	child_guests,
	infant_guests,
	currency,
	customer_total_price_gbp,
	customer_total_price_cc,
	commission_ex_vat_gbp,
	flight_buy_rate_gbp,
	margin_segment
FROM collab.covid_pii.dflo_view_booking_summary
WHERE check_in_date >= '2023-11-01'
  AND check_in_date < '2023-11-30'