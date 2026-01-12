USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.external_booking
	CLONE latest_vault.cms_mysql.external_booking
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.travelbird_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.accounts_userorder
	CLONE latest_vault.travelbird_mysql.accounts_userorder
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.currency_currency
	CLONE latest_vault.travelbird_mysql.currency_currency
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_secredittransaction
	CLONE latest_vault.travelbird_mysql.orders_secredittransaction
;


CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.integrations_bookingtransaction
	CLONE latest_vault.travelbird_mysql.integrations_bookingtransaction
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.offers_offer
	CLONE latest_vault.travelbird_mysql.offers_offer
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_order
	CLONE latest_vault.travelbird_mysql.orders_order
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_orderevent
	CLONE latest_vault.travelbird_mysql.orders_orderevent
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_orderitembase
	CLONE latest_vault.travelbird_mysql.orders_orderitembase
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_orderitemintegration
	CLONE latest_vault.travelbird_mysql.orders_orderitemintegration
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_orderitemintegration_order_items
	CLONE latest_vault.travelbird_mysql.orders_orderitemintegration_order_items
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_orderproperty
	CLONE latest_vault.travelbird_mysql.orders_orderproperty
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_payment
	CLONE latest_vault.travelbird_mysql.orders_payment
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_paymentmethod
	CLONE latest_vault.travelbird_mysql.orders_paymentmethod
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_person
	CLONE latest_vault.travelbird_mysql.orders_person
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_refundrequest
	CLONE latest_vault.travelbird_mysql.orders_refundrequest
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.tb_voucherify_redeemedvoucherifycoupon
	CLONE latest_vault.travelbird_mysql.tb_voucherify_redeemedvoucherifycoupon
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item
	CLONE data_vault_mvp.dwh.tb_order_item
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog
	CLONE data_vault_mvp.dwh.tb_order_item_changelog
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_supplier_names_for_tours
	CLONE data_vault_mvp.dwh.tb_supplier_names_for_tours
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.fx
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.tb_rates
	CLONE data_vault_mvp.fx.tb_rates
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.fpa_gsheets
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.constant_currency
	CLONE latest_vault.fpa_gsheets.constant_currency
;

-- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking
-- CLONE data_vault_mvp.dwh.tb_booking;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.transactional.tb_booking.py' \
    --method 'run' \
    --start '2025-08-04 00:00:00' \
    --end '2025-08-04 00:00:00'


WITH
	booking_promo_code AS (
		SELECT
			md.*,
			vo.discount                                                AS promo_code_amount,
			IFF(vo.order_id IS NOT NULL, md.sold_price_currency, NULL) AS promo_code_currency,
			IFF(vo.rolled_back = 1, TRUE, FALSE)                       AS promo_code_failed
		FROM data_vault_mvp_dev_robin.dwh.tb_booking__step16__model_data md
		LEFT JOIN latest_vault_dev_robin.travelbird_mysql.tb_voucherify_redeemedvoucherifycoupon vo
					  ON vo.order_id = md.order_id
	)
SELECT
	bpc.*,
	IFF(promo_code_currency = 'GBP', promo_code_amount, promo_code_amount * tbr.rate) AS promo_code_amount_gbp,
FROM booking_promo_code bpc
LEFT JOIN data_vault_mvp_dev_robin.fx.tb_rates tbr
			  ON tbr.source_currency = bpc.promo_code_currency
			  AND bpc.created_at_dts::DATE = tbr.usage_date
			  AND tbr.target_currency = 'GBP'


SELECT
	voucherify_coupon.order_id,
-- 	IFF(vo.order_id IS NOT NULL, md.sold_price_currency, NULL) AS promo_code_currency,
	IFF(voucherify_coupon.rolled_back = 1, TRUE, FALSE)                             AS promo_code_failed,
	IFF(orders.sold_price_currency = 'GBP', voucherify_coupon.discount, voucherify_coupon.discount *
																		rates.rate) AS promo_code_amount_gbp,
FROM latest_vault_dev_robin.travelbird_mysql.tb_voucherify_redeemedvoucherifycoupon voucherify_coupon
INNER JOIN data_vault_mvp_dev_robin.dwh.tb_booking__step02__model_items_to_order orders
			   ON voucherify_coupon.order_id = orders.order_id
LEFT JOIN  data_vault_mvp_dev_robin.fx.tb_rates rates
			   ON rates.source_currency = orders.sold_price_currency
			   AND voucherify_coupon.created_at_dts::DATE = rates.usage_date
			   AND rates.target_currency = 'GBP'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_booking__step15__model_promo_code


WITH
	ranked_refunds AS (
		SELECT
			se.*,
			orr.amount                                                                    AS refund_calculated_amount,
			orr.calculated_amount                                                         AS refund_actual_amount,
			CASE
				WHEN orr.status = '1' THEN 'DRAFT'
				WHEN orr.status = '2' THEN 'SUBMITTED'
				WHEN orr.status = '3' THEN 'PENDING FINANCE APPROVAL'
				WHEN orr.status = '4' THEN 'APPROVED'
				WHEN orr.status = '5' THEN 'REJECTED'
				WHEN orr.status = '6' THEN 'PSP REFUND ISSUED'
				WHEN orr.status = '7' THEN 'PSP REFUND FAILED'
				WHEN orr.status = '10' THEN 'REFUNDED'
				WHEN orr.status = '11' THEN 'UNSUBMITTED'
				WHEN orr.status = '20' THEN 'CANCELLED'
				ELSE NULL
			END                                                                           AS refund_status,
			-- Assigning a rank based on the status, with REFUNDED getting the highest priority
			ROW_NUMBER() OVER (
				PARTITION BY se.booking_id
				ORDER BY
					CASE
						WHEN orr.status = '10' THEN 1 -- REFUNDED supersedes all other statuses
						ELSE 2 -- Any other status is ranked lower
					END,
					orr.status ASC -- For non-REFUNDED statuses, order by numeric status
				)                                                                         AS refund_rank,
			se.sold_price_currency                                                        AS refund_currency,
			IFF(se.sold_price_currency = 'GBP', refund_actual_amount, refund_actual_amount *
																	  gtbr.rate)          AS refund_actual_amount_gbp,
			IFF(se.sold_price_currency = 'GBP', refund_calculated_amount, refund_calculated_amount *
																		  gtbr.rate)      AS refund_calculated_amount_gbp,
			IFF(se.sold_price_currency = 'EUR', refund_actual_amount, refund_actual_amount *
																	  etbr.rate)          AS refund_actual_amount_eur,
			IFF(se.sold_price_currency = 'EUR', refund_calculated_amount, refund_calculated_amount *
																		  etbr.rate)      AS refund_calculated_amount_eur,
			IFF(se.sold_price_currency = 'GBP', refund_actual_amount, refund_actual_amount *
																	  ccp.multiplier)     AS refund_actual_amount_gbp_constant_currency,
			IFF(se.sold_price_currency = 'GBP', refund_calculated_amount, refund_calculated_amount *
																		  ccp.multiplier) AS refund_calculated_amount_gbp_constant_currency,
			IFF(se.sold_price_currency = 'EUR', refund_actual_amount, refund_actual_amount *
																	  cce.multiplier)     AS refund_actual_amount_eur_constant_currency,
			IFF(se.sold_price_currency = 'EUR', refund_calculated_amount, refund_calculated_amount *
																		  cce.multiplier) AS refund_calculated_amount_eur_constant_currency
		FROM data_vault_mvp_dev_robin.dwh.tb_booking__step15__model_promo_code se
		LEFT JOIN latest_vault_dev_robin.travelbird_mysql.orders_refundrequest orr
					  ON se.order_id = orr.order_id
		LEFT JOIN data_vault_mvp_dev_robin.fx.tb_rates gtbr
					  ON gtbr.source_currency = se.sold_price_currency
					  AND se.created_at_dts::DATE = gtbr.usage_date
					  AND gtbr.target_currency = 'GBP'
		LEFT JOIN data_vault_mvp_dev_robin.fx.tb_rates etbr
					  ON etbr.source_currency = se.sold_price_currency
					  AND se.created_at_dts::DATE = etbr.usage_date
					  AND etbr.target_currency = 'EUR'
		LEFT JOIN latest_vault_dev_robin.fpa_gsheets.constant_currency ccp
					  ON CURRENT_DATE BETWEEN ccp.start_date AND ccp.end_date
					  AND ccp.currency = 'GBP'
					  AND ccp.category = 'Primary'
					  AND ccp.base_currency = se.sold_price_currency
		LEFT JOIN latest_vault_dev_robin.fpa_gsheets.constant_currency cce
					  ON CURRENT_DATE BETWEEN cce.start_date AND cce.end_date
					  AND cce.currency = 'EUR'
					  AND cce.category = 'Primary'
					  AND cce.base_currency = se.sold_price_currency
	)

SELECT *
FROM ranked_refunds
-- Select only the top-ranked refund status for each booking
WHERE refund_rank = 1
;

WITH
	dedupe_refunds AS (
		SELECT
			created_at_dts,
			status,
			amount,
			calculated_amount,
			order_id
		FROM latest_vault_dev_robin.travelbird_mysql.orders_refundrequest refund_request
		QUALIFY ROW_NUMBER() OVER (
			PARTITION BY refund_request.order_id
			ORDER BY
				CASE
					WHEN refund_request.status = '10' THEN 1 -- REFUNDED supersedes all other statuses
					ELSE 2 -- Any other status is ranked lower
				END,
				refund_request.status ASC -- For non-REFUNDED statuses, order by numeric status
			) = 1
	)
SELECT
	refunds.order_id,
	refunds.amount                                                                                            AS refund_calculated_amount,
	refunds.calculated_amount                                                                                 AS refund_actual_amount,
	CASE
		WHEN refunds.status = '1' THEN 'DRAFT'
		WHEN refunds.status = '2' THEN 'SUBMITTED'
		WHEN refunds.status = '3' THEN 'PENDING FINANCE APPROVAL'
		WHEN refunds.status = '4' THEN 'APPROVED'
		WHEN refunds.status = '5' THEN 'REJECTED'
		WHEN refunds.status = '6' THEN 'PSP REFUND ISSUED'
		WHEN refunds.status = '7' THEN 'PSP REFUND FAILED'
		WHEN refunds.status = '10' THEN 'REFUNDED'
		WHEN refunds.status = '11' THEN 'UNSUBMITTED'
		WHEN refunds.status = '20' THEN 'CANCELLED'
	END                                                                                                       AS refund_status,
	orders.sold_price_currency                                                                                AS refund_currency,
	IFF(orders.sold_price_currency = 'GBP', refund_actual_amount, refund_actual_amount *
																  gbp_rates.rate)                             AS refund_actual_amount_gbp,
	IFF(orders.sold_price_currency = 'GBP', refund_calculated_amount, refund_calculated_amount *
																	  gbp_rates.rate)                         AS refund_calculated_amount_gbp,
	IFF(orders.sold_price_currency = 'EUR', refund_actual_amount, refund_actual_amount *
																  eur_rates.rate)                             AS refund_actual_amount_eur,
	IFF(orders.sold_price_currency = 'EUR', refund_calculated_amount, refund_calculated_amount *
																	  eur_rates.rate)                         AS refund_calculated_amount_eur,
	IFF(orders.sold_price_currency = 'GBP', refund_actual_amount, refund_actual_amount *
																  gbp_rates_constant_currency.multiplier)     AS refund_actual_amount_gbp_constant_currency,
	IFF(orders.sold_price_currency = 'GBP', refund_calculated_amount, refund_calculated_amount *
																	  gbp_rates_constant_currency.multiplier) AS refund_calculated_amount_gbp_constant_currency,
	IFF(orders.sold_price_currency = 'EUR', refund_actual_amount, refund_actual_amount *
																  eur_rates_constant_currency.multiplier)     AS refund_actual_amount_eur_constant_currency,
	IFF(orders.sold_price_currency = 'EUR', refund_calculated_amount, refund_calculated_amount *
																	  eur_rates_constant_currency.multiplier) AS refund_calculated_amount_eur_constant_currency
FROM dedupe_refunds AS refunds
INNER JOIN data_vault_mvp_dev_robin.dwh.tb_booking__step02__model_items_to_order orders
			   ON refunds.order_id = orders.order_id
LEFT JOIN  data_vault_mvp_dev_robin.fx.tb_rates gbp_rates
			   ON orders.sold_price_currency = gbp_rates.source_currency
			   AND refunds.created_at_dts::DATE = gbp_rates.usage_date
			   AND gbp_rates.target_currency = 'GBP'
LEFT JOIN  data_vault_mvp_dev_robin.fx.tb_rates eur_rates
			   ON orders.sold_price_currency = eur_rates.source_currency
			   AND refunds.created_at_dts::DATE = eur_rates.usage_date
			   AND eur_rates.target_currency = 'EUR'
LEFT JOIN  latest_vault_dev_robin.fpa_gsheets.constant_currency gbp_rates_constant_currency
			   ON CURRENT_DATE BETWEEN gbp_rates_constant_currency.start_date AND gbp_rates_constant_currency.end_date
			   AND orders.sold_price_currency = gbp_rates_constant_currency.base_currency
			   AND gbp_rates_constant_currency.currency = 'GBP'
			   AND gbp_rates_constant_currency.category = 'Primary'
LEFT JOIN  latest_vault_dev_robin.fpa_gsheets.constant_currency eur_rates_constant_currency
			   ON CURRENT_DATE BETWEEN eur_rates_constant_currency.start_date AND eur_rates_constant_currency.end_date
			   AND eur_rates_constant_currency.base_currency = orders.sold_price_currency
			   AND eur_rates_constant_currency.currency = 'EUR'
			   AND eur_rates_constant_currency.category = 'Primary'
;


SELECT *
FROM se.data.tb_booking tb
WHERE tb.refund_status IS NOT NULL


WITH
	dedupe_refunds AS (
		SELECT
			created_at_dts,
			status,
			amount,
			calculated_amount,
			order_id
		FROM latest_vault_dev_robin.travelbird_mysql.orders_refundrequest refund_request
		QUALIFY ROW_NUMBER() OVER (
			PARTITION BY refund_request.order_id
			ORDER BY
				IFF(refund_request.status = '10', 1, 2), -- REFUNDED supersedes all other statuses
				refund_request.status ASC -- For non-REFUNDED statuses, order by numeric status
			) = 1
	)
SELECT
	refunds.order_id,
	refunds.amount                                                                                            AS refund_calculated_amount,
	refunds.calculated_amount                                                                                 AS refund_actual_amount,
	CASE
		WHEN refunds.status = '1' THEN 'DRAFT'
		WHEN refunds.status = '2' THEN 'SUBMITTED'
		WHEN refunds.status = '3' THEN 'PENDING FINANCE APPROVAL'
		WHEN refunds.status = '4' THEN 'APPROVED'
		WHEN refunds.status = '5' THEN 'REJECTED'
		WHEN refunds.status = '6' THEN 'PSP REFUND ISSUED'
		WHEN refunds.status = '7' THEN 'PSP REFUND FAILED'
		WHEN refunds.status = '10' THEN 'REFUNDED'
		WHEN refunds.status = '11' THEN 'UNSUBMITTED'
		WHEN refunds.status = '20' THEN 'CANCELLED'
	END                                                                                                       AS refund_status,
	orders.sold_price_currency                                                                                AS refund_currency,
	IFF(orders.sold_price_currency = 'GBP', refund_actual_amount, refund_actual_amount *
																  gbp_rates.rate)                             AS refund_actual_amount_gbp,
	IFF(orders.sold_price_currency = 'GBP', refund_calculated_amount, refund_calculated_amount *
																	  gbp_rates.rate)                         AS refund_calculated_amount_gbp,
	IFF(orders.sold_price_currency = 'EUR', refund_actual_amount, refund_actual_amount *
																  eur_rates.rate)                             AS refund_actual_amount_eur,
	IFF(orders.sold_price_currency = 'EUR', refund_calculated_amount, refund_calculated_amount *
																	  eur_rates.rate)                         AS refund_calculated_amount_eur,
	IFF(orders.sold_price_currency = 'GBP', refund_actual_amount, refund_actual_amount *
																  gbp_rates_constant_currency.multiplier)     AS refund_actual_amount_gbp_constant_currency,
	IFF(orders.sold_price_currency = 'GBP', refund_calculated_amount, refund_calculated_amount *
																	  gbp_rates_constant_currency.multiplier) AS refund_calculated_amount_gbp_constant_currency,
	IFF(orders.sold_price_currency = 'EUR', refund_actual_amount, refund_actual_amount *
																  eur_rates_constant_currency.multiplier)     AS refund_actual_amount_eur_constant_currency,
	IFF(orders.sold_price_currency = 'EUR', refund_calculated_amount, refund_calculated_amount *
																	  eur_rates_constant_currency.multiplier) AS refund_calculated_amount_eur_constant_currency
FROM dedupe_refunds AS refunds
INNER JOIN data_vault_mvp_dev_robin.dwh.tb_booking__step02__model_items_to_order orders
			   ON refunds.order_id = orders.order_id
LEFT JOIN  data_vault_mvp_dev_robin.fx.tb_rates gbp_rates
			   ON orders.sold_price_currency = gbp_rates.source_currency
			   AND refunds.created_at_dts::DATE = gbp_rates.usage_date
			   AND gbp_rates.target_currency = 'GBP'
LEFT JOIN  data_vault_mvp_dev_robin.fx.tb_rates eur_rates
			   ON orders.sold_price_currency = eur_rates.source_currency
			   AND refunds.created_at_dts::DATE = eur_rates.usage_date
			   AND eur_rates.target_currency = 'EUR'
LEFT JOIN  latest_vault_dev_robin.fpa_gsheets.constant_currency gbp_rates_constant_currency
			   ON CURRENT_DATE BETWEEN gbp_rates_constant_currency.start_date AND gbp_rates_constant_currency.end_date
			   AND orders.sold_price_currency = gbp_rates_constant_currency.base_currency
			   AND gbp_rates_constant_currency.currency = 'GBP'
			   AND gbp_rates_constant_currency.category = 'Primary'
LEFT JOIN  latest_vault_dev_robin.fpa_gsheets.constant_currency eur_rates_constant_currency
			   ON CURRENT_DATE BETWEEN eur_rates_constant_currency.start_date AND eur_rates_constant_currency.end_date
			   AND eur_rates_constant_currency.base_currency = orders.sold_price_currency
			   AND eur_rates_constant_currency.currency = 'EUR'
			   AND eur_rates_constant_currency.category = 'Primary'



SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
;

SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.tb_booking tb
;

-- dev
SELECT
	refund_status,
	COUNT(*),
	SUM(tb.refund_actual_amount_gbp),
	SUM(tb.refund_calculated_amount),
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
GROUP BY ALL
;

-- prod
SELECT
	refund_status,
	COUNT(*),
	SUM(tb.refund_actual_amount_gbp),
	SUM(tb.refund_calculated_amount),
FROM data_vault_mvp.dwh.tb_booking tb
GROUP BY ALL
;


4,730,570.1527
21,062,595.5301


WITH
	multiple_refund_requests AS (
		SELECT
			created_at_dts,
			status,
			amount,
			calculated_amount,
			order_id,
			COUNT(*) OVER (PARTITION BY refund_request.order_id) AS num_rows
		FROM latest_vault_dev_robin.travelbird_mysql.orders_refundrequest refund_request
		QUALIFY COUNT(*) OVER (PARTITION BY refund_request.order_id) > 1

	)

SELECT
	multiple_refund_requests.*,
	tb.transaction_id
FROM multiple_refund_requests
LEFT JOIN se.data.tb_booking tb
			  ON multiple_refund_requests.order_id = tb.order_id
ORDER BY num_rows DESC

WITH
	refund_request_modelling AS (
		SELECT
			refund_request.order_id,
			refund_request.created_at_dts,
			refund_request.status,
			CASE
				WHEN refund_request.status = '1' THEN 'DRAFT'
				WHEN refund_request.status = '2' THEN 'SUBMITTED'
				WHEN refund_request.status = '3' THEN 'PENDING FINANCE APPROVAL'
				WHEN refund_request.status = '4' THEN 'APPROVED'
				WHEN refund_request.status = '5' THEN 'REJECTED'
				WHEN refund_request.status = '6' THEN 'PSP REFUND ISSUED'
				WHEN refund_request.status = '7' THEN 'PSP REFUND FAILED'
				WHEN refund_request.status = '10' THEN 'REFUNDED'
				WHEN refund_request.status = '11' THEN 'UNSUBMITTED'
				WHEN refund_request.status = '20' THEN 'CANCELLED'
			END                                                                                                AS refund_status,
			refund_request.amount,
			refund_request.calculated_amount,
			orders.sold_price_currency                                                                         AS refund_currency,
			IFF(orders.sold_price_currency = 'GBP', amount, amount *
															gbp_rates.rate)                                    AS refund_actual_amount_gbp,
			IFF(orders.sold_price_currency = 'GBP', calculated_amount, calculated_amount *
																	   gbp_rates.rate)                         AS refund_calculated_amount_gbp,
			IFF(orders.sold_price_currency = 'EUR', amount, amount *
															eur_rates.rate)                                    AS refund_actual_amount_eur,
			IFF(orders.sold_price_currency = 'EUR', calculated_amount, calculated_amount *
																	   eur_rates.rate)                         AS refund_calculated_amount_eur,
			IFF(orders.sold_price_currency = 'GBP', amount, amount *
															gbp_rates_constant_currency.multiplier)            AS refund_actual_amount_gbp_constant_currency,
			IFF(orders.sold_price_currency = 'GBP', calculated_amount, calculated_amount *
																	   gbp_rates_constant_currency.multiplier) AS refund_calculated_amount_gbp_constant_currency,
			IFF(orders.sold_price_currency = 'EUR', amount, amount *
															eur_rates_constant_currency.multiplier)            AS refund_actual_amount_eur_constant_currency,
			IFF(orders.sold_price_currency = 'EUR', calculated_amount, calculated_amount *
																	   eur_rates_constant_currency.multiplier) AS refund_calculated_amount_eur_constant_currency
		FROM latest_vault_dev_robin.travelbird_mysql.orders_refundrequest refund_request
		INNER JOIN data_vault_mvp_dev_robin.dwh.tb_booking__step02__model_items_to_order orders
					   ON refund_request.order_id = orders.order_id
		LEFT JOIN  data_vault_mvp_dev_robin.fx.tb_rates gbp_rates
					   ON orders.sold_price_currency = gbp_rates.source_currency
					   AND refund_request.created_at_dts::DATE = gbp_rates.usage_date
					   AND gbp_rates.target_currency = 'GBP'
		LEFT JOIN  data_vault_mvp_dev_robin.fx.tb_rates eur_rates
					   ON orders.sold_price_currency = eur_rates.source_currency
					   AND refund_request.created_at_dts::DATE = eur_rates.usage_date
					   AND eur_rates.target_currency = 'EUR'
		LEFT JOIN  latest_vault_dev_robin.fpa_gsheets.constant_currency gbp_rates_constant_currency
					   ON
					   CURRENT_DATE BETWEEN gbp_rates_constant_currency.start_date AND gbp_rates_constant_currency.end_date
						   AND orders.sold_price_currency = gbp_rates_constant_currency.base_currency
						   AND gbp_rates_constant_currency.currency = 'GBP'
						   AND gbp_rates_constant_currency.category = 'Primary'
		LEFT JOIN  latest_vault_dev_robin.fpa_gsheets.constant_currency eur_rates_constant_currency
					   ON
					   CURRENT_DATE BETWEEN eur_rates_constant_currency.start_date AND eur_rates_constant_currency.end_date
						   AND eur_rates_constant_currency.base_currency = orders.sold_price_currency
						   AND eur_rates_constant_currency.currency = 'EUR'
						   AND eur_rates_constant_currency.category = 'Primary'
	)
SELECT
	refund_requests.order_id,
	refund_requests.refund_currency,
	LISTAGG(DISTINCT refund_requests.status, ', ') WITHIN GROUP (ORDER BY refund_requests.status) AS status,
	LISTAGG(DISTINCT refund_requests.refund_status, ', ')
			WITHIN GROUP (ORDER BY refund_requests.refund_status)                                 AS refund_statuses,
	-- if at least one refund request is refunded then set the order refund_status as refunded
	MAX(IFF(refund_requests.status = 10, 'REFUNDED', NULL))                                       AS refund_status,
-- 	refund_requests.refund_status,
	SUM(IFF(refund_requests.status = 10, refund_requests.amount, NULL))                           AS amount,
	SUM(IFF(refund_requests.status = 10, refund_requests.calculated_amount,
			NULL))                                                                                AS calculated_amount,
	SUM(IFF(refund_requests.status = 10, refund_requests.refund_actual_amount_gbp,
			NULL))                                                                                AS refund_actual_amount_gbp,
	SUM(IFF(refund_requests.status = 10, refund_requests.refund_calculated_amount_gbp,
			NULL))                                                                                AS refund_calculated_amount_gbp,
	SUM(IFF(refund_requests.status = 10, refund_requests.refund_actual_amount_eur,
			NULL))                                                                                AS refund_actual_amount_eur,
	SUM(IFF(refund_requests.status = 10, refund_requests.refund_calculated_amount_eur,
			NULL))                                                                                AS refund_calculated_amount_eur,
	SUM(IFF(refund_requests.status = 10, refund_requests.refund_actual_amount_gbp_constant_currency,
			NULL))                                                                                AS refund_actual_amount_gbp_constant_currency,
	SUM(IFF(refund_requests.status = 10, refund_requests.refund_calculated_amount_gbp_constant_currency,
			NULL))                                                                                AS refund_calculated_amount_gbp_constant_currency,
	SUM(IFF(refund_requests.status = 10, refund_requests.refund_actual_amount_eur_constant_currency,
			NULL))                                                                                AS refund_actual_amount_eur_constant_currency,
	SUM(IFF(refund_requests.status = 10, refund_requests.refund_calculated_amount_eur_constant_currency,
			NULL))                                                                                AS refund_calculated_amount_eur_constant_currency
-- 	created_at_dts,
FROM refund_request_modelling refund_requests
GROUP BY order_id, refund_currency


SELECT
	tb.refund_actual_amount
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
WHERE tb.order_id = 22496089


------------------------------------------------------------------------------------------------------------------------
-- se credit
SELECT
	credit_transaction.order_id,
	currency.code                                                                                 AS se_credit_currency,
	credit_transaction.amount                                                                     AS se_credit_redeemed_amount,
	IFF(currency.code = 'GBP', credit_transaction.amount, credit_transaction.amount *
														  gbp_rates.rate)                         AS se_credit_redeemed_amount_gbp,
	IFF(currency.code = 'EUR', credit_transaction.amount, credit_transaction.amount *
														  eur_rates.rate)                         AS se_credit_redeemed_amount_eur,
	IFF(currency.code = 'GBP', credit_transaction.amount, credit_transaction.amount *
														  gbp_rates_constant_currency.multiplier) AS se_credit_redeemed_amount_gbp_constant_currency,
	IFF(currency.code = 'EUR', credit_transaction.amount, credit_transaction.amount *
														  eur_rates_constant_currency.multiplier) AS se_credit_redeemed_amount_eur_constant_currency,
FROM latest_vault.travelbird_mysql.orders_secredittransaction credit_transaction
INNER JOIN latest_vault.travelbird_mysql.currency_currency currency
			   ON credit_transaction.currency_id = currency.id
LEFT JOIN  data_vault_mvp_dev_robin.fx.tb_rates gbp_rates
			   ON currency.code = gbp_rates.source_currency
			   AND credit_transaction.created_at_dts::DATE = gbp_rates.usage_date
			   AND gbp_rates.target_currency = 'GBP'
LEFT JOIN  data_vault_mvp_dev_robin.fx.tb_rates eur_rates
			   ON currency.code = eur_rates.source_currency
			   AND credit_transaction.created_at_dts::DATE = eur_rates.usage_date
			   AND eur_rates.target_currency = 'EUR'
LEFT JOIN  latest_vault_dev_robin.fpa_gsheets.constant_currency gbp_rates_constant_currency
			   ON
			   CURRENT_DATE() BETWEEN gbp_rates_constant_currency.start_date AND gbp_rates_constant_currency.end_date
				   AND currency.code = gbp_rates_constant_currency.base_currency
				   AND gbp_rates_constant_currency.currency = 'GBP'
				   AND gbp_rates_constant_currency.category = 'Primary'
LEFT JOIN  latest_vault_dev_robin.fpa_gsheets.constant_currency eur_rates_constant_currency
			   ON
			   CURRENT_DATE() BETWEEN eur_rates_constant_currency.start_date AND eur_rates_constant_currency.end_date
				   AND currency.code = eur_rates_constant_currency.base_currency
				   AND eur_rates_constant_currency.currency = 'EUR'
				   AND eur_rates_constant_currency.category = 'Primary'
WHERE credit_transaction.redeemed = 1
  AND credit_transaction.rolled_back = 0
  AND credit_transaction.order_id = 22683268
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
WHERE tb.se_credit_currency IS NOT NULL


-- dev
SELECT
	COUNT(*),
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
GROUP BY ALL
;

-- prod
SELECT
	COUNT(*),
FROM data_vault_mvp.dwh.tb_booking tb
GROUP BY ALL
;


-- dev
SELECT
	tb.se_brand,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
GROUP BY ALL
;

-- prod
SELECT
	tb.se_brand,
	COUNT(*)
FROM data_vault_mvp.dwh.tb_booking tb
GROUP BY ALL
;


-- dev
SELECT
	COUNT(DISTINCT tb.supplier_names_for_tours)
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
GROUP BY ALL
;

-- prod
SELECT
	COUNT(DISTINCT tb.supplier_names_for_tours)
FROM data_vault_mvp.dwh.tb_booking tb
GROUP BY ALL
;

-- dev
SELECT
	tb.accommodation_channel_manager,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
GROUP BY ALL
;

-- prod
SELECT
	tb.accommodation_channel_manager,
	COUNT(*)
FROM data_vault_mvp.dwh.tb_booking tb
GROUP BY ALL
;


-- dev
SELECT
	SUM(tb.adult_guests),
	SUM(tb.child_guests),
	SUM(tb.infant_guests)
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
GROUP BY ALL
;

-- prod
SELECT
	SUM(tb.adult_guests),
	SUM(tb.child_guests),
	SUM(tb.infant_guests)
FROM data_vault_mvp.dwh.tb_booking tb
GROUP BY ALL
;

-- 22253511
SELECT
	tb.order_id,
	tb.refund_actual_amount,
	tb.refund_actual_amount_gbp
FROM data_vault_mvp.dwh.tb_booking tb
WHERE tb.order_id = 22253511
;

------------------------------------------------------------------------------------------------------------------------
-- refund changes

SELECT
	SUM(tbs.refund_actual_amount_gbp)
FROM data_vault_mvp.dwh.tb_booking_snapshot tbs
WHERE tbs.view_date = '2025-08-04'
-- 4,859,203

SELECT
	SUM(tb.refund_actual_amount_gbp)
FROM data_vault_mvp.dwh.tb_booking tb
-- 21,062,595


SELECT
	tbs.se_brand,
	SUM(tbs.refund_actual_amount_gbp)
FROM data_vault_mvp.dwh.tb_booking_snapshot tbs
WHERE tbs.view_date = '2025-08-04'
GROUP BY ALL
;

/*
SE_BRAND	SUM(TBS.REFUND_ACTUAL_AMOUNT_GBP)
SE Brand	4,672,528
Travelist	186,675
*/


SELECT
	tb.se_brand,
	SUM(tb.refund_actual_amount_gbp)
FROM data_vault_mvp.dwh.tb_booking tb
GROUP BY ALL
;

/*
SE_BRAND	SUM(TB.REFUND_ACTUAL_AMOUNT_GBP)
SE Brand	20,732,086
Travelist	330,508
*/


SELECT
	DATE_TRUNC(YEAR, tbs.created_at_dts)                                 AS year,
	SUM(tbs.refund_actual_amount_gbp)                                    AS total,
	SUM(IFF(se_brand = 'SE Brand', tbs.refund_actual_amount_gbp, NULL))  AS se_brand,
	SUM(IFF(se_brand = 'Travelist', tbs.refund_actual_amount_gbp, NULL)) AS travelist,
FROM data_vault_mvp.dwh.tb_booking_snapshot tbs
WHERE tbs.view_date = '2025-08-04'
GROUP BY ALL
;

/*
YEAR	TOTAL		SE_BRAND	TRAVELIST
2019	£184,365	£184,365
2020	£575,214	£575,214
2021	£347,233	£347,233
2022	£663,169	£663,169	£0
2023	£1,049,173	£1,047,326	£1,846
2024	£1,235,065	£1,166,979	£68,086
2025	£804,985	£688,242	£116,743
*/

SELECT
	DATE_TRUNC(YEAR, tb.created_at_dts)                                 AS year,
	SUM(tb.refund_actual_amount_gbp)                                    AS total,
	SUM(IFF(se_brand = 'SE Brand', tb.refund_actual_amount_gbp, NULL))  AS se_brand,
	SUM(IFF(se_brand = 'Travelist', tb.refund_actual_amount_gbp, NULL)) AS travelist,
FROM data_vault_mvp.dwh.tb_booking tb
GROUP BY ALL
;

/*
YEAR	TOTAL		SE_BRAND	TRAVELIST
2019	£3,359,093	£3,359,093
2020	£8,200,081	£8,200,081
2021	£832,349	£832,349
2022	£2,035,830	£2,025,779	£10,051
2023	£2,597,096	£2,525,758	£71,338
2024	£2,666,128	£2,551,976	£114,152
2025	£1,386,890	£1,251,006	£135,884
*/

SELECT
	fcb.booking_id,
	fcb.transaction_id,
	fcb.booking_status_type,
	fcb.gross_revenue_gbp_constant_currency,
	fcb.margin_gross_of_toms_gbp_constant_currency
FROM se.data.fact_complete_booking fcb
INNER JOIN se.data.tb_booking tb
			   ON fcb.booking_id = tb.booking_id
			   AND tb.refund_actual_amount_gbp IS NOT NULL
WHERE fcb.booking_completed_date >= CURRENT_DATE() - 10
  AND fcb.tech_platform = 'TRAVELBIRD';
