SELECT *
FROM se.finance.travel_trust_money_out ttmo
WHERE ttmo.booking_id = 'TB-21918304';

SELECT *
FROM se.finance.travel_trust_money_in ttmi
WHERE ttmi.booking_id = 'TB-21918304';

SELECT *
FROM data_vault_mvp.finance.stripe_cash_on_booking scob
WHERE scob.booking_id = 'TB-21918304';

SELECT *
FROM data_vault_mvp.finance.stripe_refund sr
WHERE sr.booking_id = 'TB-21918304';

SELECT *
FROM se.data.tb_booking tb
WHERE tb.booking_id = 'TB-21918304'


WITH stack AS (
    SELECT
        ttmi.transaction_id,
        ttmi.transaction_tstamp,
        ttmi.payment_service_provider,
        ttmi.payment_service_provider_transaction_type,
        ttmi.cashflow_direction,
        ttmi.cashflow_type,
        ttmi.transaction_amount,
        ttmi.transaction_currency,
--     ttmi.orders_paymemt_classification,
        ttmi.booking_id,
        ttmi.travel_date,
        ttmi.return_date,
        ttmi.flight_carriers
    FROM se.finance.travel_trust_money_in ttmi
    UNION ALL
    SELECT
        ttmo.transaction_id,
        ttmo.transaction_tstamp,
        ttmo.payment_service_provider,
        ttmo.payment_service_provider_transaction_type,
        ttmo.cashflow_direction,
        ttmo.cashflow_type,
        ttmo.transaction_amount,
        ttmo.transaction_currency,
--     ttmo.pre_settlement_travel_trust_money_in_cumulative,
--     ttmo.pre_settlement_travel_trust_money_out_cumulative,
--     ttmo.pre_settlement_travel_trust_booking_balance,
--     ttmo.settlement_amount,
--     ttmo.post_settlement_travel_trust_booking_balance,
        ttmo.booking_id,
--     ttmo.booking_created_date,
        ttmo.travel_date,
        ttmo.return_date,
--     ttmo.pnr,
        ttmo.booking_flight_carriers
    FROM se.finance.travel_trust_money_out ttmo
)
SELECT *
FROM stack
WHERE stack.booking_id = 'TB-21918304';

SELECT *
FROM se.finance.travel_trust_money_out ttmo
WHERE ttmo.booking_id = 'TB-21938061';

SELECT *
FROM se.finance.travel_trust_money_in ttmi
WHERE ttmi.booking_id = 'TB-21938061';


WITH stack AS (
    SELECT
        ttmi.transaction_id,
        ttmi.transaction_tstamp,
        ttmi.payment_service_provider,
        ttmi.payment_service_provider_transaction_type,
        ttmi.cashflow_direction,
        ttmi.cashflow_type,
        ttmi.transaction_amount,
        ttmi.transaction_currency,
--     ttmi.orders_paymemt_classification,
        ttmi.booking_id,
        ttmi.travel_date,
        ttmi.return_date,
        ttmi.flight_carriers
    FROM se.finance.travel_trust_money_in ttmi
    UNION ALL
    SELECT
        ttmo.transaction_id,
        ttmo.transaction_tstamp,
        ttmo.payment_service_provider,
        ttmo.payment_service_provider_transaction_type,
        ttmo.cashflow_direction,
        ttmo.cashflow_type,
        ttmo.transaction_amount,
        ttmo.transaction_currency,
--     ttmo.pre_settlement_travel_trust_money_in_cumulative,
--     ttmo.pre_settlement_travel_trust_money_out_cumulative,
--     ttmo.pre_settlement_travel_trust_booking_balance,
--     ttmo.settlement_amount,
--     ttmo.post_settlement_travel_trust_booking_balance,
        ttmo.booking_id,
--     ttmo.booking_created_date,
        ttmo.travel_date,
        ttmo.return_date,
--     ttmo.pnr,
        ttmo.booking_flight_carriers
    FROM se.finance.travel_trust_money_out ttmo
)
SELECT *
FROM stack
WHERE stack.booking_id = 'TB-21938061';



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.tb_travel_trust_money_in CLONE data_vault_mvp.finance.tb_travel_trust_money_in;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.tb_travel_trust_money_out_flight CLONE data_vault_mvp.finance.tb_travel_trust_money_out_flight;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.tb_travel_trust_money_out_refund CLONE data_vault_mvp.finance.tb_travel_trust_money_out_refund;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.tb_travel_trust_money_out_travelled CLONE data_vault_mvp.finance.tb_travel_trust_money_out_travelled;

self_describing_task --include 'biapp/task_catalogue/dv/finance/cash_flow/travel_trust/travelbird/tb_travel_trust_money_out.py'  --method 'run' --start '2022-05-24 00:00:00' --end '2022-05-24 00:00:00'


SELECT * FROM data_vault_mvp_dev_robin.finance.tb_travel_trust_money_out__step01__union_datasets WHERE booking_id  = 'TB-21938061';

SELECT * FROM se.data.tb_order_item_changelog toic WHERE toic.booking_id = 'TB-21938061';
SELECT * FROM se.data.tb_order_item toi  WHERE toi.booking_id = 'TB-21938061';
SELECT * FROM data_vault_mvp_dev_robin.finance.tb_travel_trust_money_out__step02__model_financials WHERE booking_id = 'TB-21938061';
SELECT * FROM data_vault_mvp_dev_robin.finance.tb_travel_trust_money_out__step03__remove_money_in WHERE booking_id = 'TB-21938061';

SELECT * FROM data_vault_mvp_dev_robin.finance.tb_travel_trust_money_out WHERE booking_id = 'TB-21938061';


-- 1404.0000 -- cash on booking
-- 1509.3000 -- flight component


SELECT * FROM se.data.tb_order_item toi WHERE toi.booking_id = 'TB-21938061';
SELECT * FROM se.data.tb_order_item_changelog toic WHERE toic.order_item_id = '663512'
