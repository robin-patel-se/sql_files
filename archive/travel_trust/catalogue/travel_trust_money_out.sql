WITH union_data AS (
    SELECT ttmi.transaction_id,
           ttmi.transaction_tstamp,
           ttmi.payment_service_provider,
           ttmi.payment_service_provider_transaction_type,
           ttmi.cashflow_direction,
           ttmi.cashflow_type,
           NULL                      AS money_out_type,
           ttmi.transaction_amount   AS settlement_amount,
           ttmi.transaction_currency AS settlement_currency,
           ttmi.booking_id,
           ttmi.travel_date,
           ttmi.return_date
    FROM data_vault_mvp.finance.travel_trust_money_in ttmi

    UNION ALL

    SELECT ttmof.transaction_id,
           ttmof.transaction_tstamp,
           ttmof.payment_service_provider,
           ttmof.payment_service_provider_transaction_type,
           ttmof.cashflow_direction,
           ttmof.cashflow_type,
           ttmof.money_out_type,
           ttmof.settlement_amount,
           ttmof.settlement_currency,
           ttmof.booking_id,
           ttmof.travel_date,
           ttmof.return_date
    FROM data_vault_mvp.finance.travel_trust_money_out_flight ttmof

    UNION ALL
    SELECT ttmor.transaction_id,
           ttmor.transaction_tstamp,
           ttmor.payment_service_provider,
           ttmor.payment_service_provider_transaction_type,
           ttmor.cashflow_direction,
           ttmor.cashflow_type,
           ttmor.money_out_type,
           ttmor.settlement_amount,
           ttmor.settlement_currency,
           ttmor.booking_id,
           ttmor.travel_date,
           ttmor.return_date
    FROM data_vault_mvp.finance.travel_trust_money_out_refund ttmor

    UNION ALL
    SELECT ttmot.transaction_id,
           ttmot.transaction_tstamp,
           ttmot.payment_service_provider,
           ttmot.payment_service_provider_transaction_type,
           ttmot.cashflow_direction,
           ttmot.cashflow_type,
           ttmot.money_out_type,
           ttmot.total_travel_trust_money_in,
           ttmot.total_travel_trust_money_in_currency,
--        ttmot.sold_price,
--        ttmot.sold_price_currency,
           ttmot.booking_id,
           ttmot.travel_date,
           ttmot.return_date
    FROM data_vault_mvp.finance.travel_trust_money_out_travelled ttmot
)
SELECT ud.transaction_id,
       ud.transaction_tstamp,
       ud.payment_service_provider,
       ud.payment_service_provider_transaction_type,
       ud.cashflow_direction,
       ud.cashflow_type,
       ud.money_out_type,
       COALESCE(SUM(IFF(ud.cashflow_direction = 'money in', ud.settlement_amount, NULL))
                    OVER (PARTITION BY ud.booking_id ORDER BY ud.transaction_tstamp, ud.transaction_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)                           AS pre_money_in_cumulative,
       COALESCE(SUM(IFF(ud.cashflow_direction = 'money out', ud.settlement_amount, NULL))
                    OVER (PARTITION BY ud.booking_id ORDER BY ud.transaction_tstamp, ud.transaction_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)                           AS pre_money_out_cumulative,
       pre_money_in_cumulative - pre_money_out_cumulative                                               AS pre_booking_balance,
       ud.settlement_amount,
       ud.settlement_currency,
       COALESCE(SUM(IFF(ud.cashflow_direction = 'money in', ud.settlement_amount, NULL))
                    OVER (PARTITION BY ud.booking_id ORDER BY ud.transaction_tstamp, ud.transaction_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)                           AS currrent_money_in_cumulative,
       COALESCE(SUM(IFF(ud.cashflow_direction = 'money out', ud.settlement_amount, NULL))
                    OVER (PARTITION BY ud.booking_id ORDER BY ud.transaction_tstamp, ud.transaction_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)                           AS currrent_money_out_cumulative,
       currrent_money_in_cumulative - currrent_money_out_cumulative                                     AS current_booking_balance,
       IFF(ud.cashflow_direction = 'money out', LEAST(pre_booking_balance, ud.settlement_amount), NULL) AS travel_trust_money_out_settlement_amount,
       ud.booking_id,
       ud.travel_date,
       ud.return_date
FROM union_data ud
WHERE ud.booking_id = 'TB-21897914'
ORDER BY transaction_tstamp, transaction_id
-- GROUP BY ud.booking_id
-- HAVING count_money_out >1

SELECT *
FROM data_vault_mvp.finance.aviate_transactions a
WHERE UPPER(a.pnr_ref) = 'J3D959';

SELECT *
FROM se.finance.travel_trust_money_out ttmo
WHERE ttmo.pnr = 'J3D959';

SELECT *
FROM se.data.tb_order_item_changelog toic
WHERE toic.flight_reservation_number = 'J3D959';

SELECT *
FROM se.data.tb_order_item_changelog toic
WHERE toic.order_id = 21905881;


self_describing_task --include 'dv/finance/cash_flow/travel_trust/travel_trust_money_out.py'  --method 'run' --start '2021-06-21 00:00:00' --end '2021-06-21 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.travel_trust_money_out_refund CLONE data_vault_mvp.finance.travel_trust_money_out_refund;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.travel_trust_money_out_flight CLONE data_vault_mvp.finance.travel_trust_money_out_flight;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.travel_trust_money_out_refund CLONE data_vault_mvp.finance.travel_trust_money_out_refund;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.travel_trust_money_in CLONE data_vault_mvp.finance.travel_trust_money_in;


ALTER TABLE data_vault_mvp_dev_robin.finance.travel_trust_money_out_flight
    RENAME COLUMN settlement_amount TO transaction_amount;
ALTER TABLE data_vault_mvp_dev_robin.finance.travel_trust_money_out_refund
    RENAME COLUMN settlement_amount TO transaction_amount;

UPDATE data_vault_mvp_dev_robin.finance.stripe_refund sr
SET sr.payment_service_provider_transaction_type = 'refunds';

SELECT *
FROM data_vault_mvp_dev_robin.finance.travel_trust_money_out ttmo
WHERE ttmo.booking_id = 'TB-21897914';

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.finance.travel_trust_money_out');

CREATE OR REPLACE TABLE travel_trust_money_out
(

    transaction_id                                   VARCHAR(16777216) NOT NULL,
    transaction_tstamp                               TIMESTAMP_NTZ(9),
    payment_service_provider                         VARCHAR(16777216),
    payment_service_provider_transaction_type        VARCHAR(16777216),
    cashflow_direction                               VARCHAR(16777216),
    cashflow_type                                    VARCHAR(16777216),
    transaction_amount                               NUMBER(13, 4),
    transaction_currency                             VARCHAR(16777216),
    pre_settlement_travel_trust_money_in_cumulative  NUMBER(13, 4),
    pre_settlement_travel_trust_money_out_cumulative NUMBER(13, 4),
    pre_settlement_travel_trust_booking_balance      NUMBER(13, 4),
    settlement_amount                                NUMBER(13, 4),
    post_settlement_travel_trust_booking_balance     NUMBER(13, 4),
    booking_id                                       VARCHAR(16777216),
    travel_date                                      DATE,
    return_date                                      DATE,
    PRIMARY KEY (transaction_id)
);

self_describing_task --include 'se/finance/cash_flow/travel_trust_money_out.py'  --method 'run' --start '2021-06-21 00:00:00' --end '2021-06-21 00:00:00'

SELECT *
FROM se_dev_robin.finance.travel_trust_money_out;

SELECT *
FROM data_vault_mvp.dwh.tb_booking tb;

self_describing_task --include '/dv/finance/cash_flow/travel_trust/travel_trust_money_out_flight.py'  --method 'run' --start '2021-06-21 00:00:00' --end '2021-06-21 00:00:00'
self_describing_task --include '/dv/finance/cash_flow/travel_trust/travel_trust_money_out_refund.py'  --method 'run' --start '2021-06-21 00:00:00' --end '2021-06-21 00:00:00'
self_describing_task --include '/dv/finance/cash_flow/travel_trust/travel_trust_money_out_travelled.py'  --method 'run' --start '2021-06-21 00:00:00' --end '2021-06-21 00:00:00'
self_describing_task --include 'dv/finance/cash_flow/travel_trust/travel_trust_money_in.py'  --method 'run' --start '2021-06-21 00:00:00' --end '2021-06-21 00:00:00'
self_describing_task --include '/dv/finance/cash_flow/travel_trust/travel_trust_money_out.py'  --method 'run' --start '2021-06-21 00:00:00' --end '2021-06-21 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.travel_trust_money_in CLONE data_vault_mvp.finance.travel_trust_money_in;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.travel_trust_money_out_flight CLONE data_vault_mvp.finance.travel_trust_money_out_flight;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.travel_trust_money_out_refund CLONE data_vault_mvp.finance.travel_trust_money_out_refund;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.travel_trust_money_out_travelled CLONE data_vault_mvp.finance.travel_trust_money_out_travelled;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.enett_van_settlement_report CLONE data_vault_mvp.finance.enett_van_settlement_report
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.aviate_transactions CLONE data_vault_mvp.finance.aviate_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item CLONE data_vault_mvp.dwh.tb_order_item;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog CLONE data_vault_mvp.dwh.tb_order_item_changelog;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.stripe_cash_on_booking CLONE data_vault_mvp.finance.stripe_cash_on_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.tb_order_payment_coupon CLONE data_vault_mvp.finance.tb_order_payment_coupon;



SELECT ttmi.transaction_tstamp::DATE AS transaction_date,
       SUM(ttmi.transaction_amount)  AS money_in_total
FROM se.finance.travel_trust_money_in ttmi
GROUP BY 1
ORDER BY 1;


SELECT *
FROM data_vault_mvp_dev_robin.finance.travel_trust_money_in ttmi
WHERE ttmi.transaction_tstamp::DATE = '2021-06-21';


SELECT transaction_tstamp::DATE AS transaction_date,
       SUM(transaction_amount)
FROM se.finance.travel_trust_money_in ttmi
WHERE ttmi.transaction_tstamp::DATE = CURRENT_DATE - 1
GROUP BY 1;


SELECT ttmo.transaction_tstamp::DATE AS transaction_date,
       SUM(ttmo.settlement_amount)   AS money_out_total
FROM se.finance.travel_trust_money_out ttmo
WHERE ttmo.transaction_tstamp::DATE = CURRENT_DATE - 1
GROUP BY 1;

SELECT ttmo.transaction_tstamp::DATE AS transaction_date,
       SUM(ttmo.settlement_amount)   AS money_out_total
FROM se.finance.travel_trust_money_out ttmo
WHERE ttmo.transaction_tstamp::DATE = CURRENT_DATE - 1
GROUP BY 1;

SELECT *
FROM se_dev_robin.bi.daily_spvs_bookings;
SELECT *
FROM se_dev_robin.bi.daily_spv_weight;

self_describing_task --include 'se/bi/scv/daily_spvs_bookings.py'  --method 'run' --start '2021-06-22 00:00:00' --end '2021-06-22 00:00:00'


SELECT * FROM se.finance.travel_trust_money_in ttmi;
SELECT * FROM data_vault_mvp.finance.travel_trust_money_out_refund ttmor;

self_describing_task --include 'dv/finance/cash_flow/travel_trust/travel_trust_money_out.py'  --method 'run' --start '2021-06-23 00:00:00' --end '2021-06-23 00:00:00'
self_describing_task --include 'se/finance/cash_flow/travel_trust_money_out.py'  --method 'run' --start '2021-06-23 00:00:00' --end '2021-06-23 00:00:00'