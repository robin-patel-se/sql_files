CREATE OR REPLACE FUNCTION collab.travel_trust.travel_trust_booking(order_id INT
                                                                   )
    RETURNS BOOLEAN
AS
$$
    --shortlist of bookings that need to be protected by trust
    --these are currently defined as any package booking that is sold in the UK
    --and include flights
    SELECT IFF(order_id IN (
        SELECT tb.order_id
        FROM data_vault_mvp.dwh.tb_booking tb
        WHERE tb.territory = 'UK'
          AND tb.booking_includes_flight
          AND tb.created_at_dts >= '2020-01-01'
    ), TRUE, FALSE) AS travel_trust_booking
$$

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.enett_van_settlement_report CLONE data_vault_mvp.finance.enett_van_settlement_report;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.aviate_transactions CLONE data_vault_mvp.finance.aviate_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item CLONE data_vault_mvp.dwh.tb_order_item;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.stripe_cash_on_booking CLONE data_vault_mvp.finance.stripe_cash_on_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.tb_order_payment_coupon CLONE data_vault_mvp.finance.tb_order_payment_coupon;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.stripe_refund CLONE data_vault_mvp.finance.stripe_refund;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.stripe_chargeback CLONE data_vault_mvp.finance.stripe_chargeback;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.svb_manual_refund CLONE data_vault_mvp.finance.svb_manual_refund;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.travel_trust_money_in CLONE data_vault_mvp.finance.travel_trust_money_in;


self_describing_task --include 'dv/finance/cash_flow/travel_trust/travel_trust_money_in.py'  --method 'run' --start '2021-07-05 00:00:00' --end '2021-07-05 00:00:00'
self_describing_task --include 'dv/finance/cash_flow/travel_trust/travel_trust_money_out_flight.py'  --method 'run' --start '2021-07-05 00:00:00' --end '2021-07-05 00:00:00'
self_describing_task --include 'dv/finance/cash_flow/travel_trust/travel_trust_money_out_refund.py'  --method 'run' --start '2021-07-05 00:00:00' --end '2021-07-05 00:00:00'
self_describing_task --include 'dv/finance/cash_flow/travel_trust/travel_trust_money_out_travelled.py'  --method 'run' --start '2021-07-05 00:00:00' --end '2021-07-05 00:00:00'
self_describing_task --include 'dv/finance/cash_flow/travel_trust/travel_trust_money_out.py'  --method 'run' --start '2021-07-05 00:00:00' --end '2021-07-05 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE collab.travel_trust.travel_trust_money_out COPY GRANTS CLONE data_vault_mvp_dev_robin.finance.travel_trust_money_out;
CREATE OR REPLACE TRANSIENT TABLE collab.travel_trust.travel_trust_money_in COPY GRANTS CLONE data_vault_mvp_dev_robin.finance.travel_trust_money_in;

GRANT SELECT ON TABLE collab.travel_trust.travel_trust_money_in TO ROLE personal_role__ezraphilips;
GRANT SELECT ON TABLE collab.travel_trust.travel_trust_money_in TO ROLE personal_role__ailiemcderment;
GRANT SELECT ON TABLE collab.travel_trust.travel_trust_money_in TO ROLE personal_role__gianniraftis;

--branch name temp_travel_trust_money_out

SELECT *
FROM data_vault_mvp_dev_robin.finance.travel_trust_money_in ttmi;
SELECT *
FROM data_vault_mvp_dev_robin.finance.travel_trust_money_out_flight;
SELECT *
FROM data_vault_mvp_dev_robin.finance.travel_trust_money_out_refund;
SELECT *
FROM data_vault_mvp_dev_robin.finance.travel_trust_money_out_travelled;
SELECT *
FROM data_vault_mvp_dev_robin.finance.travel_trust_money_out;


------------------------------------------------------------------------------------------------------------------------
--found instances where the money out settlement amount is negative: TB-21897559
SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs;
--money in
SELECT *
FROM collab.travel_trust.travel_trust_money_in ttmi
WHERE ttmi.booking_id = 'TB-21897559';
--money out
SELECT *
FROM data_vault_mvp_dev_robin.finance.travel_trust_money_out ttmo
WHERE ttmo.booking_id = 'TB-21897559';

SELECT GET_DDL('table', 'CREATE OR REPLACE TRANSIENT TABLE collab.travel_trust.travel_trust_money_out COPY GRANTS CLONE data_vault_mvp_dev_robin.finance.travel_trust_money_out;
CREATE OR REPLACE TRANSIENT TABLE collab.travel_trust.travel_trust_money_in COPY GRANTS CLONE data_vault_mvp_dev_robin.finance.travel_trust_money_in;');