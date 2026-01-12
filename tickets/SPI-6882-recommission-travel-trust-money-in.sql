------------------------------------------------------------------------------------------------------------------------
-- secret escapes

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.finance;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.worldpay_cash_on_booking
CLONE data_vault_mvp.finance.worldpay_cash_on_booking;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking
CLONE data_vault_mvp.dwh.se_booking;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit
CLONE data_vault_mvp.dwh.se_credit;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.stripe_cash_on_booking
CLONE data_vault_mvp.finance.stripe_cash_on_booking;

CREATE SCHEMA IF NOT EXISTS se_dev_robin.finance;

self_describing_task --include 'biapp/task_catalogue/se/data/user_defined_routines/user_defined_routines.py'  --method 'run' --start '2025-10-12 00:00:00' --end '2025-10-12 00:00:00'

-- CREATE OR REPLACE VIEW se_dev_robin.finance.iata_lookup_from_flight_carrier
-- AS SELECT * FROM se.finance.iata_lookup_from_flight_carrier;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.se_travel_trust_money_in
CLONE data_vault_mvp.finance.se_travel_trust_money_in;

self_describing_task --include 'se_travel_trust_money_in.py'  --method 'run' --start '2025-10-12 00:00:00' --end '2025-10-12 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- travelbird

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking
CLONE data_vault_mvp.dwh.tb_booking;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.finance;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.stripe_cash_on_booking
CLONE data_vault_mvp.finance.stripe_cash_on_booking;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.tb_order_payment_coupon
CLONE data_vault_mvp.finance.tb_order_payment_coupon;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.tb_travel_trust_money_in
CLONE data_vault_mvp.finance.tb_travel_trust_money_in;

self_describing_task --include 'biapp/task_catalogue/dv/finance/cash_flow/travel_trust/travelbird/tb_travel_trust_money_in.py'  --method 'run' --start '2025-10-12 00:00:00' --end '2025-10-12 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- snapshots
self_describing_task --include 'biapp/task_catalogue/dv/finance/cash_flow/travel_trust/secret_escapes/se_travel_trust_money_in_snapshot.py'  --method 'run' --start '2025-10-12 00:00:00' --end '2025-10-12 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/finance/cash_flow/travel_trust/travelbird/tb_travel_trust_money_in_snapshot.py'  --method 'run' --start '2025-10-12 00:00:00' --end '2025-10-12 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- se finance views
self_describing_task --include 'biapp/task_catalogue/se/finance/cash_flow/travel_trust_money_in.py'  --method 'run' --start '2025-10-12 00:00:00' --end '2025-10-12 00:00:00'
self_describing_task --include 'biapp/task_catalogue/se/finance/cash_flow/travel_trust_money_in_snapshot.py'  --method 'run' --start '2025-10-12 00:00:00' --end '2025-10-12 00:00:00'


SELECT
	ttmi.transaction_tstamp::DATE,
	COUNT(*)
FROM se.finance.travel_trust_money_in ttmi
GROUP BY 1

