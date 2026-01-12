dataset_task \
--include 'finance_gsheets.safi_airlines' \
--operation ExtractOperation \
--method 'run' \
--start '2020-02-18 00:00:00' \
--end '2020-02-18 00:00:00'

dataset_task \
--include 'finance_gsheets.safi_airlines' \
--operation RawIngestOperation \
--method 'run' \
--start '2020-02-18 00:00:00' \
--end '2020-02-18 00:00:00'

dataset_task \
--include 'finance_gsheets.safi_airlines' \
--operation ProductionIngestOperation \
--method 'run' \
--start '2020-02-18 00:00:00' \
--end '2020-02-18 00:00:00'

SELECT safi_airlines.dataset_name,
       safi_airlines.dataset_source,
       safi_airlines.schedule_interval,
       safi_airlines.schedule_tstamp,
       safi_airlines.run_tstamp,
       safi_airlines.loaded_at,
       safi_airlines.filename,
       safi_airlines.file_row_number,
       safi_airlines.extract_metadata,
       safi_airlines.airline_id,
       safi_airlines.airline_name,
       safi_airlines.safi_start_date,
       safi_airlines.safi_end_date,
       safi_airlines.tickets,
       safi_airlines.turnover_currency,
       safi_airlines.turnover,
       safi_airlines.maximum_policy_currency,
       safi_airlines.maximum_policy_limit
FROM raw_vault_mvp_dev_robin.finance_gsheets.safi_airlines;


python biapp/
bau/
hygiene/
gen_hygiene_files.py \
    --data_source finance_gsheets \
    --name safi_airlines \
    --primary_key_cols airline_id \

self_describing_task --include 'staging/hygiene/finance_gsheets/safi_airlines.py'  --method 'run' --start '2021-05-17 00:00:00' --end '2021-05-17 00:00:00'

SELECT *
FROM hygiene_vault_mvp_dev_robin.finance_gsheets.safi_airlines;

self_describing_task --include 'staging/hygiene_snapshots/finance_gsheets/safi_airlines.py'  --method 'run' --start '2021-05-17 00:00:00' --end '2021-05-17 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.safi_airlines;



dataset_task \
--include 'tableau_gsheets.tableau_channel_costs' \
--operation ExtractOperation \
--method 'run' \
--start '2020-02-18 00:00:00' \
--end '2020-02-18 00:00:00'

dataset_task \
--include 'tableau_gsheets.tableau_channel_costs' \
--operation RawIngestOperation \
--method 'run' \
--start '2020-02-18 00:00:00' \
--end '2020-02-18 00:00:00'

dataset_task \
--include 'tableau_gsheets.tableau_channel_costs' \
--operation ProductionIngestOperation \
--method 'run' \
--start '2020-02-18 00:00:00' \
--end '2020-02-18 00:00:00'

/
USERS/
robin/
myrepos/
one-DATA-pipeline/
biapp/
manifests_mvp/
incoming/
tableau_gsheets/
tableau_channel_costs.json

python biapp/
bau/
hygiene/
gen_hygiene_files.py \
    --data_source tableau_gsheets \
    --name tableau_channel_costs \
    --primary_key_cols event_date, original_affiliate_territory, channel \

self_describing_task --include 'staging/hygiene/tableau_gsheets/tableau_channel_costs.py'  --method 'run' --start '2021-05-17 00:00:00' --end '2021-05-17 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/tableau_gsheets/tableau_channel_costs.py'  --method 'run' --start '2021-05-17 00:00:00' --end '2021-05-17 00:00:00'


self_describing_task --include 'se/data/udfs/udf_functions.py'  --method 'run' --start '2021-05-17 00:00:00' --end '2021-05-17 00:00:00';


SELECT *
FROM hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines;



SELECT *
FROM se.data.tb_order_item toi
WHERE toi.order_item_type = 'FLIGHT';

SELECT MIN(created_at_dts)
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderevent oo; --2019-04-01 13:22:34.521460000

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderevent CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderevent;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_flightorderitem CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_flightorderitem;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.flights_flightproduct CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.flights_flightproduct;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.tb_rates CLONE data_vault_mvp.fx.tb_rates;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_person CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_person;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderproperty CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderproperty;
self_describing_task --include '/dv/dwh/transactional/tb_order_item_changelog.py'  --method 'run' --start '2019-04-01 00:00:00' --end '2019-04-01 00:00:00'
airflow backfill --start_date '2019-04-01 03:00:00' --end_date '2019-04-01 03:00:00' --task_regex '.*' dwh__transactional__tb_order_item_changelog__daily_at_03h00

DROP TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_order_item_changelog;

self_describing_task --include 'se/data/dwh/tb_order_item_changelog.py'  --method 'run' --start '2021-05-18 00:00:00' --end '2021-05-18 00:00:00'

self_describing_task --include 'dv/dwh/transactional/tb_order_item.py'  --method 'run' --start '2021-05-18 00:00:00' --end '2021-05-18 00:00:00'

self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2021-05-18 00:00:00' --end '2021-05-18 00:00:00'

self_describing_task --include 'dv/finance/cash_flow/travel_trust_money_in.py'  --method 'run' --start '2021-05-18 00:00:00' --end '2021-05-18 00:00:00'

self_describing_task --include 'dv/finance/cash_flow/travel_trust_money_in_snapshot.py'  --method 'run' --start '2021-05-18 00:00:00' --end '2021-05-18 00:00:00'

self_describing_task --include 'se/finance/cash_flow/travel_trust_booking_components.py'  --method 'run' --start '2021-05-18 00:00:00' --end '2021-05-18 00:00:00'

self_describing_task --include 'se/finance/cash_flow/travel_trust_money_in.py'  --method 'run' --start '2021-05-18 00:00:00' --end '2021-05-18 00:00:00'

self_describing_task --include 'se/data/dwh/tb_order_item.py'  --method 'run' --start '2021-05-18 00:00:00' --end '2021-05-18 00:00:00'

SELECT DATE_TRUNC(MONTH, b.touch_start_tstamp)                    AS month,
       se.data.platform_from_touch_experience(b.touch_experience) AS platform,
       COUNT(DISTINCT b.attributed_user_id_hash)                  AS active_users
FROM se.data.scv_touch_basic_attributes b
         LEFT JOIN se.data.scv_touch_marketing_channel t ON b.touch_id = t.touch_id
WHERE t.touch_affiliate_territory IN ('UK', 'IT', 'DE')
  AND b.touch_start_tstamp::DATE >= '2021-01-01'
GROUP BY 1, 2;

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.is_server_side_event
  AND es.event_tstamp >= CURRENT_DATE;

SELECT COUNT(*)
FROM data_vault_mvp.dwh.tb_order_item_changelog toic;


airflow backfill --start_date '2021-05-18 03:00:00' --end_date '2021-05-18 03:00:00' --task_regex '.*' dwh__transactional__tb_order_item__daily_at_03h00

airflow backfill --start_date '2021-05-18 03:00:00' --end_date '2021-05-18 03:00:00' --task_regex '.*' finance__cash_flow__travel_trust_money_in__daily_at_03h00

airflow backfill --start_date '2021-05-18 03:00:00' --end_date '2021-05-18 03:00:00' --task_regex '.*' dwh__transactional__booking__daily_at_03h00

airflow backfill --start_date '2021-05-18 04:00:00' --end_date '2021-05-18 04:00:00' --task_regex '.*' finance__cash_flow__travel_trust_money_in_snapshot__daily_at_04h00

airflow backfill --start_date '2021-05-18 07:00:00' --end_date '2021-05-18 07:00:00' --task_regex '.*' se_data_object_creation__daily_at_07h00

airflow backfill --start_date '2021-05-18 07:00:00' --end_date '2021-05-18 07:00:00' --task_regex '.*' se_finance_object_creation__daily_at_07h00

SELECT *
FROM data_vault_mvp.finance.travel_trust_money_in;


SELECT scob.transaction_id,
       scob.transaction_tstamp,
       scob.payment_service_provider,
       scob.payment_service_provider_transaction_type,
       scob.cashflow_direction,
       scob.cashflow_type,
       scob.transaction_amount,
       scob.transaction_currency,
       scob.orders_paymemt_classification,
       scob.booking_id,
       tb.travel_date,
       tb.return_date,
       tb.order_flight_carriers
FROM data_vault_mvp.finance.stripe_cash_on_booking scob
         LEFT JOIN data_vault_mvp.dwh.tb_booking tb ON tb.booking_id = scob.booking_id
WHERE se.finance.travel_trust_booking(scob.tb_order_id)

UNION ALL
SELECT topc.transaction_id,
       topc.transaction_tstamp,
       topc.payment_service_provider,
       topc.payment_service_provider_transaction_type,
       topc.cashflow_direction,
       topc.cashflow_type,
       topc.amount,
       topc.currency,
       topc.orders_paymemt_classification,
       topc.booking_id,
       tb.travel_date,
       tb.return_date,
       tb.order_flight_carriers
FROM data_vault_mvp.finance.tb_order_payment_coupon topc
         LEFT JOIN data_vault_mvp.dwh.tb_booking tb ON tb.booking_id = topc.booking_id
WHERE se.finance.travel_trust_booking(topc.tb_order_id)
  AND topc.coupon_is_from_cash_credit IS DISTINCT FROM FALSE -- only put transactions that can be traced back to cash into trust
;

SELECT *
FROM data_vault_mvp.finance.aviate_transactions av QUALIFY COUNT(*) OVER (PARTITION BY av.transaction_id) > 1;

SELECT *
FROM se.data.tb_order_item toi
WHERE toi.flight_reservation_number = 'OUJPEJ'


------------------------------------------------------------------------------------------------------------------------

SELECT evsr.transaction_id,
       evsr.transaction_tstamp,
       evsr.payment_service_provider,
       evsr.payment_service_provider_transaction_type,
       evsr.cashflow_direction,
       evsr.cashflow_type,
       'flight paid / protected' AS money_out_type,
       evsr.transaction_amount   AS settlement_amount,
       evsr.transaction_currency AS settlement_currency,
       evsr.booking_id,
       tb.travel_date,
       tb.return_date,
       'SUVC'                    AS protection_type, -- all enett are SUVC protected
       evsr.pnr
FROM data_vault_mvp.finance.enett_van_settlement_report evsr
         INNER JOIN se.data.tb_booking tb ON evsr.tb_order_id = tb.order_id
WHERE se.finance.travel_trust_booking(evsr.tb_order_id)
  AND evsr.cashflow_direction = 'money out';


SELECT a.transaction_id,
       a.transaction_tstamp,
       a.payment_service_provider,
       a.payment_service_provider_transaction_type,
       a.cashflow_direction,
       a.cashflow_type,
       'flight paid / protected' AS money_out_type,
       a.transaction_amount      AS settlement_amount,
       a.transaction_currency    AS settlement_currency,
       a.booking_id,
       tb.travel_date,
       tb.return_date,
       'SAFI'                    AS protection_type, --filtering aviate transactions for when they are safi protected
       a.pnr_ref                 AS pnr
FROM data_vault_mvp.finance.aviate_transactions a
         INNER JOIN data_vault_mvp.dwh.tb_order_item oi ON a.order_item_id = oi.order_item_id
         INNER JOIN se.data.tb_booking tb ON a.tb_order_id = tb.order_id
WHERE se.finance.travel_trust_booking(a.tb_order_id)
  AND se.finance.safi_protected_airline(oi.flight_validating_airline_id) -- only want to take money out from trust for safi protected airlines
  AND a.cashflow_direction = 'money out';

self_describing_task --include 'se/finance/udfs/udf_functions.py'  --method 'run' --start '2021-05-19 00:00:00' --end '2021-05-19 00:00:00'
self_describing_task --include 'se/data/udfs/udf_functions.py'  --method 'run' --start '2021-05-19 00:00:00' --end '2021-05-19 00:00:00'


self_describing_task --include 'dv/finance/cash_flow/travel_trust_money_out_flight.py'  --method 'run' --start '2021-05-19 00:00:00' --end '2021-05-19 00:00:00'



SELECT *
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.coupons_coupon cc
         INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.coupons_couponconfig c;

self_describing_task --include 'se/finance/cash_flow/travel_trust_money_out.py'  --method 'run' --start '2021-05-19 00:00:00' --end '2021-05-19 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.finance.travel_trust_money_out_flight;

