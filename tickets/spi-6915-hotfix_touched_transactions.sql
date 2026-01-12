USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.scv.04_events_of_interest.02_module_touched_transactions.py' \
    --method 'run' \
    --start '2025-01-08 00:00:00' \
    --end '2025-01-08 00:00:00'



SELECT
	stt.event_tstamp::DATE AS date,
	COUNT(*)
FROM se.data.scv_touched_transactions stt
WHERE stt.event_tstamp >= CURRENT_DATE - 5
GROUP BY 1


-- jobs reran:
-- tableau__demand_model__daily_at_04h30
-- triggers__tableau__demand_model__daily_at_05h00
-- dwh__iterable_crm_reporting_insertions__daily_at_04h30
-- dwh__iterable_crm_reporting__daily_at_04h30
-- bi__hotel_plus_bookings_ab_test__daily_at_09h00
-- bi__customer_yearly_booking__daily_at_04h30
-- tableau__transaction_model__daily_at_04h30
-- triggers__tableau__transaction_model__daily_at_04h30
-- triggers__dbt__product_analytics__product_analytics_daily__daily_at_06h00
-- dwh__epsilon_conversions__daily_at_08h00


