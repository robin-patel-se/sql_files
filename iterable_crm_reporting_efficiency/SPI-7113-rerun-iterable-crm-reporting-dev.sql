------------------------------------------------------------------------------------------------------------------------
-- sends

-- module=/biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__sends.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iterable
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.app_push_send
	CLONE latest_vault.iterable.app_push_send
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.campaign
	CLONE latest_vault.iterable.campaign
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_send
	CLONE latest_vault.iterable.email_send
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.in_app_send
	CLONE latest_vault.iterable.in_app_send
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.web_push_send
	CLONE latest_vault.iterable.web_push_send
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
	CLONE data_vault_mvp.dwh.iterable_crm_reporting__sends
;

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__sends.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- opens

-- module=/biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__opens.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iterable
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.app_push_open
	CLONE latest_vault.iterable.app_push_open
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_open
	CLONE latest_vault.iterable.email_open
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.in_app_open
	CLONE latest_vault.iterable.in_app_open
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
-- CLONE data_vault_mvp.dwh.iterable_crm_reporting__sends;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__opens
	CLONE data_vault_mvp.dwh.iterable_crm_reporting__opens
;

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__opens
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__opens.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'


------------------------------------------------------------------------------------------------------------------------
-- clicks

-- module=/biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__clicks.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iterable
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_click
	CLONE latest_vault.iterable.email_click
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.in_app_click
	CLONE latest_vault.iterable.in_app_click
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.web_push_clicks
	CLONE latest_vault.iterable.web_push_clicks
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
-- CLONE data_vault_mvp.dwh.iterable_crm_reporting__sends;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_in_app_notification_events
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_in_app_notification_events
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__clicks
	CLONE data_vault_mvp.dwh.iterable_crm_reporting__clicks
;

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__clicks
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__clicks.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

------------------------------------------------------------------------------------------------------------------------
--unsubs

-- module=/biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__unsubs.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iterable
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_unsubscribe
	CLONE latest_vault.iterable.email_unsubscribe
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
-- CLONE data_vault_mvp.dwh.iterable_crm_reporting__sends;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__unsubs
	CLONE data_vault_mvp.dwh.iterable_crm_reporting__unsubs
;

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__unsubs
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__unsubs.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- spvs

-- module=/biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__spvs.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
-- CLONE data_vault_mvp.dwh.iterable_crm_reporting__sends;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs
	CLONE data_vault_mvp.dwh.iterable_crm_reporting__spvs
;

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__spvs.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- bookings

-- module=/biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__bookings.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
-- CLONE data_vault_mvp.dwh.iterable_crm_reporting__sends;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__bookings
	CLONE data_vault_mvp.dwh.iterable_crm_reporting__bookings
;

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__bookings
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__bookings.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- migration

-- module=/biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__migration.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__bookings
-- CLONE data_vault_mvp.dwh.iterable_crm_reporting__bookings;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__clicks
-- CLONE data_vault_mvp.dwh.iterable_crm_reporting__clicks;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__opens
-- CLONE data_vault_mvp.dwh.iterable_crm_reporting__opens;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
-- CLONE data_vault_mvp.dwh.iterable_crm_reporting__sends;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs
-- CLONE data_vault_mvp.dwh.iterable_crm_reporting__spvs;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__unsubs
-- CLONE data_vault_mvp.dwh.iterable_crm_reporting__unsubs;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

CREATE SCHEMA IF NOT EXISTS dbt_dev_robin.bi_customer_insight
;

CREATE OR REPLACE TRANSIENT TABLE dbt_dev_robin.bi_customer_insight.ci_rfv_segments_historical_weekly
	CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__migration
	CLONE data_vault_mvp.dwh.iterable_crm_reporting__migration
;


DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__migration
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__migration.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
