/*
Single customer view was trimmed as part of project Al dente to data AFTER 1st Dec 2022.

In order to ensure that email sends since the launch of iterable 2021-11-03 has scv data attached to it
we need to use the historical scv data tables to populate them.

In order to ensure we don't make hard coded references to historical scv data in our code base we can run scv related
iterable crm reporting modules in dev and swap them out with production tables.

3 modules use scv: clicks, spvs and bookings.
We don't need to process clicks with historical as it only uses scv for inappclicks which we didn't have in 2022.

So just need to populate for spvs and bookings.
*/

SELECT
	MIN(icrs.send_event_date)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs icrs
WHERE icrs.spvs_1d_lc > 0
;

-- spvs

USE ROLE personal_role__robinpatel
;

USE WAREHOUSE customer_insight_2xlarge;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
-- CLONE data_vault_mvp.dwh.iterable_crm_reporting__sends;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_module_touch_attribution
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

SELECT
	MIN(icrb.send_event_date)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__bookings icrb
WHERE icrb.bookings_1d_lc > 0
;


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
AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_module_touched_transactions
;

-- already done these as part of spvs
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
-- 	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
-- ;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
-- 	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
-- ;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
-- 	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
-- ;

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

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__bookings.py' -m --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'


./scripts/mwaa-cli production "dags backfill --start-date '2021-11-03 04:30:00' --end-date '2021-11-03 04:30:00' --donot-pickle dwh__iterable_crm_reporting__migration__daily_at_04h30"
-- mark dag run success, run all modules except spv and booking


-- USE ROLE pipelinerunner;
-- DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__sends;
-- DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__migration;

CREATE TABLE data_vault_mvp.dwh.iterable_crm_reporting__spvs CLONE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs;
CREATE TABLE data_vault_mvp.dwh.iterable_crm_reporting__bookings CLONE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__bookings;

./scripts/mwaa-cli production "dags backfill --start-date '2025-07-23 04:30:00' --end-date '2025-07-23 04:30:00' --donot-pickle dwh__iterable_crm_reporting__migration__daily_at_04h30"

SELECT count(*) FROm data_vault_mvp.dwh.iterable_crm_reporting__sends;
10,227,801,940
SELECT count(*) FROm data_vault_mvp.dwh.iterable_crm_reporting__opens;
2,975,758,110
SELECT count(*) FROm data_vault_mvp.dwh.iterable_crm_reporting__clicks;
92,625,335
SELECT COUNT(*) FROm se.data.iterable_crm_reporting__migration;
10,227,801,940
SELECT COUNT(*) FROm se.data.iterable_crm_reporting icr;
10,213,389,358


SELECT COUNT(*) FROM latest_vault.iterable.email_send es
9,912,857,463
SELECT COUNT(*) FROM data_vault_mvp.dwh.iterable_crm_reporting__sends__step03__enrich_sends
8,037,229,639

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__sends;
DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__clicks;
DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__opens;
DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__unsubs;
DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__migration;


USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__bookings
CLONE data_vault_mvp.dwh.iterable_crm_reporting__bookings;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__clicks
CLONE data_vault_mvp.dwh.iterable_crm_reporting__clicks;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__opens
CLONE data_vault_mvp.dwh.iterable_crm_reporting__opens;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
CLONE data_vault_mvp.dwh.iterable_crm_reporting__sends;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs
CLONE data_vault_mvp.dwh.iterable_crm_reporting__spvs;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__unsubs
CLONE data_vault_mvp.dwh.iterable_crm_reporting__unsubs;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
CLONE data_vault_mvp.dwh.user_attributes;

CREATE SCHEMA IF NOT EXISTS dbt_dev_robin.bi_customer_insight;

CREATE OR REPLACE TRANSIENT TABLE dbt_dev_robin.bi_customer_insight.ci_rfv_segments_historical_weekly
CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_weekly;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__migration
CLONE data_vault_mvp.dwh.iterable_crm_reporting__migration;


./scripts/mwaa-cli production "dags backfill --start-date '2025-07-27 04:30:00' --end-date '2025-07-27 04:30:00' --donot-pickle dwh__iterable_crm_reporting__migration__daily_at_04h30"

          ALTER TABLE data_vault_mvp.dwh.iterable_crm_reporting__migration RENAME TO data_vault_mvp.dwh.iterable_crm_reporting;



./scripts/mwaa-cli production "dags backfill --m --start-date '2021-11-03 04:30:00' --end-date '2021-11-03 04:30:00' --donot-pickle dwh__iterable_crm_reporting__daily_at_04h30"


          select
  ubbd.shiro_user_id
  ,ubbd.date_value
  ,sum(email_sends) as email_sends
  ,sum(email_opens_28d) as email_opens -- including uncapped versions as we're not doing a time based comparison so should be fair
  ,sum(email_clicks_28d) as email_clicks
  ,sum(case when lower(email_type) = 'newsletter' then email_sends end) as newsletter_sends
  ,sum(case when lower(email_type) = 'trigger' then email_sends end) as trigger_sends
  ,sum(case when lower(email_type) = 'lifecycle' then email_sends end) as lifecycle_sends
  ,sum(case when lower(email_type) = 'newsletter' then email_opens_28d end) as newsletter_opens
  ,sum(case when lower(email_type) = 'trigger' then email_opens_28d end) as trigger_opens
  ,sum(case when lower(email_type) = 'lifecycle' then email_opens_28d end) as lifecycle_opens
  ,sum(case when lower(email_type) = 'newsletter' then email_clicks_28d end) as newsletter_clicks
  ,sum(case when lower(email_type) = 'trigger' then email_clicks_28d end) as trigger_clicks
  ,sum(case when lower(email_type) = 'lifecycle' then email_clicks_28d end) as lifecycle_clicks
  ,sum(email_opens_1d) as email_opens_1d -- going with uncapped versions as we're not doing a time based comparison so should be fair
  ,sum(email_clicks_1d) as email_clicks_1d
    ,sum(case when lower(email_type) = 'newsletter' then email_opens_1d end) as newsletter_opens_1d
  ,sum(case when lower(email_type) = 'trigger' then email_opens_1d end) as trigger_opens_1d
  ,sum(case when lower(email_type) = 'lifecycle' then email_opens_1d end) as lifecycle_opens_1d
  ,sum(case when lower(email_type) = 'newsletter' then email_clicks_1d end) as newsletter_clicks_1d
  ,sum(case when lower(email_type) = 'trigger' then email_clicks_1d end) as trigger_clicks_1d
  ,sum(case when lower(email_type) = 'lifecycle' then email_clicks_1d end) as lifecycle_clicks_1d
  ,sum(email_opens_7d) as email_opens_7d -- adding in 7 day cap to compare to the 1y static dimensions in the user base
  ,sum(email_clicks_7d) as email_clicks_7d
  ,sum(case when lower(email_type) = 'newsletter' then email_opens_7d end) as newsletter_opens_7d
  ,sum(case when lower(email_type) = 'trigger' then email_opens_7d end) as trigger_opens_7d
  ,sum(case when lower(email_type) = 'lifecycle' then email_opens_7d end) as lifecycle_opens_7d
  ,sum(case when lower(email_type) = 'newsletter' then email_clicks_7d end) as newsletter_clicks_7d
  ,sum(case when lower(email_type) = 'trigger' then email_clicks_7d end) as trigger_clicks_7d
  ,sum(case when lower(email_type) = 'lifecycle' then email_clicks_7d end) as lifecycle_clicks_7d
  ,sum(email_unsubs_28d) as unsubs
  ,sum(bookings_1d_lnd) as email_bookings_1d_lnd_icr
  ,sum(bookings_7d_lnd) as email_bookings_7d_lnd_icr
  ,sum(bookings_28d_lnd) as email_bookings_lnd_icr
-- from DBT.BI_CUSTOMER_INSIGHT.ci_crm_total_base_frequency_test_enhanced_base ubbd
from DBT.BI_customer_insight.ci_crm_total_base_frequency_test_phase_4_enhanced_base ubbd
-- left join se.data.iterable_crm_reporting icr
left join dbt_dev.dbt_robinpatel_staging.base_dwh__iterable_crm_reporting icr
  on ubbd.shiro_user_id = icr.shiro_user_id
  and ubbd.date_value = icr.send_event_date
where crm_channel_type = 'email'
group by 1,2