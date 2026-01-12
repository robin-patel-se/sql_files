SELECT
	IFF((
			-- Google Adwords UAC campaigns
			LOWER(stai.partner_name) = 'google adwords'
				AND LOWER(stai.campaign) LIKE '%uac%'
				AND LOWER(stai.campaign) LIKE '%app_install%'
			)
			OR (
			-- Apple Search Ads install campaigns
			LOWER(stai.partner_name) = 'apple search ads'
			),
		IFF(LOWER(stai.campaign) LIKE '%brand%', 'PPC - Brand', 'PPC - Non Brand')
		, 'Other')
		AS install_channel,
	*
FROM se.data.scv_touched_app_installs stai

;

USE ROLE personal_role__robinpatel
;

CREATE OR REPLACE VIEW hygiene_vault_mvp_dev_robin.snowplow.event_stream AS
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification AS
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification
;

SELECT
	MIN(event_tstamp)
FROM se.data.scv_touched_app_installs stai
; -- 2020-01-29 14:01:20.000000000

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/04_module_touched_app_installs.py'  --method 'run' --start '2020-01-01 00:00:00' --end '2020-01-01 00:00:00'

CREATE OR REPLACE VIEW latest_vault_dev_robin.cms_mysql.affiliate AS
SELECT *
FROM latest_vault.cms_mysql.affiliate
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer AS
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer
;

CREATE OR REPLACE VIEW latest_vault_dev_robin.cms_mysql.territory AS
SELECT *
FROM latest_vault.cms_mysql.territory
;

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs mtai
;

------------------------------------------------------------------------------------------------------------------------
-- check how these channel shifts affect attribution

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/07_touch_attribution/01_module_touch_attribution.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- compare current session channels with dev

USE WAREHOUSE pipe_xlarge
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_20240126 CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;
-- prod se brand sessions by channel
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS prod_count
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
			   ON mtmc.touch_id = mtba.touch_id
WHERE mtba.touch_se_brand = 'SE Brand'
  AND mtba.touch_start_tstamp < CURRENT_DATE
GROUP BY 1
;
-- dev se brand sessions by channel
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS dev_count
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
			   ON mtmc.touch_id = mtba.touch_id
WHERE mtba.touch_se_brand = 'SE Brand'
  AND mtba.touch_start_tstamp < CURRENT_DATE
GROUP BY 1
;

-- 10k sessions appearing between dev and prod


;

-- no touch ids in dev that aren't in prod
SELECT
	touch_id
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
EXCEPT
SELECT
	touch_id
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
;

-- checking session numbers in marketing channel
SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
;
--1009516958

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
;

--1009527410


-- marketing channel in dev has the 10K more sessions
SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
;

-- 1009516958 -- which matches prod in marketing channel

-- check for no dupes
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
QUALIFY COUNT(*) OVER (PARTITION BY touch_id) > 1
;


-- multiple installations might occur on the same session
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touched_app_installs mtai
QUALIFY COUNT(*) OVER (PARTITION BY touch_id) > 1
;

-- Found app installations dupe was blowing out more sessions
-- created dedupe step


------------------------------------------------------------------------------------------------------------------------
-- archiving data for future investigation, scv changes daily
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_20240124 CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_20240124 CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs_20240124 CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_20240124_dev CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs_20240124_dev CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
;

-- https://docs.google.com/spreadsheets/d/1kqo3y5O3EWYLwERR0hW6FtXIiOqAk43xe1-kfoAZs70/edit#gid=160130711

-- shows expected shifts in channels
-- direct minus 30K
-- PPC - Brand +34.5K
-- PPC - Non Brand CPL +5.8K


SELECT
	DATE_TRUNC(MONTH, event_tstamp),
	COUNT(*)
FROM se.data.scv_touched_app_installs stai
GROUP BY 1
;


SELECT
	DATE_TRUNC(MONTH, event_tstamp),
	mtai.app_install_channel,
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs mtai
GROUP BY 1, 2
;
-- despite loads of app installs there are only a small amount of sessions being channelled

WITH
	dedupe_installs AS (
		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs mtai
		QUALIFY ROW_NUMBER() OVER (PARTITION BY mtai.touch_id ORDER BY mtai.event_tstamp) = 1
	)
SELECT
	di.event_hash,
	di.touch_id,
	di.event_tstamp,
	di.event_category,
	di.event_subcategory,
	di.app_install_channel,
	mtmc.touch_mkt_channel,
	mtmc.*

FROM dedupe_installs di
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc ON
	di.touch_id = mtmc.touch_id
;

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge
;
-- prod sessions by channel
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS prod_count
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
			   ON mtmc.touch_id = mtba.touch_id
WHERE mtba.touch_start_tstamp < CURRENT_DATE
GROUP BY 1
;

-- dev sessions by channel
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS dev_count
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
			   ON mtmc.touch_id = mtba.touch_id
WHERE mtba.touch_start_tstamp < CURRENT_DATE
GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------
-- checking session counts on se brand with last non direct attribution

-- prod se brand sessions by channel last non direct
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS prod_count
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
			   ON mtba.touch_id = mta.touch_id AND mta.attribution_model = 'last non direct'
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
			   ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp < CURRENT_DATE
  AND mtba.touch_se_brand = 'SE Brand'
GROUP BY 1
;

-- dev se brand sessions by channel last non direct
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS dev_count
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
			   ON mtba.touch_id = mta.touch_id AND mta.attribution_model = 'last non direct'
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
			   ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp < CURRENT_DATE
  AND mtba.touch_se_brand = 'SE Brand'
GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------
-- checking changes to attribution since 2023

-- prod se brand sessions by channel last non direct 2023
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS prod_count
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
			   ON mtba.touch_id = mta.touch_id AND mta.attribution_model = 'last non direct'
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
			   ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE
  AND mtba.touch_se_brand = 'SE Brand'
GROUP BY 1
;

-- dev se brand sessions by channel last non direct 2023
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS dev_count
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
			   ON mtba.touch_id = mta.touch_id AND mta.attribution_model = 'last non direct'
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
			   ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE
  AND mtba.touch_se_brand = 'SE Brand'
GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------
-- checking changes to attribution since 2023

-- prod se brand sessions by channel last paid 2023
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS prod_count
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
			   ON mtba.touch_id = mta.touch_id AND mta.attribution_model = 'last paid'
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
			   ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE
  AND mtba.touch_se_brand = 'SE Brand'
GROUP BY 1
;

-- dev se brand sessions by channel last paid 2023
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS dev_count
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
			   ON mtba.touch_id = mta.touch_id AND mta.attribution_model = 'last paid'
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
			   ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE
  AND mtba.touch_se_brand = 'SE Brand'
GROUP BY 1
;


------------------------------------------------------------------------------------------------------------------------
-- checking changes to attribution since 2023
USE ROLE personal_role__robinpatel
;

USE WAREHOUSE pipe_xlarge
;
-- prod se brand sessions by channel last click 2023
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS prod_count
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
			   ON mtba.touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE
  AND mtba.touch_se_brand = 'SE Brand'
GROUP BY 1
;

-- dev se brand sessions by channel last click 2023
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS dev_count
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
			   ON mtba.touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE
  AND mtba.touch_se_brand = 'SE Brand'
GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------

-- prod travelist sessions by channel
SELECT

	mtmc.touch_mkt_channel,
	COUNT(*) AS prod_count
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
			   ON mtmc.touch_id = mtba.touch_id
WHERE mtba.touch_se_brand = 'Travelist'
  AND mtba.touch_start_tstamp < CURRENT_DATE
GROUP BY 1
;
-- dev travelist sessions by channel
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS dev_count
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
			   ON mtmc.touch_id = mtba.touch_id
WHERE mtba.touch_se_brand = 'Travelist'
  AND mtba.touch_start_tstamp < CURRENT_DATE
GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution mta mtmc mtai
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_app_installs_20240206 CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_20240206 CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_app_installs_20240206 CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;


DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
WHERE app_install_channel IS DISTINCT FROM 'Other'
;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.utm_campaign LIKE '%App_Install%'
;


------------------------------------------------------------------------------------------------------------------------
-- dev se brand sessions by channel last paid 2023
USE WAREHOUSE pipe_xlarge
;

SELECT
	mtmc.touch_mkt_channel,
	COUNT(*) AS dev_count
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
			   ON mtba.touch_id = mta.touch_id AND mta.attribution_model = 'last paid'
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
			   ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE
  AND mtba.touch_se_brand = 'SE Brand'
GROUP BY 1
;


------------------------------------------------------------------------------------------------------------------------


SELECT
	ses.event_hash,
	ses.event_tstamp,
	ses.se_user_id,
	ff.value::VARCHAR,
	*
FROM se.data_pii.scv_event_stream ses,
	 LATERAL FLATTEN(INPUT => contexts_com_secretescapes_user_state_context_1[0]:feature_flags, OUTER => TRUE) ff
WHERE ses.contexts_com_secretescapes_user_state_context_1[0]['feature_flags']::VARCHAR IS NOT NULL
  AND ses.event_tstamp >= CURRENT_DATE - 1
  AND ff.value LIKE 'abtest.presearchtoggle%'

------------------------------------------------------------------------------------------------------------------------


	./

scripts/
mwaa-cli production 'dags backfill --mark-success --start-date "2018-01-01 03:00:00" --end-date "2018-01-01 03:00:00" single_customer_view__daily_at_03h00'

------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;

CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touched_app_installs_20240208 CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_20240208 CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20240208 CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

------------------------------------------------------------------------------------------------------------------------

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

USE ROLE personal_role__robinpatel
;

SELECT
	mtmc.touch_mkt_channel,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
GROUP BY 1
;

SELECT
	mtmc.touch_mkt_channel,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_20240208 mtmc
GROUP BY 1
;

SELECT *
FROM heap_main_production.heap.pageviews p
;

