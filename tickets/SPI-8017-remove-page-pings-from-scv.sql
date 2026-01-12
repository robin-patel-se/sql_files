/*
module=/biapp/task_catalogue/dv/dwh/scv/03_touchification/01_touchifiable_events.py make clones
*/

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.app_push_send_enhancement
	CLONE data_vault_mvp.single_customer_view_stg.app_push_send_enhancement
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events__prod
	CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events
;

/*
 self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/03_touchification/01_touchifiable_events.py'  --method 'run' --start '2022-12-01 00:00:00' --end '2022-12-01 00:00:00'
 */

-- prod
SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events__prod
-- 3020652393
-- dev
SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events -- 2988885143
;


USE ROLE personal_role__robinpatel
;

/*
module=/biapp/task_catalogue/dv/dwh/scv/03_touchification/02_01_utm_or_referrer_hostname_marker.py make clones
*/

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
	CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params
	CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
	CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname
;


/*
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/03_touchification/02_01_utm_or_referrer_hostname_marker.py'  --method 'run' --start '2025-11-18 00:00:00' --end '2025-11-18 00:00:00'
*/

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
	CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker
	CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker
;

/*
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/03_touchification/02_02_time_diff_marker.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'
*/


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
	CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker
-- 	CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker
-- ;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker
-- 	CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

/*
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/03_touchification/03_touchification.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'
*/


--prod
SELECT
	COUNT(*),
	COUNT(DISTINCT touch_id)
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
;
--dev
SELECT
	COUNT(*),
	COUNT(DISTINCT touch_id)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt
;

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

-- CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;
--
-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
-- CLONE hygiene_vault_mvp.snowplow.event_stream;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
	CLONE data_vault_mvp.dwh.se_sale
;

-- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
-- CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

/*
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/01_module_touched_spvs.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'
*/
-- prod
SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
-- dev
SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs mts


-- prod
SELECT
	event_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
WHERE mts.event_tstamp >= '2025-01-01'
GROUP BY event_tstamp::DATE
;
-- dev
SELECT
	event_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs mts
WHERE mts.event_tstamp >= '2025-01-01'
GROUP BY event_tstamp::DATE
;

-- there's a 347K diff but that is primarily yesterday (dev is running on -1 day)


/*
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/02_module_touched_transactions.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'
*/

-- prod
SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions mts
;
-- dev
SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions mts
;

/*
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/03_module_touched_searches.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'
*/
/*
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/04_module_touched_app_installs.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'
*/

USE ROLE personal_role__robinpatel
;

-- CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
--
-- CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;
--
-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
-- CLONE hygiene_vault_mvp.snowplow.event_stream;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.travelbird_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_orderproperty
	CLONE latest_vault.travelbird_mysql.orders_orderproperty
;

/*
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/06_module_touched_booking_form_views.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/07_module_touched_in_app_notification_events.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/08_module_touched_pay_button_clicks.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/09_module_events_of_interest.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/00_anomalous_user_dates.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'
*/


USE ROLE personal_role__robinpatel
;

-- CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow;
--
-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
-- CLONE hygiene_vault_mvp.snowplow.event_stream;
--
-- CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates
-- CLONE data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest
-- CLONE data_vault_mvp.single_customer_view_stg.module_events_of_interest;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
	CLONE data_vault_mvp.single_customer_view_stg.page_screen_enrichment
;

-- CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
-- CLONE data_vault_mvp.dwh.user_attributes;

-- prod
SELECT
	DATE_TRUNC(WEEK, mtba.touch_start_tstamp::DATE) AS week,
	mtba.touch_se_brand,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp >= '2024-01-01'
GROUP BY ALL
;

-- dev
SELECT
	DATE_TRUNC(WEEK, mtba.touch_start_tstamp::DATE) AS week,
	mtba.touch_se_brand,
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp >= '2024-01-01'
GROUP BY ALL
;

-- USE ROLE PERSONAL_ROLE__ROBINPATEL;

-- CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
--
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params
	CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
	CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname
;

-- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;

/*
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/01_module_touch_utm_referrer.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'
*/

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.affiliate
	CLONE latest_vault.cms_mysql.affiliate
;

-- CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
-- CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory
	CLONE latest_vault.cms_mysql.territory
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
-- CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

/*
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/07_touch_attribution/01_module_touch_attribution.py'  --method 'run' --start '2025-11-19 00:00:00' --end '2025-11-19 00:00:00'
*/

-- prod
SELECT
	DATE_TRUNC(MONTH, mtba.touch_start_tstamp)  AS month,
	COUNT(*)                                    AS sessions,
	COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_se_brand = 'Travelist'
  AND mtba.touch_start_tstamp >= '2025-01-01'
  AND mtba.touch_start_tstamp <= '2025-11-15'
GROUP BY ALL
;

-- dev
SELECT
	DATE_TRUNC(MONTH, mtba.touch_start_tstamp)  AS month,
	COUNT(*)                                    AS sessions,
	COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_se_brand = 'Travelist'
  AND mtba.touch_start_tstamp >= '2025-01-01'
  AND mtba.touch_start_tstamp <= '2025-11-15'
GROUP BY ALL
;


-- prod

SELECT
	DATE_TRUNC(MONTH, mtba.touch_start_tstamp)  AS month,
	mtmc.touch_mkt_channel,
	COUNT(*)                                    AS sessions,
	COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
	ON mtba.touch_id = mta.touch_id
	AND mta.attribution_model = 'last non direct'
INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
	ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_se_brand = 'Travelist'
  AND mtba.touch_start_tstamp >= '2025-01-01'
  AND mtba.touch_start_tstamp <= '2025-11-15'
GROUP BY ALL
;

-- dev
SELECT
	DATE_TRUNC(MONTH, mtba.touch_start_tstamp)  AS month,
	mtmc.touch_mkt_channel,
	COUNT(*)                                    AS sessions,
	COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
	ON mtba.touch_id = mta.touch_id
	AND mta.attribution_model = 'last non direct'
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
	ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_se_brand = 'Travelist'
  AND mtba.touch_start_tstamp >= '2025-01-01'
  AND mtba.touch_start_tstamp <= '2025-11-15'
GROUP BY ALL
;



-- 	CASE stmc.touch_mkt_channel
-- 		WHEN 'Afiliacja' THEN 'paid'
-- 		WHEN 'Direct' THEN 'non paid'
-- 		WHEN 'Display' THEN 'paid'
-- 		WHEN 'Google Ads Brand' THEN 'paid'
-- 		WHEN 'Google Ads Generic' THEN 'paid'
-- 		WHEN 'Google Ads Inne' THEN 'paid'
-- 		WHEN 'Magazyn' THEN 'non paid'
-- 		WHEN 'Mailing' THEN 'paid'
-- 		WHEN 'Newsletter' THEN 'crm'
-- 		WHEN 'Organic Social' THEN 'non paid'
-- 		WHEN 'Paid Social' THEN 'paid'
-- 		WHEN 'Push' THEN 'crm'
-- 		WHEN 'Referral' THEN 'non paid'
-- 		WHEN 'Remarketing' THEN 'paid'
-- 		WHEN 'Source SEO' THEN 'non paid'
-- 		WHEN 'SMS' THEN 'crm'
-- 		WHEN 'Video' THEN 'paid'
-- 		ELSE 'other'
-- 	END                                        AS channel_group,
--


-- monthly comp
WITH
	prod AS (
		SELECT
			DATE_TRUNC(MONTH, mtba.touch_start_tstamp)  AS month,
			mtmc.touch_mkt_channel,
			CASE mtmc.touch_mkt_channel
				WHEN 'Afiliacja' THEN 'paid'
				WHEN 'Direct' THEN 'non paid'
				WHEN 'Display' THEN 'paid'
				WHEN 'Google Ads Brand' THEN 'paid'
				WHEN 'Google Ads Generic' THEN 'paid'
				WHEN 'Google Ads Inne' THEN 'paid'
				WHEN 'Magazyn' THEN 'non paid'
				WHEN 'Mailing' THEN 'paid'
				WHEN 'Newsletter' THEN 'crm'
				WHEN 'Organic Social' THEN 'non paid'
				WHEN 'Paid Social' THEN 'paid'
				WHEN 'Push' THEN 'crm'
				WHEN 'Referral' THEN 'non paid'
				WHEN 'Remarketing' THEN 'paid'
				WHEN 'Source SEO' THEN 'non paid'
				WHEN 'SMS' THEN 'crm'
				WHEN 'Video' THEN 'paid'
				ELSE 'other'
			END                                         AS channel_group,
			COUNT(*)                                    AS sessions,
			COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
			AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
		WHERE mtba.touch_se_brand = 'Travelist'
		  AND mtba.touch_start_tstamp >= '2025-01-01'
		  AND mtba.touch_start_tstamp <= '2025-11-15'
		GROUP BY ALL
	),
	dev AS (

-- dev
		SELECT
			DATE_TRUNC(MONTH, mtba.touch_start_tstamp)  AS month,
			mtmc.touch_mkt_channel,
			CASE mtmc.touch_mkt_channel
				WHEN 'Afiliacja' THEN 'paid'
				WHEN 'Direct' THEN 'non paid'
				WHEN 'Display' THEN 'paid'
				WHEN 'Google Ads Brand' THEN 'paid'
				WHEN 'Google Ads Generic' THEN 'paid'
				WHEN 'Google Ads Inne' THEN 'paid'
				WHEN 'Magazyn' THEN 'non paid'
				WHEN 'Mailing' THEN 'paid'
				WHEN 'Newsletter' THEN 'crm'
				WHEN 'Organic Social' THEN 'non paid'
				WHEN 'Paid Social' THEN 'paid'
				WHEN 'Push' THEN 'crm'
				WHEN 'Referral' THEN 'non paid'
				WHEN 'Remarketing' THEN 'paid'
				WHEN 'Source SEO' THEN 'non paid'
				WHEN 'SMS' THEN 'crm'
				WHEN 'Video' THEN 'paid'
				ELSE 'other'
			END                                         AS channel_group,
			COUNT(*)                                    AS sessions,
			COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
			AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
		WHERE mtba.touch_se_brand = 'Travelist'
		  AND mtba.touch_start_tstamp >= '2025-01-01'
		  AND mtba.touch_start_tstamp <= '2025-11-15'
		GROUP BY ALL
	)
SELECT
	prod.month,
	prod.touch_mkt_channel,
	prod.channel_group,
	prod.sessions              AS prod_sessions,
	dev.sessions               AS dev_sessions,
	prod.sessions_with_booking AS prod_sessions_with_booking,
	dev.sessions_with_booking  AS dev_sessions_with_booking
FROM prod
LEFT JOIN dev
	ON prod.month = dev.month
	AND prod.touch_mkt_channel = dev.touch_mkt_channel
	AND prod.channel_group = dev.channel_group
;


-- weekly comp
WITH
	prod AS (
		SELECT
			DATE_TRUNC(WEEK, mtba.touch_start_tstamp)   AS week,
			mtmc.touch_mkt_channel,
			CASE mtmc.touch_mkt_channel
				WHEN 'Afiliacja' THEN 'paid'
				WHEN 'Direct' THEN 'non paid'
				WHEN 'Display' THEN 'paid'
				WHEN 'Google Ads Brand' THEN 'paid'
				WHEN 'Google Ads Generic' THEN 'paid'
				WHEN 'Google Ads Inne' THEN 'paid'
				WHEN 'Magazyn' THEN 'non paid'
				WHEN 'Mailing' THEN 'paid'
				WHEN 'Newsletter' THEN 'crm'
				WHEN 'Organic Social' THEN 'non paid'
				WHEN 'Paid Social' THEN 'paid'
				WHEN 'Push' THEN 'crm'
				WHEN 'Referral' THEN 'non paid'
				WHEN 'Remarketing' THEN 'paid'
				WHEN 'Source SEO' THEN 'non paid'
				WHEN 'SMS' THEN 'crm'
				WHEN 'Video' THEN 'paid'
				ELSE 'other'
			END                                         AS channel_group,
			COUNT(*)                                    AS sessions,
			COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
			AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
		WHERE mtba.touch_se_brand = 'Travelist'
		  AND mtba.touch_start_tstamp >= '2025-01-01'
		  AND mtba.touch_start_tstamp <= '2025-11-15'
		GROUP BY ALL
	),
	dev AS (

-- dev
		SELECT
			DATE_TRUNC(WEEK, mtba.touch_start_tstamp)   AS week,
			mtmc.touch_mkt_channel,
			CASE mtmc.touch_mkt_channel
				WHEN 'Afiliacja' THEN 'paid'
				WHEN 'Direct' THEN 'non paid'
				WHEN 'Display' THEN 'paid'
				WHEN 'Google Ads Brand' THEN 'paid'
				WHEN 'Google Ads Generic' THEN 'paid'
				WHEN 'Google Ads Inne' THEN 'paid'
				WHEN 'Magazyn' THEN 'non paid'
				WHEN 'Mailing' THEN 'paid'
				WHEN 'Newsletter' THEN 'crm'
				WHEN 'Organic Social' THEN 'non paid'
				WHEN 'Paid Social' THEN 'paid'
				WHEN 'Push' THEN 'crm'
				WHEN 'Referral' THEN 'non paid'
				WHEN 'Remarketing' THEN 'paid'
				WHEN 'Source SEO' THEN 'non paid'
				WHEN 'SMS' THEN 'crm'
				WHEN 'Video' THEN 'paid'
				ELSE 'other'
			END                                         AS channel_group,
			COUNT(*)                                    AS sessions,
			COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
			AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
		WHERE mtba.touch_se_brand = 'Travelist'
		  AND mtba.touch_start_tstamp >= '2025-01-01'
		  AND mtba.touch_start_tstamp <= '2025-11-15'
		GROUP BY ALL
	)
SELECT
	prod.week,
	prod.touch_mkt_channel,
	prod.channel_group,
	prod.sessions              AS prod_sessions,
	dev.sessions               AS dev_sessions,
	prod.sessions_with_booking AS prod_sessions_with_booking,
	dev.sessions_with_booking  AS dev_sessions_with_booking
FROM prod
LEFT JOIN dev
	ON prod.week = dev.week
	AND prod.touch_mkt_channel = dev.touch_mkt_channel
	AND prod.channel_group = dev.channel_group
;


SELECT *
FROM dbt.bi_product_analytics__intermediate.pda_session_metrics psm
;


-- tableau comp
WITH
	prod AS (
		SELECT
			mtba.touch_start_tstamp::DATE               AS date,
			mtmc.touch_mkt_channel,
			CASE mtmc.touch_mkt_channel
				WHEN 'Afiliacja' THEN 'paid'
				WHEN 'Direct' THEN 'non paid'
				WHEN 'Display' THEN 'paid'
				WHEN 'Google Ads Brand' THEN 'paid'
				WHEN 'Google Ads Generic' THEN 'paid'
				WHEN 'Google Ads Inne' THEN 'paid'
				WHEN 'Magazyn' THEN 'non paid'
				WHEN 'Mailing' THEN 'paid'
				WHEN 'Newsletter' THEN 'crm'
				WHEN 'Organic Social' THEN 'non paid'
				WHEN 'Paid Social' THEN 'paid'
				WHEN 'Push' THEN 'crm'
				WHEN 'Referral' THEN 'non paid'
				WHEN 'Remarketing' THEN 'paid'
				WHEN 'Source SEO' THEN 'non paid'
				WHEN 'SMS' THEN 'crm'
				WHEN 'Video' THEN 'paid'
				ELSE 'other'
			END                                         AS channel_group,
			COUNT(*)                                    AS sessions,
			COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
			AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
		WHERE mtba.touch_se_brand = 'Travelist'
		  AND mtba.touch_start_tstamp >= '2025-01-01'
		  AND mtba.touch_start_tstamp <= '2025-11-15'
		GROUP BY ALL
	),
	dev AS (

-- dev
		SELECT
			mtba.touch_start_tstamp::DATE               AS date,
			mtmc.touch_mkt_channel,
			CASE mtmc.touch_mkt_channel
				WHEN 'Afiliacja' THEN 'paid'
				WHEN 'Direct' THEN 'non paid'
				WHEN 'Display' THEN 'paid'
				WHEN 'Google Ads Brand' THEN 'paid'
				WHEN 'Google Ads Generic' THEN 'paid'
				WHEN 'Google Ads Inne' THEN 'paid'
				WHEN 'Magazyn' THEN 'non paid'
				WHEN 'Mailing' THEN 'paid'
				WHEN 'Newsletter' THEN 'crm'
				WHEN 'Organic Social' THEN 'non paid'
				WHEN 'Paid Social' THEN 'paid'
				WHEN 'Push' THEN 'crm'
				WHEN 'Referral' THEN 'non paid'
				WHEN 'Remarketing' THEN 'paid'
				WHEN 'Source SEO' THEN 'non paid'
				WHEN 'SMS' THEN 'crm'
				WHEN 'Video' THEN 'paid'
				ELSE 'other'
			END                                         AS channel_group,
			COUNT(*)                                    AS sessions,
			COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
			AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
		WHERE mtba.touch_se_brand = 'Travelist'
		  AND mtba.touch_start_tstamp >= '2025-01-01'
		  AND mtba.touch_start_tstamp <= '2025-11-15'
		GROUP BY ALL
	)
SELECT
	prod.date,
	prod.touch_mkt_channel,
	prod.channel_group,
	prod.sessions              AS prod_sessions,
	dev.sessions               AS dev_sessions,
	prod.sessions_with_booking AS prod_sessions_with_booking,
	dev.sessions_with_booking  AS dev_sessions_with_booking
FROM prod
LEFT JOIN dev
	ON prod.date = dev.date
	AND prod.touch_mkt_channel = dev.touch_mkt_channel
	AND prod.channel_group = dev.channel_group
;



USE ROLE pipelinerunner
;

CREATE OR REPLACE SCHEMA data_vault_mvp.single_customer_view_stg__spi_8017 CLONE data_vault_mvp.single_customer_view_stg
;

GRANT USAGE ON SCHEMA data_vault_mvp.single_customer_view_stg__spi_8017 TO ROLE data_team_basic
;

SHOW TABLES IN SCHEMA data_vault_mvp.single_customer_view_stg
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_events_of_interest
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_time_diff_marker
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_in_app_notification_events
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_pay_button_clicks
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touchifiable_events
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touchification
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker
;

-- DROP TABLE data_vault_mvp.single_customer_view_stg.module_unique_urls
-- ;

-- DROP TABLE data_vault_mvp.single_customer_view_stg.module_url_hostname
-- ;

-- DROP TABLE data_vault_mvp.single_customer_view_stg.module_url_params
-- ;

-- DROP TABLE data_vault_mvp.single_customer_view_stg.page_screen_enrichment
-- ;

-- DROP TABLE data_vault_mvp.single_customer_view_stg.module_extracted_params
-- ;

-- DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_associations
-- ;

-- DROP TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching
-- ;

-- DROP TABLE data_vault_mvp.single_customer_view_stg.app_push_send_enhancement
-- ;


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_brand = 'Travelist'
  AND ses.event_tstamp::DATE = CURRENT_DATE
  AND ses.event_name = 'page_ping'
  AND ses.unique_browser_id = 'd6e60f6f-3b10-4578-bb06-8d1ee5a43d95'
;


./
scripts/
mwaa-cli production "dags backfill --start-date '2022-12-01 00:00:00' --end-date '2022-12-02 00:00:00' --donot-pickle single_customer_view__daily_at_02h00"


-- backup

SELECT
	COUNT(*),
	COUNT(DISTINCT touch_id)
FROM data_vault_mvp.single_customer_view_stg__spi_8017.module_touchification mt
;

--prod
SELECT
	COUNT(*),
	COUNT(DISTINCT touch_id)
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
;
--dev
SELECT
	COUNT(*),
	COUNT(DISTINCT touch_id)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt
;


-- backup
SELECT
	DATE_TRUNC(MONTH, mtba.touch_start_tstamp)  AS month,
	mtba.touch_se_brand,
	COUNT(*)                                    AS sessions,
	COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
FROM data_vault_mvp.single_customer_view_stg__spi_8017.module_touch_basic_attributes mtba
WHERE mtba.touch_se_brand = 'Travelist'
  AND mtba.touch_start_tstamp >= '2025-01-01'
  AND mtba.touch_start_tstamp <= '2025-11-15'
GROUP BY ALL
;


-- prod
SELECT
	DATE_TRUNC(MONTH, mtba.touch_start_tstamp)  AS month,
	touch_se_brand,
	COUNT(*)                                    AS sessions,
	COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_se_brand = 'Travelist'
  AND mtba.touch_start_tstamp >= '2025-01-01'
  AND mtba.touch_start_tstamp <= '2025-11-15'
GROUP BY ALL
;

-- dev
SELECT
	DATE_TRUNC(MONTH, mtba.touch_start_tstamp)  AS month,
	mtba.touch_se_brand,
	COUNT(*)                                    AS sessions,
	COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_se_brand = 'Travelist'
  AND mtba.touch_start_tstamp >= '2025-01-01'
  AND mtba.touch_start_tstamp <= '2025-11-15'
GROUP BY ALL
;

USE WAREHOUSE pipe_xlarge
;
-- monthly comp
WITH
	prod AS (
		SELECT
			DATE_TRUNC(MONTH, mtba.touch_start_tstamp)  AS month,
			mtmc.touch_mkt_channel,
			CASE mtmc.touch_mkt_channel
				WHEN 'Afiliacja' THEN 'paid'
				WHEN 'Direct' THEN 'non paid'
				WHEN 'Display' THEN 'paid'
				WHEN 'Google Ads Brand' THEN 'paid'
				WHEN 'Google Ads Generic' THEN 'paid'
				WHEN 'Google Ads Inne' THEN 'paid'
				WHEN 'Magazyn' THEN 'non paid'
				WHEN 'Mailing' THEN 'paid'
				WHEN 'Newsletter' THEN 'crm'
				WHEN 'Organic Social' THEN 'non paid'
				WHEN 'Paid Social' THEN 'paid'
				WHEN 'Push' THEN 'crm'
				WHEN 'Referral' THEN 'non paid'
				WHEN 'Remarketing' THEN 'paid'
				WHEN 'Source SEO' THEN 'non paid'
				WHEN 'SMS' THEN 'crm'
				WHEN 'Video' THEN 'paid'
				ELSE 'other'
			END                                         AS channel_group,
			COUNT(*)                                    AS sessions,
			COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
			AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
		WHERE mtba.touch_se_brand = 'Travelist'
		  AND mtba.touch_start_tstamp >= '2025-01-01'
		  AND mtba.touch_start_tstamp <= '2025-11-15'
		GROUP BY ALL
	),
	backup AS (

-- backup
		SELECT
			DATE_TRUNC(MONTH, mtba.touch_start_tstamp)  AS month,
			mtmc.touch_mkt_channel,
			CASE mtmc.touch_mkt_channel
				WHEN 'Afiliacja' THEN 'paid'
				WHEN 'Direct' THEN 'non paid'
				WHEN 'Display' THEN 'paid'
				WHEN 'Google Ads Brand' THEN 'paid'
				WHEN 'Google Ads Generic' THEN 'paid'
				WHEN 'Google Ads Inne' THEN 'paid'
				WHEN 'Magazyn' THEN 'non paid'
				WHEN 'Mailing' THEN 'paid'
				WHEN 'Newsletter' THEN 'crm'
				WHEN 'Organic Social' THEN 'non paid'
				WHEN 'Paid Social' THEN 'paid'
				WHEN 'Push' THEN 'crm'
				WHEN 'Referral' THEN 'non paid'
				WHEN 'Remarketing' THEN 'paid'
				WHEN 'Source SEO' THEN 'non paid'
				WHEN 'SMS' THEN 'crm'
				WHEN 'Video' THEN 'paid'
				ELSE 'other'
			END                                         AS channel_group,
			COUNT(*)                                    AS sessions,
			COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
		FROM data_vault_mvp.single_customer_view_stg__spi_8017.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp.single_customer_view_stg__spi_8017.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp.single_customer_view_stg__spi_8017.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
			AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
		WHERE mtba.touch_se_brand = 'Travelist'
		  AND mtba.touch_start_tstamp >= '2025-01-01'
		  AND mtba.touch_start_tstamp <= '2025-11-15'
		GROUP BY ALL
	)
SELECT
	prod.month,
	prod.touch_mkt_channel,
	prod.channel_group,
	prod.sessions                AS prod_sessions,
	backup.sessions              AS backup_sessions,
	prod.sessions_with_booking   AS prod_sessions_with_booking,
	backup.sessions_with_booking AS backup_sessions_with_booking
FROM prod
LEFT JOIN backup
	ON prod.month = backup.month
	AND prod.touch_mkt_channel = backup.touch_mkt_channel
	AND prod.channel_group = backup.channel_group
;

-- tableau daily
WITH
	prod AS (
		SELECT
			mtba.touch_start_tstamp::DATE               AS date,
			mtmc.touch_mkt_channel,
			CASE mtmc.touch_mkt_channel
				WHEN 'Afiliacja' THEN 'paid'
				WHEN 'Direct' THEN 'non paid'
				WHEN 'Display' THEN 'paid'
				WHEN 'Google Ads Brand' THEN 'paid'
				WHEN 'Google Ads Generic' THEN 'paid'
				WHEN 'Google Ads Inne' THEN 'paid'
				WHEN 'Magazyn' THEN 'non paid'
				WHEN 'Mailing' THEN 'paid'
				WHEN 'Newsletter' THEN 'crm'
				WHEN 'Organic Social' THEN 'non paid'
				WHEN 'Paid Social' THEN 'paid'
				WHEN 'Push' THEN 'crm'
				WHEN 'Referral' THEN 'non paid'
				WHEN 'Remarketing' THEN 'paid'
				WHEN 'Source SEO' THEN 'non paid'
				WHEN 'SMS' THEN 'crm'
				WHEN 'Video' THEN 'paid'
				ELSE 'other'
			END                                         AS channel_group,
			COUNT(*)                                    AS sessions,
			COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
		FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
			AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
		WHERE mtba.touch_se_brand = 'Travelist'
		  AND mtba.touch_start_tstamp >= '2025-01-01'
		  AND mtba.touch_start_tstamp <= '2025-11-15'
		GROUP BY ALL
	),
	backup AS (

-- backup
		SELECT
			mtba.touch_start_tstamp::DATE               AS date,
			mtmc.touch_mkt_channel,
			CASE mtmc.touch_mkt_channel
				WHEN 'Afiliacja' THEN 'paid'
				WHEN 'Direct' THEN 'non paid'
				WHEN 'Display' THEN 'paid'
				WHEN 'Google Ads Brand' THEN 'paid'
				WHEN 'Google Ads Generic' THEN 'paid'
				WHEN 'Google Ads Inne' THEN 'paid'
				WHEN 'Magazyn' THEN 'non paid'
				WHEN 'Mailing' THEN 'paid'
				WHEN 'Newsletter' THEN 'crm'
				WHEN 'Organic Social' THEN 'non paid'
				WHEN 'Paid Social' THEN 'paid'
				WHEN 'Push' THEN 'crm'
				WHEN 'Referral' THEN 'non paid'
				WHEN 'Remarketing' THEN 'paid'
				WHEN 'Source SEO' THEN 'non paid'
				WHEN 'SMS' THEN 'crm'
				WHEN 'Video' THEN 'paid'
				ELSE 'other'
			END                                         AS channel_group,
			COUNT(*)                                    AS sessions,
			COUNT(IFF(mtba.touch_has_booking, 1, NULL)) AS sessions_with_booking
		FROM data_vault_mvp.single_customer_view_stg__spi_8017.module_touch_basic_attributes mtba
		INNER JOIN data_vault_mvp.single_customer_view_stg__spi_8017.module_touch_attribution mta
			ON mtba.touch_id = mta.touch_id
			AND mta.attribution_model = 'last non direct'
		INNER JOIN data_vault_mvp.single_customer_view_stg__spi_8017.module_touch_marketing_channel mtmc
			ON mta.attributed_touch_id = mtmc.touch_id
			AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'
		WHERE mtba.touch_se_brand = 'Travelist'
		  AND mtba.touch_start_tstamp >= '2025-01-01'
		  AND mtba.touch_start_tstamp <= '2025-11-15'
		GROUP BY ALL
	)
SELECT
	prod.date,
	prod.touch_mkt_channel,
	prod.channel_group,
	prod.sessions                AS prod_sessions,
	backup.sessions              AS backup_sessions,
	prod.sessions_with_booking   AS prod_sessions_with_booking,
	backup.sessions_with_booking AS backup_sessions_with_booking
FROM prod
LEFT JOIN backup
	ON prod.date = backup.date
	AND prod.touch_mkt_channel = backup.touch_mkt_channel
	AND prod.channel_group = backup.channel_group
;

SELECT GET_DDL('table', 'se.data.booking_analysis')
;

SELECT *
FROM se.data.sales_kingfisher sk
;

SELECT *
FROM se.data.sales_kingfisher_facilities skf
;

SELECT *
FROM se.data.dim_sale ds
WHERE ds.sale_active

SELECT *
FROM se.data.se_sale_attributes ssa
;

SELECT *
FROM se.data.tb_offer t
;


SELECT *
FROM se.data.dim_sale ds
LEFT JOIN
where ds.sale_active
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_index CLONE data_vault_mvp.dwh.user_booking_index
;

SELECT
	LOWER(table_catalog) || '.' || LOWER(table_schema) || '.' || LOWER(table_name) AS table_to_drop,
	'DROP TABLE IF EXISTS ' || LOWER(table_catalog) || '.' || LOWER(table_schema) || '.' ||
	LOWER(table_name)                                                              AS drop_statement
-- do not state this Snowflake owned object in the sources dictionary (we do not want to inadvertantly CLONE it)
FROM snowflake.account_usage.table_storage_metrics table_storage_metrics
WHERE LOWER(table_storage_metrics.table_catalog) = LOWER('hygiene_vault_dev_robin')
  AND LOWER(table_storage_metrics.table_schema) =
	  IFF('purge_all_schemas' = 'purge_all_schemas',
		  LOWER(table_storage_metrics.table_schema),
		  LOWER('purge_all_schemas')
	  )
  AND table_storage_metrics.deleted = FALSE
  AND DATEDIFF(
			  'days',
			  table_storage_metrics.table_created::DATE,
			  CURRENT_DATE()
	  ) > 30
  AND (((table_storage_metrics.active_bytes / 1024) / 1024) / 1024)::DECIMAL(13, 2) >= 0.0125
;

USE ROLE pipelinerunner
;

-- owned by pipelinerunner
SHOW GRANTS ON TABLE hygiene_vault_dev_robin.cms_mysql.anonymisation_list__shiro_user__20250820t003000
;

SHOW GRANTS ON TABLE hygiene_vault_dev_robin.cms_mysql.shiro_user
;

SHOW GRANTS ON TABLE hygiene_vault_dev_robin.cms_mysql.deduplicate_anonymisation_list__shiro_user__20250820t003000
;

SELECT *
FROM cirium_data_share.public.airports
;

GRANT IMPORTED PRIVILEGES ON DATABASE cirium_data_share TO ROLE pipelinerunner
;



SELECT
	touchification.touch_id,
	events.event_hash,
	events.event_name,
	events.event_tstamp,
	events.login_type,
	events.useragent,
	events.event_id,
	events.v_tracker
FROM data_vault_mvp.single_customer_view_stg.module_touchification touchification
INNER JOIN se.data_pii.scv_event_stream events
	ON touchification.event_hash = events.event_hash
	AND events.login_type IS NOT NULL
	AND events.event_tstamp::date = '2025-08-08'
WHERE touchification.updated_at::DATE >= '2025-11-25'
  AND touchification.touch_id = 'e29bd6d8e76c7a853998063b0201596bf2db261821d44d38c7b91940e5c2fa44'
	./

scripts/
mwaa-cli production "dags backfill --start-date '2022-12-01 03:30:00' --end-date '2022-12-02 03:30:00' --donot-pickle bi__session_metrics__daily_at_03h30";

USE ROLE personal_role__robinpatel
;

SELECT
	COUNT(*)
FROM se.bi.session_metrics
; -- on full refresh: 536,906,499

SELECT *
FROM se.bi.session_metrics
WHERE touch_se_brand = 'SE Brand'
;

-- pipeline
SELECT
	sm.touch_start_tstamp::DATE,
	sm.touch_se_brand,
	COUNT(*) AS sessions
FROM se.bi.session_metrics sm
WHERE sm.touch_start_tstamp BETWEEN '2025-01-01' AND '2025-01-15'
GROUP BY ALL;

-- dbt
SELECT
	psm.touch_start_tstamp::DATE,
	psm.touch_se_brand,
	COUNT(*) AS sessions
FROM dbt.bi_product_analytics__intermediate.pda_session_metrics psm
WHERE psm.touch_start_tstamp BETWEEN '2025-01-01' AND '2025-01-15'
GROUP BY ALL;

