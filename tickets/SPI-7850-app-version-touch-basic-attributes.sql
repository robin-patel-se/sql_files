-- module=/biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/01_module_touch_basic_attributes.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates
	CLONE data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
	CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest
	CLONE data_vault_mvp.single_customer_view_stg.module_events_of_interest
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
	CLONE data_vault_mvp.single_customer_view_stg.page_screen_enrichment
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

-- optional statement to create the module target table --
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.scv.05_touch_basic_attributes.01_module_touch_basic_attributes.py' \
    --method 'run' \
    --start '2025-10-07 00:00:00' \
    --end '2025-10-07 00:00:00'



SELECT
	-- correct server events
	t.touch_id,
	t.event_index_within_touch,
	COALESCE(te.device_platform, 'not specified')                                     AS device_platform,
	e.event_hash,
	e.posa_territory,
	e.se_brand,
	e.event_name,
	e.login_type,
	e.event_tstamp,
	-- for page views we have enriched data which means we have a diff
	-- min and max tstamp so we create concept of max event_tstamp
	pse.max_event_tstamp                                                              AS page_screen_enrichment_max_event_tstamp,
	COALESCE(pse.max_event_tstamp, e.event_tstamp)                                    AS max_event_tstamp,
	-- things we need to remove:
	IFF(e.event_name = 'system_webhook', NULL, e.page_url)                            AS page_url,
	IFF(e.event_name = 'system_webhook', NULL, e.page_urlpath)                        AS page_urlpath,
	IFF(e.event_name = 'system_webhook', NULL, e.page_urlhost)                        AS page_urlhost,
	IFF(e.event_name = 'system_webhook', NULL, e.page_referrer)                       AS page_referrer,
	IFF(e.event_name = 'system_webhook', NULL,
		e.unstruct_event_com_snowplowanalytics_mobile_screen_view_1:name::VARCHAR)    AS screen_view_name,
	IFF(e.event_name = 'system_webhook', NULL, e.se_action)                           AS se_action,
	IFF(e.event_name = 'system_webhook', NULL, e.user_ipaddress)                      AS user_ipaddress,
	IFF(e.event_name = 'system_webhook', NULL, e.geo_country)                         AS geo_country,
	IFF(e.event_name = 'system_webhook', NULL, e.geo_city)                            AS geo_city,
	IFF(e.event_name = 'system_webhook', NULL, e.geo_zipcode)                         AS geo_zipcode,
	IFF(e.event_name = 'system_webhook', NULL, e.geo_latitude)                        AS geo_latitude,
	IFF(e.event_name = 'system_webhook', NULL, e.geo_longitude)                       AS geo_longitude,
	IFF(e.event_name = 'system_webhook', NULL, e.geo_region_name)                     AS geo_region_name,
	IFF(e.event_name = 'system_webhook', NULL, e.useragent)                           AS useragent,
	IFF(e.event_name = 'system_webhook', NULL, e.br_name)                             AS br_name,
	IFF(e.event_name = 'system_webhook', NULL, e.br_family)                           AS br_family,
	IFF(e.event_name = 'system_webhook', NULL, e.br_lang)                             AS br_lang,
	IFF(e.event_name = 'system_webhook', NULL, e.os_name)                             AS os_name,
	IFF(e.event_name = 'system_webhook', NULL, e.os_family)                           AS os_family,
	IFF(e.event_name = 'system_webhook', NULL, e.os_manufacturer)                     AS os_manufacturer,
	IFF(e.event_name = 'system_webhook', NULL, e.dvce_screenwidth)                    AS dvce_screenwidth,
	IFF(e.event_name = 'system_webhook', NULL, e.dvce_screenheight)                   AS dvce_screenheight,
	IFF(e.event_name = 'system_webhook', NULL,
		e.contexts_com_secretescapes_app_state_context_1[0])                          AS app_state_context,
	IFF(e.event_name = 'system_webhook', NULL,
		e.contexts_com_snowplowanalytics_mobile_application_1[0])                     AS mobile_application_context,
	IFF(e.event_name = 'system_webhook', e.unstruct_event_com_iterable_system_webhook_1,
		NULL)                                                                         AS app_push_open_context,
	IFF(e.event_name = 'system_webhook', NULL,
		e.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR) AS app_version
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_clone__step01__get_source_batch batch
	ON t.touch_id = batch.touch_id
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events te
	ON t.event_hash = te.event_hash
INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
	ON t.event_hash = e.event_hash
LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment pse
	ON t.event_hash = pse.event_hash
;


SELECT
	stba.mobile_application_context['version']::VARCHAR AS app_version,
	count(*)
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.mobile_application_context IS NOT NULL
GROUP BY ALL