SELECT *
FROM snowplow.atomic.events SAMPLE SYSTEM (3) SEED (82)
;
------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;

CREATE DATABASE infinite_lambda_db
;

CREATE SCHEMA infinite_lambda_db.data_share
;

------------------------------------------------------------------------------------------------------------------------

USE WAREHOUSE pipe_xlarge
;

CREATE OR REPLACE SECURE VIEW infinite_lambda_db.data_share.event_stream_sample AS
SELECT
	app_id,
	platform,
	etl_tstamp,
	collector_tstamp,
	dvce_created_tstamp,
	event,
	event_id,
	txn_id,
	name_tracker,
	v_tracker,
	v_collector,
	v_etl,
	user_id,
-- 	user_ipaddress,
	user_fingerprint,
	domain_userid,
	domain_sessionidx,
	network_userid,
-- 	geo_country,
-- 	geo_region,
-- 	geo_city,
-- 	geo_zipcode,
-- 	geo_latitude,
-- 	geo_longitude,
-- 	geo_region_name,
-- 	ip_isp,
-- 	ip_organization,
-- 	ip_domain,
-- 	ip_netspeed,
	page_url,
	page_title,
	page_referrer,
	page_urlscheme,
	page_urlhost,
	page_urlport,
	page_urlpath,
	page_urlquery,
	page_urlfragment,
	refr_urlscheme,
	refr_urlhost,
	refr_urlport,
	refr_urlpath,
	refr_urlquery,
	refr_urlfragment,
	refr_medium,
	refr_source,
	refr_term,
	mkt_medium,
	mkt_source,
	mkt_term,
	mkt_content,
	mkt_campaign,
	se_category,
	se_action,
	se_label,
	se_property,
	se_value,
	tr_orderid,
	tr_affiliation,
	tr_total,
	tr_tax,
	tr_shipping,
	tr_city,
	tr_state,
	tr_country,
	ti_orderid,
	ti_sku,
	ti_name,
	ti_category,
	ti_price,
	ti_quantity,
	pp_xoffset_min,
	pp_xoffset_max,
	pp_yoffset_min,
	pp_yoffset_max,
	useragent,
	br_name,
	br_family,
	br_version,
	br_type,
	br_renderengine,
	br_lang,
	br_features_pdf,
	br_features_flash,
	br_features_java,
	br_features_director,
	br_features_quicktime,
	br_features_realplayer,
	br_features_windowsmedia,
	br_features_gears,
	br_features_silverlight,
	br_cookies,
	br_colordepth,
	br_viewwidth,
	br_viewheight,
	os_name,
	os_family,
	os_manufacturer,
	os_timezone,
	dvce_type,
	dvce_ismobile,
	dvce_screenwidth,
	dvce_screenheight,
	doc_charset,
	doc_width,
	doc_height,
	tr_currency,
	tr_total_base,
	tr_tax_base,
	tr_shipping_base,
	ti_currency,
	ti_price_base,
	base_currency,
	geo_timezone,
	mkt_clickid,
	mkt_network,
	etl_tags,
	dvce_sent_tstamp,
	refr_domain_userid,
	refr_dvce_tstamp,
	domain_sessionid,
	derived_tstamp,
	event_vendor,
	event_name,
	event_format,
	event_version,
	event_fingerprint,
	true_tstamp,
	contexts_com_optimizely_snowplow_optimizely_summary_1,
	contexts_com_secretescapes_all_pages_session_login_type_context_1,
	contexts_com_secretescapes_sale_page_context_1,
	contexts_com_secretescapes_user_state_context_1,
	contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
	contexts_com_snowplowanalytics_snowplow_web_page_1,
	contexts_org_w3_performance_timing_1,
	unstruct_event_com_snowplowanalytics_snowplow_link_click_1,
	contexts_com_optimizely_optimizelyx_summary_1,
	contexts_com_secretescapes_collection_context_1,
	contexts_com_secretescapes_filter_context_1,
	contexts_com_secretescapes_screen_context_1,
	contexts_com_snowplowanalytics_snowplow_application_background_1,
	contexts_com_snowplowanalytics_snowplow_application_foreground_1,
	contexts_com_snowplowanalytics_snowplow_client_session_1,
	contexts_com_snowplowanalytics_snowplow_mobile_context_1,
	unstruct_event_com_snowplowanalytics_snowplow_screen_view_1,
	unstruct_event_com_secretescapes_searched_with_refinement_event_1,
	contexts_com_optimizely_experiment_1,
	contexts_com_optimizely_state_1,
	contexts_com_optimizely_variation_1,
	contexts_com_optimizely_visitor_1,
	contexts_com_optimizely_visitor_audience_1,
	contexts_com_optimizely_visitor_dimension_1,
	unstruct_event_com_snowplowanalytics_mobile_screen_view_1,
	contexts_com_secretescapes_booking_context_1,
	contexts_com_secretescapes_content_context_1,
	contexts_com_secretescapes_environment_context_1,
	contexts_com_secretescapes_secret_escapes_sale_context_1,
-- 	contexts_com_secretescapes_user_context_1,
	contexts_com_secretescapes_who_puts_product_in_front_customer_context_1,
	unstruct_event_com_secretescapes_booking_update_event_1,
	contexts_com_secretescapes_product_display_context_1,
-- 	unstruct_event_com_branch_secretescapes_install_1,
	contexts_com_secretescapes_search_context_1,
	contexts_com_google_analytics_cookies_1,
	unstruct_event_com_snowplowanalytics_snowplow_application_error_1,
	contexts_com_secretescapes_content_element_interaction_context_1,
	contexts_com_secretescapes_content_elements_rendered_context_1,
	contexts_com_secretescapes_content_element_viewed_context_1,
	contexts_com_secretescapes_searched_with_refinement_event_1
-- 	unstruct_event_com_branch_secretescapes_purchase_1
-- 	unstruct_event_com_iterable_system_webhook_1
FROM snowplow.atomic.events
WHERE collector_tstamp >= '2022-01-01'
;

DROP VIEW infinite_lambda_db.data_share.event_stream_sample
;

-- filter results for containing @
-- ^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} -- ip address regex

USE ROLE personal_role__robinpatel
;

SELECT *
FROM infinite_lambda_db.data_share.event_stream_sample
;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE infinite_lambda_db.data_share.event_stream_sample CLONE snowplow.atomic.events
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN geo_country
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN geo_region
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN geo_city
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN geo_zipcode
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN geo_latitude
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN geo_longitude
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN geo_region_name
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN ip_isp
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN ip_organization
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN ip_domain
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN ip_netspeed
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN contexts_com_secretescapes_user_context_1
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN unstruct_event_com_branch_secretescapes_install_1
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN unstruct_event_com_branch_secretescapes_purchase_1
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN unstruct_event_com_iterable_system_webhook_1
;

ALTER TABLE infinite_lambda_db.data_share.event_stream_sample
	DROP COLUMN user_ipaddress
;
------------------------------------------------------------------------------------------------------------------------


CREATE SHARE infinite_lambda_data_share
;

GRANT USAGE ON DATABASE infinite_lambda_db TO SHARE infinite_lambda_data_share
;

GRANT USAGE ON SCHEMA infinite_lambda_db.data_share TO SHARE infinite_lambda_data_share
;

GRANT SELECT ON TABLE infinite_lambda_db.data_share.event_stream_sample TO SHARE infinite_lambda_data_share
;

SHOW GRANTS TO SHARE infinite_lambda_data_share
;

ALTER SHARE infinite_lambda_data_share ADD ACCOUNTS =gezjpyc.INFINITELAMBDA
;
;

SHOW GRANTS OF SHARE infinite_lambda_data_share
;


------------------------------------------------------------------------------------------------------------------------
-- Our IL account is in: eu-west-1.
-- Our account identifier for this: GEZJPYC.INFINITELAMBDA
------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

SELECT
-- 	schedule_tstamp,
-- 	run_tstamp,
-- 	operation_id,
-- 	created_at,
-- 	updated_at,
touch_id,
-- 	attributed_user_id,
stitched_identity_type,
touch_logged_in,
touch_start_tstamp,
touch_end_tstamp,
touch_duration_seconds,
touch_posa_territory,
touch_hostname_territory,
touch_experience,
touch_landing_page,
touch_landing_pagepath,
touch_hostname,
touch_exit_pagepath,
touch_referrer_url,
touch_event_count,
touch_has_booking,
is_se_internal_touch,
-- 	user_ipaddress,
-- 	geo_country,
-- 	geo_city,
-- 	geo_zipcode,
-- 	geo_latitude,
-- 	geo_longitude,
-- 	geo_region_name,
-- 	useragent,
br_name,
br_family,
os_name,
os_family,
os_manufacturer,
dvce_screenwidth,
dvce_screenheight,
-- 	app_push_open_context,
touch_se_brand
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes stba
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN schedule_tstamp
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN run_tstamp
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN operation_id
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN created_at
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN updated_at
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN attributed_user_id
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN user_ipaddress
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN geo_country
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN geo_city
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN geo_zipcode
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN geo_latitude
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN geo_longitude
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN geo_region_name
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN useragent
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	DROP COLUMN app_push_open_context
;

USE ROLE accountadmin
;

CREATE OR REPLACE TRANSIENT TABLE infinite_lambda_db.data_share.module_touch_basic_attributes
	CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
;

GRANT SELECT ON TABLE infinite_lambda_db.data_share.module_touch_basic_attributes TO SHARE infinite_lambda_data_share
;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

SELECT
-- 	schedule_tstamp,
-- 	run_tstamp,
-- 	operation_id,
-- 	created_at,
-- 	updated_at,
event_hash,
touch_id,
event_tstamp,
event_category,
event_subcategory,
page_url,
search_context,
check_in_date,
check_out_date,
flexible_search,
had_results,
location,
location_search,
months,
months_search,
num_results,
refine_by_travel_type_search,
refine_by_trip_type_search,
specific_dates_search,
travel_types,
trip_types,
weekend_only_search,
triggered_by,
filter_context,
se_brand
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
;


ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	DROP COLUMN schedule_tstamp
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	DROP COLUMN run_tstamp
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	DROP COLUMN operation_id
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	DROP COLUMN created_at
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	DROP COLUMN updated_at
;

USE ROLE accountadmin
;

CREATE OR REPLACE TRANSIENT TABLE infinite_lambda_db.data_share.module_touched_searches
	CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
;

GRANT SELECT ON TABLE infinite_lambda_db.data_share.module_touched_searches TO SHARE infinite_lambda_data_share
;


------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;


SELECT
-- 	schedule_tstamp,
-- 	run_tstamp,
-- 	operation_id,
-- 	created_at,
-- 	updated_at,
event_hash,
attributed_user_id,
stitched_identity_type,
event_tstamp,
touch_id,
event_index_within_touch
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	DROP COLUMN schedule_tstamp
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	DROP COLUMN run_tstamp
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	DROP COLUMN operation_id
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	DROP COLUMN created_at
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	DROP COLUMN updated_at
;


USE ROLE accountadmin
;

CREATE OR REPLACE TRANSIENT TABLE infinite_lambda_db.data_share.module_touchification
	CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
;

GRANT SELECT ON TABLE infinite_lambda_db.data_share.module_touchification TO SHARE infinite_lambda_data_share
;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

SELECT
-- 	schedule_tstamp,
-- 	run_tstamp,
-- 	operation_id,
-- 	created_at,
-- 	updated_at,
event_hash,
touch_id,
event_tstamp,
se_sale_id,
event_category,
event_subcategory,
page_url
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	DROP COLUMN schedule_tstamp
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	DROP COLUMN run_tstamp
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	DROP COLUMN operation_id
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	DROP COLUMN created_at
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	DROP COLUMN updated_at
;

USE ROLE accountadmin
;

CREATE OR REPLACE TRANSIENT TABLE infinite_lambda_db.data_share.module_touched_spvs
	CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
;

GRANT SELECT ON TABLE infinite_lambda_db.data_share.module_touched_spvs TO SHARE infinite_lambda_data_share
;

------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

SELECT
-- 	schedule_tstamp,
-- 	run_tstamp,
-- 	operation_id,
-- 	created_at,
-- 	updated_at,
touch_id,
touch_mkt_channel,
touch_landing_page,
touch_hostname,
touch_hostname_territory,
-- 	attributed_user_id,
utm_campaign,
utm_medium,
utm_source,
utm_term,
utm_content,
click_id,
sub_affiliate_name,
affiliate,
touch_affiliate_territory,
awadgroupid,
awcampaignid,
referrer_hostname,
referrer_medium
-- 	landing_page_parameters
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	DROP COLUMN schedule_tstamp
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	DROP COLUMN run_tstamp
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	DROP COLUMN operation_id
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	DROP COLUMN created_at
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	DROP COLUMN updated_at
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	DROP COLUMN attributed_user_id
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	DROP COLUMN landing_page_parameters
;

USE ROLE accountadmin
;

CREATE OR REPLACE TRANSIENT TABLE infinite_lambda_db.data_share.module_touch_marketing_channel
	CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
;

GRANT SELECT ON TABLE infinite_lambda_db.data_share.module_touch_marketing_channel TO SHARE infinite_lambda_data_share
;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

SELECT
-- 	schedule_tstamp,
-- 	run_tstamp,
-- 	operation_id,
-- 	created_at,
-- 	updated_at,
	event_hash,
	touch_id,
	event_tstamp,
	booking_id,
	event_category,
	event_subcategory
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions mtt
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	DROP COLUMN schedule_tstamp
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	DROP COLUMN run_tstamp
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	DROP COLUMN operation_id
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	DROP COLUMN created_at
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	DROP COLUMN updated_at
;


USE ROLE accountadmin
;

CREATE OR REPLACE TRANSIENT TABLE infinite_lambda_db.data_share.module_touched_transactions
	CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
;

GRANT SELECT ON TABLE infinite_lambda_db.data_share.module_touched_transactions TO SHARE infinite_lambda_data_share
;