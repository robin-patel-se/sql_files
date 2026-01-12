SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_hostname LIKE '%travelist%'
  AND mtba.stitched_identity_type = 'se_user_id';


SELECT mtba.touch_posa_territory,
       mtba.touch_hostname_territory,
       mtmc.touch_affiliate_territory,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
WHERE mtba.touch_hostname LIKE '%travelist%'
  AND mtba.stitched_identity_type = 'se_user_id'
GROUP BY GROUPING SETS (mtba.touch_posa_territory, mtba.touch_hostname_territory, mtmc.touch_affiliate_territory);


USE WAREHOUSE pipe_2xlarge;

DROP VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

SELECT mtmc.touch_hostname,
       mtmc.touch_affiliate_territory,
       COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_hostname LIKE '%travelist%'
GROUP BY 1, 2;

-- SE TECH as territory
'livetest.oferty.travelist.pl',
'staging.travelist.pl',


'admin.oferty.travelist.pl',
'oferty.travelist.pl',
'partner.travelist.pl',
'travelist.pl',
'vision.travelist.pl',
'zagranica.travelist.pl',
'travelist.hu',


UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
SET mtmc.touch_affiliate_territory = 'PL'
WHERE mtmc.touch_hostname IN (
                              'oferty.travelist.pl',
                              'partner.travelist.pl',
                              'travelist.pl',
                              'vision.travelist.pl',
                              'zagranica.travelist.pl',
                              'travelist.hu'
    );


UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
SET mtmc.touch_affiliate_territory = 'SE TECH'
WHERE mtmc.touch_hostname IN (
                              'admin.oferty.travelist.pl',
                              'livetest.oferty.travelist.pl',
                              'staging.travelist.pl'
    );


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.page_urlhost IN (
                          'admin.oferty.travelist.pl',
                          'livetest.oferty.travelist.pl',
                          'oferty.travelist.pl',
                          'partner.travelist.pl',
                          'staging.travelist.pl',
                          'travelist.hu',
                          'travelist.pl',
                          'vision.travelist.pl',
                          'zagranica.travelist.pl'
    )
  AND es.event_tstamp >= CURRENT_DATE - 1
LIMIT 10;


------------------------------------------------------------------------------------------------------------------------
-- remove se_user_id from the event_stream for travelist events, this is to avoid it flowing through identity stitching and stitching these events to se users

self_describing_task --include 'staging/hygiene/snowplow/event_stream.py'  --method 'run' --start '2022-01-12 00:00:00' --end '2022-01-12 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;

UPDATE hygiene_vault_mvp_dev_robin.snowplow.event_stream es
SET es.se_user_id = NULL,
    es.updated_at = CURRENT_TIMESTAMP
WHERE es.page_urlhost IN (
                          'admin.oferty.travelist.pl',
                          'livetest.oferty.travelist.pl',
                          'oferty.travelist.pl',
                          'partner.travelist.pl',
                          'staging.travelist.pl',
                          'travelist.hu',
                          'travelist.pl',
                          'vision.travelist.pl',
                          'zagranica.travelist.pl'
    );


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream_travelist_events AS
SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.page_urlhost IN ('admin.oferty.travelist.pl',
                          'livetest.oferty.travelist.pl',
                          'oferty.travelist.pl',
                          'partner.travelist.pl',
                          'staging.travelist.pl',
                          'travelist.hu',
                          'travelist.pl',
                          'vision.travelist.pl',
                          'zagranica.travelist.pl'
    );

DELETE
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.page_urlhost IN ('admin.oferty.travelist.pl',
                          'livetest.oferty.travelist.pl',
                          'oferty.travelist.pl',
                          'partner.travelist.pl',
                          'staging.travelist.pl',
                          'travelist.hu',
                          'travelist.pl',
                          'vision.travelist.pl',
                          'zagranica.travelist.pl'
    );

INSERT INTO hygiene_vault_mvp_dev_robin.snowplow.event_stream
SELECT este.schedule_tstamp,
       este.run_tstamp,
       este.operation_id,
       este.created_at,
       CURRENT_TIMESTAMP, --updated at
       este.event_hash,
       NULL,              --se_user_id
       este.email_address,
       este.booking_id,
       este.unique_browser_id,
       este.cookie_id,
       este.session_userid,
       este.is_robot_spider_event,
       este.is_internal_ip_address_event,
       este.is_server_side_event,
       este.event_tstamp,
       este.posa_territory,
       este.device_platform,
       este.login_type,
       este.se_sale_id,
       este.product_configuration,
       este.product_line,
       este.affiliate_name,
       este.original_affiliate_name,
       este.app_id,
       este.platform,
       este.etl_tstamp,
       este.collector_tstamp,
       este.dvce_created_tstamp,
       este.event,
       este.event_id,
       este.txn_id,
       este.name_tracker,
       este.v_tracker,
       este.v_collector,
       este.v_etl,
       este.user_id,
       este.user_ipaddress,
       este.user_fingerprint,
       este.domain_userid,
       este.domain_sessionidx,
       este.network_userid,
       este.geo_country,
       este.geo_region,
       este.geo_city,
       este.geo_zipcode,
       este.geo_latitude,
       este.geo_longitude,
       este.geo_region_name,
       este.ip_isp,
       este.ip_organization,
       este.ip_domain,
       este.ip_netspeed,
       este.page_url,
       este.page_title,
       este.page_referrer,
       este.page_urlscheme,
       este.page_urlhost,
       este.page_urlport,
       este.page_urlpath,
       este.page_urlquery,
       este.page_urlfragment,
       este.refr_urlscheme,
       este.refr_urlhost,
       este.refr_urlport,
       este.refr_urlpath,
       este.refr_urlquery,
       este.refr_urlfragment,
       este.refr_medium,
       este.refr_source,
       este.refr_term,
       este.mkt_medium,
       este.mkt_source,
       este.mkt_term,
       este.mkt_content,
       este.mkt_campaign,
       este.se_category,
       este.se_action,
       este.se_label,
       este.se_property,
       este.se_value,
       este.tr_orderid,
       este.tr_affiliation,
       este.tr_total,
       este.tr_tax,
       este.tr_shipping,
       este.tr_city,
       este.tr_state,
       este.tr_country,
       este.ti_orderid,
       este.ti_sku,
       este.ti_name,
       este.ti_category,
       este.ti_price,
       este.ti_quantity,
       este.pp_xoffset_min,
       este.pp_xoffset_max,
       este.pp_yoffset_min,
       este.pp_yoffset_max,
       este.useragent,
       este.br_name,
       este.br_family,
       este.br_version,
       este.br_type,
       este.br_renderengine,
       este.br_lang,
       este.br_features_pdf,
       este.br_features_flash,
       este.br_features_java,
       este.br_features_director,
       este.br_features_quicktime,
       este.br_features_realplayer,
       este.br_features_windowsmedia,
       este.br_features_gears,
       este.br_features_silverlight,
       este.br_cookies,
       este.br_colordepth,
       este.br_viewwidth,
       este.br_viewheight,
       este.os_name,
       este.os_family,
       este.os_manufacturer,
       este.os_timezone,
       este.dvce_type,
       este.dvce_ismobile,
       este.dvce_screenwidth,
       este.dvce_screenheight,
       este.doc_charset,
       este.doc_width,
       este.doc_height,
       este.tr_currency,
       este.tr_total_base,
       este.tr_tax_base,
       este.tr_shipping_base,
       este.ti_currency,
       este.ti_price_base,
       este.base_currency,
       este.geo_timezone,
       este.mkt_clickid,
       este.mkt_network,
       este.etl_tags,
       este.dvce_sent_tstamp,
       este.refr_domain_userid,
       este.refr_dvce_tstamp,
       este.domain_sessionid,
       este.derived_tstamp,
       este.event_vendor,
       este.event_name,
       este.event_format,
       este.event_version,
       este.event_fingerprint,
       este.true_tstamp,
       este.contexts_com_optimizely_snowplow_optimizely_summary_1,
       este.contexts_com_secretescapes_all_pages_session_login_type_context_1,
       este.contexts_com_secretescapes_sale_page_context_1,
       este.contexts_com_secretescapes_user_state_context_1,
       este.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
       este.contexts_com_snowplowanalytics_snowplow_web_page_1,
       este.contexts_org_w3_performance_timing_1,
       este.unstruct_event_com_snowplowanalytics_snowplow_link_click_1,
       este.contexts_com_optimizely_optimizelyx_summary_1,
       este.contexts_com_secretescapes_collection_context_1,
       este.contexts_com_secretescapes_filter_context_1,
       este.contexts_com_secretescapes_screen_context_1,
       este.contexts_com_snowplowanalytics_snowplow_application_background_1,
       este.contexts_com_snowplowanalytics_snowplow_application_foreground_1,
       este.contexts_com_snowplowanalytics_snowplow_client_session_1,
       este.contexts_com_snowplowanalytics_snowplow_mobile_context_1,
       este.unstruct_event_com_snowplowanalytics_snowplow_screen_view_1,
       este.unstruct_event_com_secretescapes_searched_with_refinement_event_1,
       este.contexts_com_optimizely_experiment_1,
       este.contexts_com_optimizely_state_1,
       este.contexts_com_optimizely_variation_1,
       este.contexts_com_optimizely_visitor_1,
       este.contexts_com_optimizely_visitor_audience_1,
       este.contexts_com_optimizely_visitor_dimension_1,
       este.unstruct_event_com_snowplowanalytics_mobile_screen_view_1,
       este.contexts_com_secretescapes_booking_context_1,
       este.contexts_com_secretescapes_content_context_1,
       este.contexts_com_secretescapes_environment_context_1,
       este.contexts_com_secretescapes_secret_escapes_sale_context_1,
       este.contexts_com_secretescapes_user_context_1,
       este.contexts_com_secretescapes_who_puts_product_in_front_customer_context_1,
       este.unstruct_event_com_secretescapes_booking_update_event_1,
       este.contexts_com_secretescapes_product_display_context_1,
       este.unstruct_event_com_branch_secretescapes_install_1,
       este.contexts_com_secretescapes_search_context_1,
       este.contexts_com_google_analytics_cookies_1,
       este.unstruct_event_com_snowplowanalytics_snowplow_application_error_1,
       este.contexts_com_secretescapes_content_element_interaction_context_1,
       este.contexts_com_secretescapes_content_elements_rendered_context_1,
       este.contexts_com_secretescapes_content_element_viewed_context_1,
       este.contexts_com_secretescapes_searched_with_refinement_event_1
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream_travelist_events este;

------------------------------------------------------------------------------------------------------------------------
-- investigate which sessions need to be not stitched with se users

-- get a list of unique browser ids that belong to
SELECT DISTINCT es.unique_browser_id
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.page_urlhost IN (
                          'admin.oferty.travelist.pl',
                          'livetest.oferty.travelist.pl',
                          'oferty.travelist.pl',
                          'partner.travelist.pl',
                          'staging.travelist.pl',
                          'travelist.hu',
                          'travelist.pl',
                          'vision.travelist.pl',
                          'zagranica.travelist.pl'
    )


SELECT e.contexts_com_secretescapes_product_display_context_1[0]['se_group_brand']::VARCHAR = 'Travelist Brand',
       e.user_id IS NULL,
       COUNT(*)
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= CURRENT_DATE - 1
  AND e.page_urlhost IN (
                         'admin.oferty.travelist.pl',
                         'livetest.oferty.travelist.pl',
                         'oferty.travelist.pl',
                         'partner.travelist.pl',
                         'staging.travelist.pl',
                         'travelist.hu',
                         'travelist.pl',
                         'vision.travelist.pl',
                         'zagranica.travelist.pl')
GROUP BY 1, 2;


SELECT e.contexts_com_secretescapes_product_display_context_1[0]['se_group_brand']::VARCHAR,
       e.user_id IS NULL,
       COUNT(*)
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= CURRENT_DATE - 1
  AND e.page_urlhost IN (
                         'admin.oferty.travelist.pl',
                         'livetest.oferty.travelist.pl',
                         'oferty.travelist.pl',
                         'partner.travelist.pl',
                         'staging.travelist.pl',
                         'travelist.hu',
                         'travelist.pl',
                         'vision.travelist.pl',
                         'zagranica.travelist.pl')
GROUP BY 1, 2;

SELECT mtba.stitched_identity_type,
       DATE_TRUNC(WEEK, mtba.touch_start_tstamp) AS week,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_hostname IN (
                              'admin.oferty.travelist.pl',
                              'livetest.oferty.travelist.pl',
                              'oferty.travelist.pl',
                              'partner.travelist.pl',
                              'staging.travelist.pl',
                              'travelist.hu',
                              'travelist.pl',
                              'vision.travelist.pl',
                              'zagranica.travelist.pl')
GROUP BY GROUPING SETS (1, 2);

SELECT DATE_TRUNC(WEEK, mtba.touch_start_tstamp) AS week, COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_hostname IN (
                              'admin.oferty.travelist.pl',
                              'livetest.oferty.travelist.pl',
                              'oferty.travelist.pl',
                              'partner.travelist.pl',
                              'staging.travelist.pl',
                              'travelist.hu',
                              'travelist.pl',
                              'vision.travelist.pl',
                              'zagranica.travelist.pl')
  AND mtba.stitched_identity_type = 'se_user_id'
GROUP BY 1

USE WAREHOUSE pipe_2xlarge;
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.page_urlhost IN (
                          'admin.oferty.travelist.pl',
                          'livetest.oferty.travelist.pl',
                          'oferty.travelist.pl',
                          'partner.travelist.pl',
                          'staging.travelist.pl',
                          'travelist.hu',
                          'travelist.pl',
                          'vision.travelist.pl',
                          'zagranica.travelist.pl')
  AND es.se_user_id IS NOT NULL;

USE WAREHOUSE pipe_xlarge;

SELECT stmc.hostname_posa_territory,
       COUNT(*)
FROM se.data.scv_touch_marketing_channel stmc
WHERE stmc.touch_affiliate_territory = 'PL'
GROUP BY 1;

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                                                     INNER JOIN data_vault_mvp.
    AND sua.current_affiliate_territory = 'UK'
WHERE stba.stitched_identity_type = 'se_user_id'
  AND se.data.se_week(stba.touch_start_tstamp::DATE) = 53
  AND se.data.se_year(stba.touch_start_tstamp::DATE) = 2021
  AND stba.touch_hostname_territory NOT IN ('UK', 'ANOMALOUS');

select 1