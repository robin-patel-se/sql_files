SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-11-03'
  AND es.event_hash = '9a50d5731446f14075df1df7e38cc67524836fb9ec4ed6832b85f97a022cf79c';



SELECT es.se_category, count(*)
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-11-03'
GROUP BY 1;


SELECT get_ddl('table', 'snowplow.atomic.events');


--how many events will be sessionised
SELECT event_name, e.se_category, count(*)
FROM snowplow.atomic.events e
WHERE e.etl_tstamp::DATE = '2020-11-03'
GROUP BY 1, 2;

SELECT event_name, count(*)
FROM snowplow.atomic.events e
WHERE e.etl_tstamp::DATE = '2020-11-03'
  AND (e.event_name IN ('page_view',
                        'screen_view',
                        'transaction_item',
                        'transaction',
                        'booking_update_event')
    OR (e.event_name = 'event'
        AND e.se_category IN ('content interaction',
                              'content rendered',
                              'content viewed'
            )))
GROUP BY 1;

SELECT se_category, count(*)
FROM snowplow.atomic.events e
WHERE e.etl_tstamp::DATE = '2020-11-03'
  AND e.event_name = 'event'
  AND e.se_category IN ('content interaction',
                        'content rendered',
                        'content viewed'
    )

GROUP BY 1;

DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;


self_describing_task --include 'hygiene/snowplow/events.py'  --method 'run' --start '2020-11-03 00:00:00' --end '2020-11-03 00:00:00';


SELECT *
FROM snowplow.atomic.events e
WHERE e.event_id IN ('0c90b49d-10c2-4710-848b-e09a22ad65f1', 'db59164c-24c8-4500-a31e-1de354ed5978')
  AND e.etl_tstamp::DATE = '2020-11-03';



CREATE OR REPLACE TABLE events
(
    app_id                                                                  VARCHAR(16777216),
    platform                                                                VARCHAR(16777216),
    etl_tstamp                                                              TIMESTAMP_NTZ(9),
    collector_tstamp                                                        TIMESTAMP_NTZ(9)  NOT NULL,
    dvce_created_tstamp                                                     TIMESTAMP_NTZ(9),
    event                                                                   VARCHAR(16777216),
    event_id                                                                VARCHAR(16777216) NOT NULL,
    txn_id                                                                  NUMBER(38, 0),
    name_tracker                                                            VARCHAR(16777216),
    v_tracker                                                               VARCHAR(16777216),
    v_collector                                                             VARCHAR(16777216) NOT NULL,
    v_etl                                                                   VARCHAR(16777216) NOT NULL,
    user_id                                                                 VARCHAR(16777216),
    user_ipaddress                                                          VARCHAR(16777216),
    user_fingerprint                                                        VARCHAR(16777216),
    domain_userid                                                           VARCHAR(16777216),
    domain_sessionidx                                                       NUMBER(38, 0),
    network_userid                                                          VARCHAR(16777216),
    geo_country                                                             VARCHAR(16777216),
    geo_region                                                              VARCHAR(16777216),
    geo_city                                                                VARCHAR(16777216),
    geo_zipcode                                                             VARCHAR(16777216),
    geo_latitude                                                            FLOAT,
    geo_longitude                                                           FLOAT,
    geo_region_name                                                         VARCHAR(16777216),
    ip_isp                                                                  VARCHAR(16777216),
    ip_organization                                                         VARCHAR(16777216),
    ip_domain                                                               VARCHAR(16777216),
    ip_netspeed                                                             VARCHAR(16777216),
    page_url                                                                VARCHAR(16777216),
    page_title                                                              VARCHAR(16777216),
    page_referrer                                                           VARCHAR(16777216),
    page_urlscheme                                                          VARCHAR(16777216),
    page_urlhost                                                            VARCHAR(16777216),
    page_urlport                                                            NUMBER(38, 0),
    page_urlpath                                                            VARCHAR(16777216),
    page_urlquery                                                           VARCHAR(16777216),
    page_urlfragment                                                        VARCHAR(16777216),
    refr_urlscheme                                                          VARCHAR(16777216),
    refr_urlhost                                                            VARCHAR(16777216),
    refr_urlport                                                            NUMBER(38, 0),
    refr_urlpath                                                            VARCHAR(16777216),
    refr_urlquery                                                           VARCHAR(16777216),
    refr_urlfragment                                                        VARCHAR(16777216),
    refr_medium                                                             VARCHAR(16777216),
    refr_source                                                             VARCHAR(16777216),
    refr_term                                                               VARCHAR(16777216),
    mkt_medium                                                              VARCHAR(16777216),
    mkt_source                                                              VARCHAR(16777216),
    mkt_term                                                                VARCHAR(16777216),
    mkt_content                                                             VARCHAR(16777216),
    mkt_campaign                                                            VARCHAR(16777216),
    se_category                                                             VARCHAR(16777216),
    se_action                                                               VARCHAR(16777216),
    se_label                                                                VARCHAR(16777216),
    se_property                                                             VARCHAR(16777216),
    se_value                                                                FLOAT,
    tr_orderid                                                              VARCHAR(16777216),
    tr_affiliation                                                          VARCHAR(16777216),
    tr_total                                                                NUMBER(18, 2),
    tr_tax                                                                  NUMBER(18, 2),
    tr_shipping                                                             NUMBER(18, 2),
    tr_city                                                                 VARCHAR(16777216),
    tr_state                                                                VARCHAR(16777216),
    tr_country                                                              VARCHAR(16777216),
    ti_orderid                                                              VARCHAR(16777216),
    ti_sku                                                                  VARCHAR(16777216),
    ti_name                                                                 VARCHAR(16777216),
    ti_category                                                             VARCHAR(16777216),
    ti_price                                                                NUMBER(18, 2),
    ti_quantity                                                             NUMBER(38, 0),
    pp_xoffset_min                                                          NUMBER(38, 0),
    pp_xoffset_max                                                          NUMBER(38, 0),
    pp_yoffset_min                                                          NUMBER(38, 0),
    pp_yoffset_max                                                          NUMBER(38, 0),
    useragent                                                               VARCHAR(16777216),
    br_name                                                                 VARCHAR(16777216),
    br_family                                                               VARCHAR(16777216),
    br_version                                                              VARCHAR(16777216),
    br_type                                                                 VARCHAR(16777216),
    br_renderengine                                                         VARCHAR(16777216),
    br_lang                                                                 VARCHAR(16777216),
    br_features_pdf                                                         BOOLEAN,
    br_features_flash                                                       BOOLEAN,
    br_features_java                                                        BOOLEAN,
    br_features_director                                                    BOOLEAN,
    br_features_quicktime                                                   BOOLEAN,
    br_features_realplayer                                                  BOOLEAN,
    br_features_windowsmedia                                                BOOLEAN,
    br_features_gears                                                       BOOLEAN,
    br_features_silverlight                                                 BOOLEAN,
    br_cookies                                                              BOOLEAN,
    br_colordepth                                                           VARCHAR(16777216),
    br_viewwidth                                                            NUMBER(38, 0),
    br_viewheight                                                           NUMBER(38, 0),
    os_name                                                                 VARCHAR(16777216),
    os_family                                                               VARCHAR(16777216),
    os_manufacturer                                                         VARCHAR(16777216),
    os_timezone                                                             VARCHAR(16777216),
    dvce_type                                                               VARCHAR(16777216),
    dvce_ismobile                                                           BOOLEAN,
    dvce_screenwidth                                                        NUMBER(38, 0),
    dvce_screenheight                                                       NUMBER(38, 0),
    doc_charset                                                             VARCHAR(16777216),
    doc_width                                                               NUMBER(38, 0),
    doc_height                                                              NUMBER(38, 0),
    tr_currency                                                             VARCHAR(16777216),
    tr_total_base                                                           NUMBER(18, 2),
    tr_tax_base                                                             NUMBER(18, 2),
    tr_shipping_base                                                        NUMBER(18, 2),
    ti_currency                                                             VARCHAR(16777216),
    ti_price_base                                                           NUMBER(18, 2),
    base_currency                                                           VARCHAR(16777216),
    geo_timezone                                                            VARCHAR(16777216),
    mkt_clickid                                                             VARCHAR(16777216),
    mkt_network                                                             VARCHAR(16777216),
    etl_tags                                                                VARCHAR(16777216),
    dvce_sent_tstamp                                                        TIMESTAMP_NTZ(9),
    refr_domain_userid                                                      VARCHAR(16777216),
    refr_dvce_tstamp                                                        TIMESTAMP_NTZ(9),
    domain_sessionid                                                        VARCHAR(16777216),
    derived_tstamp                                                          TIMESTAMP_NTZ(9),
    event_vendor                                                            VARCHAR(16777216),
    event_name                                                              VARCHAR(16777216),
    event_format                                                            VARCHAR(16777216),
    event_version                                                           VARCHAR(16777216),
    event_fingerprint                                                       VARCHAR(16777216),
    true_tstamp                                                             TIMESTAMP_NTZ(9),
    contexts_com_optimizely_snowplow_optimizely_summary_1                   ARRAY,
    contexts_com_secretescapes_all_pages_session_login_type_context_1       ARRAY,
    contexts_com_secretescapes_sale_page_context_1                          ARRAY,
    contexts_com_secretescapes_user_state_context_1                         ARRAY,
    contexts_com_snowplowanalytics_snowplow_ua_parser_context_1             ARRAY,
    contexts_com_snowplowanalytics_snowplow_web_page_1                      ARRAY,
    contexts_org_w3_performance_timing_1                                    ARRAY,
    unstruct_event_com_snowplowanalytics_snowplow_link_click_1              OBJECT,
    contexts_com_optimizely_optimizelyx_summary_1                           ARRAY,
    contexts_com_secretescapes_collection_context_1                         ARRAY,
    contexts_com_secretescapes_filter_context_1                             ARRAY,
    contexts_com_secretescapes_screen_context_1                             ARRAY,
    contexts_com_snowplowanalytics_snowplow_application_background_1        ARRAY,
    contexts_com_snowplowanalytics_snowplow_application_foreground_1        ARRAY,
    contexts_com_snowplowanalytics_snowplow_client_session_1                ARRAY,
    contexts_com_snowplowanalytics_snowplow_mobile_context_1                ARRAY,
    unstruct_event_com_snowplowanalytics_snowplow_screen_view_1             OBJECT,
    unstruct_event_com_secretescapes_searched_with_refinement_event_1       OBJECT,
    contexts_com_optimizely_experiment_1                                    ARRAY,
    contexts_com_optimizely_state_1                                         ARRAY,
    contexts_com_optimizely_variation_1                                     ARRAY,
    contexts_com_optimizely_visitor_1                                       ARRAY,
    contexts_com_optimizely_visitor_audience_1                              ARRAY,
    contexts_com_optimizely_visitor_dimension_1                             ARRAY,
    unstruct_event_com_snowplowanalytics_mobile_screen_view_1               OBJECT,
    contexts_com_secretescapes_booking_context_1                            ARRAY,
    contexts_com_secretescapes_content_context_1                            ARRAY,
    contexts_com_secretescapes_environment_context_1                        ARRAY,
    contexts_com_secretescapes_secret_escapes_sale_context_1                ARRAY,
    contexts_com_secretescapes_user_context_1                               ARRAY,
    contexts_com_secretescapes_who_puts_product_in_front_customer_context_1 ARRAY,
    unstruct_event_com_secretescapes_booking_update_event_1                 OBJECT,
    contexts_com_secretescapes_product_display_context_1                    ARRAY,
    unstruct_event_com_branch_secretescapes_install_1                       OBJECT,
    contexts_com_secretescapes_search_context_1                             ARRAY,
    contexts_com_google_analytics_cookies_1                                 ARRAY,
    unstruct_event_com_snowplowanalytics_snowplow_application_error_1       OBJECT,
    contexts_com_secretescapes_content_element_interaction_context_1        ARRAY,
    contexts_com_secretescapes_content_elements_rendered_context_1          ARRAY,
    contexts_com_secretescapes_content_element_viewed_context_1             ARRAY,
    CONSTRAINT event_id_pk PRIMARY KEY (event_id)
);



CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;



SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es;
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification;

SELECT COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification;

SELECT count(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es;
SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es;

DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;

self_describing_task --include 'hygiene/snowplow/events.py'  --method 'run' --start '2020-11-05 00:00:00' --end '2020-11-05 00:00:00'

DROP SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg;

airflow backfill --start_date '2020-11-04 03:00:00' --end_date '2020-11-04 03:00:00' --task_regex '.*' single_customer_view__daily_at_03h00

SELECT count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls;


SELECT event_name, count(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
WHERE e.etl_tstamp::DATE = '2020-11-03'
  AND (e.event_name IN ('page_view',
                        'screen_view',
                        'transaction_item',
                        'transaction',
                        'booking_update_event')
    OR (e.event_name = 'event'
        AND e.se_category IN ('content interaction',
                              'content rendered',
                              'content viewed'
            )))
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------

SELECT pARSE_URL(sts.page_url):host::VARCHAR AS hostname, count(*)
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp >= '2020-11-09'
GROUP BY 1;

SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_hostname = 'de.sales.secretescapes.com'
  AND stba.touch_start_tstamp::DATE = '2020-11-09';


SELECT * FROM hygiene_vault_mvp.snowplow.event_stream es WHERE es.event_tstamp >= '2020-11-09' AND es.page_urlhost = 'de.sales.secretescapes.com';

SELECT * FROM hygiene_vault_mvp.snowplow.event_stream es WHERE es.event_tstamp >= '2020-11-09' AND es.page_urlhost = 'de.sales.secretescapes.com' AND es.page_urlquery IS NULL;

SELECT ssa.se_sale_id FROM se.data.se_sale_attributes ssa WHERE hotel_code = '001w000001DVHS5';

SELECT ss.se_sale_id,
       ss.date_created,
       ss.company_name,
       ss.product_configuration,
       ss.original_contractor_name,
       ss.current_contractor_name FROM data_vault_mvp.dwh.se_sale ss WHERE ss.company_name = 'Carbis Bay Hotel'