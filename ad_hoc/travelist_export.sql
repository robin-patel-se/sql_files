WITH
	travelist_events AS (
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
			user_ipaddress,
			user_fingerprint,
			domain_userid,
			domain_sessionidx,
			network_userid,
			geo_country,
			geo_region,
			geo_city,
			geo_zipcode,
			geo_latitude,
			geo_longitude,
			geo_region_name,
			ip_isp,
			ip_organization,
			ip_domain,
			ip_netspeed,
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
			contexts_com_secretescapes_user_context_1,
			contexts_com_secretescapes_who_puts_product_in_front_customer_context_1,
			unstruct_event_com_secretescapes_booking_update_event_1,
			contexts_com_secretescapes_product_display_context_1,
			unstruct_event_com_branch_secretescapes_install_1,
			contexts_com_secretescapes_search_context_1,
			contexts_com_google_analytics_cookies_1,
			unstruct_event_com_snowplowanalytics_snowplow_application_error_1,
			contexts_com_secretescapes_content_element_interaction_context_1,
			contexts_com_secretescapes_content_elements_rendered_context_1,
			contexts_com_secretescapes_content_element_viewed_context_1,
			contexts_com_secretescapes_searched_with_refinement_event_1,
			unstruct_event_com_secretescapes_content_element_interaction_context_1,
			unstruct_event_com_secretescapes_content_elements_rendered_context_1,
			contexts_com_secretescapes_voucher_context_1,
			contexts_com_snowplowanalytics_mobile_application_1,
			contexts_com_snowplowanalytics_mobile_screen_1,
			contexts_com_snowplowanalytics_snowplow_gdpr_1,
			unstruct_event_com_snowplowanalytics_mobile_application_install_1,
			unstruct_event_com_branch_secretescapes_purchase_1,
			unstruct_event_com_secretescapes_authorisation_event_1,
			load_tstamp,
			contexts_com_snowplowanalytics_snowplow_duplicate_1,

			-- create a column to partition rows by the `row_limit` variable

			FLOOR(
					ROW_NUMBER() OVER (
						PARTITION BY
							platform
						ORDER BY
							etl_tstamp
						) / 100000
			) + 1
											 AS row_partition,

			-- create a column which will allow the output files to be partitioned
			-- by the ['platform', 'row_partition'] columns.
			platform || '-' || row_partition AS unload_bucket

		FROM snowplow.atomic.events
		WHERE etl_tstamp >= TIMESTAMPADD('hour', -1, '2024-10-07 07:00:00'::TIMESTAMP) AND
			  collector_tstamp::DATE >= '2018-01-01' AND
			  contexts_com_secretescapes_product_display_context_1[0]:se_group_brand = 'Travelist Brand' AND
			  event_name IS DISTINCT FROM 'page_ping'

	)
SELECT
	te.contexts_com_secretescapes_search_context_1,
	IFF(te.contexts_com_secretescapes_search_context_1[0] = {}, NULL, te.contexts_com_secretescapes_search_context_1) AS contexts_com_secretescapes_search_context_1_new,
	ARRAY_SIZE(contexts_com_secretescapes_search_context_1_new[0]['fuzzy_results_list'])                              AS search_context_fuzzy_result_array_size,
	ARRAY_SIZE(contexts_com_secretescapes_search_context_1_new[0]['results_list'])                                    AS search_context_fuzzy_result_array_size,
	ARRAY_CONSTRUCT(OBJECT_DELETE(contexts_com_secretescapes_search_context_1_new[0], 'fuzzy_results_list',
								  'results_list')),
	te.contexts_com_secretescapes_filter_context_1,
	ARRAY_SIZE(te.contexts_com_secretescapes_filter_context_1[0]['options'])                                          AS filter_context_options_array_size,
	ARRAY_CONSTRUCT(OBJECT_DELETE(contexts_com_secretescapes_filter_context_1[0], 'options')),
	*
FROM travelist_events te
WHERE te.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND contexts_com_secretescapes_search_context_1_new IS NOT NULL



SELECT
	ARRAY_SIZE(te.contexts_com_secretescapes_filter_context_1)                          AS filter_context_array_size,
	ARRAY_SIZE(te.contexts_com_secretescapes_filter_context_1[0]['options'])            AS filter_context_options_array_size,
	te.contexts_com_secretescapes_filter_context_1,
	ARRAY_SIZE(te.contexts_com_secretescapes_filter_context_1)                          AS search_results_context_array_size,
	ARRAY_SIZE(te.contexts_com_secretescapes_filter_context_1[0]['results_list'])       AS search_results_context_results_list_array_size,
	ARRAY_SIZE(te.contexts_com_secretescapes_filter_context_1[0]['fuzzy_results_list']) AS search_results_context_fuzzy_results_list_array_size,
	te.contexts_com_secretescapes_search_context_1,

	*
FROM travelist_events te
;

CREATE OR REPLACE TRANSIENT TABLE unload_vault_mvp_dev_robin.travelist.dev_output AS
SELECT *
FROM unload_vault_mvp_dev_robin.travelist.atomic_events_hourly__20241006t000000__hourly
;

CREATE OR REPLACE TRANSIENT TABLE unload_vault_mvp_dev_robin.travelist.master_output AS
SELECT *
FROM unload_vault_mvp_dev_robin.travelist.atomic_events_hourly__20241006t000000__hourly
;

SHOW TABLES IN SCHEMA unload_vault_mvp_dev_robin.travelist
;


SELECT *
FROM unload_vault_mvp_dev_robin.travelist.dev_output
;

self_describing_task --include 'biapp/task_catalogue/staging/outgoing/travelist/atomic_events_hourly/modelling.py'  --method 'run' --start '2024-10-07 0:00:00' --end '2024-10-07 00:00:00'

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.tvl_atomic_events AS (
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
		user_ipaddress,
		user_fingerprint,
		domain_userid,
		domain_sessionidx,
		network_userid,
		geo_country,
		geo_region,
		geo_city,
		geo_zipcode,
		geo_latitude,
		geo_longitude,
		geo_region_name,
		ip_isp,
		ip_organization,
		ip_domain,
		ip_netspeed,
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
		contexts_com_secretescapes_user_context_1,
		contexts_com_secretescapes_who_puts_product_in_front_customer_context_1,
		unstruct_event_com_secretescapes_booking_update_event_1,
		contexts_com_secretescapes_product_display_context_1,
		unstruct_event_com_branch_secretescapes_install_1,
		contexts_com_secretescapes_search_context_1,
		contexts_com_google_analytics_cookies_1,
		unstruct_event_com_snowplowanalytics_snowplow_application_error_1,
		contexts_com_secretescapes_content_element_interaction_context_1,
		contexts_com_secretescapes_content_elements_rendered_context_1,
		contexts_com_secretescapes_content_element_viewed_context_1,
		contexts_com_secretescapes_searched_with_refinement_event_1,
		unstruct_event_com_secretescapes_content_element_interaction_context_1,
		unstruct_event_com_secretescapes_content_elements_rendered_context_1,
		contexts_com_secretescapes_voucher_context_1,
		contexts_com_snowplowanalytics_mobile_application_1,
		contexts_com_snowplowanalytics_mobile_screen_1,
		contexts_com_snowplowanalytics_snowplow_gdpr_1,
		unstruct_event_com_snowplowanalytics_mobile_application_install_1,
		unstruct_event_com_branch_secretescapes_purchase_1,
		unstruct_event_com_secretescapes_authorisation_event_1,
		load_tstamp,
		contexts_com_snowplowanalytics_snowplow_duplicate_1,

		-- create a column to partition rows by the `row_limit` variable

		FLOOR(
				ROW_NUMBER() OVER (
					PARTITION BY
						platform
					ORDER BY
						etl_tstamp
					) / 100000
		) + 1
										 AS row_partition,

		-- create a column which will allow the output files to be partitioned
		-- by the ['platform', 'row_partition'] columns.
		platform || '-' || row_partition AS unload_bucket

	FROM snowplow.atomic.events
	WHERE collector_tstamp::DATE >= '2024-07-01' AND
		  contexts_com_secretescapes_product_display_context_1[0]:se_group_brand = 'Travelist Brand' AND
		  event_name IS DISTINCT FROM 'page_ping'
)
;


SELECT
	tae.derived_tstamp::DATE   AS date,
	tae.se_category = 'Search' AS search_se_category,
	COUNT(*)
FROM scratch.robinpatel.tvl_atomic_events tae
GROUP BY 1, 2
;

SELECT
	tae.se_category,
	COUNT(*)
FROM scratch.robinpatel.tvl_atomic_events tae
WHERE tae.event_name = 'event'
  AND tae.derived_tstamp::DATE = '2024-10-06'
GROUP BY 1
ORDER BY 2 DESC
;

dataset_task
\
    --include 'outgoing.travelist.atomic_events_hourly' \
    --operation DistributeOperation \
    --method 'run' \
    --upstream \
    --start '2024-10-07 00:00:00' \
    --end '2024-10-07 00:00:00'