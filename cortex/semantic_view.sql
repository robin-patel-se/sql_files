USE ROLE securityadmin;

GRANT OWNERSHIP ON ALL SEMANTIC VIEWS IN SCHEMA se.data
TO ROLE ai_admin
COPY CURRENT GRANTS;





SELECT
	sm.geo_city,
	count(*)
	FROM se.bi.session_metrics sm
WHERE sm.touch_start_tstamp >= current_date - 10
GROUP BY ALL
	ORDER BY count(*) desc
;

          - CFNetwork


GEO_CITY
          - Berlin
          - Vienna
          - Munich
          - Warsaw
          - Frankfurt am Main
          - Stuttgart
          - Hamburg
          - Dublin
          - Poznan
          - Cologne
          - Bielefeld
          - Wroclaw
          - Leipzig
          - City of London;



SELECT
	touch_id,
	attributed_user_id,
	stitched_identity_type,
	touch_logged_in,
	touch_start_tstamp,
	touch_end_tstamp,
	touch_duration_seconds,
	touch_affiliate_territory,
	touch_mkt_channel,
	channel_category,
	last_non_direct_touch_mkt_channel,
	last_non_direct_channel_category,
	last_paid_touch_mkt_channel,
	last_paid_channel_category,
	touch_experience,
	platform,
	touch_landing_page,
	touch_landing_pagepath,
	landing_page_category,
	touch_hostname,
	touch_exit_pagepath,
	touch_referrer_url,
	touch_se_brand,
	touch_event_count,
	touch_has_booking,
	is_se_internal_touch,
	user_ipaddress,
	geo_country,
	geo_city,
	geo_zipcode,
	geo_latitude,
	geo_longitude,
	geo_region_name,
	useragent,
	br_name,
	br_family,
	os_name,
	os_family,
	os_manufacturer,
	dvce_screenwidth,
	dvce_screenheight,
	app_state_context,
	landing_app_state,
	spvs,
	has_spv,
	unique_spvs,
	spvs_hotel,
	has_spv_hotel,
	unique_spvs_hotel,
	spvs_hotel_plus,
	has_spv_hotel_plus,
	unique_spvs_hotel_plus,
	spvs_catalogue,
	has_spv_catalogue,
	unique_spvs_catalogue,
	spvs_package,
	has_spv_package,
	unique_spvs_package,
	booking_form_views,
	has_booking_form_view,
	booking_form_views_hotel,
	has_booking_form_view_hotel,
	booking_form_views_hotel_plus,
	has_booking_form_view_hotel_plus,
	booking_form_views_catalogue,
	has_booking_form_view_catalogue,
	booking_form_views_package,
	has_booking_form_view_package,
	booking_id_list,
	booking_id_array,
	bookings,
	has_booking,
	bookings_hotel,
	has_booking_hotel,
	bookings_hotel_plus,
	has_booking_hotel_plus,
	bookings_catalogue,
	has_booking_catalogue,
	bookings_package,
	has_booking_package,
	margin_gbp,
	searches,
	has_search,
	user_searches,
	has_user_search,
	page_load_searches,
	has_page_load_search,
	min_price_filter_searches,
	max_price_filter_searches,
	sort_by_searches,
	pay_button_clicks,
	has_pay_button_click,
	feature_flag_array,
	feature_flag_test_array,
	first_login_type,
	last_login_type,
	login_types_list,
	login_types_array,
	login_types_count
FROM se.bi.session_metrics sm

