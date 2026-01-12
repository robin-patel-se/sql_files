USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest
	CLONE data_vault_mvp.single_customer_view_stg.module_events_of_interest
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.product_reservation
	CLONE latest_vault.cms_mysql.product_reservation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.reservation
	CLONE latest_vault.cms_mysql.reservation
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.traffic_model__events_of_interest_agg
	CLONE data_vault_mvp.bi.traffic_model__events_of_interest_agg
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.scv.session_level_metrics.session_level_metrics__events_of_interest_agg.py' \
    --method 'run' \
    --start '2025-11-25 00:00:00' \
    --end '2025-11-25 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.dim_sale ds
;


SELECT
	touch_id,
	spvs,
	unique_spvs,
	spvs_hotel_plus,
	unique_spvs_hotel_plus,
	spvs_catalogue,
	unique_spvs_catalogue,
	booking_form_views,
	booking_form_views_hotel_plus,
	booking_form_views_catalogue,
	bookings,
	booking_id_list,
	bookings_hotel_plus,
	margin_gbp,
	gross_revenue_gbp,
	searches,
	user_searches,
	page_load_searches,
	min_price_filter_searches,
	max_price_filter_searches,
	sort_by_searches
FROM data_vault_mvp_dev_robin.bi.traffic_model__events_of_interest_agg_clone__step03__agg_event_metrics
;

DROP TABLE data_vault_mvp_dev_robin.bi.traffic_model__events_of_interest_agg
;


SELECT
	stmeoi.event_category,
	stmeoi.event_subcategory,
	COUNT(*)
FROM se.data.scv_touched_module_events_of_interest stmeoi
GROUP BY ALL
;



SELECT
	events.touch_id,
	-- spvs
	SUM(IFF(events.event_subcategory = 'SPV', 1, NULL))                   AS spvs,
	COUNT(DISTINCT
		  IFF(events.event_subcategory = 'SPV', events.se_sale_id, NULL)) AS unique_spvs,
	SUM(IFF(events.event_subcategory = 'SPV' AND dim_sale.sale_type IN ('Hotel', 'Hotel Plus'), 1,
			NULL))                                                        AS spvs_hotel_plus,
	COUNT(DISTINCT
		  IFF(events.event_subcategory = 'SPV' AND dim_sale.sale_type IN ('Hotel', 'Hotel Plus'), events.se_sale_id,
			  NULL))                                                      AS unique_spvs_hotel_plus,
	SUM(IFF(events.event_subcategory = 'SPV' AND dim_sale.sale_type = 'Catalogue', 1,
			NULL))                                                        AS spvs_catalogue,
	COUNT(DISTINCT IFF(events.event_subcategory = 'SPV' AND dim_sale.sale_type = 'Catalogue', events.se_sale_id,
					   NULL))                                             AS unique_spvs_catalogue,

	-- bfvs
	SUM(IFF(events.event_subcategory = 'booking_form_view', 1, NULL))     AS booking_form_views,
	SUM((IFF(
			events.event_subcategory = 'booking_form_view'
				AND dim_sale.sale_type IN ('Hotel', 'Hotel Plus')
				AND COALESCE(has_flights.has_flights, fact_booking.booking_includes_flight) = TRUE,
			1,
			NULL)))                                                       AS booking_form_views_hotel_plus,
	SUM(IFF(events.event_subcategory = 'booking_form_view' AND dim_sale.sale_type = 'Catalogue', 1,
			NULL))                                                        AS booking_form_views_catalogue,

	-- bookings
	COUNT(DISTINCT fact_booking.booking_id)                               AS bookings,
	LISTAGG(DISTINCT fact_booking.booking_id, ', ')                       AS booking_id_list,
	ARRAY_AGG(DISTINCT fact_booking.booking_id)
			  WITHIN GROUP ( ORDER BY fact_booking.booking_id ASC)        AS booking_id_array,
	COUNT(DISTINCT
		  IFF(dim_sale.sale_product IN ('Hotel', 'Hotel Plus')
				  AND fact_booking.booking_includes_flight,
			  fact_booking.booking_id,
			  NULL))                                                      AS bookings_hotel_plus,

	-- margin
	SUM(fact_booking.margin
		_gross_of_toms_gbp_constant_currency)                             AS margin_gbp,
	SUM(fact_booking.gross_revenue_gbp_constant_currency)                 AS gross_revenue_gbp,

	-- searches
	SUM(IFF(events.event_subcategory = 'search', 1, NULL))                AS searches,
	SUM(IFF(events.event_subcategory = 'search' AND events.triggered_by = 'user', 1,
			NULL))                                                        AS user_searches,
	SUM(IFF(events.event_subcategory = 'search' AND events.triggered_by = 'pageLoad', 1,
			NULL))                                                        AS page_load_searches,
	SUM(IFF(events.event_subcategory = 'search' AND
			TRY_TO_NUMBER(PARSE_URL(events.page_url, 1)['parameters']['minPrice']::VARCHAR) IS NOT NULL, 1,
			NULL))                                                        AS min_price_filter_searches,
	SUM(IFF(events.event_subcategory = 'search' AND
			TRY_TO_NUMBER(PARSE_URL(events.page_url, 1)['parameters']['maxPrice']::VARCHAR) IS NOT NULL, 1,
			NULL))                                                        AS max_price_filter_searches,
	SUM(IFF(events.event_subcategory = 'search' AND
			PARSE_URL(events.page_url, 1)['parameters']['sortBy']::VARCHAR IS NOT NULL, 1,
			NULL))                                                        AS sort_by_searches,


	-- pay button clicks
	SUM(IFF(events.event_subcategory = 'pay_button_click', 1, NULL))      AS pay_button_clicks
FROM data_vault_mvp_dev_robin.bi.traffic_model__events_of_interest_agg__step01__get_source_batch batch
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest events
	ON batch.touch_id = events.touch_id
	AND events.updated_at >= TIMESTAMPADD('day', -1, '2025-11-23 03:30:00'::TIMESTAMP)
LEFT JOIN data_vault_mvp_dev_robin.dwh.fact_booking fact_booking
	ON events.booking_id = fact_booking.booking_id
	AND events.event_category = 'transaction'
	AND fact_booking.booking_status_type IN ('live', 'cancelled')
LEFT JOIN data_vault_mvp_dev_robin.dwh.dim_sale dim_sale
	ON COALESCE(events.se_sale_id, fact_booking.se_sale_id) = dim_sale.se_sale_id
LEFT JOIN data_vault_mvp_dev_robin.bi.traffic_model__events_of_interest_agg__step02__booking_has_flights has_flights
	ON events.booking_id = has_flights.booking_id
WHERE events.event_tstamp >= '2023-01-01'
GROUP BY events.touch_id
;


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_level_metrics__feature_flags
	CLONE data_vault_mvp.bi.session_level_metrics__feature_flags
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.scv.session_level_metrics.session_level_metrics__feature_flags.py' \
    --method 'run' \
    --start '2025-11-25 00:00:00' \
    --end '2025-11-25 00:00:00'

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

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_level_metrics__login_types
	CLONE data_vault_mvp.bi.session_level_metrics__login_types
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.scv.session_level_metrics.session_level_metrics__login_types.py' \
    --method 'run' \
    --start '2025-11-25 00:00:00' \
    --end '2025-11-25 00:00:00'

;

self_describing_task --include 'biapp/task_catalogue/dv/bi/scv/session_metrics/session_metrics__events_of_interest.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/bi/scv/session_metrics/session_metrics__feature_flags.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/bi/scv/session_metrics/session_metrics__login_types.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

SELECT *
FROM dbt.bi_product_analytics__intermediate.pda_session_metrics psm


SELECT GET_DDL('table', 'dbt.bi_product_analytics__intermediate.pda_session_metrics')
;

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.bi
;

self_describing_task --include 'biapp/task_catalogue/dv/bi/scv/session_metrics/session_metrics.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
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
	lnd_touch_mkt_channel,
	lnd_channel_category,
	lp_touch_mkt_channel,
	lp_channel_category,
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
	spvs_hotel_plus,
	has_spv_hotel_plus,
	unique_spvs_hotel_plus,
	booking_form_views,
	has_booking_form_view,
	booking_form_views_hotel_plus,
	has_booking_form_view_hotel_plus,
	booking_form_views_catalogue,
	has_booking_form_view_catalogue,
	booking_id_list,
	booking_id_array,
	bookings,
	has_booking,
	bookings_hotel_plus,
	has_booking_hotel_plus,
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
FROM data_vault_mvp_dev_robin.bi.session_metrics
;

CREATE OR REPLACE VIEW se_dev_robin.bi.session_metrics
			(
			 touch_id COMMENT 'A unique identifier for each session.',
			 attributed_user_id COMMENT 'The unique identifier attributed to a user.',
			 stitched_identity_type COMMENT 'The type of stitched identity.',
			 touch_logged_in COMMENT 'A flag indicating whether a user has logged in.',
			 touch_start_tstamp COMMENT 'The timestamp when the session was first interacted with.',
			 touch_end_tstamp COMMENT 'The timestamp when the session ended.',
			 touch_duration_seconds COMMENT 'The duration of time a user spent interacting with the system in seconds.',
			 touch_affiliate_territory COMMENT 'The country or region where the affiliate is located.',
			 touch_mkt_channel COMMENT 'The marketing touchpoint or channel through which a user interacted with the system.',
			 channel_category COMMENT 'The category of the channel through which a session was initiated.',
			 lnd_touch_mkt_channel COMMENT 'The marketing channel through which a user was last touched.',
			 lnd_channel_category COMMENT 'The category of the landing page or channel.',
			 lp_touch_mkt_channel COMMENT 'The marketing channel through which a customer last interacted with the business.',
			 lp_channel_category COMMENT 'The type of the channel through which a user is interacting with the system.',
			 touch_experience COMMENT 'The type of user touch experience.',
			 platform COMMENT 'The platform used to access the session.',
			 touch_landing_page COMMENT 'The URL of the landing page that was touched by the user.',
			 touch_landing_pagepath COMMENT 'The path to the landing page.',
			 landing_page_category COMMENT 'The category of the landing page.',
			 touch_hostname COMMENT 'The hostname of the device that was interacted with.',
			 touch_exit_pagepath COMMENT 'The path to the page that the user exited to.',
			 touch_referrer_url COMMENT 'The URL of the referrer that initiated the session.',
			 touch_se_brand COMMENT 'The brand of the touch screen.',
			 touch_event_count COMMENT 'The count of touch events.',
			 touch_has_booking COMMENT 'Whether the session has a booking.',
			 is_se_internal_touch COMMENT 'A flag indicating whether the session is an internal touch.',
			 user_ipaddress COMMENT 'The IP address of the user.',
			 geo_country COMMENT 'The country of origin.',
			 geo_city COMMENT 'The city where the session took place.',
			 geo_zipcode COMMENT 'The geographic zip code.',
			 geo_latitude COMMENT 'The geographic latitude of a location.',
			 geo_longitude COMMENT 'The longitude of a geographic location.',
			 geo_region_name COMMENT 'The geographic region name.',
			 useragent COMMENT 'The user agent string of the client browser.',
			 br_name COMMENT 'Browser name.',
			 br_family COMMENT 'Browser family.',
			 os_name COMMENT 'The operating system name.',
			 os_family COMMENT 'The operating system family.',
			 os_manufacturer COMMENT 'The manufacturer of the operating system.',
			 dvce_screenwidth COMMENT 'The width of the screen in pixels.',
			 dvce_screenheight COMMENT 'The height of the screen in pixels.',
			 app_state_context COMMENT 'The state of the application.',
			 landing_app_state COMMENT 'The state in which the landing application was launched.',
			 spvs COMMENT 'The number of seconds spent in a session.',
			 has_spv COMMENT 'Whether the session has a Single Page View (SPV).',
			 unique_spvs COMMENT 'Unique session metrics identifiers.',
			 spvs_hotel_plus COMMENT 'The number of hotel plus sessions.',
			 has_spv_hotel_plus COMMENT 'Whether the session has a hotel plus feature.',
			 unique_spvs_hotel_plus COMMENT 'A count of unique sessions with hotel plus features.',
			 booking_form_views COMMENT 'The number of times a booking form has been viewed.',
			 has_booking_form_view COMMENT 'Whether the booking form has been viewed.',
			 booking_form_views_hotel_plus COMMENT 'The number of times a hotel booking form was viewed on the Hotel Plus platform.',
			 has_booking_form_view_hotel_plus COMMENT 'Whether the booking form view for hotel plus is available.',
			 booking_form_views_catalogue COMMENT 'The number of times a booking form catalogue has been viewed.',
			 has_booking_form_view_catalogue COMMENT 'Whether the booking form view catalogue is available.',
			 booking_id_list COMMENT 'Booking IDs.',
			 booking_id_array COMMENT 'An array of unique identifiers for bookings.',
			 bookings COMMENT 'The number of bookings.',
			 has_booking COMMENT 'Whether the session has a booking.',
			 bookings_hotel_plus COMMENT 'The number of hotel bookings.',
			 has_booking_hotel_plus COMMENT 'A column holding data of type BooleanType.',
			 margin_gbp COMMENT 'Margin in British Pounds.',
			 searches COMMENT 'The number of searches performed.',
			 has_search COMMENT 'A column holding data of type BooleanType.',
			 user_searches COMMENT 'The number of searches performed by the user.',
			 has_user_search COMMENT 'Whether the user has performed a search.',
			 page_load_searches COMMENT 'The number of page load searches.',
			 has_page_load_search COMMENT 'Whether the page load search was executed.',
			 min_price_filter_searches COMMENT 'The minimum price filter applied to searches.',
			 max_price_filter_searches COMMENT 'The maximum price filter searches.',
			 sort_by_searches COMMENT 'The number of searches used to sort the session.',
			 pay_button_clicks COMMENT 'The number of times the pay button was clicked.',
			 has_pay_button_click COMMENT 'Whether the pay button was clicked.',
			 feature_flag_array COMMENT 'An array of feature flag data.',
			 feature_flag_test_array COMMENT 'Feature flags for testing.',
			 first_login_type COMMENT 'The type of the user''s first login.',
			 last_login_type COMMENT 'The type of the last login method used by the user.',
			 login_types_list COMMENT 'The types of login methods used by a user.',
			 login_types_array COMMENT 'The types of login methods used by users.',
			 login_types_count COMMENT 'The count of different login types.'
				) COMMENT ='The table contains records of user interactions with a digital platform, capturing metrics about session duration, user attributes, and platform usage. Each record represents a single user session and includes details about the user''s behavior, platform, and marketing channel interactions.'
AS
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
	lnd_touch_mkt_channel,
	lnd_channel_category,
	lp_touch_mkt_channel,
	lp_channel_category,
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
	spvs_hotel_plus,
	has_spv_hotel_plus,
	unique_spvs_hotel_plus,
	booking_form_views,
	has_booking_form_view,
	booking_form_views_hotel_plus,
	has_booking_form_view_hotel_plus,
	booking_form_views_catalogue,
	has_booking_form_view_catalogue,
	booking_id_list,
	booking_id_array,
	bookings,
	has_booking,
	bookings_hotel_plus,
	has_booking_hotel_plus,
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
FROM data_vault_mvp_dev_robin.bi.session_metrics
;


SELECT
	ds.sale_type,
	ds.product_configuration,
	ds.sale_product,
	COUNT(*)
FROM se.data.dim_sale ds
WHERE ds.sale_active
GROUP BY ALL
;

SELECT NULL IS DISTINCT FROM TRUE
;


SELECT
	events.touch_id,
	-- spvs
	SUM(IFF(events.event_subcategory = 'SPV', 1, NULL))                   AS spvs,
	COUNT(DISTINCT
		  IFF(events.event_subcategory = 'SPV', events.se_sale_id, NULL)) AS unique_spvs,
	SUM(IFF(events.event_subcategory = 'SPV' AND dim_sale.sale_type = 'Hotel', 1,
			NULL))                                                        AS spvs_hotel,
	COUNT(DISTINCT IFF(events.event_subcategory = 'SPV' AND dim_sale.sale_type = 'Hotel', events.se_sale_id,
					   NULL))                                             AS unique_spvs_hotel,
	SUM(IFF(events.event_subcategory = 'SPV' AND dim_sale.sale_type = 'Hotel Plus', 1,
			NULL))                                                        AS spvs_hotel_plus,
	COUNT(DISTINCT IFF(events.event_subcategory = 'SPV' AND dim_sale.sale_type = 'Hotel Plus', events.se_sale_id,
					   NULL))                                             AS unique_spvs_hotel_plus,
	SUM(IFF(events.event_subcategory = 'SPV' AND dim_sale.sale_type = 'Catalogue', 1,
			NULL))                                                        AS spvs_catalogue,
	COUNT(DISTINCT IFF(events.event_subcategory = 'SPV' AND dim_sale.sale_type = 'Catalogue', events.se_sale_id,
					   NULL))                                             AS unique_spvs_catalogue,
	SUM(IFF(events.event_subcategory = 'SPV' AND dim_sale.sale_type LIKE ANY ('Catalogue', 'IHP%'), 1,
			NULL))                                                        AS spvs_package,
	COUNT(DISTINCT
		  IFF(events.event_subcategory = 'SPV' AND dim_sale.sale_type LIKE ANY ('Catalogue', 'IHP%'), events.se_sale_id,
			  NULL))                                                      AS unique_spvs_package,

	-- bfvs
	SUM(IFF(events.event_subcategory = 'booking_form_view', 1, NULL))     AS booking_form_views,
	SUM((IFF(
			events.event_subcategory = 'booking_form_view' AND dim_sale.sale_type = 'Hotel'
				AND COALESCE(has_flights.has_flights, fact_booking.booking_includes_flight) IS DISTINCT FROM TRUE,
			1,
			NULL)))                                                       AS booking_form_views_hotel,
	SUM((IFF(
			events.event_subcategory = 'booking_form_view'
				AND dim_sale.sale_type IN ('Hotel', 'Hotel Plus')
				AND COALESCE(has_flights.has_flights, fact_booking.booking_includes_flight) = TRUE,
			1,
			NULL)))                                                       AS booking_form_views_hotel_plus,
	SUM(IFF(events.event_subcategory = 'booking_form_view' AND dim_sale.sale_type = 'Catalogue', 1,
			NULL))                                                        AS booking_form_views_catalogue,
	SUM(IFF(events.event_subcategory = 'booking_form_view' AND dim_sale.sale_type LIKE ANY ('Catalogue', 'IHP%'), 1,
			NULL))                                                        AS booking_form_views_package,

	-- bookings
	COUNT(DISTINCT fact_booking.booking_id)                               AS bookings,
	COUNT(DISTINCT
		  IFF(events.event_subcategory = 'booking_form_view' AND dim_sale.sale_type = 'Hotel', fact_booking.booking_id,
			  NULL))                                                      AS bookings_hotel,
	COUNT(DISTINCT IFF(events.event_subcategory = 'booking_form_view' AND dim_sale.sale_type = 'Hotel Plus',
					   fact_booking.booking_id,
					   NULL))                                             AS bookings_hotel_plus,
	COUNT(DISTINCT IFF(events.event_subcategory = 'booking_form_view' AND dim_sale.sale_type = 'Catalogue',
					   fact_booking.booking_id,
					   NULL))                                             AS bookings_catalogue,
	COUNT(DISTINCT IFF(events.event_subcategory = 'booking_form_view' LIKE ANY ('Catalogue', 'IHP%'),
					   fact_booking.booking_id,
					   NULL))                                             AS bookings_package,

	LISTAGG(DISTINCT fact_booking.booking_id, ', ')                       AS booking_id_list,
	ARRAY_AGG(DISTINCT fact_booking.booking_id)
			  WITHIN GROUP ( ORDER BY fact_booking.booking_id ASC)        AS booking_id_array,
	COUNT(DISTINCT
		  IFF(dim_sale.sale_product IN ('Hotel', 'Hotel Plus')
				  AND fact_booking.booking_includes_flight,
			  fact_booking.booking_id,
			  NULL))                                                      AS bookings_hotel_plus,

	-- margin
	SUM(fact_booking.margin_gross_of_toms_gbp_constant_currency)          AS margin_gbp,
	SUM(fact_booking.gross_revenue_gbp_constant_currency)                 AS gross_revenue_gbp,

	-- searches
	SUM(IFF(events.event_subcategory = 'search', 1, NULL))                AS searches,
	SUM(IFF(events.event_subcategory = 'search' AND events.triggered_by = 'user', 1,
			NULL))                                                        AS user_searches,
	SUM(IFF(events.event_subcategory = 'search' AND events.triggered_by = 'pageLoad', 1,
			NULL))                                                        AS page_load_searches,
	SUM(IFF(events.event_subcategory = 'search' AND
			TRY_TO_NUMBER(PARSE_URL(events.page_url, 1)['parameters']['minPrice']::VARCHAR) IS NOT NULL, 1,
			NULL))                                                        AS min_price_filter_searches,
	SUM(IFF(events.event_subcategory = 'search' AND
			TRY_TO_NUMBER(PARSE_URL(events.page_url, 1)['parameters']['maxPrice']::VARCHAR) IS NOT NULL, 1,
			NULL))                                                        AS max_price_filter_searches,
	SUM(IFF(events.event_subcategory = 'search' AND
			PARSE_URL(events.page_url, 1)['parameters']['sortBy']::VARCHAR IS NOT NULL, 1,
			NULL))                                                        AS sort_by_searches,

	-- pay button clicks
	SUM(IFF(events.event_subcategory = 'pay_button_click', 1, NULL))      AS pay_button_clicks
FROM data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest__step01__get_source_batch batch
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest events
	ON batch.touch_id = events.touch_id
	AND events.updated_at >= TIMESTAMPADD('day', -1, '2025-11-24 03:30:00'::TIMESTAMP)
LEFT JOIN data_vault_mvp_dev_robin.dwh.fact_booking fact_booking
	ON events.booking_id = fact_booking.booking_id
	AND events.event_category = 'transaction'
	AND fact_booking.booking_status_type IN ('live', 'cancelled')
LEFT JOIN data_vault_mvp_dev_robin.dwh.dim_sale dim_sale
	ON COALESCE(events.se_sale_id, fact_booking.se_sale_id) = dim_sale.se_sale_id
LEFT JOIN data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest__step02__booking_has_flights has_flights
	ON events.booking_id = has_flights.booking_id
GROUP BY events.touch_id


SELECT
	fb.booking_includes_flight,
	COUNT(*)
FROM se.data.fact_booking fb
GROUP BY 1
;


DROP TABLE data_vault_mvp_dev_robin.bi.session_metrics
;

-- existing session metrics
SELECT
	session_metrics.touch_start_tstamp::DATE AS date,
	COUNT(*)
FROM dbt.bi_product_analytics__intermediate.pda_session_metrics session_metrics
WHERE session_metrics.touch_start_tstamp >= CURRENT_DATE - 30
GROUP BY ALL
;

-- new session metrics
SELECT
	session_metrics.touch_start_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp.bi.session_metrics session_metrics
WHERE session_metrics.touch_start_tstamp >= CURRENT_DATE - 30
GROUP BY ALL
;


-- scv
SELECT
	session_metrics.touch_start_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes session_metrics
WHERE session_metrics.touch_start_tstamp >= CURRENT_DATE - 30
GROUP BY ALL
;

USE WAREHOUSE pipe_large
;
-- existing session metrics by brand
SELECT
	session_metrics.touch_start_tstamp::DATE AS date,
	session_metrics.touch_se_brand,
	COUNT(*)
FROM dbt.bi_product_analytics__intermediate.pda_session_metrics session_metrics
WHERE session_metrics.touch_start_tstamp >= CURRENT_DATE - 30
GROUP BY ALL
;

-- new session metrics by brand
SELECT
	session_metrics.touch_start_tstamp::DATE AS date,
	session_metrics.touch_se_brand,
	COUNT(*)
FROM data_vault_mvp.bi.session_metrics session_metrics
WHERE session_metrics.touch_start_tstamp >= CURRENT_DATE - 30
GROUP BY ALL
;


-- scv by brand
SELECT
	session_metrics.touch_start_tstamp::DATE AS date,
	session_metrics.touch_se_brand,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes session_metrics
WHERE session_metrics.touch_start_tstamp >= CURRENT_DATE - 30
GROUP BY ALL
;


-- SCV
SELECT
	session_metrics.touch_start_tstamp::DATE AS date,
-- 	session_metrics.touch_se_brand,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes session_metrics
WHERE session_metrics.touch_start_tstamp >= CURRENT_DATE - 30
  AND session_metrics.stitched_identity_type = 'se_user_id'
GROUP BY ALL
;


-- augmented
SELECT
	session_metrics.touch_start_tstamp::DATE AS date,
-- 	session_metrics.touch_se_brand,
	COUNT(*)
FROM data_vault_mvp.dwh.touch_attributes_augmented session_metrics
WHERE session_metrics.touch_start_tstamp >= CURRENT_DATE - 30
GROUP BY ALL
;

SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.geo_country NOT IN ('CN', 'IE')