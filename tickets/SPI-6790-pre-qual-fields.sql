USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_offer_product
	CLONE latest_vault.cms_mysql.base_offer_product
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.contractor
	CLONE latest_vault.cms_mysql.contractor
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.city
	CLONE latest_vault.cms_mysql.city
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.country
	CLONE latest_vault.cms_mysql.country
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.country_division
	CLONE latest_vault.cms_mysql.country_division
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.location_info
	CLONE latest_vault.cms_mysql.location_info
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_promotion
	CLONE data_vault_mvp.dwh.se_promotion
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.supplier
	CLONE latest_vault.cms_mysql.supplier
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.web_redirect
	CLONE latest_vault.cms_mysql.web_redirect
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.web_redirect_company
	CLONE latest_vault.cms_mysql.web_redirect_company
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review
	CLONE data_vault_mvp.dwh.user_booking_review
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.allocation
	CLONE latest_vault.cms_mysql.allocation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_sale
	CLONE latest_vault.cms_mysql.base_sale
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_sale_translation
	CLONE latest_vault.cms_mysql.base_sale_translation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.company
	CLONE latest_vault.cms_mysql.company
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.days_before_policy
	CLONE latest_vault.cms_mysql.days_before_policy
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.hotel
	CLONE latest_vault.cms_mysql.hotel
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.in_house_package
	CLONE latest_vault.cms_mysql.in_house_package
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.ihp_sale_company
	CLONE latest_vault.cms_mysql.ihp_sale_company
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.offer
	CLONE latest_vault.cms_mysql.offer
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.product
	CLONE latest_vault.cms_mysql.product
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.product_provider
	CLONE latest_vault.cms_mysql.product_provider
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale
	CLONE latest_vault.cms_mysql.sale
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_flight_config
	CLONE latest_vault.cms_mysql.sale_flight_config
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_translation
	CLONE latest_vault.cms_mysql.sale_translation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_company
	CLONE latest_vault.cms_mysql.sale_company
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_territory
	CLONE latest_vault.cms_mysql.sale_territory
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sales_kingfisher
	CLONE data_vault_mvp.dwh.sales_kingfisher
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_offer
	CLONE data_vault_mvp.dwh.se_offer
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sfsc__account
	CLONE data_vault_mvp.dwh.sfsc__account
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sfsc__opportunity
	CLONE data_vault_mvp.dwh.sfsc__opportunity
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_territory
	CLONE data_vault_mvp.dwh.se_territory
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
	CLONE data_vault_mvp.dwh.se_sale
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.transactional.se_sale.py' \
    --method 'run' \
    --start '2025-09-22 00:00:00' \
    --end '2025-09-22 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale__step03__model_base_sale_translation
;


SHOW TABLES IN SCHEMA data_vault_mvp_dev_robin.dwh
;

CREATE SCHEMA data_vault_mvp_dev_robin.dwh_se_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step01__model_ihp_company CLONE data_vault_mvp_dev_robin.dwh.se_sale__step01__model_ihp_company
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step02__model_wrd_company CLONE data_vault_mvp_dev_robin.dwh.se_sale__step02__model_wrd_company
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step03__model_base_sale_translation CLONE data_vault_mvp_dev_robin.dwh.se_sale__step03__model_base_sale_translation
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step04__model_offer_cancellation_at_sale_level CLONE data_vault_mvp_dev_robin.dwh.se_sale__step04__model_offer_cancellation_at_sale_level
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step05__new_model_source_batch CLONE data_vault_mvp_dev_robin.dwh.se_sale__step05__new_model_source_batch
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step06__model_sale_translation CLONE data_vault_mvp_dev_robin.dwh.se_sale__step06__model_sale_translation
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step07__model_sale_flight_config CLONE data_vault_mvp_dev_robin.dwh.se_sale__step07__model_sale_flight_config
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step08__model_offer_allocation CLONE data_vault_mvp_dev_robin.dwh.se_sale__step08__model_offer_allocation
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step09__model_sale_territory CLONE data_vault_mvp_dev_robin.dwh.se_sale__step09__model_sale_territory
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step10__model_sale_company CLONE data_vault_mvp_dev_robin.dwh.se_sale__step10__model_sale_company
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step11__old_model_source_batch CLONE data_vault_mvp_dev_robin.dwh.se_sale__step11__old_model_source_batch
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step12__model_se_api CLONE data_vault_mvp_dev_robin.dwh.se_sale__step12__model_se_api
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step14__model_union CLONE data_vault_mvp_dev_robin.dwh.se_sale__step14__model_union
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step15__enhance_union_with_contractor CLONE data_vault_mvp_dev_robin.dwh.se_sale__step15__enhance_union_with_contractor
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step16__model_user_booking_review CLONE data_vault_mvp_dev_robin.dwh.se_sale__step16__model_user_booking_review
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step17__model_travel_distance_in_metres CLONE data_vault_mvp_dev_robin.dwh.se_sale__step17__model_travel_distance_in_metres
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step18__model_data CLONE data_vault_mvp_dev_robin.dwh.se_sale__step18__model_data
;

SELECT
	se_sale_id,
	HASH(*)
FROM data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step05__new_model_source_batch
;

SELECT
	se_sale_id,
	HASH(*)
FROM data_vault_mvp_dev_robin.dwh.se_sale__step05__new_model_source_batch
;

SELECT
	COALESCE(prod.se_sale_id, dev.se_sale_id) AS se_sale_id,
	HASH(prod.*)                              AS prod_hash,
	HASH(dev.*)                               AS dev_hash,
FROM data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step05__new_model_source_batch AS prod
FULL OUTER JOIN data_vault_mvp_dev_robin.dwh.se_sale__step05__new_model_source_batch AS dev
	ON prod.se_sale_id = dev.se_sale_id
WHERE prod_hash != dev_hash
;



SELECT
	COALESCE(prod.se_sale_id, dev.se_sale_id) AS se_sale_id,
	HASH(prod.*)                              AS prod_hash,
	HASH(dev.*)                               AS dev_hash,
FROM data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step11__old_model_source_batch AS prod
FULL OUTER JOIN data_vault_mvp_dev_robin.dwh.se_sale__step11__old_model_source_batch AS dev
	ON prod.se_sale_id = dev.se_sale_id
WHERE prod_hash != dev_hash
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step11__old_model_source_batch AS prod
WHERE prod.se_sale_id = '27466'
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale__step11__old_model_source_batch AS dev
WHERE dev.se_sale_id = '27466'
;


SELECT
	sale_type,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step11__old_model_source_batch AS prod
GROUP BY 1
;

SELECT
	sale_type,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.se_sale__step11__old_model_source_batch AS dev
GROUP BY 1
;


WITH
	prod AS (
		SELECT
			se_sale_id,
			HASH(* exclude(array_sale_translation, company_array)) AS prod_hash
		FROM data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step15__enhance_union_with_contractor
	),
	dev AS (
		SELECT
			se_sale_id,
			HASH(* exclude(array_sale_translation, company_array)) AS dev_hash
		FROM data_vault_mvp_dev_robin.dwh.se_sale__step15__enhance_union_with_contractor
	)
SELECT
	COALESCE(prod.se_sale_id, dev.se_sale_id) AS se_sale_id,
	prod_hash,
	dev_hash,
FROM prod
FULL OUTER JOIN dev
	ON prod.se_sale_id = dev.se_sale_id
WHERE prod_hash != dev_hash
;


WITH
	prod AS (
		SELECT
			se_sale_id,
			HASH(* exclude(array_sale_translation, company_array)) AS prod_hash
		FROM data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step15__enhance_union_with_contractor
	),
	dev AS (
		SELECT
			se_sale_id,
			HASH(* exclude(array_sale_translation, company_array)) AS dev_hash
		FROM data_vault_mvp_dev_robin.dwh.se_sale__step15__enhance_union_with_contractor
	)
SELECT
	COALESCE(prod.se_sale_id, dev.se_sale_id) AS se_sale_id,
	prod_hash,
	dev_hash,
FROM prod
FULL OUTER JOIN dev
	ON prod.se_sale_id = dev.se_sale_id
WHERE prod_hash != dev_hash
;


WITH
	prod AS (
		SELECT
			se_sale_id,
			HASH(* exclude(array_sale_translation, company_array)) AS prod_hash
		FROM data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step18__model_data sss18md
	),
	dev AS (
		SELECT
			se_sale_id,
			HASH(* exclude(array_sale_translation, company_array)) AS dev_hash
		FROM data_vault_mvp_dev_robin.dwh.se_sale__model_data
	)
SELECT
	COALESCE(prod.se_sale_id, dev.se_sale_id) AS se_sale_id,
	prod_hash,
	dev_hash,
FROM prod
FULL OUTER JOIN dev
	ON prod.se_sale_id = dev.se_sale_id
WHERE prod_hash != dev_hash
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh_se_sale.se_sale__step18__model_data
WHERE se_sale_id = 'A47156'
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale__model_data
WHERE se_sale_id = 'A47156'
;



WITH
	prod AS (
		SELECT
			se_sale_id,
			HASH(* exclude(array_sale_translation, company_array, schedule_tstamp, run_tstamp, operation_id, created_at,
						   updated_at)) AS prod_hash
		FROM data_vault_mvp.dwh.se_sale
	),
	dev AS (
		SELECT
			se_sale_id,
			HASH(* exclude(array_sale_translation, company_array, schedule_tstamp, run_tstamp, operation_id, created_at,
						   updated_at)) AS dev_hash
		FROM data_vault_mvp_dev_robin.dwh.se_sale
	)
SELECT
	COALESCE(prod.se_sale_id, dev.se_sale_id) AS se_sale_id,
	prod_hash,
	dev_hash,
FROM prod
FULL OUTER JOIN dev
	ON prod.se_sale_id = dev.se_sale_id
WHERE prod_hash != dev_hash
;


SELECT * EXCLUDE (array_sale_translation, company_array, schedule_tstamp, run_tstamp, operation_id, created_at,
	updated_at)
FROM data_vault_mvp.dwh.se_sale
WHERE se_sale_id = 'A35950'
;

SELECT * EXCLUDE (array_sale_translation, company_array, schedule_tstamp, run_tstamp, operation_id, created_at,
	updated_at)
FROM data_vault_mvp_dev_robin.dwh.se_sale
WHERE se_sale_id = 'A35950'
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale__old_data_model__model_data
;



SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.se_sale__old_data_model__model_data')
;

CREATE OR REPLACE TRANSIENT TABLE se_sale__old_data_model__model_data
(
	se_sale_id                   VARCHAR,
	sale_id                      NUMBER,
	salesforce_opportunity_id    VARCHAR,
	exclusive_sale               BOOLEAN,
	smart_stay_sale              NUMBER,
	sale_name                    VARCHAR,
	destination_name             VARCHAR,
	sale_name_object             OBJECT,
	sale_active                  BOOLEAN,
	type                         VARCHAR,
	hotel_chain_link             VARCHAR,
	closest_airport_code         VARCHAR,
	is_team20package             BOOLEAN,
	sale_able_to_sell_flights    BOOLEAN,
	sale_product                 VARCHAR,
	sale_type                    VARCHAR,
	product_type                 VARCHAR,
	product_configuration        VARCHAR,
	product_line                 VARCHAR,
	data_model                   VARCHAR,
	hotel_location_info_id       NUMBER,
	active                       NUMBER,
	default_hotel_offer_id       NUMBER,
	commission                   NUMBER,
	commission_type              VARCHAR,
	original_contractor_id       NUMBER,
	original_joint_contractor_id NUMBER,
	date_created                 TIMESTAMP,
	destination_type             VARCHAR,
	start_date                   TIMESTAMP,
	end_date                     TIMESTAMP,
	base_currency                VARCHAR,
	location_info_id             NUMBER,
	company_id                   VARCHAR,
	company_name                 VARCHAR,
	company_array                ARRAY,
	hotel_code                   VARCHAR,
	posa_territory               VARCHAR,
	posa_territory_array         ARRAY,
	posa_country                 VARCHAR,
	posa_country_array           ARRAY,
	posa_currency                VARCHAR,
	posa_currency_array          ARRAY,
	posa_latitude                VARCHAR,
	posa_latitude_array          ARRAY,
	posa_longitude               VARCHAR,
	posa_longitude_array         ARRAY,
	posu_division                VARCHAR,
	posu_country                 VARCHAR,
	posu_city                    VARCHAR,
	supplier_id                  NUMBER,
	supplier_name                VARCHAR,
	latitude                     NUMBER,
	longitude                    NUMBER,
	board_type                   VARCHAR,
	location                     VARCHAR,
	array_sale_translation       ARRAY,
	posa_territory_id            VARCHAR,
	type_of_third_party          VARCHAR,
	is_connected_to_se           BOOLEAN,
	pre_qualification_status     VARCHAR
)
;

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.se_sale__model_data')
;


CREATE OR REPLACE TRANSIENT TABLE se_sale__model_data
(
	se_sale_id                                 VARCHAR,
	base_sale_id                               NUMBER,
	salesforce_opportunity_id                  VARCHAR,
	exclusive_sale                             BOOLEAN,
	smart_stay_sale                            NUMBER,
	sale_name                                  VARCHAR,
	destination_name                           VARCHAR,
	sale_name_object                           OBJECT,
	sale_active                                BOOLEAN,
	class                                      VARCHAR,
	has_flights_available                      NUMBER,
	default_preferred_airport_code             VARCHAR,
	sale_product                               VARCHAR,
	sale_type                                  VARCHAR,
	product_type                               VARCHAR,
	product_configuration                      VARCHAR,
	product_line                               VARCHAR,
	data_model                                 VARCHAR,
	hotel_location_info_id                     NUMBER,
	active                                     NUMBER,
	default_hotel_offer_id                     NUMBER,
	commission                                 NUMBER,
	commission_type                            VARCHAR,
	original_contractor_id                     NUMBER,
	original_joint_contractor_id               NUMBER,
	hotel_contractor_name                      VARCHAR,
	date_created                               TIMESTAMP,
	destination_type                           VARCHAR,
	start_date                                 TIMESTAMP,
	end_date                                   TIMESTAMP,
	hotel_id                                   NUMBER,
	base_currency                              VARCHAR,
	city_district_id                           NUMBER,
	company_id                                 VARCHAR,
	company_name                               VARCHAR,
	company_array                              ARRAY,
	hotel_code                                 VARCHAR,
	latitude                                   FLOAT,
	longitude                                  FLOAT,
	location_info_id                           NUMBER,
	cms_channel_manager                        VARCHAR,
	redirect_url                               VARCHAR,
	posa_territory                             VARCHAR,
	posa_country                               VARCHAR,
	posa_currency                              VARCHAR,
	posa_latitude                              VARCHAR,
	posa_longitude                             VARCHAR,
	posu_division                              VARCHAR,
	posu_country                               VARCHAR,
	posu_city                                  VARCHAR,
	supplier_id                                NUMBER,
	supplier_name                              VARCHAR,
	travel_type                                VARCHAR,
	is_flashsale                               NUMBER,
	is_cancellable                             BOOLEAN,
	cancellation_policy_number_of_days         NUMBER,
	list_of_se_offer_ids                       VARCHAR,
	list_of_se_offer_ids_is_cancellable        VARCHAR,
	list_of_cancellation_policy_number_of_days VARCHAR,
	array_sale_translation                     ARRAY,
	posa_territory_id                          VARCHAR,
)
;


WITH
	prod AS (
		SELECT
			se_sale_id,
			HASH(* exclude(schedule_tstamp, run_tstamp, created_at, updated_at, operation_id, array_sale_translation,
						   company_array)) AS prod_hash
		FROM data_vault_mvp.dwh.se_sale
	),
	dev AS (
		SELECT
			se_sale_id,
			HASH(* exclude(schedule_tstamp, run_tstamp, created_at, updated_at, operation_id, array_sale_translation,
						   company_array)) AS dev_hash
		FROM data_vault_mvp_dev_robin.dwh.se_sale
	)
SELECT
	COALESCE(prod.se_sale_id, dev.se_sale_id) AS se_sale_id,
	prod_hash,
	dev_hash,
FROM prod
FULL OUTER JOIN dev
	ON prod.se_sale_id = dev.se_sale_id
WHERE prod_hash != dev_hash
;

-- prod
SELECT * EXCLUDE (schedule_tstamp, run_tstamp, created_at, updated_at, operation_id)
FROM data_vault_mvp.dwh.se_sale
WHERE se_sale.se_sale_id = 'A72792'

-- dev
SELECT * EXCLUDE (schedule_tstamp, run_tstamp, created_at, updated_at, operation_id)
FROM data_vault_mvp_dev_robin.dwh.se_sale
WHERE se_sale.se_sale_id = 'A72792'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale__old_data_model ssodm
WHERE ssodm.se_sale_id = '4286'
;

SELECT getddl(TABLE, data_vault_mvp.dwh.se_sale_attributes)
;

SELECT GET_DDL('table', 'data_vault_mvp.dwh.se_sale_attributes')
;


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
-- CLONE data_vault_mvp.dwh.se_sale;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer
	CLONE data_vault_mvp.dwh.tb_offer
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.fpa_gsheets
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.posu_categorisation
	CLONE latest_vault.fpa_gsheets.posu_categorisation
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_attributes
	CLONE data_vault_mvp.dwh.se_sale_attributes
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.transactional.se_sale_attributes.py' \
    --method 'run' \
    --start '2025-09-24 00:00:00' \
    --end '2025-09-24 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss


SELECT
	city_bookings.shiro_user_id,
	OBJECT_AGG(city_bookings.posu_city, city_bookings.city_bookings) AS city_bookings
FROM data_vault_mvp_dev_robin.dwh.user_booking_metrics__step05__city_bookings city_bookings
GROUP BY city_bookings.shiro_user_id
;


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
	CLONE data_vault_mvp.dwh.se_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.global_sale_attributes
	CLONE data_vault_mvp.dwh.global_sale_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog
	CLONE data_vault_mvp.dwh.tb_order_item_changelog
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_index
	CLONE data_vault_mvp.dwh.user_booking_index
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review
	CLONE data_vault_mvp.dwh.user_booking_review
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.harmonised_sale_calendar_view_snapshot
	CLONE data_vault_mvp.dwh.harmonised_sale_calendar_view_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_metrics
	CLONE data_vault_mvp.dwh.user_booking_metrics
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.user_attributes.user_booking_metrics.py' \
    --method 'run' \
    --start '2025-09-25 00:00:00' \
    --end '2025-09-25 00:00:00'

SELECT *
FROM data_vault_mvp.dwh.dim_sale ds
QUALIFY COUNT(*) OVER (PARTITION BY ds.se_sale_id ) > 1
;



WITH
	step05__city_bookings AS (
		SELECT
			fact_booking.shiro_user_id,
			dim_sale.posu_city,
			dim_sale.posu_country,
			COUNT(fact_booking.booking_id) AS city_bookings
		FROM data_vault_mvp_dev_robin.dwh.fact_booking fact_booking
		INNER JOIN data_vault_mvp_dev_robin.dwh.dim_sale dim_sale
			ON fact_booking.se_sale_id = dim_sale.se_sale_id
		WHERE fact_booking.booking_status_type IN ('live')
		  AND fact_booking.booking_completed_date >= DATEADD(YEAR, -3, CURRENT_DATE)
		  AND fact_booking.shiro_user_id IS NOT NULL
		GROUP BY fact_booking.shiro_user_id,
				 dim_sale.posu_city,
				 dim_sale.posu_country
	),
	city_bookings AS (
		SELECT
			step05__city_bookings.shiro_user_id,
			step05__city_bookings.posu_city,
			SUM(step05__city_bookings.city_bookings) AS city_bookings
		FROM step05__city_bookings
		GROUP BY step05__city_bookings.shiro_user_id,
				 step05__city_bookings.posu_city
	)

SELECT
	city_bookings.shiro_user_id,
	OBJECT_AGG(city_bookings.posu_city, city_bookings.city_bookings) AS city_bookings
FROM city_bookings city_bookings
GROUP BY city_bookings.shiro_user_id