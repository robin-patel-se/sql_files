SELECT
	sk.territory,
	record['links']['sale'],
	sk.sale_url,
	CASE sk.territory
		WHEN 'BE' THEN 'https://be.secretescapes.com'
		WHEN 'CH' THEN 'https://ch.secretescapes.com'
		WHEN 'DE' THEN 'https://www.secretescapes.de'
		WHEN 'DK' THEN 'https://dk.secretescapes.com'
		WHEN 'IE' THEN 'https://ie.secretescapes.com'
		WHEN 'IT' THEN 'https://it.secretescapes.com'
		WHEN 'NL' THEN 'https://nl.secretescapes.com'
		WHEN 'NO' THEN 'https://no.secretescapes.com'
		WHEN 'TB-BE_FR' THEN 'https://fr.travelbird.be'
		WHEN 'TB-BE_NL' THEN 'https://travelbird.be'
		WHEN 'TB-NL' THEN 'https://travelbird.nl'
		WHEN 'SE' THEN 'https://www.secretescapes.se'
		WHEN 'UK' THEN 'https://www.secretescapes.com'
		ELSE 'https://www.secretescapes.com'
	END                                         AS territory_domain,
	territory_domain || record['links']['sale'] AS sale_url2
FROM latest_vault.kingfisher.sales_kingfisher sk
;

CASE WHEN sk.territory = 'BE' THEN 'https://be.secretescapes.com' WHEN sk.territory = 'CH' THEN 'https://ch.secretescapes.com' WHEN sk.territory = 'DE' THEN 'https://www.secretescapes.de' WHEN sk.territory = 'DK' THEN 'https://dk.secretescapes.com' WHEN sk.territory = 'IE' THEN 'https://ie.secretescapes.com' WHEN sk.territory = 'IT' THEN 'https://it.secretescapes.com' WHEN sk.territory = 'NL' THEN 'https://nl.secretescapes.com' WHEN sk.territory = 'NO' THEN 'https://no.secretescapes.com' WHEN sk.territory = 'TB-BE_FR' THEN 'https://fr.travelbird.be' WHEN sk.territory = 'TB-BE_NL' THEN 'https://travelbird.be' WHEN sk.territory = 'TB-NL' THEN 'https://travelbird.nl' WHEN sk.territory = 'SE' THEN 'https://www.secretescapes.se' WHEN sk.territory = 'UK' THEN 'https://www.secretescapes.com' ELSE 'https://www.secretescapes.com' END || record['links']['sale']



dataset_task --include 'kingfisher.sales_kingfisher' --operation ExtractOperation --method 'run' --start '2024-09-09 00:30:00' --end '2024-09-09 00:30:00'
dataset_task --include 'kingfisher.sales_kingfisher' --operation IngestOperation --method 'run' --start '2024-09-09 00:30:00' --end '2024-09-09 00:30:00'
dataset_task --include 'kingfisher.sales_kingfisher' --operation HygieneOperation --method 'run' --start '2024-09-09 00:30:00' --end '2024-09-09 00:30:00'
dataset_task --include 'kingfisher.sales_kingfisher' --operation LatestRecordsOperation --method 'run' --start '2024-09-09 00:30:00' --end '2024-09-09 00:30:00'

SELECT *
FROM raw_vault.kingfisher.sales_kingfisher
WHERE territory = 'ES'
;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_dev_robin.kingfisher.sales_kingfisher CLONE raw_vault.kingfisher.sales_kingfisher
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.kingfisher.sales_kingfisher CLONE hygiene_vault.kingfisher.sales_kingfisher
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.kingfisher.sales_kingfisher CLONE latest_vault.kingfisher.sales_kingfisher
;

DROP TABLE raw_vault_dev_robin.kingfisher.sales_kingfisher
;

DROP TABLE hygiene_vault_dev_robin.kingfisher.sales_kingfisher
;

DROP TABLE latest_vault_dev_robin.kingfisher.sales_kingfisher
;


SELECT
	id,
	territory,
	sale_url,
	title
FROM latest_vault_dev_robin.kingfisher.sales_kingfisher
WHERE row_loaded_at = (
	SELECT MAX(row_loaded_at) FROM latest_vault_dev_robin.kingfisher.sales_kingfisher
)
;



SELECT
	COUNT(*)
FROM latest_vault.kingfisher.sales_kingfisher
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.kingfisher.sales_kingfisher CLONE latest_vault.kingfisher.sales_kingfisher

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.se_sale__step01__model_data')
;

SELECT GET_DDL('table', 'latest_vault.kingfisher.sales_kingfisher')
;


CREATE OR REPLACE TRANSIENT TABLE se_sale__step01__model_data
(
	se_sale_id                         VARCHAR,
	id                                 VARCHAR,
	second_opinion                     VARCHAR,
	summary                            VARCHAR,
	we_like                            VARCHAR,
	main_paragraph                     VARCHAR,
	hotel_details                      VARCHAR,
	room_description                   VARCHAR,
	travel_details                     VARCHAR,
	destination_name                   VARCHAR,
	deal_includes                      VARCHAR,
	title                              VARCHAR,
	reason_to_love                     VARCHAR,
	current_sale_visitors              NUMBER,
	times_booked                       NUMBER,
	dates_start                        TIMESTAMP_NTZ(9),
	dates_end                          TIMESTAMP_NTZ(9),
	end_date_display                   VARCHAR,
	type                               VARCHAR,
	offer_ids                          ARRAY,
	photos                             ARRAY,
	tags                               ARRAY,
	badges                             ARRAY,
	current_sale                       BOOLEAN,
	continent_id                       NUMBER,
	continent_name                     VARCHAR,
	division_id                        NUMBER,
	division_name                      VARCHAR,
	country_id                         NUMBER,
	country_name                       VARCHAR,
	city_id                            NUMBER,
	city_name                          VARCHAR,
	city_district_id                   VARCHAR,
	city_district_name                 VARCHAR,
	latitude                           NUMBER(9, 6),
	longitude                          NUMBER(9, 6),
	is_hotel_chain                     BOOLEAN,
	is_deposit_sale                    BOOLEAN,
	is_time_limited                    BOOLEAN,
	is_hidden_for_app                  BOOLEAN,
	is_catalogue                       BOOLEAN,
	is_connected                       BOOLEAN,
	display_order                      NUMBER,
	is_zero_deposit                    BOOLEAN,
	is_refundable                      BOOLEAN,
	is_dynamic_package                 BOOLEAN,
	is_exclusive                       BOOLEAN,
	is_current                         BOOLEAN,
	is_smart_stay                      BOOLEAN,
	is_editors_pick                    BOOLEAN,
	is_hidden_for_whitelabels          BOOLEAN,
	is_mysterious                      BOOLEAN,
	is_package                         BOOLEAN,
	sale_url                           VARCHAR,
	links_sale                         VARCHAR,
	links_price_comparison             VARCHAR,
	links_trip_advisor                 VARCHAR,
	number_of_hotel_nights             NUMBER,
	discount_tooltip                   VARCHAR,
	discount                           NUMBER(13, 2),
	discount_display                   VARCHAR,
	pricing_model_for_display          VARCHAR,
	rack_rate_unit                     NUMBER(13, 2),
	rack_rate_for_display              VARCHAR,
	rack_rate_unit_per_person          NUMBER(13, 2),
	deposit_from_price_unit            NUMBER(13, 2),
	deposit_from_price_for_display     VARCHAR,
	deposit_from_price_unit_per_person NUMBER(13, 2),
	currency_code                      VARCHAR,
	max_number_of_adults               NUMBER,
	total_price_for_display            VARCHAR,
	show_rack_rate                     BOOLEAN,
	show_prices                        BOOLEAN,
	show_discount                      BOOLEAN,
	lead_rate_unit_label               VARCHAR,
	lead_rate_label                    VARCHAR,
	lead_rate_tooltip                  VARCHAR,
	lead_rate_unit                     NUMBER(13, 2),
	lead_rate_for_display              VARCHAR,
	lead_rate_unit_per_person          NUMBER(13, 2),
	travel_type                        VARCHAR,
	has_flights_available              BOOLEAN,
	has_flights_included               BOOLEAN,
	cancellation_summary               VARCHAR,
	cancellation_description_warning   VARCHAR,
	cancellation_description           VARCHAR,
	hash                               VARCHAR,
	promotion                          VARCHAR,
	month_availability                 ARRAY,
	territory                          VARCHAR,
	record                             VARIANT,
	sale_active                        BOOLEAN
)
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.sales_kingfisher
WHERE sale_active
;

/*
module=/biapp/task_catalogue/dv/dwh/transactional/se_sale.py make clones
*/

SELECT
	destination_name
FROM data_vault_mvp_dev_robin.dwh.sales_kingfisher
WHERE sale_active
;

------------------------------------------------------------------------------------------------------------------------
-- se sale

USE ROLE personal_role__robinpatel
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


self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/se_sale.py'  --method 'run' --start '2024-09-10 00:00:00' --end '2024-09-10 00:00:00'


------------------------------------------------------------------------------------------------------------------------
-- product catalogue

USE ROLE personal_role__robinpatel
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.affiliate
	CLONE latest_vault.cms_mysql.affiliate
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_offer_translation
	CLONE latest_vault.cms_mysql.base_offer_translation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.hotel_sale_offer
	CLONE latest_vault.cms_mysql.hotel_sale_offer
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sales_kingfisher
--  CLONE data_vault_mvp.dwh.sales_kingfisher;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory
	CLONE latest_vault.cms_mysql.territory
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review
	CLONE data_vault_mvp.dwh.user_booking_review
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/iterable/catalogue_product.py'  --method 'run' --start '2024-09-10 00:00:00' --end '2024-09-10 00:00:00'