USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iata
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iata.iata_airport_code_mapping
	CLONE latest_vault.iata.iata_airport_code_mapping
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.icao
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.icao.icao_mapping
	CLONE latest_vault.icao.icao_mapping
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iso
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iso.iso_country_subdivisions
	CLONE latest_vault.iso.iso_country_subdivisions
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.flightservice_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.flightservice_mysql.orders_flightbooking
	CLONE latest_vault.flightservice_mysql.orders_flightbooking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.flightservice_mysql.orders_orderchange
	CLONE latest_vault.flightservice_mysql.orders_orderchange
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.flightservice_mysql.orders_segment
	CLONE latest_vault.flightservice_mysql.orders_segment
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sales_kingfisher
	CLONE data_vault_mvp.dwh.sales_kingfisher
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking
	CLONE data_vault_mvp.dwh.se_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking
	CLONE data_vault_mvp.dwh.tb_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
	CLONE data_vault_mvp.dwh.se_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.epsilon__conversions
	CLONE data_vault_mvp.dwh.epsilon__conversions
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.epsilon.conversions.py' \
    --method 'run' \
    --start '2024-10-03 00:00:00' \
    --end '2024-10-03 00:00:00'


SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.epsilon__conversions
;

SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.epsilon__conversions
;

------------------------------------------------------------------------------------------------------------------------
-- product

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sales_kingfisher
	CLONE data_vault_mvp.dwh.sales_kingfisher
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
	CLONE data_vault_mvp.dwh.se_sale
;

self_describing_task
\
	--include 'biapp/task_catalogue/dv/dwh/epsilon/product.py' \
	--method 'run' \
	--start '2024-10-02 00:00:00' \
	--end '2024-10-02 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- google hotel api ari


USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.marketing_gsheets;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.marketing_gsheets.hotels_api_availability_dates
CLONE latest_vault.marketing_gsheets.hotels_api_availability_dates;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_offer
CLONE latest_vault.cms_mysql.base_offer;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_offer_translation
CLONE latest_vault.cms_mysql.base_offer_translation;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.harmonised_offer_calendar_view
AS SELECT * FROM data_vault_mvp.dwh.harmonised_offer_calendar_view;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.marketing_gsheets.hotels_api_hotel_code_restriction
CLONE latest_vault.marketing_gsheets.hotels_api_hotel_code_restriction;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.hotel_sale_offer
CLONE latest_vault.cms_mysql.hotel_sale_offer;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.marketing_gsheets.hotels_api_partner_id
CLONE latest_vault.marketing_gsheets.hotels_api_partner_id;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.marketing_gsheets.hotels_api_partner_key
CLONE latest_vault.marketing_gsheets.hotels_api_partner_key;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sales_kingfisher
CLONE data_vault_mvp.dwh.sales_kingfisher;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_company_attributes
CLONE data_vault_mvp.dwh.se_company_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_offer
CLONE data_vault_mvp.dwh.se_offer;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
CLONE data_vault_mvp.dwh.se_sale;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.marketing_gsheets.hotels_api_sale_restriction
CLONE latest_vault.marketing_gsheets.hotels_api_sale_restriction;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory
CLONE latest_vault.cms_mysql.territory;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.marketing_gsheets.hotels_api_territories
CLONE latest_vault.marketing_gsheets.hotels_api_territories;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.google__hotel_api_ari
CLONE data_vault_mvp.dwh.google__hotel_api_ari;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.google_ads.google_hotel_api_ari.py' \
    --method 'run' \
    --start '2024-10-03 00:00:00' \
    --end '2024-10-03 00:00:00';


SELECT * FROM data_vault_mvp_dev_robin.dwh.google__hotel_api_ari;
SELECT * FROM data_vault_mvp.dwh.google__hotel_api_ari;