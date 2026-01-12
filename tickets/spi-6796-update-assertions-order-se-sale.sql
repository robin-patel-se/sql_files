USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_offer_product
CLONE latest_vault.cms_mysql.base_offer_product;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.contractor
CLONE latest_vault.cms_mysql.contractor;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.city
CLONE latest_vault.cms_mysql.city;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.country
CLONE latest_vault.cms_mysql.country;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.country_division
CLONE latest_vault.cms_mysql.country_division;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.location_info
CLONE latest_vault.cms_mysql.location_info;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_promotion
CLONE data_vault_mvp.dwh.se_promotion;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.supplier
CLONE latest_vault.cms_mysql.supplier;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.web_redirect
CLONE latest_vault.cms_mysql.web_redirect;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.web_redirect_company
CLONE latest_vault.cms_mysql.web_redirect_company;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review
CLONE data_vault_mvp.dwh.user_booking_review;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.allocation
CLONE latest_vault.cms_mysql.allocation;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_sale
CLONE latest_vault.cms_mysql.base_sale;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_sale_translation
CLONE latest_vault.cms_mysql.base_sale_translation;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.company
CLONE latest_vault.cms_mysql.company;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.days_before_policy
CLONE latest_vault.cms_mysql.days_before_policy;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.hotel
CLONE latest_vault.cms_mysql.hotel;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.in_house_package
CLONE latest_vault.cms_mysql.in_house_package;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.ihp_sale_company
CLONE latest_vault.cms_mysql.ihp_sale_company;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.offer
CLONE latest_vault.cms_mysql.offer;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.product
CLONE latest_vault.cms_mysql.product;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.product_provider
CLONE latest_vault.cms_mysql.product_provider;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale
CLONE latest_vault.cms_mysql.sale;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_flight_config
CLONE latest_vault.cms_mysql.sale_flight_config;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_translation
CLONE latest_vault.cms_mysql.sale_translation;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_company
CLONE latest_vault.cms_mysql.sale_company;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_territory
CLONE latest_vault.cms_mysql.sale_territory;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sales_kingfisher
CLONE data_vault_mvp.dwh.sales_kingfisher;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_offer
CLONE data_vault_mvp.dwh.se_offer;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sfsc__account
CLONE data_vault_mvp.dwh.sfsc__account;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sfsc__opportunity
CLONE data_vault_mvp.dwh.sfsc__opportunity;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_territory
CLONE data_vault_mvp.dwh.se_territory;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
CLONE data_vault_mvp.dwh.se_sale;

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.transactional.se_sale.py' \
    --method 'run' \
    --start '2024-12-04 00:00:00' \
    --end '2024-12-04 00:00:00'