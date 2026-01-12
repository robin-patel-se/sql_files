self_describing_task --include 'dv/dwh/user_attributes/user_attributes.py'  --method 'run' --start '2021-07-29 00:00:00' --end '2021-07-29 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.affiliate CLONE hygiene_snapshot_vault_mvp.cms_mysql.affiliate;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.chiasma_sql_server.affiliate_classification CLONE raw_vault_mvp.chiasma_sql_server.affiliate_classification;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.chiasma_sql_server.user_acquisition_platform CLONE raw_vault_mvp.chiasma_sql_server.user_acquisition_platform;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.profile CLONE hygiene_snapshot_vault_mvp.cms_mysql.profile;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.shiro_user CLONE hygiene_snapshot_vault_mvp.cms_mysql.shiro_user;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.territory;


SELECT *
FROM raw_vault_mvp.chiasma_sql_server.affiliate_classification ac;


SELECT GET_DDL('table', 'COLLAB.DACH.SFMC_NPS');

SELECT *
FROM raw_vault_mvp_dev_robin.chiasma_sql_server.affiliate_classification ac
SELECT *
FROM raw_vault_mvp_dev_robin.chiasma_sql_server.user_acquisition_platform uap



SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.user_attributes__model_data');

CREATE OR REPLACE TRANSIENT TABLE user_attributes__model_data
(
    shiro_user_id                            NUMBER(38, 0),
    first_name                               VARCHAR(16777216),
    surname                                  VARCHAR(16777216),
    email                                    VARCHAR(16777216),
    address1                                 VARCHAR(16777216),
    address2                                 VARCHAR(16777216),
    city                                     VARCHAR(16777216),
    country                                  VARCHAR(16777216),
    postcode                                 VARCHAR(16777216),
    mobile_phone                             VARCHAR(16777216),
    home_phone                               VARCHAR(16777216),
    original_affiliate_id                    NUMBER(38, 0),
    original_affiliate_name                  VARCHAR(16777216),
    original_affiliate_territory_id          NUMBER(38, 0),
    original_affiliate_territory             VARCHAR(16777216),
    member_original_affiliate_classification VARCHAR(16777216),
    current_affiliate_id                     NUMBER(38, 0),
    current_affiliate_name                   VARCHAR(16777216),
    current_affiliate_territory_id           NUMBER(38, 0),
    current_affiliate_territory              VARCHAR(16777216),
    cohort_id                                NUMBER(38, 0),
    cohort_year_month                        VARCHAR(16777216),
    signup_tstamp                            TIMESTAMP_NTZ(9),
    acquisition_platform                     VARCHAR(16777216),
    email_opt_in                             NUMBER(38, 0),
    email_opt_in_status                      VARCHAR(16777216),
    push_opt_in                              VARCHAR(16777216)
);

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_attributes__model_data;


SELECT '2021-07-28 03:00:00',
       '2021-07-30 08:51:13',
       'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/user_attributes/user_attributes.py__20210728T030000__daily_at_03h00',
       CURRENT_TIMESTAMP()::TIMESTAMP,
       CURRENT_TIMESTAMP()::TIMESTAMP,

       batch.shiro_user_id,
       batch.first_name,
       batch.surname,
       batch.email,
       batch.address1,
       batch.address2,
       batch.city,
       batch.country,
       batch.postcode,
       batch.mobile_phone,
       batch.home_phone,
       batch.original_affiliate_id,
       batch.original_affiliate_name,
       batch.original_affiliate_territory_id,
       batch.original_affiliate_territory,
       batch.member_original_affiliate_classification,
       batch.current_affiliate_id,
       batch.current_affiliate_name,
       batch.current_affiliate_territory_id,
       batch.current_affiliate_territory,
       batch.cohort_id,
       batch.cohort_year_month,
       batch.signup_tstamp,
       batch.acquisition_platform,
       batch.email_opt_in,
       batch.email_opt_in_status,
       batch.push_opt_in

FROM data_vault_mvp_dev_robin.dwh.user_attributes__model_data
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_attributes__model_data;


CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.user_attributes CLONE data_vault_mvp.dwh.user_attributes;

SELECT ua.shiro_user_id,
       ua.first_name,
       ua.surname,
       ua.email,
       ua.address1,
       ua.address2,
       ua.city,
       ua.country,
       ua.postcode,
       ua.mobile_phone,
       ua.home_phone,
       ua.original_affiliate_id,
       ua.original_affiliate_name,
       ua.original_affiliate_territory_id,
       ua.original_affiliate_territory,
       ua.member_original_affiliate_classification,
       ua.current_affiliate_id,
       ua.current_affiliate_name,
       ua.current_affiliate_territory_id,
       ua.current_affiliate_territory,
       ua.cohort_id,
       ua.cohort_year_month,
       ua.signup_tstamp,
       ua.acquisition_platform,
       ua.email_opt_in,
       ua.email_opt_in_status,
       ua.push_opt_in
FROM scratch.robinpatel.user_attributes ua
    EXCEPT
SELECT u.shiro_user_id,
       u.first_name,
       u.surname,
       u.email,
       u.address1,
       u.address2,
       u.city,
       u.country,
       u.postcode,
       u.mobile_phone,
       u.home_phone,
       u.original_affiliate_id,
       u.original_affiliate_name,
       u.original_affiliate_territory_id,
       u.original_affiliate_territory,
       u.member_original_affiliate_classification,
       u.current_affiliate_id,
       u.current_affiliate_name,
       u.current_affiliate_territory_id,
       u.current_affiliate_territory,
       u.cohort_id,
       u.cohort_year_month,
       u.signup_tstamp,
       u.acquisition_platform,
       u.email_opt_in,
       u.email_opt_in_status,
       u.push_opt_in
FROM data_vault_mvp.dwh.user_attributes u;

------------------------------------------------------------------------------------------------------------------------
--create a union of both se and tb promotor scores

--tb reviews
SELECT *
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.reviews_npsscore rn;
SELECT order_id,
       COUNT(*)
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.reviews_npsscore rn
GROUP BY 1
HAVING COUNT(*) > 1;
--se reviews
SELECT *
FROM hygiene_snapshot_vault_mvp.sfmc.net_promoter_score nps;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.sfmc.net_promoter_score CLONE raw_vault_mvp.sfmc.net_promoter_score;
self_describing_task --include 'staging/hygiene_snapshots/sfmc/net_promoter_score.py'  --method 'run' --start '2021-04-19 00:00:00' --end '2021-04-19 00:00:00'

SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.sfmc.net_promoter_score nps; --2021-04-19 08:35:16.569370000

SELECT *
FROM hygiene_vault_mvp_dev_robin.sfmc.net_promoter_score;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.travelbird_mysql.reviews_npsscore CLONE raw_vault_mvp.travelbird_mysql.reviews_npsscore;

SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.travelbird_mysql.reviews_npsscore; --2021-07-30 00:38:26.427999000

self_describing_task --include 'staging/hygiene_snapshots/travelbird_mysql/reviews_npsscore.py'  --method 'run' --start '2021-07-30 00:00:00' --end '2021-07-30 00:00:00'



CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp.travelbird_mysql.reviews_npsscore_20210730 CLONE hygiene_vault_mvp.travelbird_mysql.reviews_npsscore;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp.travelbird_mysql.reviews_npsscore_20210730 CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.reviews_npsscore;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp.sfmc.net_promoter_score_20210730 CLONE hygiene_vault_mvp.sfmc.net_promoter_score;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp.sfmc.net_promoter_score_20210730 CLONE hygiene_snapshot_vault_mvp.sfmc.net_promoter_score;

DROP TABLE hygiene_vault_mvp.sfmc.net_promoter_score;
DROP TABLE hygiene_snapshot_vault_mvp.sfmc.net_promoter_score;

DROP TABLE hygiene_vault_mvp.travelbird_mysql.reviews_npsscore;
DROP TABLE hygiene_snapshot_vault_mvp.travelbird_mysql.reviews_npsscore;

airflow backfill --start_date '2021-04-19 00:00:00' --end_date '2021-04-20 00:00:00' --task_regex '.*' hygiene_snapshots__sfmc__net_promoter_score__daily_at_01h00
airflow backfill --start_date '2021-07-30 00:00:00' --end_date '2021-07-30 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__reviews_npsscore__daily_at_01h00

--se reviews
SELECT nps.booking_id,
       nps.event_date       AS review_tstamp,
       nps.event_date::DATE AS review_date,
       nps.feedback_score   AS customer_score,
       fb.booking_status_type,
       fb.territory,
       fb.shiro_user_id
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.net_promoter_score nps
    LEFT JOIN data_vault_mvp.dwh.fact_booking fb ON nps.booking_id = fb.booking_id

UNION ALL
--tb reviews

SELECT rn.booking_id,
       rn.created_at__o AS review_tstamp,
       rn.created_at__o AS review_date,
       rn.score         AS customer_score,
       fb.booking_status_type,
       fb.territory,
       fb.shiro_user_id
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.reviews_npsscore rn
    LEFT JOIN data_vault_mvp.dwh.fact_booking fb ON rn.booking_id = fb.booking_id;

self_describing_task --include 'dv/dwh/reviews/user_booking_review.py'  --method 'run' --start '2021-07-29 00:00:00' --end '2021-07-29 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_booking_review;

self_describing_task --include 'se/data/dwh/user_booking_review.py'  --method 'run' --start '2021-07-29 00:00:00' --end '2021-07-29 00:00:00'

------------------------------------------------------------------------------------------------------------------------
--se_sale adjust to include scores

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.location_info_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.location_info_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.city_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.city_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.web_redirect_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.web_redirect_snapshot;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.cms_mysql_snapshots.web_redirect_company_snapshot AS
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.web_redirect_company_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.supplier_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.supplier_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.contractor_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.contractor_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_promotion CLONE data_vault_mvp.dwh.se_promotion;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.days_before_policy_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.days_before_policy_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.account CLONE hygiene_snapshot_vault_mvp.sfsc.account;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_sale;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_flight_config;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.in_house_package CLONE hygiene_snapshot_vault_mvp.cms_mysql.in_house_package;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale_translation CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_sale_translation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.product CLONE hygiene_snapshot_vault_mvp.cms_mysql.product;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.territory;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_translation CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_translation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.allocation CLONE hygiene_snapshot_vault_mvp.cms_mysql.allocation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_territory;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.company CLONE hygiene_snapshot_vault_mvp.cms_mysql.company;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_company CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_company;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.ihp_sale_company CLONE hygiene_snapshot_vault_mvp.cms_mysql.ihp_sale_company;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.opportunity CLONE hygiene_snapshot_vault_mvp.sfsc.opportunity;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.se_api.sales_list CLONE hygiene_snapshot_vault_mvp.se_api.sales_list;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.posu_categorisation;

self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2021-07-29 00:00:00' --end '2021-07-29 00:00:00'

--post deployement check
WITH dupes AS (
    SELECT dev.se_sale_id,
           dev.base_sale_id,
           dev.sale_id,
           dev.salesforce_opportunity_id,
           dev.exclusive_sale,
--            dev.sale_name,
--            dev.destination_name,
--            dev.sale_name_object,
           dev.sale_active,
           dev.class,
           dev.has_flights_available,
           dev.default_preferred_airport_code,
           dev.type,
           dev.hotel_chain_link,
           dev.closest_airport_code,
           dev.is_team20package,
           dev.sale_able_to_sell_flights,
           dev.sale_product,
           dev.sale_type,
           dev.product_type,
           dev.product_configuration,
           dev.product_line,
           dev.data_model,
           dev.hotel_location_info_id,
           dev.active,
           dev.default_hotel_offer_id,
           dev.commission,
           dev.commission_type,
           dev.original_contractor_id,
           dev.original_contractor_name,
           dev.original_joint_contractor_id,
           dev.original_joint_contractor_name,
           dev.date_created,
           dev.destination_type,
           dev.start_date,
           dev.end_date,
           dev.hotel_id,
           dev.base_currency,
           dev.city_district_id,
--            dev.company_id,
--            dev.company_name,
--            dev.company_array,
           dev.hotel_code,
           dev.latitude,
           dev.longitude,
           dev.location_info_id,
           dev.redirect_url,
--            dev.posa_territory,
--            dev.posa_territory_array,
--            dev.posa_country,
--            dev.posa_country_array,
--            dev.posa_currency,
--            dev.posa_currency_array,
           dev.posu_division,
           dev.posu_country,
           dev.posu_city,
           dev.supplier_id,
           dev.supplier_name,
--            dev.current_contractor_id,
--            dev.current_contractor_name,
--            dev.current_joint_contractor_id,
--            dev.current_joint_contractor_name,
           dev.posu_categorisation_id,
           dev.travel_type,
           dev.is_flashsale,
           dev.deal_category,
           dev.pulled_type,
           dev.pulled_reason,
           dev.salesforce_opportunity_id_full,
           dev.salesforce_account_id,
           dev.deal_profile,
           dev.salesforce_proposed_start_date,
           dev.salesforce_deal_label_multi,
           dev.salesforce_stage_name,
           dev.salesforce_repeat,
           dev.salesforce_currency_hotel_sales,
           dev.salesforce_currencyisocode,
           dev.salesforce_opted_in_for_always_on,
           dev.salesforce_parentid,
           dev.salesforce_opted_in_for_refundable_deals,
           dev.salesforce_opted_in_for_suvc,
           dev.salesforce_red_flag,
           dev.salesforce_red_flag_reason,
           dev.target_account_list,
           dev.star_rating,
           dev.rating_booking_com,
           dev.promotion_label,
           dev.promotion_description,
           dev.se_api_lead_rate,
           dev.se_api_lead_rate_per_person,
           dev.se_api_currency,
           dev.se_api_show_discount,
           dev.se_api_show_prices,
           dev.se_api_discount,
           dev.se_api_url,
           dev.cancellation_policy_id,
           dev.cancellation_policy_number_of_days,
           dev.cancellation_policy_percentage
--            dev.reviews,
--            dev.promoter_reviews,
--            dev.passive_reviews,
--            dev.detractor_reviews,
--            dev.avg_review_score
    FROM data_vault_mvp_dev_robin.dwh.se_sale dev
        EXCEPT
    SELECT prod.se_sale_id,
           prod.base_sale_id,
           prod.sale_id,
           prod.salesforce_opportunity_id,
           prod.exclusive_sale,
--            prod.sale_name,
--            prod.destination_name,
--            prod.sale_name_object,
           prod.sale_active,
           prod.class,
           prod.has_flights_available,
           prod.default_preferred_airport_code,
           prod.type,
           prod.hotel_chain_link,
           prod.closest_airport_code,
           prod.is_team20package,
           prod.sale_able_to_sell_flights,
           prod.sale_product,
           prod.sale_type,
           prod.product_type,
           prod.product_configuration,
           prod.product_line,
           prod.data_model,
           prod.hotel_location_info_id,
           prod.active,
           prod.default_hotel_offer_id,
           prod.commission,
           prod.commission_type,
           prod.original_contractor_id,
           prod.original_contractor_name,
           prod.original_joint_contractor_id,
           prod.original_joint_contractor_name,
           prod.date_created,
           prod.destination_type,
           prod.start_date,
           prod.end_date,
           prod.hotel_id,
           prod.base_currency,
           prod.city_district_id,
--            prod.company_id,
--            prod.company_name,
--            prod.company_array,
           prod.hotel_code,
           prod.latitude,
           prod.longitude,
           prod.location_info_id,
           prod.redirect_url,
--            prod.posa_territory,
--            prod.posa_territory_array,
--            prod.posa_country,
--            prod.posa_country_array,
--            prod.posa_currency,
--            prod.posa_currency_array,
           prod.posu_division,
           prod.posu_country,
           prod.posu_city,
           prod.supplier_id,
           prod.supplier_name,
--            prod.current_contractor_id,
--            prod.current_contractor_name,
--            prod.current_joint_contractor_id,
--            prod.current_joint_contractor_name,
           prod.posu_categorisation_id,
           prod.travel_type,
           prod.is_flashsale,
           prod.deal_category,
           prod.pulled_type,
           prod.pulled_reason,
           prod.salesforce_opportunity_id_full,
           prod.salesforce_account_id,
           prod.deal_profile,
           prod.salesforce_proposed_start_date,
           prod.salesforce_deal_label_multi,
           prod.salesforce_stage_name,
           prod.salesforce_repeat,
           prod.salesforce_currency_hotel_sales,
           prod.salesforce_currencyisocode,
           prod.salesforce_opted_in_for_always_on,
           prod.salesforce_parentid,
           prod.salesforce_opted_in_for_refundable_deals,
           prod.salesforce_opted_in_for_suvc,
           prod.salesforce_red_flag,
           prod.salesforce_red_flag_reason,
           prod.target_account_list,
           prod.star_rating,
           prod.rating_booking_com,
           prod.promotion_label,
           prod.promotion_description,
           prod.se_api_lead_rate,
           prod.se_api_lead_rate_per_person,
           prod.se_api_currency,
           prod.se_api_show_discount,
           prod.se_api_show_prices,
           prod.se_api_discount,
           prod.se_api_url,
           prod.cancellation_policy_id,
           prod.cancellation_policy_number_of_days,
           prod.cancellation_policy_percentage
    FROM data_vault_mvp.dwh.se_sale prod
)
   , stack AS (
    SELECT 'dev' AS source,
           dev.se_sale_id,
           dev.base_sale_id,
           dev.sale_id,
           dev.salesforce_opportunity_id,
           dev.exclusive_sale,
           dev.sale_name,
           dev.destination_name,
           dev.sale_name_object,
           dev.sale_active,
           dev.class,
           dev.has_flights_available,
           dev.default_preferred_airport_code,
           dev.type,
           dev.hotel_chain_link,
           dev.closest_airport_code,
           dev.is_team20package,
           dev.sale_able_to_sell_flights,
           dev.sale_product,
           dev.sale_type,
           dev.product_type,
           dev.product_configuration,
           dev.product_line,
           dev.data_model,
           dev.hotel_location_info_id,
           dev.active,
           dev.default_hotel_offer_id,
           dev.commission,
           dev.commission_type,
           dev.original_contractor_id,
           dev.original_contractor_name,
           dev.original_joint_contractor_id,
           dev.original_joint_contractor_name,
           dev.date_created,
           dev.destination_type,
           dev.start_date,
           dev.end_date,
           dev.hotel_id,
           dev.base_currency,
           dev.city_district_id,
           dev.company_id,
           dev.company_name,
           dev.company_array,
           dev.hotel_code,
           dev.latitude,
           dev.longitude,
           dev.location_info_id,
           dev.redirect_url,
           dev.posa_territory,
           dev.posa_territory_array,
           dev.posa_country,
           dev.posa_country_array,
           dev.posa_currency,
           dev.posa_currency_array,
           dev.posu_division,
           dev.posu_country,
           dev.posu_city,
           dev.supplier_id,
           dev.supplier_name,
           dev.current_contractor_id,
           dev.current_contractor_name,
           dev.current_joint_contractor_id,
           dev.current_joint_contractor_name,
           dev.posu_categorisation_id,
           dev.travel_type,
           dev.is_flashsale,
           dev.deal_category,
           dev.pulled_type,
           dev.pulled_reason,
           dev.salesforce_opportunity_id_full,
           dev.salesforce_account_id,
           dev.deal_profile,
           dev.salesforce_proposed_start_date,
           dev.salesforce_deal_label_multi,
           dev.salesforce_stage_name,
           dev.salesforce_repeat,
           dev.salesforce_currency_hotel_sales,
           dev.salesforce_currencyisocode,
           dev.salesforce_opted_in_for_always_on,
           dev.salesforce_parentid,
           dev.salesforce_opted_in_for_refundable_deals,
           dev.salesforce_opted_in_for_suvc,
           dev.salesforce_red_flag,
           dev.salesforce_red_flag_reason,
           dev.target_account_list,
           dev.star_rating,
           dev.rating_booking_com,
           dev.promotion_label,
           dev.promotion_description,
           dev.se_api_lead_rate,
           dev.se_api_lead_rate_per_person,
           dev.se_api_currency,
           dev.se_api_show_discount,
           dev.se_api_show_prices,
           dev.se_api_discount,
           dev.se_api_url,
           dev.cancellation_policy_id,
           dev.cancellation_policy_number_of_days,
           dev.cancellation_policy_percentage
    FROM data_vault_mvp_dev_robin.dwh.se_sale dev

    UNION ALL

    SELECT 'prod' AS source,
           ss.se_sale_id,
           ss.base_sale_id,
           ss.sale_id,
           ss.salesforce_opportunity_id,
           ss.exclusive_sale,
           ss.sale_name,
           ss.destination_name,
           ss.sale_name_object,
           ss.sale_active,
           ss.class,
           ss.has_flights_available,
           ss.default_preferred_airport_code,
           ss.type,
           ss.hotel_chain_link,
           ss.closest_airport_code,
           ss.is_team20package,
           ss.sale_able_to_sell_flights,
           ss.sale_product,
           ss.sale_type,
           ss.product_type,
           ss.product_configuration,
           ss.product_line,
           ss.data_model,
           ss.hotel_location_info_id,
           ss.active,
           ss.default_hotel_offer_id,
           ss.commission,
           ss.commission_type,
           ss.original_contractor_id,
           ss.original_contractor_name,
           ss.original_joint_contractor_id,
           ss.original_joint_contractor_name,
           ss.date_created,
           ss.destination_type,
           ss.start_date,
           ss.end_date,
           ss.hotel_id,
           ss.base_currency,
           ss.city_district_id,
           ss.company_id,
           ss.company_name,
           ss.company_array,
           ss.hotel_code,
           ss.latitude,
           ss.longitude,
           ss.location_info_id,
           ss.redirect_url,
           ss.posa_territory,
           ss.posa_territory_array,
           ss.posa_country,
           ss.posa_country_array,
           ss.posa_currency,
           ss.posa_currency_array,
           ss.posu_division,
           ss.posu_country,
           ss.posu_city,
           ss.supplier_id,
           ss.supplier_name,
           ss.current_contractor_id,
           ss.current_contractor_name,
           ss.current_joint_contractor_id,
           ss.current_joint_contractor_name,
           ss.posu_categorisation_id,
           ss.travel_type,
           ss.is_flashsale,
           ss.deal_category,
           ss.pulled_type,
           ss.pulled_reason,
           ss.salesforce_opportunity_id_full,
           ss.salesforce_account_id,
           ss.deal_profile,
           ss.salesforce_proposed_start_date,
           ss.salesforce_deal_label_multi,
           ss.salesforce_stage_name,
           ss.salesforce_repeat,
           ss.salesforce_currency_hotel_sales,
           ss.salesforce_currencyisocode,
           ss.salesforce_opted_in_for_always_on,
           ss.salesforce_parentid,
           ss.salesforce_opted_in_for_refundable_deals,
           ss.salesforce_opted_in_for_suvc,
           ss.salesforce_red_flag,
           ss.salesforce_red_flag_reason,
           ss.target_account_list,
           ss.star_rating,
           ss.rating_booking_com,
           ss.promotion_label,
           ss.promotion_description,
           ss.se_api_lead_rate,
           ss.se_api_lead_rate_per_person,
           ss.se_api_currency,
           ss.se_api_show_discount,
           ss.se_api_show_prices,
           ss.se_api_discount,
           ss.se_api_url,
           ss.cancellation_policy_id,
           ss.cancellation_policy_number_of_days,
           ss.cancellation_policy_percentage
    FROM data_vault_mvp.dwh.se_sale ss
)
SELECT s.source,
       s.se_sale_id,
       s.base_sale_id,
       s.sale_id,
       s.salesforce_opportunity_id,
       s.exclusive_sale,
--        s.sale_name,
--        s.destination_name,
       s.sale_name_object,
       s.sale_active,
       s.class,
       s.has_flights_available,
       s.default_preferred_airport_code,
       s.type,
       s.hotel_chain_link,
       s.closest_airport_code,
       s.is_team20package,
       s.sale_able_to_sell_flights,
       s.sale_product,
       s.sale_type,
       s.product_type,
       s.product_configuration,
       s.product_line,
       s.data_model,
       s.hotel_location_info_id,
       s.active,
       s.default_hotel_offer_id,
       s.commission,
       s.commission_type,
       s.original_contractor_id,
       s.original_contractor_name,
       s.original_joint_contractor_id,
       s.original_joint_contractor_name,
       s.date_created,
       s.destination_type,
       s.start_date,
       s.end_date,
       s.hotel_id,
       s.base_currency,
       s.city_district_id,
--        s.company_id,
--        s.company_name,
--        s.company_array,
       s.hotel_code,
       s.latitude,
       s.longitude,
       s.location_info_id,
       s.redirect_url,
--        s.posa_territory,
--        s.posa_territory_array,
--        s.posa_country,
--        s.posa_country_array,
--        s.posa_currency,
--        s.posa_currency_array,
       s.posu_division,
       s.posu_country,
       s.posu_city,
       s.supplier_id,
       s.supplier_name,
       s.current_contractor_id,
       s.current_contractor_name,
       s.current_joint_contractor_id,
       s.current_joint_contractor_name,
       s.posu_categorisation_id,
       s.travel_type,
       s.is_flashsale,
       s.deal_category,
       s.pulled_type,
       s.pulled_reason,
       s.salesforce_opportunity_id_full,
       s.salesforce_account_id,
       s.deal_profile,
       s.salesforce_proposed_start_date,
       s.salesforce_deal_label_multi,
       s.salesforce_stage_name,
       s.salesforce_repeat,
       s.salesforce_currency_hotel_sales,
       s.salesforce_currencyisocode,
       s.salesforce_opted_in_for_always_on,
       s.salesforce_parentid,
       s.salesforce_opted_in_for_refundable_deals,
       s.salesforce_opted_in_for_suvc,
       s.salesforce_red_flag,
       s.salesforce_red_flag_reason,
       s.target_account_list,
       s.star_rating,
       s.rating_booking_com,
       s.promotion_label,
       s.promotion_description,
       s.se_api_lead_rate,
       s.se_api_lead_rate_per_person,
       s.se_api_currency,
       s.se_api_show_discount,
       s.se_api_show_prices,
       s.se_api_discount,
       s.se_api_url,
       s.cancellation_policy_id,
       s.cancellation_policy_number_of_days,
       s.cancellation_policy_percentage
FROM stack s
    INNER JOIN dupes d ON s.se_sale_id = d.se_sale_id
ORDER BY s.se_sale_id
;

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss

SELECT ubs.se_sale_id,
       COUNT(*)                                 AS reviews,
       COUNT_IF(customer_score >= 9)            AS promoter_reviews,
       COUNT_IF(customer_score BETWEEN 7 AND 8) AS passive_reviews,
       COUNT_IF(customer_score <= 6)            AS detractor_reviews,
       AVG(customer_score)                      AS avg_review_score,
       (promoter_reviews / reviews) - (detractor_reviews / reviews) AS nps_score
FROM data_vault_mvp_dev_robin.dwh.user_booking_review ubs
GROUP BY 1

self_describing_task --include 'dv/dwh/transactional/tb_offer.py'  --method 'run' --start '2021-07-29 00:00:00' --end '2021-07-29 00:00:00'


SELECT * FROM data_vault_mvp_dev_robin.dwh.tb_offer t;