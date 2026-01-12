CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.inclusion CLONE hygiene_snapshot_vault_mvp.sfsc.inclusion;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.account CLONE hygiene_snapshot_vault_mvp.sfsc.account;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.opportunity CLONE hygiene_snapshot_vault_mvp.sfsc.opportunity;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.offers CLONE hygiene_snapshot_vault_mvp.sfsc.offers;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.rates CLONE data_vault_mvp.fx.rates;

self_describing_task --include 'dv/dwh/sfsc/offer_inclusion.py'  --method 'run' --start '2022-01-31 00:00:00' --end '2022-01-31 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.offer_inclusion;


SELECT *
FROM snowflake.account_usage.query_history qh
WHERE LOWER(qh.query_text) LIKE '%data_vault_mvp.dwh.offer_inclusion%'

SELECT *
FROM se.data.salesforce_offer_inclusion soi;

SELECT *
FROM se.data.salesforce_offer_inclusion soi;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.offer_inclusion;

SELECT o.*,
       ss.data_model
FROM hygiene_snapshot_vault_mvp.sfsc.offers o
    LEFT JOIN data_vault_mvp.dwh.se_sale ss ON LEFT(o.opportunity__c, 15) = ss.salesforce_opportunity_id
;


SELECT soir.primary_key_hash,
       COALESCE(soir.hotel_code, s.hotel_code)           AS hotel_code,
       COALESCE(soir.hotel_name, s.hotel_name)           AS hotel_name,
       COALESCE(soir.allocation_date, s.allocation_date) AS allocation_date,
       soir.core_inclusions_mari,
       s.core_inclusions_mari
FROM data_vault_mvp.dwh.se_offers_inclusions_rates soir
    FULL OUTER JOIN data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates s ON soir.primary_key_hash = s.primary_key_hash
WHERE soir.core_inclusions_mari != s.core_inclusions_mari;

SELECT soir.primary_key_hash,
       soir.salesforce_opportunity_id,
       soir.deal_stage_rank,
       soir.deal_type,
       soir.hotel_id,
       soir.hotel_code,
       soir.hotel_name,
       soir.salesforce_hotel_name,
       soir.booking_com_name__c,
       soir.ota_posa,
       soir.offer_name,
       soir.room_type_name,
       soir.rate_plan_name,
       soir.rate_plan_rack_code,
       soir.hotel_rate_rack_code,
       soir.no_total_rooms,
       soir.no_available_rooms,
       soir.min_length_of_stay,
       soir.max_length_of_stay,
       soir.rate_type,
       soir.fixed_length_of_stay,
       soir.los,
       soir.rate_local_calculated,
       soir.occupancy_adults,
       soir.cms_offer_id,
       soir.salesforce_offer_id,
       soir.currency_local,
       soir.allocation_date,
       soir.rate_local,
       soir.single_rate_local,
       soir.room_type_ota_name,
       soir.board_basis,
       soir.inclusion_level_agg,
       soir.inclusion_type_agg,
       soir.currency_code_local_agg,
       soir.inclusion_value_local_agg,
       soir.core_inclusions_mari,
       soir.supplementary_inclusions_mari,
       soir.core_per_person_first_night,
       soir.core_per_person_per_stay,
       soir.core_per_person_per_day,
       soir.core_per_room_first_night,
       soir.core_per_room_per_stay,
       soir.core_per_room_per_day,
       soir.supplementary_per_person_first_night,
       soir.supplementary_per_person_per_stay,
       soir.supplementary_per_person_per_day,
       soir.supplementary_per_room_first_night,
       soir.supplementary_per_room_per_stay,
       soir.supplementary_per_room_per_day,
       soir.validation_check_hotel_id,
       soir.validation_check_currency_local,
       soir.validation_check_booking_com_name__c,
       soir.validation_check_ota_posa,
       soir.validation_check_los,
       soir.validation_check_rate_local_calculated,
       soir.validation_check_room_type_ota_name,
       soir.validation_check_allocation_date,
       soir.validation_check_deal_type,
       soir.validation_check_board_basis,
       soir.validation_check
FROM data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates soir;


self_describing_task --include 'se/data/sfsc/salesforce_offer_inclusion.py'  --method 'run' --start '2022-02-02 00:00:00' --end '2022-02-02 00:00:00'


SELECT *
FROM data_vault_mvp.dwh.se_offers_inclusions_rates

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.se_offers_inclusions_rates_20220203 CLONE data_vault_mvp.dwh.se_offers_inclusions_rates;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_rate_plan_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.product CLONE hygiene_snapshot_vault_mvp.cms_mysql.product;

self_describing_task --include 'dv/dwh/mari/cms_mari_link.py'  --method 'run' --start '2022-02-02 00:00:00' --end '2022-02-02 00:00:00'



