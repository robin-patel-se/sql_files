CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.salesforce_opportunity_sale AS
SELECT LEFT(o.id, 15)                     AS opportunity_id,
       o.id                               AS opportunity_id_full,
       o.destination_manager__c           AS destination_manager,
       o.of_incomplete_actions__c         AS number_of_complete_actions,
       o.deal_profile__c                  AS deal_profile,
       o.proposed_start_date__c           AS proposed_start_date,
       o.deal_label_multi__c              AS deal_label_multi,
       o.stagename                        AS stage_name,
       o.repeat__c                        AS repeat,
       o.deal_category__c                 AS deal_category,
       o.currencyisocode                  AS currency,
       o.owner__c                         AS owner,
       o.owners_role__c                   AS owner_role,
       o.joint_cm_owner_name__c           AS joint_owner,
       o.percentage_commission__c         AS percentage_commission,

       a.id                               AS account_id,
       a.currency_hotel_sales__c          AS currency_hotel_sales,
       a.currencyisocode                  AS hotel_account_currency,
       a.opted_in_for_always_on__c        AS opted_in_for_always_on,
       a.parentid                         AS parentid,
       a.opted_in_for_refundable_deals__c AS opted_in_for_refundable_deals,
       a.opted_in_for_suvc__c             AS opted_in_for_suvc,
       a.red_flag__c                      AS red_flag,
       a.red_flag_reason__c               AS red_flag_reason,
       a.number_of_rooms__c               AS no_rooms,
       a.target_account_list__c           AS target_account_list,
       a2.id                              AS parent_account_id,
       a2.name                            AS parent_account_name
FROM hygiene_snapshot_vault_mvp.sfsc.opportunity o
         LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a
                   ON o.accountid = a.id
         LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a2 ON a.parentid = a2.id;


SELECT GET_DDL('table', 'scratch.robinpatel.salesforce_opportunity_sale');

CREATE OR REPLACE TRANSIENT TABLE salesforce_opportunity_sale
(
    opportunity_id                VARCHAR,
    opportunity_id_full           VARCHAR,
    destination_manager           VARCHAR,
    number_of_complete_actions    FLOAT,
    deal_profile                  VARCHAR,
    proposed_start_date           DATE,
    deal_label_multi              VARCHAR,
    stage_name                    VARCHAR,
    repeat                        VARCHAR,
    deal_category                 VARCHAR,
    currency                      VARCHAR,
    owner                         VARCHAR,
    owner_role                    VARCHAR,
    joint_owner                   VARCHAR,
    percentage_commission         FLOAT,
    account_id                    VARCHAR,
    currency_hotel_sales          VARCHAR,
    hotel_account_currency        VARCHAR,
    opted_in_for_always_on        VARCHAR,
    parentid                      VARCHAR,
    opted_in_for_refundable_deals VARCHAR,
    opted_in_for_suvc             VARCHAR,
    red_flag                      BOOLEAN,
    red_flag_reason               VARCHAR,
    no_rooms                      FLOAT,
    target_account_list           VARCHAR,
    parent_account_id             VARCHAR,
    parent_account_name           VARCHAR
);

SELECT *
FROM hygiene_snapshot_vault_mvp.sfsc.opportunity o
WHERE o.id = '0066900001I1NBRAA3';

SELECT *
FROM hygiene_snapshot_vault_mvp.sfsc.account a
WHERE a.id = '0016900002YsnEAAAZ';

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_sale;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_flight_config;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.se_api.sales_list CLONE hygiene_snapshot_vault_mvp.se_api.sales_list;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.in_house_package_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.in_house_package_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.location_info_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.location_info_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.city_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.city_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_sale_translation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_sale_translation_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.territory_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_translation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_translation_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.offer_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.offer_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.allocation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.allocation_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_territory_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_affiliate_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_affiliate_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_sale_affiliate_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_sale_affiliate_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.web_redirect_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.web_redirect_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.company_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_company_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.ihp_sale_company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.ihp_sale_company_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.web_redirect_company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.web_redirect_company_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.supplier_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.supplier_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.contractor_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.contractor_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.opportunity CLONE hygiene_snapshot_vault_mvp.sfsc.opportunity;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_promotion CLONE data_vault_mvp.dwh.se_promotion;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.account CLONE hygiene_snapshot_vault_mvp.sfsc.account;

self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2021-01-28 00:00:00' --end '2021-01-28 00:00:00'


self_describing_task --include 'dv/dwh/salesforce/salesforce_sale_opportunity.py'  --method 'run' --start '2021-01-28 00:00:00' --end '2021-01-28 00:00:00'


SELECT salesforce_sale_opportunity.schedule_tstamp,
       salesforce_sale_opportunity.run_tstamp,
       salesforce_sale_opportunity.operation_id,
       salesforce_sale_opportunity.created_at,
       salesforce_sale_opportunity.updated_at,
       salesforce_sale_opportunity.opportunity_id,
       salesforce_sale_opportunity.opportunity_id_full,
       salesforce_sale_opportunity.destination_manager,
       salesforce_sale_opportunity.number_of_complete_actions,
       salesforce_sale_opportunity.deal_profile,
       salesforce_sale_opportunity.proposed_start_date,
       salesforce_sale_opportunity.deal_label_multi,
       salesforce_sale_opportunity.stage_name,
       salesforce_sale_opportunity.repeat,
       salesforce_sale_opportunity.deal_category,
       salesforce_sale_opportunity.currency,
       salesforce_sale_opportunity.owner,
       salesforce_sale_opportunity.owner_role,
       salesforce_sale_opportunity.joint_owner,
       salesforce_sale_opportunity.percentage_commission,
       salesforce_sale_opportunity.account_id,
       salesforce_sale_opportunity.currency_hotel_sales,
       salesforce_sale_opportunity.hotel_account_currency,
       salesforce_sale_opportunity.opted_in_for_always_on,
       salesforce_sale_opportunity.parentid,
       salesforce_sale_opportunity.opted_in_for_refundable_deals,
       salesforce_sale_opportunity.opted_in_for_suvc,
       salesforce_sale_opportunity.red_flag,
       salesforce_sale_opportunity.red_flag_reason,
       salesforce_sale_opportunity.no_rooms,
       salesforce_sale_opportunity.target_account_list,
       salesforce_sale_opportunity.parent_account_id,
       salesforce_sale_opportunity.parent_account_name
FROm data_vault_mvp_dev_robin.dwh.salesforce_sale_opportunity;

self_describing_task --include 'se/data/dwh/salesforce_sale_opportunity.py'  --method 'run' --start '2021-01-28 00:00:00' --end '2021-01-28 00:00:00'

self_describing_task --include 'se/data/dwh/se_sale_attributes.py'  --method 'run' --start '2021-01-28 00:00:00' --end '2021-01-28 00:00:00'