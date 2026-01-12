SELECT *
FROM hygiene_snapshot_vault_mvp.sfsc.opportunity o;
SELECT *
FROM hygiene_snapshot_vault_mvp.sfsc.account a;

SELECT ssa.se_sale_id,
       ssa.sale_name,
       ssa.hotel_code,
       a.business_legal_name__c,
       a.email__c,
       a.confirmation_emails__c,
       a.addr_one_ns__c,
       a.addr_two_ns__c,
       a.contract_country__c,
       a.address_country_netsuite__c,
       a.billingcity,
       a.cms_url__c,
       a.contract_type__c,
       a.currencyisocode,
       a.business_status__c,
       a.account_contact_status__c,
       a.geolocation__latitude__s,
       a.geolocation__longitude__s,
       a.star_rating__c,

       a.shippingstreet,
       a.shippingcity,
       a.shippingcountry,
       a.shippingpostalcode,
       a.shippingstate,
       a.billingstreet,
       a.billingcity,
       a.billingcountry,
       a.billingpostalcode,
       a.billingstate

FROM se.data.se_sale_attributes ssa
         LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON ssa.hotel_code = LEFT(id, 15)
WHERE ssa.hotel_code = '001w000001W8TeO';

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.account CLONE hygiene_snapshot_vault_mvp.sfsc.account;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.opportunity CLONE hygiene_snapshot_vault_mvp.sfsc.opportunity;

self_describing_task --include 'dv/dwh/salesforce/salesforce_sale_opportunity.py'  --method 'run' --start '2021-02-11 00:00:00' --end '2021-02-11 00:00:00'

DROP TABLE data_vault_mvp_dev_robin.dwh.salesforce_sale_opportunity;

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.salesforce_sale_opportunity');

CREATE OR REPLACE TRANSIENT TABLE salesforce_sale_opportunity
(
    schedule_tstamp                       TIMESTAMP_NTZ(9),
    run_tstamp                            TIMESTAMP_NTZ(9),
    operation_id                          VARCHAR(16777216),
    created_at                            TIMESTAMP_NTZ(9),
    updated_at                            TIMESTAMP_NTZ(9),
    opportunity_id                        VARCHAR(16777216) NOT NULL,
    opportunity_id_full                   VARCHAR(16777216),
    destination_manager                   VARCHAR(16777216),
    number_of_incomplete_actions          FLOAT,
    deal_profile                          VARCHAR(16777216),
    proposed_start_date                   DATE,
    deal_label_multi                      VARCHAR(16777216),
    stage_name                            VARCHAR(16777216),
    repeat                                VARCHAR(16777216),
    deal_category                         VARCHAR(16777216),
    currency                              VARCHAR(16777216),
    owner                                 VARCHAR(16777216),
    owner_role                            VARCHAR(16777216),
    joint_owner                           VARCHAR(16777216),
    percentage_commission                 FLOAT,
    account_id                            VARCHAR(16777216),
    account_name                          VARCHAR(16777216),
    account_business_legal_name           VARCHAR(16777216),
    account_email                         VARCHAR(16777216),
    account_confirmation_email            VARCHAR(16777216),
    account_address1                      VARCHAR(16777216),
    account_address2                      VARCHAR(16777216),
    account_contract_country              VARCHAR(16777216),
    account_country_netsuite              VARCHAR(16777216),
    account_billing_city                  VARCHAR(16777216),
    account_shipping_city                 VARCHAR(16777216),
    account_cms_url                       VARCHAR(16777216),
    account_contract_type                 VARCHAR(16777216),
    account_currency                      VARCHAR(16777216),
    account_business_status               VARCHAR(16777216),
    account_contract_status               VARCHAR(16777216),
    account_longitude                     VARCHAR(16777216),
    account_latitude                      VARCHAR(16777216),
    account_star_rating                   VARCHAR(16777216),
    account_currency_hotel_sales          VARCHAR(16777216),
    account_hotel_account_currency        VARCHAR(16777216),
    account_opted_in_for_always_on        VARCHAR(16777216),
    account_parentid                      VARCHAR(16777216),
    account_opted_in_for_refundable_deals VARCHAR(16777216),
    account_opted_in_for_suvc             VARCHAR(16777216),
    account_red_flag                      BOOLEAN,
    account_red_flag_reason               VARCHAR(16777216),
    account_no_rooms                      FLOAT,
    account_target_account_list           VARCHAR(16777216),
    parent_account_id                     VARCHAR(16777216),
    parent_account_name                   VARCHAR(16777216),
    PRIMARY KEY (opportunity_id)
);

SELECT *
FROM data_vault_mvp_dev_robin.dwh.salesforce_sale_opportunity;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.global_sale_attributes CLONE data_vault_mvp.dwh.global_sale_attributes;

self_describing_task --include 'se/data/dwh/global_sale_attributes.py'  --method 'run' --start '2021-02-11 00:00:00' --end '2021-02-11 00:00:00'
self_describing_task --include 'se/data_pii/dwh/global_sale_attributes.py'  --method 'run' --start '2021-02-11 00:00:00' --end '2021-02-11 00:00:00'


self_describing_task --include 'dv/dwh/transactional/global_sale_attributes.py'  --method 'run' --start '2021-02-11 00:00:00' --end '2021-02-11 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.salesforce_sale_opportunity;

SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.orders_instalment_snapshot;

self_describing_task --include 'se/finance/finance_models/se_voucher.py'  --method 'run' --start '2021-02-15 00:00:00' --end '2021-02-15 00:00:00'
self_describing_task --include 'se/data_pidd/finance_models/se_voucher.py'  --method 'run' --start '2021-02-15 00:00:00' --end '2021-02-15 00:00:00'
self_describing_task --include 'se/data/finance_models/se_voucher.py'  --method 'run' --start '2021-02-15 00:00:00' --end '2021-02-15 00:00:00'


SELECT DISTINCT
       LEFT(gsa.account_id, 15) AS sf_account_id,
       gsa.account_id AS sf_account_id_full,
       gsa.account_name,
       gsa.account_shipping_street,
       gsa.account_shipping_city,
       gsa.account_shipping_country,
       gsa.account_shipping_postcode,
       gsa.account_confirmation_email,
       gsa.account_email
FROM se.data_pii.global_sale_attributes gsa
WHERE LEFT(gsa.account_id, 15) = '001w000001W8TeO';

SELECT * FROm data_vault_mvp.dwh.athena_email_reporting aer;

self_describing_task --include 'se/data/dwh/athena_email_reporting.py'  --method 'run' --start '2021-02-15 00:00:00' --end '2021-02-15 00:00:00'

SELECT get_ddl('table', 'se.data_pii.global_sale_attributes');


