CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.global_sale_attributes AS
SELECT *
FROM se.data.global_sale_attributes gsa;

SELECT GET_DDL('table', 'scratch.robinpatel.global_sale_attributes');

CREATE OR REPLACE TRANSIENT TABLE global_sale_attributes
(
    global_sale_id VARCHAR,
    deal_segment VARCHAR,
    global_sale_start_date DATE,
    opportunity_id VARCHAR,
    opportunity_id_full VARCHAR,
    destination_manager_id VARCHAR,
    destination_manager VARCHAR,
    number_of_incomplete_actions FLOAT,
    deal_profile VARCHAR,
    proposed_start_date DATE,
    deal_label_multi VARCHAR,
    stage_name VARCHAR,
    repeat VARCHAR,
    deal_category VARCHAR,
    pulled_type VARCHAR,
    pulled_reason VARCHAR,
    currency VARCHAR,
    owner VARCHAR,
    owner_role VARCHAR,
    joint_owner VARCHAR,
    percentage_commission FLOAT,
    is_live_on_site BOOLEAN,
    territories_excluded VARCHAR,
    territory_availability VARCHAR,
    account_id VARCHAR,
    account_name VARCHAR,
    account_business_legal_name VARCHAR,
    status VARCHAR,
    preloaded_by VARCHAR,
    first_price_check_by VARCHAR,
    allocation_loaded_by VARCHAR,
    account_shipping_street VARCHAR,
    account_shipping_city VARCHAR,
    account_shipping_country VARCHAR,
    account_shipping_postcode VARCHAR,
    account_shipping_state VARCHAR,
    account_billing_street VARCHAR,
    account_billing_city VARCHAR,
    account_billing_country VARCHAR,
    account_billing_postcode VARCHAR,
    account_billing_state VARCHAR,
    account_cms_url VARCHAR,
    account_contract_type VARCHAR,
    account_currency VARCHAR,
    account_business_status VARCHAR,
    account_contract_status VARCHAR,
    account_longitude VARCHAR,
    account_latitude VARCHAR,
    account_currency_hotel_sales VARCHAR,
    account_opted_in_for_always_on VARCHAR,
    account_opted_in_for_refundable_deals VARCHAR,
    account_opted_in_for_suvc VARCHAR,
    account_red_flag BOOLEAN,
    account_red_flag_reason VARCHAR,
    account_no_rooms FLOAT,
    account_target_account_list VARCHAR,
    account_star_rating VARCHAR,
    account_rating_booking_com VARCHAR,
    account_owner_id VARCHAR,
    account_owner_name VARCHAR,
    parent_account_id VARCHAR,
    parent_account_name VARCHAR
);


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.salesforce_sale_opportunity CLONE data_vault_mvp.dwh.salesforce_sale_opportunity;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS SELECT * FROM data_vault_mvp.dwh.fact_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active CLONE data_vault_mvp.dwh.sale_active;

self_describing_task --include 'dv/dwh/transactional/global_sale_attributes.py'  --method 'run' --start '2021-07-22 00:00:00' --end '2021-07-22 00:00:00'

self_describing_task --include 'se/data/dwh/global_sale_attributes.py'  --method 'run' --start '2021-07-22 00:00:00' --end '2021-07-22 00:00:00'
self_describing_task --include 'se/data_pii/dwh/global_sale_attributes.py'  --method 'run' --start '2021-07-22 00:00:00' --end '2021-07-22 00:00:00'


airflow clear --start_date '2021-07-25 00:00:00' --end_date '2021-07-26 00:00:00' --task_regex '.*' dwh__transactional__global_sale_attributes__daily_at_03h00
airflow backfill --start_date '2021-07-25 00:00:00' --end_date '2021-07-26 00:00:00' --task_regex '.*' dwh__transactional__global_sale_attributes__daily_at_03h00

self_describing_task --include 'se/bi/dim_sale.py'  --method 'run' --start '2021-07-25 00:00:00' --end '2021-07-25 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.global_sale_attributes AS SELECT * FROM se.data.global_sale_attributes gsa;
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.bi_dim_sale AS SELECT * FROM se.bi.dim_sale ds;

SELECT * FROM scratch.robinpatel.global_sale_attributes gsa
EXCEPT
SELECT * FROM se.data.global_sale_attributes g;

SELECT * FROM scratch.robinpatel.bi_dim_sale bds
EXCEPT
SELECT * FROM se.bi.dim_sale ds;