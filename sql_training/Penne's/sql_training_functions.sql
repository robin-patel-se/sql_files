------------------------------------------------------------------------------------------------------------------------
--conditional expression functions

SELECT COALESCE(NULL, 'hello');

SELECT CASE WHEN 1=1 THEN 'hello' END;

SELECT ZEROIFNULL(NULL);


------------------------------------------------------------------------------------------------------------------------
--string functions

SELECT CONCAT(' Hello, ', 'my name is ', 'Paul');

SELECT UPPER('Hello, my name is Paul');

SELECT INITCAP('Hello, my name is Paul');


------------------------------------------------------------------------------------------------------------------------
--number functions

SELECT ABS(-22), ABS(22);

SELECT ROUND(5.4), ROUND(5.5), ROUND(5.6);


------------------------------------------------------------------------------------------------------------------------
--context functions

SELECT CURRENT_DATE;

------------------------------------------------------------------------------------------------------------------------
--date functions

SELECT DATE_PART('month', CURRENT_DATE);

SELECT DATEDIFF('day', CURRENT_DATE, CURRENT_DATE - 1) AS date_diff;

SELECT DAYOFWEEK(CURRENT_DATE);



SELECT ssa.posa_country,
       ssa.product_configuration
FROM se.data.se_sale_attributes ssa;

select current_date +1


-- union

-- except

SELECT *
FROM se.data.master_se_booking_list msbl
WHERE msbl.cr_credit_deleted > 0;

USE WAREHOUSE pipe_xlarge;
SELECT COUNT(*)
FROM se.data.se_booking sb
WHERE sb.booking_completed_date = '2019-01-20'
;



CREATE OR REPLACE VIEW {target_table_ref
} __step01__get_source_batch
AS
SELECT DISTINCT -- TODO: this shouldn't be necessary but some opportunities seem to exist under different account_ids (connected deals with multiple suppliers maybe?). For now we will exclude them but we need to revisit this.
       opportunity.sale_id,
       opportunity.deal_category,
       opportunity.deal_profile,
       account.star_rating
FROM (
         SELECT DISTINCT
                LAST_VALUE(sale_id) IGNORE NULLS OVER (PARTITION BY sale_id ORDER BY loaded_at)                 AS sale_id,
                LAST_VALUE(account_id) IGNORE NULLS OVER (PARTITION BY sale_id ORDER BY loaded_at)              AS account_id,
                LAST_VALUE(deal_category)
                           IGNORE NULLS OVER (PARTITION BY sale_id ORDER BY loaded_at)                          AS deal_category, -- if the deal cat was removed from SF, make sure we still send whatever the last non-null value was. It also appears some sales _change_ opportunity IDs so partitioning by `id` falls over so we partition by sale_id/account_id instead.
                LAST_VALUE(deal_profile)
                           IGNORE NULLS OVER (PARTITION BY sale_id ORDER BY loaded_at)                          AS deal_profile LAST_VALUE(sale_id__c) IGNORE NULLS OVER(PARTITION BY sale_id__c ORDER BY row_loaded_at) AS sale_id,
                LAST_VALUE(accountid) IGNORE NULLS OVER (PARTITION BY sale_id__c ORDER BY row_loaded_at)        AS account_id,
                LAST_VALUE(deal_category__c)
                           IGNORE NULLS OVER (PARTITION BY sale_id__c ORDER BY row_loaded_at)                   AS deal_category, -- if the deal cat was removed from SF, make sure we still send whatever the last non-null value was. It also appears some sales _change_ opportunity IDs so partitioning by `id` falls over so we partition by sale_id__c/account_id instead.
                LAST_VALUE(deal_profile__c) IGNORE NULLS OVER (PARTITION BY sale_id__c ORDER BY row_loaded_at)  AS deal_profile
         FROM {sources['opportunity']}
         WHERE sale_id__c IS NOT NULL -- only distribute deals which have been loaded in the CMS
           AND ( -- only distribute deals which have at least one of these attributes
                 deal_category__c IS NOT NULL
                 OR deal_profile__c IS NOT NULL
             )
           AND loaded_at >= TIMESTAMPADD('day', -1, '{schedule_tstamp}'::TIMESTAMP)
           AND row_loaded_at >= TIMESTAMPADD('day', -1, '{schedule_tstamp}'::TIMESTAMP)
     ) AS opportunity
         LEFT JOIN (
    SELECT DISTINCT
           id,
           LAST_VALUE(star_rating__c) OVER (PARTITION BY id ORDER BY row_loaded_at) AS star_rating
    FROM {sources['account']}
) AS account ON opportunity.account_id::VARCHAR = account.id::VARCHAR

CREATE OR REPLACE TABLE
;
SELECT *
FROM data_vault_mvp.information_schema.tables t
WHERE t.table_schema = 'SINGLE_CUSTOMER_VIEW_STG';

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations CLONE data_vault_mvp.single_customer_view_stg.module_identity_associations;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker CLONE data_vault_mvp.single_customer_view_stg.module_time_diff_marker;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs_bkup CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs_bkup;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params CLONE data_vault_mvp.single_customer_view_stg.module_url_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker CLONE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream clone hygiene_vault_mvp.snowplow.event_stream;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification AS
    SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
WHERE mt.updated_at >= current_date -2
LIMIT 10000;


self_describing_task --include 'dv/dwh/events/07_events_of_interest/01_module_touched_spvs.py'  --method 'run' --start '2021-01-20 00:00:00' --end '2021-01-20 00:00:00'

