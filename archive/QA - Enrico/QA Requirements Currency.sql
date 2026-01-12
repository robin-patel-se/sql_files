------------------------------------------------------------------------------------------------------------------------
--currency

SELECT o.id              AS sf_opportunity_id,
       o.currencyisocode AS sf_opportunity_currency,
       o.accountid       AS sf_opportunity_account_id
FROM hygiene_snapshot_vault_mvp.sfsc.opportunity o
WHERE LEFT(id, 15) = '0066900001MSgpz';


SELECT id                AS sf_account_id,
       a.currencyisocode AS sf_account_currency
FROM hygiene_snapshot_vault_mvp.sfsc.account a
WHERE id = '001w000001TOkXyAAL'

SELECT os.opportunity__c  AS sf_opportunity_id,
       name               AS sf_offer_name,
       os.currencyisocode AS sf_offer_currency
FROM data_vault_mvp.sfsc_snapshots.offers_snapshot os
WHERE LEFT(os.opportunity__c, 15) = '0066900001MSgpz';



SELECT ssa.salesforce_opportunity_id,
       ssa.base_currency              AS cms_sale_currency,
       COUNT(DISTINCT ssa.se_sale_id) AS territory_sales
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_type = 'Hotel'
  AND ssa.salesforce_opportunity_id = '0066900001MSgpz'--TODO REMOVE
GROUP BY 1, 2;


SELECT hs.code AS account_id,
       hs.currency
FROM data_vault_mvp.mari_snapshots.hotel_snapshot hs
WHERE hs.code = '001w000001TOkXy' --sf account id left 15
;

SELECT hs.code AS account_id,
       hs.currency
FROM data_vault_mvp.mari_snapshots.hotel_snapshot hs
WHERE hs.code = '001w000001TOkXy' --sf account id left 15
;

SELECT rps.room_type_id                                   AS mari_room_type_id,
       rps.name                                           AS mari_name,
       rps.code                                           AS mari_code,
       rps.rack_code                                      AS mari_rack_code,
       rps.currency                                       AS mari_rate_plan_currency,
       hs.code                                            AS hotel_code,
       hs.code || ':' || rps.code || ':' || rps.rack_code AS mari_hotel_rate_rack_code
FROM data_vault_mvp.mari_snapshots.rate_plan_snapshot rps
    INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rps.room_type_id = rts.id
    INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
WHERE hs.code = '001w000001TOkXy';

--currency opp + offer + cms + mari hotel
--currency opp + cms + mari hotel + mari rate plan

CREATE OR REPLACE VIEW collab.quality_assurance.currency_sf_qa COPY GRANTS AS
(
WITH agg_to_global AS (
    SELECT ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.sale_active,
           ssa.base_currency              AS cms_sale_currency,
           COUNT(DISTINCT ssa.se_sale_id) AS territory_sales
    FROM se.data.se_sale_attributes ssa
    WHERE ssa.sale_type = 'Hotel'
    GROUP BY 1, 2, 3, 4
)

SELECT atg.salesforce_opportunity_id,
       atg.company_name         AS cms_company_name,
       a.name                   AS sf_account_name,
       atg.sale_active,
       atg.territory_sales,
       atg.cms_sale_currency,
       o.proposed_start_date__c AS sf_proposed_start_date,
       o.currencyisocode        AS sf_opportunity_currency,
       a.currencyisocode        AS sf_account_currency,
       a2.name                  AS sf_third_party_account_name,
       a2.currencyisocode       AS sf_third_party_account_currency,
       os.name                  AS sf_offer_name,
       os.currencyisocode       AS sf_offer_currency,
       COALESCE(atg.cms_sale_currency = sf_opportunity_currency
                    AND sf_opportunity_currency = sf_account_currency
                    AND sf_account_currency = sf_offer_currency
                    AND sf_account_currency = sf_third_party_account_currency
           , FALSE)             AS all_columns_match
FROM agg_to_global atg
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON atg.salesforce_opportunity_id = LEFT(id, 15)
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a2 ON o.third_party_provider__c = a2.id
    LEFT JOIN data_vault_mvp.sfsc_snapshots.offers_snapshot os ON o.id = os.opportunity__c
    );
GRANT SELECT ON TABLE collab.quality_assurance.currency_sf_qa TO ROLE personal_role__enricosanson;

SELECT *
FROM collab.quality_assurance.currency_sf_qa
WHERE currency_sf_qa.salesforce_opportunity_id = '0066900001MTPl5'


CREATE OR REPLACE VIEW collab.quality_assurance.currency_mari_qa COPY GRANTS AS
(
WITH agg_to_global AS (
    SELECT ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.sale_active,
           ssa.base_currency              AS cms_sale_currency,
           COUNT(DISTINCT ssa.se_sale_id) AS territory_sales
    FROM se.data.se_sale_attributes ssa
    WHERE ssa.sale_type = 'Hotel'
    GROUP BY 1, 2, 3, 4
),
     offer_details AS (
         SELECT hrp.rack_rate_code,
                hrp.rate_code,
                h.hotel_code,
                so.offer_active
         FROM data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrp
             INNER JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON hrp.hotel_product_id = p.id
             INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON p.hotel_id = h.id
             INNER JOIN data_vault_mvp.dwh.se_offer so ON p.id = so.product_id
     )

SELECT atg.salesforce_opportunity_id,
       hs.code                                  AS hotel_code,
       atg.company_name                         AS cms_company_name,
       a.name                                   AS sf_account_name,
       a.currencyisocode                        AS sf_account_currency,
       a2.name                                  AS sf_third_party_account_name,
       a2.currencyisocode                       AS sf_third_party_account_currency,
       atg.sale_active,
       atg.territory_sales,
       atg.cms_sale_currency,
       o.proposed_start_date__c                 AS sf_proposed_start_date,
       hs.currency                              AS mari_hotel_currency,
       rps.name                                 AS mari_rate_plan_name,
       rps.currency                             AS mari_rate_plan_currency,
       IFF(od.offer_active = TRUE, TRUE, FALSE) AS offer_active,
       COALESCE(atg.cms_sale_currency = mari_hotel_currency
                    AND mari_hotel_currency = mari_rate_plan_currency
                    AND mari_rate_plan_currency = sf_third_party_account_currency,
                FALSE)                          AS all_columns_match
FROM agg_to_global atg
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON atg.salesforce_opportunity_id = LEFT(id, 15)
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a2 ON o.third_party_provider__c = a2.id
    LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON LEFT(a.id, 15) = hs.code
    LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON hs.id = rts.hotel_id
    LEFT JOIN data_vault_mvp.mari_snapshots.rate_plan_snapshot rps ON rts.id = rps.room_type_id
    LEFT JOIN offer_details od ON hs.code = od.hotel_code AND rps.code = od.rate_code AND rps.rack_code = od.rack_rate_code
    );

GRANT SELECT ON VIEW collab.quality_assurance.currency_mari_qa TO ROLE personal_role__enricosanson;


SELECT *
FROM collab.quality_assurance.currency_mari_qa
WHERE currency_mari_qa.all_columns_match = FALSE
  AND currency_mari_qa.sale_active;

SELECT *
FROM collab.quality_assurance.currency_mari_qa
WHERE currency_mari_qa.salesforce_opportunity_id = '0066900001MSKjv';
--
-- Hi Robin, how are you?
-- I have tried this query
-- SELECT *
-- FROM collab.quality_assurance.currency_mari_qa
-- WHERE sf_proposed_start_date BETWEEN CURRENT_DATE-7 AND CURRENT_DATE
--   AND all_columns_match = FALSE;
--
-- For the opportunity ID 0066900001MSKjv  "mari rate plan" field is empty.
-- However in MARI the field is actually populated.
-- Do you know why the rate plan is not EUR?

WITH offer_details AS (
    SELECT hrp.rack_rate_code,
           hrp.rate_code,
           h.hotel_code,
           so.offer_active
    FROM data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrp
        INNER JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON hrp.hotel_product_id = p.id
        INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON p.hotel_id = h.id
        INNER JOIN data_vault_mvp.dwh.se_offer so ON p.id = so.product_id
)
SELECT o.id,
       a.id,
       hs.currency,
       rts.*,
       rps.*,
       hs.code || ':' || rts.max_adults || ':' || rts.code || ':' || rps.code || ',' || rps.rack_code,
       od.offer_active
FROM hygiene_snapshot_vault_mvp.sfsc.opportunity o
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
    LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON LEFT(a.id, 15) = hs.code
    LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON hs.id = rts.hotel_id
    LEFT JOIN data_vault_mvp.mari_snapshots.rate_plan_snapshot rps ON rts.id = rps.room_type_id
    LEFT JOIN offer_details od ON hs.code = od.hotel_code AND rps.code = od.rate_code AND rps.rack_code = od.rack_rate_code
WHERE --LEFT(o.id, 15) = '0061r00001HQqcY'
      hs.code = '001w000001SIHT3' -- enrico example of 4 occupancy
;
------------------------------------------------------------------------------------------------------------------------
--investigate bugs: https://secretescapes.atlassian.net/browse/MUSE-967
/* issue 1: offers showing despite being inactive*/
SELECT *
FROM collab.quality_assurance.currency_mari_qa
WHERE salesforce_opportunity_id IN ('0061r00001HRCSo');


SELECT GET_DDL('table', 'collab.quality_assurance.currency_mari_qa');


CREATE OR REPLACE VIEW collab.quality_assurance.currency_mari_qa COPY GRANTS AS
(
WITH agg_to_global AS (
    SELECT ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.sale_active,
           ssa.base_currency              AS cms_sale_currency,
           COUNT(DISTINCT ssa.se_sale_id) AS territory_sales
    FROM se.data.se_sale_attributes ssa
    WHERE ssa.sale_type = 'Hotel'
    GROUP BY 1, 2, 3, 4
),
     offer_details AS (
         SELECT hrp.rack_rate_code,
                hrp.rate_code,
                h.hotel_code,
                so.offer_active,
                so.se_offer_id
         FROM data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrp
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.product p ON hrp.hotel_product_id = p.id
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.hotel h ON p.hotel_id = h.id
             INNER JOIN data_vault_mvp.dwh.se_offer so ON p.id = so.product_id
     )

SELECT atg.salesforce_opportunity_id,
       hs.code                                  AS hotel_code,
       od.se_offer_id,
       atg.company_name                         AS cms_company_name,
       a.name                                   AS sf_account_name,
       a.currencyisocode                        AS sf_account_currency,
       a2.name                                  AS sf_third_party_account_name,
       a2.currencyisocode                       AS sf_third_party_account_currency,
       atg.sale_active,
       atg.territory_sales,
       atg.cms_sale_currency,
       o.proposed_start_date__c                 AS sf_proposed_start_date,
       hs.currency                              AS mari_hotel_currency,
       rps.name                                 AS mari_rate_plan_name,
       rps.currency                             AS mari_rate_plan_currency,
       IFF(od.offer_active = TRUE, TRUE, FALSE) AS offer_active,
       COALESCE(atg.cms_sale_currency = mari_hotel_currency
                    AND mari_hotel_currency = mari_rate_plan_currency
                    AND mari_rate_plan_currency = sf_third_party_account_currency,
                FALSE)                          AS all_columns_match
FROM agg_to_global atg
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON atg.salesforce_opportunity_id = LEFT(id, 15)
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a2 ON o.third_party_provider__c = a2.id
    LEFT JOIN hygiene_snapshot_vault_mvp.mari.hotel hs ON LEFT(a.id, 15) = hs.code
    LEFT JOIN hygiene_snapshot_vault_mvp.mari.room_type rts ON hs.id = rts.hotel_id
    LEFT JOIN hygiene_snapshot_vault_mvp.mari.rate_plan rps ON rts.id = rps.room_type_id
    LEFT JOIN offer_details od ON hs.code = od.hotel_code AND rps.code = od.rate_code AND rps.rack_code = od.rack_rate_code
    );


--lift code from view:
WITH agg_to_global AS (
    SELECT ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.sale_active,
           ssa.base_currency              AS cms_sale_currency,
           LISTAGG(ssa.se_sale_id, ', ')  AS sale_ids,
           COUNT(DISTINCT ssa.se_sale_id) AS territory_sales
    FROM se.data.se_sale_attributes ssa
    WHERE ssa.sale_type = 'Hotel'
    GROUP BY 1, 2, 3, 4
),
     offer_details AS (
         SELECT hrp.rack_rate_code,
                hrp.rate_code,
                h.hotel_code,
                so.offer_active,
                so.se_offer_id
         FROM data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrp
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.product p ON hrp.hotel_product_id = p.id
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.hotel h ON p.hotel_id = h.id
             INNER JOIN data_vault_mvp.dwh.se_offer so ON p.id = so.product_id
     ),
     create_view AS (

         SELECT atg.salesforce_opportunity_id,
                atg.sale_ids,
                hs.code                                  AS hotel_code,
                atg.company_name                         AS cms_company_name,
                a.name                                   AS sf_account_name,
                a.currencyisocode                        AS sf_account_currency,
                a2.name                                  AS sf_third_party_account_name,
                a2.currencyisocode                       AS sf_third_party_account_currency,
                atg.sale_active,
                atg.territory_sales,
                atg.cms_sale_currency,
                o.proposed_start_date__c                 AS sf_proposed_start_date,
                hs.currency                              AS mari_hotel_currency,
                rps.name                                 AS mari_rate_plan_name,
                rps.currency                             AS mari_rate_plan_currency,
                IFF(od.offer_active = TRUE, TRUE, FALSE) AS offer_active,
                COALESCE(atg.cms_sale_currency = mari_hotel_currency
                             AND mari_hotel_currency = mari_rate_plan_currency
                             AND mari_rate_plan_currency = sf_third_party_account_currency,
                         FALSE)                          AS all_columns_match,
                od.se_offer_id
         FROM agg_to_global atg
             LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON atg.salesforce_opportunity_id = LEFT(id, 15)
             LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
             LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a2 ON o.third_party_provider__c = a2.id
             LEFT JOIN hygiene_snapshot_vault_mvp.mari.hotel hs ON LEFT(a.id, 15) = hs.code
             LEFT JOIN hygiene_snapshot_vault_mvp.mari.room_type rts ON hs.id = rts.hotel_id
             LEFT JOIN hygiene_snapshot_vault_mvp.mari.rate_plan rps ON rts.id = rps.room_type_id
             LEFT JOIN offer_details od ON hs.code = od.hotel_code AND rps.code = od.rate_code AND rps.rack_code = od.rack_rate_code
     )
SELECT *
FROM create_view cv
WHERE cv.salesforce_opportunity_id = '0061r00001HRCSo';


SELECT *
FROM collab.quality_assurance.currency_mari_qa
WHERE salesforce_opportunity_id IN ('0061r00001HRCSo');

-- the offer appears to be marked as active in cms
-- https://cms.secretescapes.com/hotelSale/edit/0061r00001HRCSo

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM collab.quality_assurance.currency_mari_qa
WHERE salesforce_opportunity_id IN ('0066900001I2RjO'

    python biapp / bau / hygiene / gen_hygiene_files.py \
--data_source cms_mysql \
--name hotel_rate_plan \
--primary_key_cols id \
--new_record_col updated_at_dts \
--detect_deleted_records)

SELECT COUNT(*)
FROM data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot;

SELECT
       'A' || bo.id            AS se_offer_id, --replication at this point, for when we harmonise odm and ndm
       bo.id                   AS base_offer_id,
--        bot.offer_name,
--        bot.offer_name_object,
       bo.active = 1           AS active_flag,
       COUNT(DISTINCT hso.sale_id) AS count_connected_sales,
       active_flag AND count_connected_sales >= 1 AS offer_active

FROM hygiene_snapshot_vault_mvp.cms_mysql.base_offer bo
--     LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.base_offer_translation bot ON bo.id = bot.offer_id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.hotel_sale_offer hso ON bo.id = hso.offer_id --row must exist in here if connected
GROUP BY 1, 2, 3
ORDER BY count_connected_sales;


self_describing_task --include 'dv/dwh/transactional/se_offer.py'  --method 'run' --start '2021-09-23 00:00:00' --end '2021-09-23 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_offer_translation CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_offer_translation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel_sale_offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel_sale_offer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.cms_mari_link CLONE data_vault_mvp.dwh.cms_mari_link;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;

SELECT * FROM data_vault_mvp_dev_robin.dwh.se_offer;
SELECT COUNT(*) FROM data_vault_mvp_dev_robin.dwh.se_offer;
SELECT COUNT(*) FROM data_vault_mvp.dwh.se_offer;

SELECT
       prod.se_offer_id,
       prod.base_offer_id,
--        prod.offer_name,
       prod.offer_name_object,
--        prod.offer_active,
       prod.hotel_rate_plan_id,
       prod.product_id,
       prod.hotel_code,
       prod.rate_code,
       prod.rack_rate_code,
       prod.connected_sales,
       prod.connected_active_sales,
       prod.global_sales,
       prod.active_global_sales
FROM data_vault_mvp.dwh.se_offer prod
    EXCEPT
SELECT
       dev.se_offer_id,
       dev.base_offer_id,
--        dev.offer_name,
       dev.offer_name_object,
--        dev.cms_active_flag,
--        dev.count_connected_sales,
--        dev.offer_active,
       dev.hotel_rate_plan_id,
       dev.product_id,
       dev.hotel_code,
       dev.rate_code,
       dev.rack_rate_code,
       dev.connected_sales,
       dev.connected_active_sales,
       dev.global_sales,
       dev.active_global_sales
FROM data_vault_mvp_dev_robin.dwh.se_offer dev;