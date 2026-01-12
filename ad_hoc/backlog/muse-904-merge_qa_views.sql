SELECT GET_DDL('table', 'COLLAB.QUALITY_ASSURANCE.CURRENCY_SF_QA');


-- CREATE OR REPLACE VIEW CURRENCY_SF_QA COPY GRANTS AS
-- (
-- WITH agg_to_global AS (
--     SELECT ssa.salesforce_opportunity_id,
--            ssa.company_name,
--            ssa.sale_active,
--            ssa.base_currency              AS cms_sale_currency,
--            COUNT(DISTINCT ssa.se_sale_id) AS territory_sales
--     FROM se.data.se_sale_attributes ssa
--     WHERE ssa.sale_type = 'Hotel'
--     GROUP BY 1, 2, 3, 4
-- )
--
-- SELECT atg.salesforce_opportunity_id,
--        atg.company_name         AS cms_company_name,
--        a.name                   AS sf_account_name,
--        atg.sale_active,
--        atg.territory_sales,
--        atg.cms_sale_currency,
--        o.proposed_start_date__c AS sf_proposed_start_date,
--        o.currencyisocode        AS sf_opportunity_currency,
--        a.currencyisocode        AS sf_account_currency,
--        a2.name                  AS sf_third_party_account_name,
--        a2.currencyisocode       AS sf_third_party_account_currency,
--        os.name                  AS sf_offer_name,
--        os.currencyisocode       AS sf_offer_currency,
--        COALESCE(atg.cms_sale_currency = sf_opportunity_currency
--                     AND sf_opportunity_currency = sf_account_currency
--                     AND sf_account_currency = sf_offer_currency
--                     AND sf_account_currency = sf_third_party_account_currency
--            , FALSE)             AS all_columns_match
-- FROM agg_to_global atg
--     LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON atg.salesforce_opportunity_id = LEFT(id, 15)
--     LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
--     LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a2 ON o.third_party_provider__c = a2.id
--     LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.offers os ON o.id = os.opportunity__c
--     );

SELECT GET_DDL('table', 'COLLAB.QUALITY_ASSURANCE.CURRENCY_MARI_QA');


-- CREATE OR REPLACE VIEW CURRENCY_MARI_QA COPY GRANTS AS
-- (
-- WITH agg_to_global AS (
--     SELECT ssa.salesforce_opportunity_id,
--            ssa.company_name,
--            ssa.sale_active,
--            ssa.base_currency                      AS cms_sale_currency,
--            COUNT(DISTINCT ssa.se_sale_id)         AS territory_sales,
--            LISTAGG(DISTINCT ssa.se_sale_id, ', ') AS list_sale_ids
--     FROM se.data.se_sale_attributes ssa
--     WHERE ssa.sale_type = 'Hotel'
--     GROUP BY 1, 2, 3, 4
-- ),
--      offer_details AS (
--          SELECT hrp.rack_rate_code,
--                 hrp.rate_code,
--                 h.hotel_code,
--                 so.offer_active,
--                 so.se_offer_id
--          FROM data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrp
--              INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.product p ON hrp.hotel_product_id = p.id
--              INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.hotel h ON p.hotel_id = h.id
--              INNER JOIN data_vault_mvp.dwh.se_offer so ON p.id = so.product_id
--      )
--
-- SELECT atg.salesforce_opportunity_id,
--        atg.list_sale_ids,
--        hs.code                                  AS hotel_code,
--        od.se_offer_id,
--        atg.company_name                         AS cms_company_name,
--        a.name                                   AS sf_account_name,
--        a.currencyisocode                        AS sf_account_currency,
--        a2.name                                  AS sf_third_party_account_name,
--        a2.currencyisocode                       AS sf_third_party_account_currency,
--        atg.sale_active,
--        atg.territory_sales,
--        atg.cms_sale_currency,
--        o.proposed_start_date__c                 AS sf_proposed_start_date,
--        hs.currency                              AS mari_hotel_currency,
--        rps.name                                 AS mari_rate_plan_name,
--        rps.currency                             AS mari_rate_plan_currency,
--        IFF(od.offer_active = TRUE, TRUE, FALSE) AS offer_active,
--        COALESCE(atg.cms_sale_currency = mari_hotel_currency
--                     AND mari_hotel_currency = mari_rate_plan_currency
--                     AND mari_rate_plan_currency = sf_third_party_account_currency,
--                 FALSE)                          AS all_columns_match
-- FROM agg_to_global atg
--     LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON atg.salesforce_opportunity_id = LEFT(id, 15)
--     LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
--     LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a2 ON o.third_party_provider__c = a2.id
--     LEFT JOIN latest_vault.mari.hotel hs ON LEFT(a.id, 15) = hs.code
--     LEFT JOIN latest_vault.mari.room_type rts ON hs.id = rts.hotel_id
--     LEFT JOIN latest_vault.mari.rate_plan rps ON rts.id = rps.room_type_id
--     LEFT JOIN offer_details od ON hs.code = od.hotel_code AND rps.code = od.rate_code AND rps.rack_code = od.rack_rate_code
--     );


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
       os.offer_id__c           AS se_offer_id,
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
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.offers os ON o.id = os.opportunity__c
    );



CREATE OR REPLACE VIEW collab.quality_assurance.currency_sf_mari_qa COPY GRANTS AS
(
WITH agg_to_global AS (
    SELECT ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.sale_active,
           ssa.hotel_code,
           ssa.base_currency                      AS cms_sale_currency,
           COUNT(DISTINCT ssa.se_sale_id)         AS territory_sales,
           LISTAGG(DISTINCT ssa.se_sale_id, ', ') AS list_sale_ids
    FROM se.data.se_sale_attributes ssa
    WHERE ssa.sale_type = 'Hotel'
    GROUP BY 1, 2, 3, 4, 5
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
     salesforce_data AS (
         SELECT LEFT(o.id, 15)           AS salesforce_opportunity_id,
                o.id                     AS salesforce_opportunity_id_full,
                a.name                   AS sf_account_name,
                'A' || os.offer_id__c    AS se_offer_id,
                o.proposed_start_date__c AS sf_proposed_start_date,
                o.currencyisocode        AS sf_opportunity_currency,
                a.currencyisocode        AS sf_account_currency,
                a2.name                  AS sf_third_party_account_name,
                a2.currencyisocode       AS sf_third_party_account_currency,
                os.name                  AS sf_offer_name,
                os.currencyisocode       AS sf_offer_currency
         FROM hygiene_snapshot_vault_mvp.sfsc.opportunity o
             LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
             LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a2 ON o.third_party_provider__c = a2.id
             LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.offers os ON o.id = os.opportunity__c
     )

SELECT atg.salesforce_opportunity_id,
       atg.list_sale_ids,
       od.se_offer_id,
       atg.company_name                         AS cms_company_name,
       atg.sale_active,
       atg.territory_sales,
       atg.cms_sale_currency,
       hs.code                                  AS hotel_code,
       hs.currency                              AS mari_hotel_currency,
       rps.name                                 AS mari_rate_plan_name,
       rps.currency                             AS mari_rate_plan_currency,
       IFF(od.offer_active = TRUE, TRUE, FALSE) AS offer_active,
       sd.sf_account_name,
       sd.sf_account_currency,
       sd.sf_third_party_account_name,
       sd.sf_third_party_account_currency,
       sd.sf_proposed_start_date,
       sd.sf_opportunity_currency,
       sd.sf_offer_name,
       sd.sf_offer_currency,
       COALESCE(atg.cms_sale_currency = mari_hotel_currency
                    AND atg.cms_sale_currency = mari_rate_plan_currency
                    AND atg.cms_sale_currency = sd.sf_opportunity_currency
                    AND atg.cms_sale_currency = sd.sf_account_currency
                    AND atg.cms_sale_currency = sd.sf_offer_currency,
                FALSE)                          AS all_columns_match
FROM agg_to_global atg
    LEFT JOIN latest_vault.mari.hotel hs ON atg.hotel_code = hs.code
    LEFT JOIN latest_vault.mari.room_type rts ON hs.id = rts.hotel_id
    LEFT JOIN latest_vault.mari.rate_plan rps ON rts.id = rps.room_type_id
    LEFT JOIN offer_details od ON hs.code = od.hotel_code AND rps.code = od.rate_code AND rps.rack_code = od.rack_rate_code
    LEFT JOIN salesforce_data sd ON atg.salesforce_opportunity_id = sd.salesforce_opportunity_id AND od.se_offer_id = sd.se_offer_id
    );

GRANT SELECT ON TABLE collab.quality_assurance.currency_sf_mari_qa TO ROLE personal_role__enricosanson;
