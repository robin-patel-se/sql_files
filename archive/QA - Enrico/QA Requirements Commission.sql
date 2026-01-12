------------------------------------------------------------------------------------------------------------------------
--commission
SELECT *
FROM hygiene_snapshot_vault_mvp.sfsc.opportunity o
WHERE id = '0066900001I1NBRAA3';


CREATE SCHEMA collab.quality_assurance;
GRANT USAGE ON SCHEMA collab.quality_assurance TO ROLE personal_role__enricosanson;


CREATE OR REPLACE VIEW collab.quality_assurance.commission_qa COPY GRANTS AS
(
WITH agg_to_global AS (
    SELECT ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.hotel_id,
           ssa.commission                 AS cms_sale_commission,
           ssa.commission_type            AS cms_sale_commission_type,
           ssa.sale_active,
           COUNT(DISTINCT ssa.se_sale_id) AS territory_sales
    FROM se.data.se_sale_attributes ssa
    WHERE ssa.sale_type = 'Hotel'
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT atg.salesforce_opportunity_id,
--        atg.hotel_id,
       atg.company_name                                                         AS cms_company_name,
       a.name                                                                   AS sf_account_name,
       atg.sale_active,
       atg.territory_sales,
       atg.cms_sale_commission,
       atg.cms_sale_commission_type,
       h.commission                                                             AS cms_hotel_commission,
       h.commission_type                                                        AS cms_hotel_commission_type,
       o.proposed_start_date__c                                                 AS sf_proposed_start_date,
       o.percentage_commission__c / 100                                         AS sf_opportunity_commission,
       os.name                                                                  AS sf_offer_name,
       os.percentage_commission__c / 100                                        AS sf_offer_commission,
       COALESCE(cms_sale_commission = cms_hotel_commission
                    AND cms_hotel_commission = sf_opportunity_commission
                    AND sf_opportunity_commission = sf_offer_commission, FALSE) AS all_collumns_match
FROM agg_to_global atg
         LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.hotel h ON atg.hotel_id = h.id
         LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON atg.salesforce_opportunity_id = LEFT(o.id, 15)
         LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
         LEFT JOIN data_vault_mvp.sfsc_snapshots.offers_snapshot os ON atg.salesforce_opportunity_id = LEFT(os.opportunity__c, 15)
    )
;

GRANT SELECT ON TABLE collab.quality_assurance.commission_qa TO ROLE personal_role__enricosanson;
GRANT USAGE ON SCHEMA collab.quality_assurance TO ROLE personal_role__enricosanson;


SELECT *
FROM collab.quality_assurance.commission_qa
WHERE commission_qa.sf_proposed_start_date BETWEEN '2021-01-18' AND CURRENT_DATE
  AND all_collumns_match = FALSE
;


