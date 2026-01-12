--quality assurance test
--detect mistake, create action for a person who's loaded
--cms and salesforce and mari

-- https://docs.google.com/spreadsheets/d/1neE83BCyzKvrvw279Sdn42uYXR8MdrDqGER7G0M0Nkc/edit#gid=1361013191
-- Specifications sheet
 


------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW collab.quality_assurance.cancellation_policy_qa COPY GRANTS AS
(
WITH agg_to_global AS (
    SELECT DISTINCT
           ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.sale_active,
           ssa.cancellation_policy_id IS NOT NULL AS cancellable,
           ssa.cancellation_policy_number_of_days,
           ssa.salesforce_proposed_start_date     AS sf_proposed_start_date
    FROM se.data.se_sale_attributes ssa
    WHERE ssa.sale_type = 'Hotel'
)
SELECT o.id                                   AS salesforce_opportunity_id,
       atg.company_name,
       atg.sale_active,
       atg.sf_proposed_start_date,
       o.cancellation_terms__c                AS sf_cancellation_terms,
       atg.cancellable                        AS cms_cancellable,
       atg.cancellation_policy_number_of_days AS cms_cancellation_policy_number_of_days
FROM agg_to_global atg
         LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON atg.salesforce_opportunity_id = LEFT(o.id, 15)
    );

GRANT SELECT ON VIEW collab.quality_assurance.cancellation_policy_qa TO ROLE personal_role__enricosanson;

SELECT *
FROM collab.quality_assurance.cancellation_policy_qa
WHERE cancellation_policy_qa.cms_cancellable
  AND cancellation_policy_qa.sale_active;


SELECT ssa.salesforce_opportunity_id,
       ssa.sale_active,
       ssa.salesforce_proposed_start_date,
       ssa.sale_name,
       gsa.account_name,
       gsa.owner                   AS sf_cm,
       ssa.current_contractor_name AS cms_cm
FROM se.data.se_sale_attributes AS ssa
         LEFT JOIN se.data.global_sale_attributes AS gsa ON ssa.salesforce_opportunity_id = gsa.opportunity_id
WHERE ssa.salesforce_proposed_start_date BETWEEN '2020-04-01' AND CURRENT_DATE
  AND ssa.posa_country IN ('United Kingdom')
  AND product_type = 'Hotel';