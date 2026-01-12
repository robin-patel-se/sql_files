SELECT GET_DDL('table', 'collab.quality_assurance.cancellation_policy_qa')
;

-- CREATE OR REPLACE VIEW collab.quality_assurance.cancellation_policy_qa COPY GRANTS
-- AS
-- (
-- WITH
-- 	agg_to_global AS (
-- 		SELECT DISTINCT
-- 			ssa.salesforce_opportunity_id,
-- 			ssa.company_name,
-- 			ssa.sale_active,
-- 			ssa.cancellation_policy_id IS NOT NULL AS cancellable,
-- 			ssa.cancellation_policy_number_of_days,
-- 			ssa.salesforce_proposed_start_date     AS sf_proposed_start_date
-- 		FROM se.data.se_sale_attributes ssa
-- 		WHERE ssa.sale_type IN ('Hotel', 'Hotel Plus')
-- 	)
-- SELECT
-- 	o.id                                   AS salesforce_opportunity_id,
-- 	atg.company_name,
-- 	atg.sale_active,
-- 	atg.sf_proposed_start_date,
-- 	o.cancellation_terms__c                AS sf_cancellation_terms,
-- 	atg.cancellable                        AS cms_cancellable,
-- 	atg.cancellation_policy_number_of_days AS cms_cancellation_policy_number_of_days
-- FROM agg_to_global atg
-- 	LEFT JOIN /*latest_vault.sfsc.opportunity*/ data_vault_mvp.dwh.sfsc__opportunity o
-- 			  ON atg.salesforce_opportunity_id = LEFT(o.id, 15)
-- 	)
-- ;


CREATE OR REPLACE VIEW collab.quality_assurance.cancellation_policy_qa COPY GRANTS
AS
(
WITH
	agg_to_global AS (
		SELECT DISTINCT
			ssa.salesforce_opportunity_id,
			ssa.company_name,
			ssa.sale_active,
			ssa.cancellation_policy_id IS NOT NULL AS cancellable,
			ssa.cancellation_policy_number_of_days,
			ssa.salesforce_proposed_start_date     AS sf_proposed_start_date
		FROM se.data.se_sale_attributes ssa
		WHERE ssa.sale_type IN ('Hotel', 'Hotel Plus')
	)
SELECT
	o.id                                   AS salesforce_opportunity_id,
	atg.company_name,
	atg.sale_active,
	atg.sf_proposed_start_date,
	o.cancellation_terms__c                AS sf_cancellation_terms,
	atg.cancellable                        AS cms_cancellable,
	atg.cancellation_policy_number_of_days AS cms_cancellation_policy_number_of_days
FROM agg_to_global atg
	LEFT JOIN /*latest_vault.sfsc.opportunity*/ data_vault_mvp.dwh.sfsc__opportunity o
			  ON atg.salesforce_opportunity_id = LEFT(o.id, 15)
	)
;


SELECT *
FROM se.data.se_offer_attributes soa
WHERE soa.cancellation_type IS NOT NULL
;

USE ROLE pipelinerunner;

CREATE OR REPLACE VIEW collab.quality_assurance.cancellation_policy_qa COPY GRANTS
AS
(
SELECT
	ssa.salesforce_opportunity_id,
	ssa.company_name,
	ssa.se_sale_id,
	ssa.sale_active,
	ssa.salesforce_proposed_start_date AS sf_proposed_start_date,
	'A' || hso.offer_id                AS se_offer_id,
	soa.cancellation_type              AS offer_cancellation_type,
	soa.cancellation_policy_option     AS offer_cancellation_policy_option,
	o.cancellation_terms__c            AS sf_cancellation_terms
FROM se.data.se_sale_attributes ssa
	INNER JOIN latest_vault.cms_mysql.hotel_sale_offer hso ON hso.sale_id = ssa.se_sale_id
	INNER JOIN se.data.se_offer_attributes soa ON 'A' || hso.offer_id = soa.se_offer_id
	LEFT JOIN  data_vault_mvp.dwh.sfsc__opportunity o ON ssa.salesforce_opportunity_id = LEFT(o.id, 15)
WHERE ssa.sale_type IN ('Hotel', 'Hotel Plus')
	);


