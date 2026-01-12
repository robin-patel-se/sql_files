------------------------------------------------------------------------------------------------------------------------
--VAT

SELECT h.hotel_code, h.vat_exclusive
FROM data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h;

SELECT ssa.salesforce_opportunity_id,
       ssa.company_name,
       ssa.posu_country,
       ssa.hotel_code,
       hs.hotel_code,
       IFF(hs.vat_exclusive = 1, 'YES', 'NO') AS apply_vat
FROM se.data.se_sale_attributes ssa
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot hs ON ssa.hotel_code = hs.hotel_code
WHERE ssa.salesforce_opportunity_id = '0066900001MSK8M'

CREATE OR REPLACE VIEW collab.quality_assurance.vat_qa COPY GRANTS AS
(
WITH agg_to_global AS (
    SELECT DISTINCT
           ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.posu_country                   AS cms_posu_country,
           o.country__c                       AS sf_posu_country,
           ssa.sale_active,
           ssa.hotel_code,
           ssa.start_date::DATE               AS sale_start_date,
           ssa.salesforce_proposed_start_date AS sf_proposed_start_date
    FROM se.data.se_sale_attributes ssa
             LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON ssa.salesforce_opportunity_id = LEFT(o.id, 15)
    WHERE ssa.sale_type = 'Hotel'
)
SELECT atg.salesforce_opportunity_id,
       atg.company_name,
       atg.cms_posu_country,
       atg.sf_posu_country,
       atg.sale_active,
       atg.hotel_code,
       atg.sale_start_date,
       atg.sf_proposed_start_date,
       IFF(hs.vat_exclusive = 1, 'YES', 'NO') AS apply_vat,
       CASE
           WHEN atg.sf_posu_country = 'UNITED KINGDOM' AND hs.vat_exclusive = 1
               THEN TRUE
           WHEN atg.sf_posu_country != 'UNITED KINGDOM' AND hs.vat_exclusive = 0
               THEN TRUE
           ELSE FALSE
           END                                AS has_vat_applied_correctly
FROM agg_to_global atg
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot hs ON atg.hotel_code = hs.hotel_code
    );

GRANT SELECT ON VIEW collab.quality_assurance.vat_qa TO ROLE personal_role__enricosanson;

SELECT *
FROM collab.quality_assurance.vat_qa vq
WHERE vq.has_vat_applied_correctly = FALSE
  AND vq.sale_active;

SELECT *
FROM hygiene_snapshot_vault_mvp.sfsc.opportunity o
WHERE LEFT(id, 15) IN ('0061r00001FGB36',
                       '0061r00001In7F1',
                       '006APPLITOOLS03',
                       '006notacompa977',
                       '006notarealsal1');
