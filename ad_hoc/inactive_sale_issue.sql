--global sale level
--active but end date is in the past
--global sale id, all the sale id's in that global sale id are inactive and the end date is in the future

WITH margin AS (
    SELECT s.salesforce_opportunity_id,
           SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
    FROM se.data.fact_complete_booking fcb
             LEFT JOIN se.data.se_sale_attributes s ON fcb.sale_id = s.se_sale_id
    GROUP BY 1
)
SELECT ssa.salesforce_opportunity_id AS global_sale_id,
       ssa.data_model,
       ssa.product_configuration,
       ssa.posu_cluster,
       ssa.posu_sub_region,
       ssa.posu_region,
       ssa.company_name,
       m.margin,
       'active, end date in past'    AS flag,
       MAX(ssa.end_date)             AS max_end_date,
       MAX(ssa.active) = 0           AS no_active_sales
FROM se.data.se_sale_attributes ssa
         LEFT JOIN margin m ON ssa.salesforce_opportunity_id = m.salesforce_opportunity_id
WHERE ssa.active
  AND ssa.end_date < current_date
  AND (ssa.product_configuration = 'Hotel' AND ssa.data_model = 'New Data Model')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

UNION ALL

SELECT ssa.salesforce_opportunity_id        AS global_sale_id,
       ssa.data_model,
       ssa.product_configuration,
       ssa.posu_cluster,
       ssa.posu_sub_region,
       ssa.posu_region,
       ssa.company_name,
       m.margin,
       'not active, end date in the future' AS flag,
       MAX(ssa.end_date)                    AS max_end_date,
       MAX(ssa.active) = 0                  AS no_active_sales
FROM se.data.se_sale_attributes ssa
         LEFT JOIN margin m ON ssa.salesforce_opportunity_id = m.salesforce_opportunity_id
WHERE ssa.end_date > current_date
  AND (ssa.product_configuration = 'Hotel' AND ssa.data_model = 'New Data Model')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
HAVING no_active_sales;


