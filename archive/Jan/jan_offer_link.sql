--Live AOHO deals
WITH sales AS (
    SELECT ssa.salesforce_opportunity_id_full,
           ssa.company_name,
           ssa.se_sale_id,
           ssa.posu_cluster,
           ssa.posu_cluster_region,
           ssa.posu_cluster_sub_region,
           ssa.posa_territory,
           shso.offer_id,
           ssa.base_currency
    FROM se.data.se_sale_attributes ssa
             LEFT JOIN se.data.se_hotel_sale_offer shso ON ssa.se_sale_id = shso.sale_id
    WHERE sale_active
      AND LOWER(product_type) = 'hotel'
--       AND salesforce_opportunity_id_full = '0061r00000tfnPzAAI'
),
--Inclusion value per offer splitted by Core/Supplement/Total
     offers AS (
         SELECT os.id,
                os.name,
                os.opportunity__c,
                os.currencyisocode,
                os.inclusion_summary__c,
                ROUND(os.total_inclusion_value__c - os.supplementary_inclusions_value__c, 0) AS core,
                ROUND(supplementary_inclusions_value__c, 0)                                  AS supplement,
                ROUND(os.total_inclusion_value__c, 0)                                        AS total_value
         FROM data_vault_mvp.sfsc_snapshots.offers_snapshot os
--          WHERE opportunity__c = '0061r00000tfnPzAAI'
     ),
--include lead rate and discount
     rates AS (
         SELECT cms_link.offer_id,
                rr.room_type_id,
                rr.rate_plan_name,
                rr.rate_plan_code_rack_code,
                rr.currency,
                MIN(IFF(rr.rate_rc > 0, ROUND(rr.rate_rc, 0), NULL))           AS lead_rate_rc,
                MIN(IFF(rr.rack_rate_rc > 0, ROUND(rr.rack_rate_rc, 0), NULL)) AS rack_rate_rc,
                ROUND(AVG(rr.discount_precentage), 2)                          AS avg_discount
         FROM se.data.se_room_rates rr
                  INNER JOIN se.data.se_cms_mari_link cms_link
                             ON cms_link.hotel_code = rr.hotel_code
                                 AND cms_link.rate_code = rr.rate_plan_code
                                 AND cms_link.rack_rate_code = rr.rate_plan_rack_code
                  LEFT JOIN se.data.se_offer_attributes so ON so.se_offer_id = 'A' || cms_link.offer_id::varchar
         GROUP BY 1, 2, 3, 4, 5
     )
--JOIN
SELECT s.salesforce_opportunity_id_full AS global_sale_id,
       s.company_name,
       s.se_sale_id,
       s.posu_cluster,
       s.posu_cluster_region,
       s.posu_cluster_sub_region,
       s.posa_territory,
       r.offer_id,
       r.room_type_id,
       r.rate_plan_name,
       o.name                           AS sf_offer_name,
       r.rate_plan_code_rack_code,
       o.inclusion_summary__c,
       r.currency,
       o.core                           AS core_inclusions,
       o.supplement                     AS suppl_inclusions,
       o.total_value                    AS total_inclusions,
       AVG(r.lead_rate_rc)              AS avg_lead,
       AVG(r.rack_rate_rc)              AS avg_bar,
       AVG(r.avg_discount)              AS avg_discount
FROM sales s
         --following left join
         LEFT JOIN rates r ON s.offer_id = r.offer_id
         LEFT JOIN offers o ON s.salesforce_opportunity_id_full = o.opportunity__c
    AND (IFF(LENGTH(LOWER(REGEXP_REPLACE(r.rate_plan_name, '( |\\(|\\))'))) = LENGTH(LOWER(REGEXP_REPLACE(o.name, '( |\\(|\\))')))
        , LOWER(REGEXP_REPLACE(r.rate_plan_name, '( |\\(|\\))')) = LOWER(REGEXP_REPLACE(o.name, '( |\\(|\\))')),
             EDITDISTANCE(LOWER(REGEXP_REPLACE(r.rate_plan_name, '( |\\(|\\))')), LOWER(REGEXP_REPLACE(o.name, '( |\\(|\\))'))) <
             5
        )
                                   )

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
ORDER BY global_sale_id ASC,
         s.company_name ASC,
         s.se_sale_id ASC
;


SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.base_offer_translation_snapshot bots;

SELECT *
FROM data_vault_mvp.mari_snapshots.room_type_snapshot rts;

SET var_a = 'Orania45';
SET var_b = 'Orania45 BB';

SELECT $var_a,
       $var_b,
       EDITDISTANCE($var_a, $var_b),
       LOWER(REGEXP_REPLACE($var_a, ' ')) AS one,
       LOWER(REGEXP_REPLACE($var_b, ' ')) AS two,
       LENGTH(one)                        AS len_one,
       LENGTH(two)                        AS len_two,
       IFF(len_one = len_two THEN)
;

SELECT *
FROM data_vault_mvp.mari_snapshots.rate_plan_snapshot rps;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.base_offer_translation_snapshot bots;

SELECT *
FROM se.data.se_offer so;

SELECT *
FROM data_vault_mvp.mari_snapshots.rate_plan_snapshot rps;

SELECT REGEXP_REPLACE('Junior Suite (BB)', '( |\\(|\\))')
;


SELECT e.collector_tstamp::DATE,
       COUNT(*)
FROM snowplow.atomic.events e
WHERE e.collector_tstamp::DATE >= '2021-01-01'
  AND se_action = 'sign up'
GROUP BY 1;