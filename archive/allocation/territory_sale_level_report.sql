WITH bookings AS (
    --bookings by sale id
    SELECT f.sale_id                       AS se_sale_id,
           count(*)                        AS trx,
           sum(f.margin_gross_of_toms_gbp) AS margin
    FROM se.data.fact_complete_booking f
    GROUP BY 1
),
     spvs AS (
         --spvs by sale id
         SELECT sp.se_sale_id,
                count(*) AS spvs
         FROM se.data.scv_touched_spvs sp
         GROUP BY 1
     )
SELECT ss.se_sale_id                AS territory_sale_id,
       ss.salesforce_opportunity_id AS global_sale_id,
       ss.sale_name,
       ss.sale_name_object,
       ss.sale_active,
       ss.posa_territory,
       ss.hotel_code,
       hs.name                      AS hotel_name,
       COALESCE(b.trx, 0)           AS trx,
       COALESCE(b.margin, 0)        AS margin,
       COALESCE(s.spvs, 0)          AS spvs
FROM se.data.se_sale_attributes ss
         LEFT JOIN bookings b ON ss.se_sale_id = b.se_sale_id
         LEFT JOIN spvs s ON ss.se_sale_id = s.se_sale_id
         LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON ss.hotel_code = hs.code
WHERE ss.sale_active
ORDER BY ss.salesforce_opportunity_id;
