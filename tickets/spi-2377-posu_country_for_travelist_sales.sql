SELECT
    ssa.se_sale_id,
    ssa.posa_territory,
    ssa.se_brand,
    *
FROM se.data.se_sale_attributes ssa
WHERE ssa.posa_territory IS NULL;


SELECT *
FROM se.data.tb_offer t
WHERE t.posa_territory IS NULL;

