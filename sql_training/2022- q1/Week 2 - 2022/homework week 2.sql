SELECT ssa.posu_region,
       COUNT(*)
FROM se.data.se_sale_attributes ssa
WHERE sale_active = TRUE
GROUP BY 1;


SELECT ssa.posu_country,
       AVG(ssa.nps_score)
FROM se.data.se_sale_attributes ssa
WHERE ssa.posu_region = 'Asia POSu'
GROUP BY 1;