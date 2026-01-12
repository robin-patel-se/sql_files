SELECT ssa.product_configuration,
       ssa.product_type,
       COUNT(*)
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
GROUP BY GROUPING SETS (ssa.product_configuration, ssa.product_type)