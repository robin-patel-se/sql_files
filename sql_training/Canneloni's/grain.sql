SELECT *
FROM se.data.se_booking sb;


SELECT *
FROM (
         SELECT sb.sale_id,
                SUM(sb.margin_gross_of_toms_gbp) AS margin
         FROM se.data.se_booking sb
         GROUP BY 1
     )
WHERE margin > 1000
;

SELECT ssa.posu_country
FROM se.data.se_sale_attributes ssa