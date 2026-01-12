SELECT fcb.se_sale_id,
       COUNT(*)
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date >= '2021-01-01'
  AND fcb.tech_platform = 'SECRET_ESCAPES'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;


SELECT fcb.booking_id,
       fcb.sale_id,
       fcb.booking_completed_date,
       fcb.margin_gross_of_toms_gbp_constant_currency
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date >= '2021-01-01'
  AND fcb.se_sale_id IN (
                         'A21822',
                         'A21854',
                         'A21272',
                         'A21274',
                         'A16155',
                         'A16642',
                         'A17104',
                         'A12308',
                         'A21693',
                         'A20246'
    )
LIMIT 100;



SELECT ssa.se_sale_id,
       ssa.sale_name,
       ssa.company_name,
       ssa.sale_active,
       ssa.start_date,
       ssa.end_date,
       ssa.posa_territory,
       ssa.posu_country,
       ssa.posu_city
FROM se.data.se_sale_attributes ssa
WHERE ssa.se_sale_id IN ('A21822',
                         'A21854',
                         'A21272',
                         'A21274',
                         'A16155',
                         'A16642',
                         'A17104',
                         'A12308',
                         'A21693',
                         'A20246'
    );

SELECT ssa.se_sale_id,
       ssa.sale_name,
       ssa.company_name,
       ssa.sale_active,
       ssa.start_date,
       ssa.end_date,
       ssa.posa_territory,
       ssa.posu_country,
       ssa.posu_city
FROM se.data.se_sale_attributes ssa
WHERE ssa.se_sale_id NOT IN ('A21822',
                         'A21854',
                         'A21272',
                         'A21274',
                         'A16155',
                         'A16642',
                         'A17104',
                         'A12308',
                         'A21693',
                         'A20246'
    );


SELECT * FROM se.data_pii.se_credit sc


