SELECT ssa.company_name,
       sb.currency                     AS customer_currency,
       sb.sale_base_currency           AS supplier_currency,
       sb.booking_completed_date::DATE AS date,
       sb.cc_rate_to_sc,
       COUNT(*)                        AS bookings
FROM se.data.se_booking sb
         LEFT JOIN se.data.se_sale_attributes ssa ON sb.se_sale_id = ssa.se_sale_id
WHERE sb.booking_completed_date BETWEEN '2020-12-10' AND CURRENT_DATE
  AND sb.booking_status = 'COMPLETE'
  AND ssa.company_name = 'Grand Hyatt Dubai'
GROUP BY 1, 2, 3, 4, 5
;


SELECT ssa.company_name,
       COUNT(*) AS bookings
FROM se.data.se_booking sb
         LEFT JOIN se.data.se_sale_attributes ssa ON sb.se_sale_id = ssa.se_sale_id
WHERE sb.booking_completed_date BETWEEN '2020-12-10' AND CURRENT_DATE
  AND sb.booking_status = 'COMPLETE'
  AND sb.sale_base_currency IS DISTINCT FROM 'GBP'
GROUP BY 1
ORDER BY 2 DESC;