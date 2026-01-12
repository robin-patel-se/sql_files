WITH distinct_continent_to_country AS (
    SELECT DISTINCT
           s.country_id,
           s.continent_id
    FROM se.data.se_location_info s
)

   , booking_info AS (
    SELECT fcb.check_in_date,
           CASE
               WHEN fcb.check_in_date <= CURRENT_DATE + 7 THEN '1. Up to 7 Days'
               WHEN fcb.check_in_date <= CURRENT_DATE + 14 THEN '2. 8 to 14 Days'
               WHEN fcb.check_in_date <= CURRENT_DATE + 30 THEN '3. 15 to 30 Days'
               WHEN fcb.check_in_date <= CURRENT_DATE + 90 THEN '4. 31 to 90 Days'
               ELSE '5. Up to 180 days'
               END AS check_in_group,
           CASE
               WHEN se.data.posa_category_from_territory(fcb.territory) = 'UK' THEN 'UK'
               WHEN se.data.posa_category_from_territory(fcb.territory) = 'USA' THEN 'US'
               WHEN se.data.posa_category_from_territory(fcb.territory) IN ('Asia', 'Other') THEN 'ROW'
               ELSE 'EU'
               END AS from_group,
           CASE
               WHEN ds.posu_country IN (
                                        'Wales/Cymru',
                                        'Northern Ireland',
                                        'England',
                                        'Scotland'
                   ) THEN 'UK'
               WHEN ds.posu_country = 'USA' THEN 'US'
               WHEN sli.continent_id = 2 THEN 'EU' --europe
               ELSE 'ROW'
               END AS to_group,
           sli.continent_id,
           fcb.territory,
           ds.posu_country,
           fcb.booking_id,
           fcb.gross_revenue_gbp,
           fcb.adult_guests
    FROM se.data.fact_complete_booking fcb
        INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
        LEFT JOIN  se.data.se_country sc ON ds.posu_country = sc.name
        LEFT JOIN  distinct_continent_to_country sli ON sc.id = sli.country_id
    WHERE fcb.check_in_date <= CURRENT_DATE + 180 -- forward booking within 180 days
      AND fcb.check_in_date >= CURRENT_DATE -- forward bookings
)
SELECT bi.check_in_group,
       bi.from_group,
       bi.to_group,
       COUNT(DISTINCT bi.booking_id) AS bookings,
       SUM(bi.gross_revenue_gbp)     AS gross_revenue_gbp,
       SUM(bi.adult_guests)          AS pax
FROM booking_info bi
GROUP BY 1, 2, 3
;
USE WAREHOUSE pipe_xlarge;



SELECT DISTINCT se.data.posa_category_from_territory(fcb.territory)
FROM se.data.fact_complete_booking fcb;


SELECT DISTINCT posu_country
FROM se.data.dim_sale ds
WHERE ds.posu_cluster_region = 'UK';

SELECT *
FROM se.data.se_location_info sli

-- Belgium
-- DACH
-- France
-- Italy
-- Netherlands
-- Poland
-- Scandi
-- Spain
-- CEE

-- Asia
-- Other
-- UK
-- USA

---test 1
SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active;

--test2
SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss
WHERE ss.sale_active;