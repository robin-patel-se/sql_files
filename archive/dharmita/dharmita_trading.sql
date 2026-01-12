--dharmita data request 05/01/2020

-- SPV,
-- Transactions,
-- gross revenue,
-- margin,
-- by
-- Territory,
-- posu region
-- product types + sale types
-- by daily date (option for me change dates)

WITH spvs AS (
    SELECT sts.event_tstamp::DATE         AS date,
           stmc.touch_affiliate_territory AS territory,
           ds.posu_region,
           ds.product_type,
           ds.product_configuration,
           COUNT(*)                       AS spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
             INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
    WHERE sts.event_tstamp >= '2021-01-01' --change date filter here
    GROUP BY 1, 2, 3, 4, 5
),
     transactions AS (
         SELECT fcb.booking_completed_date::DATE                    AS date,
                fcb.territory,
                ds.posu_region,
                ds.product_type,
                ds.product_configuration,
                COUNT(*)                                            AS bookings,
                SUM(fcb.gross_revenue_gbp)                          AS gross_revenue_gbp,
                SUM(fcb.margin_gross_of_toms_gbp)                   AS margin_gross_of_toms_gbp,
                SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gross_of_toms_gbp_constant_currency
         FROM se.data.fact_complete_booking fcb
                  INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
         WHERE fcb.booking_completed_date >= '2021-01-01' --change date filter here
         GROUP BY 1, 2, 3, 4, 5
     )
SELECT COALESCE(s.date, t.date)                                                 AS date,
       se.data.posa_category_from_territory(COALESCE(s.territory, t.territory)) AS territory,
       COALESCE(s.posu_region, t.posu_region)                                   AS posu_region,
       COALESCE(s.product_type, t.product_type)                                 AS product_type,
       COALESCE(s.product_configuration, t.product_configuration)               AS product_configuration,
       COALESCE(s.spvs, 0)                                                      AS spvs,
       COALESCE(t.bookings, 0)                                                  AS bookings,
       COALESCE(t.gross_revenue_gbp, 0)                                         AS gross_revenue_gbp,
       COALESCE(t.margin_gross_of_toms_gbp, 0)                                  AS margin_gross_of_toms_gbp,
       COALESCE(t.margin_gross_of_toms_gbp_constant_currency, 0)                AS margin_gross_of_toms_gbp_constant_currency
FROM spvs s
         FULL OUTER JOIN transactions t ON
        s.date = t.date AND
        s.territory = t.territory AND
        s.posu_region = t.posu_region AND
        s.product_type = t.product_type AND
        s.product_configuration = t.product_configuration;