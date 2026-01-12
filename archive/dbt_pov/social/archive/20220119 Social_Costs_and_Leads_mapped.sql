WITH mapped_goals AS (
    SELECT
        wg.country_code,
        wg.month_start,
        CASE
            WHEN wg.affiliate_category = 'FB CPA' THEN 'Facebook CPA'
            WHEN wg.affiliate_category = 'Facebook' THEN 'Facebook CPL'
            ELSE wg.affiliate_category END AS affiliate_category,
        wg.spend,
        wg.margin,
        wg.members
    FROM collab.performance_analytics.mapped_working_goals wg
    WHERE wg.affiliate_category IN ('FB CPA', 'Facebook')
),


     costs AS (
         SELECT
             sm.affiliate_id,
             sm.date,
             TO_VARCHAR(sm.date, 'mon-yy') AS month_start,
             ac.territory_id,
             ac.affiliate_category,
             SUM(sm.cost)                  AS cost,
             SUM(sm.bookings)              AS bookings,
             SUM(sm.conversion_value)      AS conversion_value,
             SUM(sm.clicks)                AS clicks,
             SUM(bg.signups)               AS signups


         FROM collab.performance_analytics.social_cost_mapped sm
             LEFT JOIN collab.performance_analytics.ppc_leads_data bg
                       ON sm.affiliate_id = bg.affiliate_id AND sm.date = bg.date
             JOIN      collab.performance_analytics.affiliate_categories ac ON ac.id = sm.affiliate_id
             AND ac.affiliate_category IN ('Facebook CPA', 'Facebook CPL')

         GROUP BY 1, 2, 3, 4, 5
     ),

     calendar AS (
         SELECT
             date_value,
             CASE
                 WHEN bg.affiliate_category = 'FB CPA' THEN 'Facebook CPA'
                 WHEN bg.affiliate_category = 'Facebook' THEN 'Facebook CPL'
                 ELSE bg.affiliate_category END AS affiliate_category,
             bg.country_code,
             TO_VARCHAR(date_value, 'mon-yy')   AS month_start


         FROM se.data.se_calendar sc
             JOIN collab.performance_analytics.mapped_working_goals bg
                  ON bg.month_start = TO_VARCHAR(sc.date_value, 'mon-yy')
         WHERE bg.affiliate_category IN ('FB CPA', 'Facebook')
           AND year = 2021
         GROUP BY 1, 2, 3
     ),


     affiliate_count AS (
         SELECT
             sc.affiliate_category,
             sc.month_start,
             sc.country_code,
             c.date,
             sc.date_value,
             DATE_TRUNC('month', sc.date_value)                             AS first_month,
             DATE_TRUNC('month', sc.date_value) + INTERVAL '1 month'        AS next_month,
             DATEDIFF(DAY, first_month, next_month)                         AS days_in_month,
             CASE WHEN c.date IS NULL THEN 1 ELSE COUNT(c.affiliate_id) END AS aff_count


         FROM calendar sc
             LEFT JOIN costs c ON c.date = sc.date_value AND c.affiliate_category = sc.affiliate_category AND c.territory_id = sc.country_code AND sc.month_start = c.month_start

         WHERE sc.month_start = 'Nov-21'

         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
     )


SELECT
    c.territory_id,
    c.date,
    c.month_start,
    c.affiliate_category,
    c.affiliate_id,
    SUM((bm.spend / ac.days_in_month) / ac.aff_count)   AS d_spend,
    SUM((bm.members / ac.days_in_month) / ac.aff_count) AS d_members,
    SUM((bm.margin / ac.days_in_month) / ac.aff_count)  AS d_margin,
    SUM(c.cost)                                         AS cost,
    SUM(c.conversion_value)                             AS margin,
    SUM(c.clicks)                                       AS clicks,
    SUM(c.signups)                                      AS signups

FROM affiliate_count ac
    JOIN      mapped_goals bm ON bm.affiliate_category = ac.affiliate_category AND bm.month_start = ac.month_start AND bm.country_code = ac.country_code
    LEFT JOIN costs c ON ac.affiliate_category = c.affiliate_category AND ac.date_value = c.date AND ac.country_code = c.territory_id


GROUP BY 1, 2, 3, 4, 5
