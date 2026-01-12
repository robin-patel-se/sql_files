--- for COSTS dataset
WITH cost AS (
    SELECT
        apm.affiliate_id,
        adp.date,
        DATE_TRUNC('month', adp.date)                      AS first_month,
        DATE_TRUNC('month', adp.date) + INTERVAL '1 month' AS next_month,
        TO_VARCHAR(adp.date, 'mon-yy')                     AS month_start,
        adp.brand,
        apm.affiliate_name                                 AS name,
        adp.filter,
        adp.clicks,
        adp.conversions,
        adp.conversions_value,
        adp.goal,
        SUM(adp.spend)                                     AS cost
    FROM collab.performance_analytics.ppc_mapped_costs adp
        JOIN latest_vault.marketing_gsheets.google_ads_and_bing_ads_affiliate_mapping apm
             ON adp.affiliate_id = apm.affiliate_id
                 AND adp.filter = apm.filter
                 AND adp.brand = apm.brand
                 AND adp.goal = apm.goal
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
),
     dashboard_columns AS (
         SELECT
             tu.affiliate_id,
             tu.date,
             tu.month_start,
             DATEDIFF(DAY, first_month, next_month)                                       AS days_in_month,
             tu.brand,
             tu.goal,
             tu.name,
             tu.filter,
             tu.clicks,
             tu.conversions,
             tu.conversions_value,
             tu.cost,
             IFF(goal = '%CPL', NULL, (tu.cost / NULLIF(tu.conversions, 0)))              AS cpa,
             IFF(goal = '%CPL', NULL, (tu.conversions_value / NULLIF(tu.cost, 0)))        AS roas,
             IFF(goal = '%CPL', NULL, (tu.conversions / NULLIF(tu.clicks, 0)))            AS conversion_rate,
             IFF(goal = '%CPL', NULL, (tu.conversions_value / NULLIF(tu.conversions, 0))) AS mpt,
             o.affilaite_territory                                                        AS territory_id,
             CASE
                 WHEN afc.category = 'PPC_NON_BRAND_CPA' THEN 'PPC CPA Non - Brand'
                 WHEN afc.category = 'PPC_BRAND_CPA' THEN 'PPC CPA Brand'
                 WHEN afc.category = 'PPC_NON_BRAND_CPL' THEN 'PPC Non - Brand'
                 WHEN afc.category = 'PPC_BRAND_CPL' THEN 'PPC Brand'
                 ELSE afc.category
                 END                                                                      AS affiliate_category,
             o.signups,
             o.weekly,
             o.non,
             o.both
         FROM cost tu
             INNER JOIN se.data.se_affiliate afc ON tu.affiliate_id = afc.id
             INNER JOIN collab.performance_analytics.ppc_leads_data o ON tu.affiliate_id = o.affiliate_id AND tu.date = o.date
         WHERE afc.category IN ('PPC_NON_BRAND_CPA', 'PPC_BRAND_CPA', 'PPC_NON_BRAND_CPL', 'PPC_BRAND_CPL')
     ),

     budgets AS (
         SELECT
             month_start,
             territory_code,
             CASE
                 WHEN territory_code = 'UK' THEN '1'
                 WHEN territory_code = 'SE' THEN '2'
                 WHEN territory_code = 'DE' THEN '4'
                 WHEN territory_code = 'US' THEN '7'
                 WHEN territory_code = 'DK' THEN '8'
                 WHEN territory_code = 'NO' THEN '9'
                 WHEN territory_code = 'CH' THEN '10'
                 WHEN territory_code = 'IT' THEN '11'
                 WHEN territory_code = 'NL' THEN '12'
                 WHEN territory_code = 'ES' THEN '13'
                 WHEN territory_code = 'BE' THEN '14'
                 WHEN territory_code = 'FR' THEN '15'
                 WHEN territory_code = 'SG' THEN '17'
                 WHEN territory_code = 'HK' THEN '18'
                 WHEN territory_code = 'PH' THEN '19'
                 WHEN territory_code = 'ID' THEN '20'
                 WHEN territory_code = 'MY' THEN '21'
                 WHEN territory_code = 'AT' THEN '22'
                 WHEN territory_code = 'TB-BE_FR' THEN '25'
                 WHEN territory_code = 'TB-BE_NL' THEN '36'
                 WHEN territory_code = 'TB-NL' THEN '27'
                 ELSE 'N/A'
                 END               AS country_code,
             CASE
                 WHEN category = 'PPC CPA' THEN 'PPC CPA Non - Brand'
                 WHEN category = 'Free sign ups' THEN 'PPC Brand'
                 WHEN category = 'PPC Non-Brand' THEN 'PPC Non - Brand'
                 WHEN category = 'PPC CPA Brand' THEN 'PPC CPA Brand'
                 ELSE category END AS affiliate_category,
             SUM(spend)            AS spend,
             SUM(margin)           AS margin,
             SUM(members)          AS members
         FROM latest_vault.marketing_gsheets.goals
         WHERE budget_stage = 'WORKING - Budget Original'
         GROUP BY 1, 2, 3, 4
     ),

     calendar AS (
         SELECT
             sc.date_value,
             bg.affiliate_category,
             bg.country_code,
             bg.territory_code,
             DATE_TRUNC('month', sc.date_value) AS month_start
         FROM se.data.se_calendar sc
             INNER JOIN budgets bg ON bg.month_start = DATE_TRUNC('month', sc.date_value)
         WHERE year >= 2021
           AND bg.affiliate_category IN ('PPC CPA Non - Brand', 'PPC CPA Brand', 'PPC Non - Brand', 'PPC Brand')
           --and year <2022
         GROUP BY 1, 2, 3, 4
     )
        ,

     affiliate_count AS (
         SELECT
             sc.affiliate_category,
             sc.month_start,
             sc.territory_code,
             sc.country_code,
             mdt.date,
             sc.date_value,
             DATE_TRUNC('month', sc.date_value)                      AS first_month,
             DATE_TRUNC('month', sc.date_value) + INTERVAL '1 month' AS next_month,
             DATEDIFF(DAY, first_month, next_month)                  AS days_in_month,
             IFF(mdt.date IS NULL, 1, COUNT(mdt.affiliate_id))       AS aff_count
         FROM calendar sc
             LEFT JOIN dashboard_columns mdt
                       ON mdt.date = sc.date_value
                           AND mdt.affiliate_category = sc.affiliate_category
                           AND mdt.territory_id = sc.country_code
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
     )


SELECT
    ac.territory_code,
    ac.date_value,
    ac.month_start,
    ac.affiliate_category,
    bg.affiliate_id,
    CAST(SUM((bm.spend / ac.days_in_month) / ac.aff_count) AS NUMERIC(36, 2))   AS d_spend,
    CAST(SUM((bm.members / ac.days_in_month) / ac.aff_count) AS NUMERIC(36, 0)) AS d_members,
    CAST(SUM((bm.margin / ac.days_in_month) / ac.aff_count) AS NUMERIC(36, 2))  AS d_margin,
    SUM(bg.clicks)                                                              AS clicks,
    CAST(SUM(bg.cost) AS NUMERIC(36, 0))                                        AS cost


FROM affiliate_count ac
    INNER JOIN budgets bm
               ON bm.affiliate_category = ac.affiliate_category
                   AND bm.month_start = ac.month_start
                   AND bm.country_code = ac.country_code
    LEFT JOIN  dashboard_columns bg
               ON ac.affiliate_category = bg.affiliate_category
                   AND ac.date = bg.date
                   AND ac.country_code = bg.territory_id
                   AND ac.date = bg.date
                   AND ac.country_code = bg.territory_id


WHERE ac.affiliate_category IN ('PPC CPA Non - Brand', 'PPC CPA Brand', 'PPC Non - Brand', 'PPC Brand')
  AND bg.affiliate_id IS NOT NULL
-- and bg.affiliate_id = 865
-- and ac.date_value = '2021-11-30'
GROUP BY 1, 2, 3, 4, 5;
