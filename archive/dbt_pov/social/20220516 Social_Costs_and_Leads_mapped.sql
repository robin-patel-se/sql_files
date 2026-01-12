WITH budgets AS (
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
            END      AS country_code,
        category     AS affiliate_category,
        SUM(spend)   AS spend,
        SUM(margin)  AS margin,
        SUM(members) AS members
    FROM latest_vault.marketing_gsheets.goals
    WHERE budget_stage = 'WORKING - Budget Original'
    GROUP BY 1, 2, 3, 4
),
     mapped_goals AS (
         SELECT
             wg.country_code,
             wg.month_start,
             CASE
                 WHEN wg.affiliate_category = 'FB CPA' THEN 'PAID_SOCIAL_CPA'
                 WHEN wg.affiliate_category = 'Facebook' THEN 'PAID_SOCIAL_CPL'
                 ELSE wg.affiliate_category
                 END AS affiliate_category,
             wg.spend,
             wg.margin,
             wg.members
         FROM budgets wg
         WHERE wg.affiliate_category IN ('FB CPA', 'Facebook')
     ),
     costs AS (
         SELECT
             sm.affiliate_id,
             sm.date,
             DATE_TRUNC('month', sm.date) AS month_start,
             sm.final_territory_id        AS territory_id,
             ac.category                  AS affiliate_category,
             SUM(sm.cost)                 AS cost,
             SUM(sm.bookings)             AS bookings,
             SUM(sm.conversion_value)     AS conversion_value,
             SUM(sm.clicks)               AS clicks,
             SUM(bg.signups)              AS signups


         FROM collab.performance_analytics.social_cost_mapped sm
             LEFT JOIN  collab.performance_analytics.ppc_leads_data bg
                        ON sm.affiliate_id = bg.affiliate_id
                            AND sm.date = bg.date
             INNER JOIN se.data.se_affiliate ac ON ac.id = sm.affiliate_id
             AND ac.category IN ('PAID_SOCIAL_CPA', 'PAID_SOCIAL_CPL')
         GROUP BY 1, 2, 3, 4, 5
     ),
     calendar AS (
         SELECT
             date_value,
             CASE
                 WHEN bg.affiliate_category = 'FB CPA' THEN 'PAID_SOCIAL_CPA'
                 WHEN bg.affiliate_category = 'Facebook' THEN 'PAID_SOCIAL_CPL'
                 ELSE bg.affiliate_category END AS affiliate_category,
             bg.country_code,
             DATE_TRUNC('month', date_value)    AS month_start


         FROM se.data.se_calendar sc
             INNER JOIN budgets bg ON bg.month_start = DATE_TRUNC('month', sc.date_value)
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
             DATE_TRUNC('month', sc.date_value)                      AS first_month,
             DATE_TRUNC('month', sc.date_value) + INTERVAL '1 month' AS next_month,
             DATEDIFF(DAY, first_month, next_month)                  AS days_in_month,
             IFF(c.date IS NULL, 1, COUNT(c.affiliate_id))           AS aff_count
         FROM calendar sc
             LEFT JOIN costs c ON c.date = sc.date_value
             AND c.affiliate_category = sc.affiliate_category
             AND c.territory_id = sc.country_code
             AND sc.month_start = c.month_start
         WHERE sc.month_start = '2021-09-01'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
     )

SELECT
    c.territory_id,
    c.date,
    c.month_start,
    CASE
        WHEN c.affiliate_category = 'AFFILIATE_PROGRAM' THEN 'Affiliate Program'
        WHEN c.affiliate_category = 'PPC_NON_BRAND_CPA' THEN 'PPC - Non Brand CPA'
        WHEN c.affiliate_category = 'PAID_SOCIAL_CPL' THEN 'Paid Social CPA'
        WHEN c.affiliate_category = 'OTHER' THEN 'Other'
        WHEN c.affiliate_category = 'DISPLAY_CPL' THEN 'Display CPL'
        WHEN c.affiliate_category = 'DIRECT' THEN 'Direct'
        WHEN c.affiliate_category = 'PARTNER' THEN 'Partner'
        WHEN c.affiliate_category = 'BLOG' THEN 'Blog'
        WHEN c.affiliate_category = 'PPC_BRAND_CPL' THEN 'PPC Brand CPL'
        WHEN c.affiliate_category = 'DISPLAY_CPA' THEN 'Display CPA'
        WHEN c.affiliate_category = 'PPC_NON_BRAND_CPL' THEN 'PPC - Non Brand CPL'
        WHEN c.affiliate_category = 'ORGANIC' THEN 'Organic'
        WHEN c.affiliate_category = 'PPC_BRAND_CPA' THEN 'PPC Brand CPA'
        WHEN c.affiliate_category = 'PARTNER_WHITE_LABEL' THEN 'Dealchecker'
        WHEN c.affiliate_category = 'PAID_SOCIAL_CPA' THEN 'Paid Social CPA'
        WHEN c.affiliate_category = 'DEALCHECKER' THEN 'Dealchecker'
        ELSE 'N/A'
        END                                             AS category
        ,
    c.affiliate_id,
    SUM((bm.spend / ac.days_in_month) / ac.aff_count)   AS d_spend,
    SUM((bm.members / ac.days_in_month) / ac.aff_count) AS d_members,
    SUM((bm.margin / ac.days_in_month) / ac.aff_count)  AS d_margin,
    SUM(c.cost)                                         AS cost,
    SUM(c.conversion_value)                             AS margin,
    SUM(c.clicks)                                       AS clicks,
    SUM(c.signups)                                      AS signups

FROM affiliate_count ac
    INNER JOIN mapped_goals bm
               ON bm.affiliate_category = ac.affiliate_category
                   AND bm.month_start = ac.month_start
                   AND bm.country_code = ac.country_code
    LEFT JOIN  costs c
               ON ac.affiliate_category = c.affiliate_category
                   AND ac.date_value = c.date
                   AND ac.country_code = c.territory_id
WHERE affiliate_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5;


SELECT GET_DDL('table', 'collab.performance_analytics.social_cost_mapped');


CREATE OR REPLACE VIEW social_cost_mapped
            (
             affiliate_id,
             affiliate_name,
             date,
             first_month,
             next_month,
             month_start,
             days_in_month,
             account_id,
             clicks,
             bookings,
             conversion_value,
             cost,
             web_content_views,
             account_level_territory_id,
             campaign_level_territory,
             campaign_level_territory_id,
             final_territory_id,
             territory,
             platform,
             goal,
             partner,
             filter,
             dat
                )
AS
(
WITH costs AS (
    SELECT
        cp.date_value                                           AS date,
        DATE_TRUNC('month', cp.date_value)                      AS first_month,
        DATE_TRUNC('month', cp.date_value) + INTERVAL '1 month' AS next_month,
        TO_VARCHAR(cp.date_value, 'mon-yy')                     AS month_start,
        cp.account_id                                           AS account_id,

        CASE
            WHEN LEFT(cp.campaign_name, 2) LIKE 'IE%' THEN 'IE'
            WHEN (LEFT(cp.campaign_name, 2) LIKE '%CH%' OR LEFT(cp.campaign_name, 2) LIKE 'CH%') THEN 'CH'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'BE%' THEN 'BE'
            --    when (cp.campaign_name like '%FR%'  or cp.campaign_name like 'FR%')  then 'FR'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'SG%' THEN 'SG'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'HK%' THEN 'HK'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'PH%' THEN 'PH'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'ID%' THEN 'ID'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'MY%' THEN 'MY'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'AT%' THEN 'AT'
            --    when (cp.campaign_name like '%TB-BE_FR%' or cp.campaign_name like 'TB-BE_FR%' )  then 'TB-BE_FR'
            --    when (cp.campaign_name like '%TB-BE_NL%' or cp.campaign_name like 'TB-BE_NL%' )then 'TB-BE_NL'
            --    when (cp.campaign_name like '%TB-NL%' or cp.campaign_name like 'TB-NL%' ) then 'TB-NL'
            ELSE 'N/A'
            END                                                 AS campaign_level_territory,


        CASE
            WHEN LEFT(cp.campaign_name, 2) LIKE 'IE%' THEN '1'
            WHEN (LEFT(cp.campaign_name, 2) LIKE '%CH%' OR LEFT(cp.campaign_name, 2) LIKE 'CH%') THEN '10'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'BE%' THEN '14'
            --    when (cp.campaign_name like '%FR%'  or cp.campaign_name like 'FR%')  then 'FR'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'SG%' THEN '17' -- ?
            WHEN LEFT(cp.campaign_name, 2) LIKE 'HK%' THEN '18'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'PH%' THEN '19'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'ID%' THEN '20'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'MY%' THEN '21'
            WHEN LEFT(cp.campaign_name, 2) LIKE 'AT%' THEN '4'
            ELSE 'N/A'
            END                                                 AS campaign_level_territory_id,


        CASE
            WHEN account_name LIKE 'Secret Escapes BeNeLux' THEN '12'
            WHEN account_name LIKE 'Secret Escapes DE CPA' THEN '4'
            WHEN account_name LIKE 'Secret Escapes UK CPA' THEN '1'
            WHEN account_name LIKE 'Secret Escapes UK (NEW)' THEN '1'
            WHEN account_name LIKE 'Secret Escapes FR CPA' THEN '15'
            WHEN account_name LIKE 'Secret Escapes Germany (NEW)' THEN '4'
            WHEN account_name LIKE 'Secret Escapes BE' THEN '14'
            WHEN account_name LIKE 'Secret Escapes BeNeLux' THEN '14'
            WHEN account_name LIKE 'Secret Escapes FR' THEN '15'
            WHEN account_name LIKE 'Secret Escapes NL CPA' THEN '12'
            WHEN account_name LIKE 'Secret Escapes Sweden' THEN '2'
            WHEN account_name LIKE 'Secret Escapes SWE CPA' THEN '2'
            WHEN account_name LIKE 'Secret Escapes IT CPA' THEN '11'
            WHEN account_name LIKE 'Secret Escapes IT (NEW)' THEN '11'
            WHEN account_name LIKE 'Secret Escapes DK' THEN '8'
            WHEN account_name LIKE 'Secret Escapes NO' THEN '9'
            WHEN account_name LIKE 'Secret Escapes App DE' THEN '4'
            WHEN account_name LIKE 'Secret Escapes App UK' THEN '1'
            ELSE '0' END                                        AS account_territory_id,


        CASE
            WHEN campaign_name LIKE '%APP%' THEN 'APP'
            WHEN campaign_name LIKE '%competitors%' THEN 'Competitors'
            WHEN campaign_name LIKE '%competitors%' AND campaign_name LIKE '%auto%' THEN 'Competitors'
            WHEN campaign_name LIKE '%DPA%' THEN 'DPA'
            WHEN campaign_name LIKE '%DPA%' AND campaign_name LIKE '%ALLNF%' THEN 'DPA'
            WHEN campaign_name LIKE '%DAT%' THEN 'DAT'
            WHEN campaign_name LIKE '%Flow%' THEN 'Flow'
            WHEN (campaign_name LIKE '%Instagram%' OR campaign_name LIKE '%instagram%') THEN 'Instagram'
            WHEN campaign_name LIKE '%LG%' THEN 'LG'
            WHEN (campaign_name LIKE '%leadopt%' OR campaign_name LIKE '%LeadOpt') THEN 'LeadOpt'
            WHEN (campaign_name LIKE '%Vdeo%' OR campaign_name LIKE '%Video') THEN 'Video'
            WHEN campaign_name LIKE '%LTV%' THEN 'LTV'
            WHEN campaign_name LIKE '%travel-intent%' THEN 'Travel-intent'
            WHEN campaign_name LIKE '%mobile%' THEN 'Mobile'
            WHEN campaign_name LIKE '%desktop%' THEN 'Desktop'
            WHEN (campaign_name LIKE '%Both%' OR campaign_name LIKE '%ALLNF%' OR campaign_name LIKE '%ALLNF')
                THEN 'Both'
            ELSE 'Auto'
            END                                                 AS filter,


        --  Cp.CAMPAIGN_NAME,
        SUM(cp.clicks)                                          AS clicks,
        SUM(cp.website_purchases)                               AS bookings,
        SUM(cp.website_purchases_conversion_value)              AS conversion_value,
        SUM(spend)                                              AS cost,
        SUM(website_content_views)                              AS web_content_views

    FROM latest_vault.facebook_marketing.ads cp

    WHERE account_id != '1316855191666061'

          --and DATE = '2021-08-29' and account_id = 1070451729639743

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     mapped_costs AS (
         SELECT
             tu.filter,
             tu.date,
             tu.first_month,
             tu.next_month,
             tu.month_start,
             tu.account_id,
             DATEDIFF(DAY, tu.first_month, tu.next_month) AS days_in_month,
             clicks,
             bookings,
             conversion_value,
             cost,
             web_content_views,
             --tu.campaign_level_territory_id as campaign_level_territory_id,
             tu.account_territory_id                      AS account_level_territory_id,
             tu.campaign_level_territory                  AS campaign_level_territory,
             tu.campaign_level_territory_id               AS campaign_level_territory_id


         FROM costs tu
     )


SELECT
    sm.affiliate_id,
    sm.affiliate_name,
    mc.date,
    mc.first_month,
    mc.next_month,
    mc.month_start,
    mc.days_in_month,
    mc.account_id,
    mc.clicks,
    mc.bookings,
    mc.conversion_value,
    mc.cost,
    mc.web_content_views,
    mc.account_level_territory_id,
    mc.campaign_level_territory,
    mc.campaign_level_territory_id,
    CASE
        WHEN mc.campaign_level_territory_id = 'N/A' THEN mc.account_level_territory_id
        WHEN mc.campaign_level_territory_id <> mc.account_level_territory_id THEN mc.campaign_level_territory_id
        ELSE mc.account_level_territory_id END AS final_territory_id,
    sm.territory,
    sm.platform,
    sm.goal,
    sm.partner,
    sm.filter,
    sm.dat
--*
FROM mapped_costs mc
    INNER JOIN latest_vault.marketing_gsheets.facebook_ads_affiliate_mapping sm
               ON mc.filter = sm.filter AND mc.account_id = sm.account_id
                   AND mc.account_level_territory_id = sm.territory_id


WHERE mc.cost IS NOT NULL
    );

SELECT *
FROM data_vault_mvp.bi.paid_social_performance_and_goals;




WITH base_data AS (
    SELECT
        hotel_code,
        MAX(IFF(calendar_date = '2022-06-02', 1, 0)) AS a,
        MAX(IFF(calendar_date = '2022-06-03', 1, 0)) AS b,
        MAX(IFF(calendar_date = '2022-06-04', 1, 0)) AS c
    FROM data_vault_mvp_dev_kirsten.dwh.synxis_offer_calendar_view
    WHERE offer_available_in_calendar = TRUE
    GROUP BY 1
)
SELECT
    SUM(a),
    SUM(b),
    SUM(c)
FROM base_data;