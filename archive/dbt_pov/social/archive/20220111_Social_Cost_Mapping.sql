CREATE OR REPLACE VIEW collab.performance_analytics.social_cost_mapped COPY GRANTS AS
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
    sm.name,
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
    JOIN collab.performance_analytics.social_mapping sm
         ON mc.filter = sm.filter AND mc.account_id = sm.account_id
             AND mc.account_level_territory_id = sm.territory_id
             AND mc.campaign_level_territory::VARCHAR = sm.campaign_terrritory::VARCHAR

WHERE mc.cost IS NOT NULL

--and sm.AFFILIATE_ID = 2619 and mc.DATE = '2021-08-29'
---and month_start = 'Oct-21'
    );
