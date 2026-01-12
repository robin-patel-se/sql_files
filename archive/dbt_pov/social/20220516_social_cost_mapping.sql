WITH costs AS (
    SELECT
        cp.date_value                                           AS date,
        DATE_TRUNC('month', cp.date_value)                      AS first_month,
        DATE_TRUNC('month', cp.date_value) + INTERVAL '1 month' AS next_month,
        TO_VARCHAR(cp.date_value, 'mon-yy')                     AS month_start,
        cp.account_id                                           AS account_id,
        CASE
            WHEN LEFT(cp.campaign_name, 2) = 'IE' THEN 'IE'
            WHEN LEFT(cp.campaign_name, 2) = 'CH' THEN 'CH'
            WHEN LEFT(cp.campaign_name, 2) = 'BE' THEN 'BE'
            WHEN LEFT(cp.campaign_name, 2) = 'SG' THEN 'SG'
            WHEN LEFT(cp.campaign_name, 2) = 'HK' THEN 'HK'
            WHEN LEFT(cp.campaign_name, 2) = 'PH' THEN 'PH'
            WHEN LEFT(cp.campaign_name, 2) = 'ID' THEN 'ID'
            WHEN LEFT(cp.campaign_name, 2) = 'MY' THEN 'MY'
            WHEN LEFT(cp.campaign_name, 2) = 'AT' THEN 'AT'
            ELSE 'N/A'
            END                                                 AS campaign_level_territory,
        CASE
            WHEN LEFT(cp.campaign_name, 2) = 'IE' THEN '1'
            WHEN LEFT(cp.campaign_name, 2) = 'CH' THEN '10'
            WHEN LEFT(cp.campaign_name, 2) = 'BE' THEN '14'
            WHEN LEFT(cp.campaign_name, 2) = 'SG' THEN '17'
            WHEN LEFT(cp.campaign_name, 2) = 'HK' THEN '18'
            WHEN LEFT(cp.campaign_name, 2) = 'PH' THEN '19'
            WHEN LEFT(cp.campaign_name, 2) = 'ID' THEN '20'
            WHEN LEFT(cp.campaign_name, 2) = 'MY' THEN '21'
            WHEN LEFT(cp.campaign_name, 2) = 'AT' THEN '4'
            ELSE 'N/A'
            END                                                 AS campaign_level_territory_id,
        CASE
            WHEN account_name = 'Secret Escapes BeNeLux' THEN '12'
            WHEN account_name = 'Secret Escapes DE CPA' THEN '4'
            WHEN account_name = 'Secret Escapes UK CPA' THEN '1'
            WHEN account_name = 'Secret Escapes UK (NEW)' THEN '1'
            WHEN account_name = 'Secret Escapes FR CPA' THEN '15'
            WHEN account_name = 'Secret Escapes Germany (NEW)' THEN '4'
            WHEN account_name = 'Secret Escapes BE' THEN '14'
            WHEN account_name = 'Secret Escapes BeNeLux' THEN '14'
            WHEN account_name = 'Secret Escapes FR' THEN '15'
            WHEN account_name = 'Secret Escapes NL CPA' THEN '12'
            WHEN account_name = 'Secret Escapes Sweden' THEN '2'
            WHEN account_name = 'Secret Escapes SWE CPA' THEN '2'
            WHEN account_name = 'Secret Escapes IT CPA' THEN '11'
            WHEN account_name = 'Secret Escapes IT (NEW)' THEN '11'
            WHEN account_name = 'Secret Escapes DK' THEN '8'
            WHEN account_name = 'Secret Escapes NO' THEN '9'
            WHEN account_name = 'Secret Escapes App DE' THEN '4'
            WHEN account_name = 'Secret Escapes App UK' THEN '1'
            ELSE '0' END                                        AS account_territory_id,
        CASE
            WHEN campaign_name LIKE '%APP%' THEN 'APP'
            WHEN campaign_name LIKE '%competitors%' THEN 'Competitors'
            WHEN campaign_name LIKE '%competitors%' AND campaign_name LIKE '%auto%' THEN 'Competitors'
            WHEN campaign_name LIKE '%DPA%' THEN 'DPA'
            WHEN campaign_name LIKE '%DPA%' AND campaign_name LIKE '%ALLNF%' THEN 'DPA'
            WHEN campaign_name LIKE '%DAT%' THEN 'DAT'
            WHEN campaign_name LIKE '%Flow%' THEN 'Flow'
            WHEN LOWER(campaign_name) LIKE '%Instagram%' THEN 'Instagram'
            WHEN campaign_name LIKE '%LG%' THEN 'LG'
            WHEN LOWER(campaign_name) LIKE '%leadopt%' THEN 'LeadOpt'
            WHEN LOWER(campaign_name) LIKE '%video%' THEN 'Video'
            WHEN campaign_name LIKE '%LTV%' THEN 'LTV'
            WHEN campaign_name LIKE '%travel-intent%' THEN 'Travel-intent'
            WHEN campaign_name LIKE '%mobile%' THEN 'Mobile'
            WHEN campaign_name LIKE '%desktop%' THEN 'Desktop'
            WHEN (campaign_name LIKE '%Both%' OR campaign_name LIKE '%ALLNF%' OR campaign_name LIKE '%ALLNF')
                THEN 'Both'
            ELSE 'Auto'
            END                                                 AS filter,
        SUM(cp.clicks)                                          AS clicks,
        SUM(cp.website_purchases)                               AS bookings,
        SUM(cp.website_purchases_conversion_value)              AS conversion_value,
        SUM(spend)                                              AS cost,
        SUM(website_content_views)                              AS web_content_views

    FROM latest_vault.facebook_marketing.ads cp
    WHERE account_id != '1316855191666061'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
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

WHERE mc.cost IS NOT NULL;



SELECT * FROm dbt_dev.dbt_robinpatel.ppc_leads;