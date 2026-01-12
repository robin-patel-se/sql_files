-- script for mapping campaign level data from google Ads according to a set of filters from campaign names

CREATE OR REPLACE VIEW collab.performance_analytics.ppc_mapped_costs COPY GRANTS AS
(
-- aggregate ad partners metrics up to common grain and then stack ontop of one another
WITH google AS (
    -- aggregate google ads data up to common grain
    SELECT
        cp.date_value::DATE                                                               AS date,
        IFF(ca.campaign_name LIKE '%Brand%', 'Brand', 'Non-Brand')                        AS brand,
        IFF(ca.campaign_name LIKE '%CPA%' OR ca.customer_id = '6377974099', 'CPA', 'CPL') AS goal,
        CASE
            WHEN (LOWER(ca.campaign_name) LIKE '%area%') THEN 'Area'
            WHEN (LOWER(ca.campaign_name) LIKE '%live%') THEN 'Live'
            WHEN (LOWER(ca.campaign_name) LIKE '%dsa%') THEN 'DSA'
            WHEN (LOWER(ca.campaign_name) LIKE '%youtube%') THEN 'Youtube'
            WHEN (LOWER(ca.campaign_name) LIKE '%pkg%') THEN 'PKG'
            WHEN (LOWER(ca.campaign_name) LIKE '%gsp%') THEN 'GSP'
            ELSE 'N/A'
            END                                                                           AS filter,
        ca.customer_id::VARCHAR                                                           AS account_number,
        ca.campaign_name,
        SUM(cp.clicks)                                                                    AS clicks,
        SUM(cp.conversions)                                                               AS conversions,
        SUM(cp.conversions_value)                                                         AS conversions_value,
        SUM(cost_micros / 1e6)                                                            AS cost
    FROM latest_vault.google_ads.ad_performance cp
        INNER JOIN latest_vault.google_ads.campaign_attributes ca ON ca.campaign_resource_name = cp.campaign_resource_name
    GROUP BY 1, 2, 3, 4, 5, 6
),
     bing AS (
         -- aggregate bing ads data up to common grain
         SELECT
             cp.time_period::DATE                                       AS date,
             IFF(ca.campaign_name LIKE '%Brand%', 'Brand', 'Non-Brand') AS brand,
             IFF(ca.campaign_name LIKE '%CPA%', 'CPA', 'CPL')           AS goal,
             CASE
                 WHEN (LOWER(ca.campaign_name) LIKE '%area%') THEN 'Area'
                 WHEN (LOWER(ca.campaign_name) LIKE '%live%') THEN 'Live'
                 WHEN (LOWER(ca.campaign_name) LIKE '%dsa%') THEN 'DSA'
                 WHEN (LOWER(ca.campaign_name) LIKE '%youtube%') THEN 'Youtube'
                 WHEN (LOWER(ca.campaign_name) LIKE '%pkg%') THEN 'PKG'
                 WHEN (LOWER(ca.campaign_name) LIKE '%gsp%') THEN 'GSP'
                 ELSE 'N/A'
                 END                                                    AS filter,
             ca.account_number::VARCHAR                                 AS account_number,
             ca.campaign_name,
             SUM(cp.clicks)                                             AS clicks,
             SUM(cp.conversions)                                        AS conversions,
             SUM(cp.revenue)                                            AS conversions_value,
             SUM(spend)                                                 AS cost
         FROM latest_vault.bing_ads.ad_performance cp
             INNER JOIN latest_vault.bing_ads.campaign_attributes ca ON ca.base_campaign_id = cp.base_campaign_id
         GROUP BY 1, 2, 3, 4, 5, 6
     )

--stack ad partners data on top of one another at common grain
SELECT
    g.date,
    g.brand,
    g.goal,
    g.filter,
    g.account_number,
    g.campaign_name,
    g.clicks,
    g.conversions,
    g.conversions_value,
    g.cost,
    'google ads' AS marketing_partner
FROM google g
UNION ALL
SELECT
    b.date,
    b.brand,
    b.goal,
    b.filter,
    b.account_number,
    b.campaign_name,
    b.clicks,
    b.conversions,
    b.conversions_value,
    b.cost,
    'bing ads' AS marketing_partner
FROM bing b
    );


SELECT *
FROM collab.performance_analytics.ppc_mapped_costs;


SELECT GET_DDL('table', 'collab.performance_analytics.ppc_mapped_costs');

------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE VIEW ppc_mapped_costs
            (
             date,
             affiliate_id,
             affiliate_name,
             brand,
             goal,
             filter,
             clicks,
             conversions,
             conversions_value,
             spend,
             currency
                )
AS

(
WITH bing AS
         (
             SELECT
                 bp.time_period::DATE AS date,
                 am.affiliate_id,
                 am.affiliate_name,
                 am.brand             AS brand,
                 am.goal              AS goal,
                 am.filter            AS filter,
                 SUM(bp.clicks)       AS clicks,
                 SUM(bp.conversions)  AS conversions,
                 SUM(bp.revenue)      AS conversions_value,
                 SUM(bp.spend)        AS spend,
                 am.currency
             FROM latest_vault.marketing_gsheets.google_ads_and_bing_ads_affiliate_mapping am
                 JOIN latest_vault.bing_ads.ad_performance bp

             WHERE partner = 'Bing'
             GROUP BY 1, 2, 3, 4, 5, 6, 11
         ),

     google AS (

         SELECT
             ap.date_value::DATE       AS date,
             am.affiliate_id,
             am.affiliate_name,
             am.brand                  AS brand,
             am.goal                   AS goal,
             am.filter                 AS filter,
             SUM(ap.clicks)            AS clicks,
             SUM(ap.conversions)       AS conversions,
             SUM(ap.conversions_value) AS conversions_value,
             SUM(ap.cost_micros / 1e6) AS spend,
             am.currency
         FROM latest_vault.marketing_gsheets.google_ads_and_bing_ads_affiliate_mapping am
             JOIN latest_vault.google_ads.ad_performance ap
                  ON am.id = ap.customer_id
         WHERE partner = 'Google'
         GROUP BY 1, 2, 3, 4, 5, 6, 11
     )

SELECT *
FROM google
UNION
SELECT *
FROM bing);
