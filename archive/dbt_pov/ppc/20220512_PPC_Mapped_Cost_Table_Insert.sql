CREATE OR REPLACE VIEW collab.performance_analytics.ppc_mapped_costs COPY GRANTS AS

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
                 am.currency,
                 am.partner,
                 SUM(bp.clicks)       AS clicks,
                 SUM(bp.conversions)  AS conversions,
                 SUM(bp.revenue)      AS conversions_value,
                 SUM(bp.spend)        AS spend
             FROM latest_vault.marketing_gsheets.google_ads_and_bing_ads_affiliate_mapping am
                 INNER JOIN latest_vault.bing_ads.ad_performance bp ON am.id = bp.account_number
             WHERE am.partner = 'Bing'
             GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
         ),

     google AS (

         SELECT
             ap.date_value::DATE       AS date,
             am.affiliate_id,
             am.affiliate_name,
             am.brand                  AS brand,
             am.goal                   AS goal,
             am.filter                 AS filter,
             am.currency,
             am.partner,
             SUM(ap.clicks)            AS clicks,
             SUM(ap.conversions)       AS conversions,
             SUM(ap.conversions_value) AS conversions_value,
             SUM(ap.cost_micros / 1e6) AS spend
         FROM latest_vault.marketing_gsheets.google_ads_and_bing_ads_affiliate_mapping am
             INNER JOIN latest_vault.google_ads.ad_performance ap ON am.id = ap.customer_id
         WHERE partner = 'Google'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
     )

SELECT
    g.date,
    g.affiliate_id,
    g.affiliate_name,
    g.brand,
    g.goal,
    g.filter,
    g.clicks,
    g.conversions,
    g.conversions_value,
    g.spend,
    g.currency,

FROM google g
UNION
SELECT
    b.date,
    b.affiliate_id,
    b.affiliate_name,
    b.brand,
    b.goal,
    b.filter,
    b.clicks,
    b.conversions,
    b.conversions_value,
    b.spend,
    b.currency
FROM bing b
);


SELECT *
FROM se.data.synxis_hotel_rooms_and_rates shrar;