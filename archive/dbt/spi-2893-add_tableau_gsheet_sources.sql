SELECT

    month,
    CASE
        WHEN territory IN ('BE', 'TB-BE_FR', 'TB-BE_NL') THEN 'BE'
        WHEN territory IN ('NL', 'TB-NL') THEN 'NL'
        WHEN territory IN ('AT', 'DE') THEN 'DE'
        ELSE territory END        AS spend_territory,
    CASE
        WHEN
            tracker_category IN ('Retargeting', 'GHA', 'PPC CPA', 'FB CPA', 'Affiliate CPA') THEN 'CPA'
        WHEN tracker_category IN ('Display/Email', 'Paid Social', 'PPC Non-Brand', 'GSP', 'GDN') THEN 'CPL'
        ELSE 'Brand' END          AS category,
    CASE
        WHEN tracker_category IN ('Display/Email', 'GSP', 'GDN') THEN 'Display/Email'
        WHEN tracker_category IN ('GHA', 'PPC CPA') THEN 'PPC CPA'
        ELSE tracker_category END AS channel,
    SUM(gbp_cost)                 AS cost_gbp

FROM latest_vault.tableau_gsheets.tableau_channel_territory_costs
GROUP BY 1, 2, 3, 4

UNION

SELECT
    month,
    CASE
        WHEN territory IN ('BE', 'TB-BE_FR', 'TB-BE_NL') THEN 'BE'
        WHEN territory IN ('NL', 'TB-NL') THEN 'NL'
        WHEN territory IN ('AT', 'DE') THEN 'DE'
        ELSE territory END                                            AS spend_territory,
    'TV'                                                              AS category,
    'TV'                                                              AS channel,
    SUM(COALESCE(media_spend_gbp, 0)) + SUM(COALESCE(other_spend, 0)) AS cost_gbp
FROM dbt_dev.bi_staging.tv_agencies
GROUP BY 1, 2, 3, 4;

SELECT
    ctc.date,
    ctc.month,
    ctc.week_begin,
    ctc.tracker_category,
    ctc.territory,
    ctc.date__o,
    ctc.month__o,
    ctc.week_begin__o,
    ctc.leads,
    ctc.gbp_cost,
    ctc.margin,
    ctc.bookings,
    ctc.clicks,
    ctc.impressions
FROM latest_vault.tableau_gsheets.tableau_channel_territory_costs ctc;

SELECT * FROM data_vault_mvp.bi.total_marketing_costs;


SELECT * FROM data_vault_mvp.bi.cohort_v4_monthly_active_users cv4mau;