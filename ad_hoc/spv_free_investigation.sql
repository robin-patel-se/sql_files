--last click
SELECT se.data.se_week(sts.event_tstamp::DATE)                      AS se_week,
       stmc.touch_affiliate_territory,
       COUNT(*),
       SUM(IFF(stmc.channel_category = 'Test', 1, 0))               AS test,
       SUM(IFF(stmc.channel_category = 'Email - Triggers', 1, 0))   AS email_triggers,
       SUM(IFF(stmc.channel_category = 'Paid', 1, 0))               AS paid,
       SUM(IFF(stmc.channel_category = 'Free', 1, 0))               AS free,
       SUM(IFF(stmc.channel_category = 'Email - Newsletter', 1, 0)) AS email_newsletter,
       SUM(IFF(stmc.channel_category = 'Other', 1, 0))              AS other
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_attribution sta ON sts.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE se.data.se_year(sts.event_tstamp::DATE) = 2021
  AND stmc.touch_affiliate_territory IN ('UK', 'DE')
GROUP BY 1, 2;

--last non direct
SELECT se.data.se_week(sts.event_tstamp::DATE)                      AS se_week,
       stmc.touch_affiliate_territory,
       COUNT(*),
       SUM(IFF(stmc.channel_category = 'Test', 1, 0))               AS test,
       SUM(IFF(stmc.channel_category = 'Email - Triggers', 1, 0))   AS email_triggers,
       SUM(IFF(stmc.channel_category = 'Paid', 1, 0))               AS paid,
       SUM(IFF(stmc.channel_category = 'Free', 1, 0))               AS free,
       SUM(IFF(stmc.channel_category = 'Email - Newsletter', 1, 0)) AS email_newsletter,
       SUM(IFF(stmc.channel_category = 'Other', 1, 0))              AS other
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_attribution sta ON sts.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE se.data.se_year(sts.event_tstamp::DATE) = 2021
  AND stmc.touch_affiliate_territory IN ('UK', 'DE')
GROUP BY 1, 2;


-- last non direct channel level
SELECT se.data.se_week(sts.event_tstamp::DATE)                             AS se_week,
       stmc.touch_affiliate_territory,
       COUNT(*),
       SUM(IFF(stmc.channel_category = 'Free', 1, 0))                      AS free,
       SUM(IFF(stmc.touch_mkt_channel = 'Blog', 1, 0))                     AS blog,
       SUM(IFF(stmc.touch_mkt_channel = 'Direct', 1, 0))                   AS direct,
       SUM(IFF(stmc.touch_mkt_channel = 'Organic Search Brand', 1, 0))     AS organic_search_brand,
       SUM(IFF(stmc.touch_mkt_channel = 'Organic Search Non-Brand', 1, 0)) AS organic_search_nonbrand,
       SUM(IFF(stmc.touch_mkt_channel = 'Organic Social', 1, 0))           AS organic_social,

       SUM(IFF(stmc.channel_category = 'Paid', 1, 0))                      AS paid,
       SUM(IFF(stmc.touch_mkt_channel = 'Affiliate Program', 1, 0))        AS affiliate_program,
       SUM(IFF(stmc.touch_mkt_channel = 'Display CPA', 1, 0))              AS display_cpa,
       SUM(IFF(stmc.touch_mkt_channel = 'Display CPL', 1, 0))              AS display_cpl,
       SUM(IFF(stmc.touch_mkt_channel = 'Paid Social CPA', 1, 0))          AS paid_social_cpa,
       SUM(IFF(stmc.touch_mkt_channel = 'Paid Social CPL', 1, 0))          AS paid_social_cpl,
       SUM(IFF(stmc.touch_mkt_channel = 'PPC - Brand', 1, 0))              AS ppc__brand,
       SUM(IFF(stmc.touch_mkt_channel = 'PPC - Non Brand CPA', 1, 0))      AS ppc__non_brand_cpa,
       SUM(IFF(stmc.touch_mkt_channel = 'PPC - Non Brand CPL', 1, 0))      AS ppc__non_brand_cpl,
       SUM(IFF(stmc.touch_mkt_channel = 'PPC - Undefined', 1, 0))          AS ppc__undefined,

       SUM(IFF(stmc.channel_category = 'Other', 1, 0))                     AS other,
       SUM(IFF(stmc.touch_mkt_channel = 'Email - Other', 1, 0))            AS email__other,
       SUM(IFF(stmc.touch_mkt_channel = 'Media', 1, 0))                    AS media,
       SUM(IFF(stmc.touch_mkt_channel = 'Other', 1, 0))                    AS other,
       SUM(IFF(stmc.touch_mkt_channel = 'Partner', 1, 0))                  AS partner,
       SUM(IFF(stmc.touch_mkt_channel = 'Test', 1, 0))                     AS test,
       SUM(IFF(stmc.touch_mkt_channel = 'YouTube', 1, 0))                  AS youtube,

       SUM(IFF(stmc.touch_mkt_channel = 'Email - Newsletter', 1, 0))       AS email__newsletter,
       SUM(IFF(stmc.touch_mkt_channel = 'Email - Triggers', 1, 0))         AS email__triggers
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_attribution sta ON sts.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE se.data.se_year(sts.event_tstamp::DATE) = 2021
  AND stmc.touch_affiliate_territory IN ('UK', 'DE')
GROUP BY 1, 2;

-- last click channel level
SELECT se.data.se_week(sts.event_tstamp::DATE)                             AS se_week,
       stmc.touch_affiliate_territory,
       COUNT(*),
       SUM(IFF(stmc.channel_category = 'Free', 1, 0))                      AS free,
       SUM(IFF(stmc.touch_mkt_channel = 'Blog', 1, 0))                     AS blog,
       SUM(IFF(stmc.touch_mkt_channel = 'Direct', 1, 0))                   AS direct,
       SUM(IFF(stmc.touch_mkt_channel = 'Organic Search Brand', 1, 0))     AS organic_search_brand,
       SUM(IFF(stmc.touch_mkt_channel = 'Organic Search Non-Brand', 1, 0)) AS organic_search_nonbrand,
       SUM(IFF(stmc.touch_mkt_channel = 'Organic Social', 1, 0))           AS organic_social,

       SUM(IFF(stmc.channel_category = 'Paid', 1, 0))                      AS paid,
       SUM(IFF(stmc.touch_mkt_channel = 'Affiliate Program', 1, 0))        AS affiliate_program,
       SUM(IFF(stmc.touch_mkt_channel = 'Display CPA', 1, 0))              AS display_cpa,
       SUM(IFF(stmc.touch_mkt_channel = 'Display CPL', 1, 0))              AS display_cpl,
       SUM(IFF(stmc.touch_mkt_channel = 'Paid Social CPA', 1, 0))          AS paid_social_cpa,
       SUM(IFF(stmc.touch_mkt_channel = 'Paid Social CPL', 1, 0))          AS paid_social_cpl,
       SUM(IFF(stmc.touch_mkt_channel = 'PPC - Brand', 1, 0))              AS ppc__brand,
       SUM(IFF(stmc.touch_mkt_channel = 'PPC - Non Brand CPA', 1, 0))      AS ppc__non_brand_cpa,
       SUM(IFF(stmc.touch_mkt_channel = 'PPC - Non Brand CPL', 1, 0))      AS ppc__non_brand_cpl,
       SUM(IFF(stmc.touch_mkt_channel = 'PPC - Undefined', 1, 0))          AS ppc__undefined,

       SUM(IFF(stmc.channel_category = 'Other', 1, 0))                     AS other,
       SUM(IFF(stmc.touch_mkt_channel = 'Email - Other', 1, 0))            AS email__other,
       SUM(IFF(stmc.touch_mkt_channel = 'Media', 1, 0))                    AS media,
       SUM(IFF(stmc.touch_mkt_channel = 'Other', 1, 0))                    AS other,
       SUM(IFF(stmc.touch_mkt_channel = 'Partner', 1, 0))                  AS partner,
       SUM(IFF(stmc.touch_mkt_channel = 'Test', 1, 0))                     AS test,
       SUM(IFF(stmc.touch_mkt_channel = 'YouTube', 1, 0))                  AS youtube,

       SUM(IFF(stmc.touch_mkt_channel = 'Email - Newsletter', 1, 0))       AS email__newsletter,
       SUM(IFF(stmc.touch_mkt_channel = 'Email - Triggers', 1, 0))         AS email__triggers
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE se.data.se_year(sts.event_tstamp::DATE) = 2021
  AND stmc.touch_affiliate_territory IN ('UK', 'DE')
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------

--last click total spvs
SELECT se.data.se_week(sts.event_tstamp::DATE) AS se_week,
       COUNT(*)                                AS spvs
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE se.data.se_year(sts.event_tstamp::DATE) = 2021
GROUP BY 1;


--last non direct total spvs
SELECT se.data.se_week(sts.event_tstamp::DATE) AS se_week,
       COUNT(*)                                AS spvs
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_attribution sta ON sts.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE se.data.se_year(sts.event_tstamp::DATE) = 2021
GROUP BY 1;

SELECT *
FROM se.data.scv_touch_attribution sta
WHERE sta.attribution_model = 'last non direct'
    QUALIFY COUNT(*) OVER (PARTITION BY sta.touch_id) > 1;


SELECT sta.attribution_model,
       count(*)
FROM se.data.scv_touch_attribution sta
GROUP BY 1;