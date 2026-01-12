/*I noticed a large increase in SPVs from the Direct channel in the UK last week, giving it a 300%+ increase against the 2019 equivalent week. Highlighted below. This for w/c 27th Dec 21.

image.png
This increase was in Web (mobile and desktop) platforms.

It could be the increase from sessions and SPV per Sessions lead to the higher than expected SPVs in Direct, but just seem very high compared to historical trends.

Can someone in Data help to confirm if there's any potential issues with SPVs in Direct that could've led to the much higher SPVs before we take it as the truth?
  */

SELECT sc.se_week,
       stmc.channel_category,
       COUNT(*)
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.se_calendar sc ON sts.event_tstamp::DATE = sc.date_value
    INNER JOIN se.data.scv_touch_attribution sta ON sts.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE sts.event_tstamp BETWEEN '2021-12-01' AND CURRENT_DATE
GROUP BY 1, 2;

SELECT sc.se_week,
       stmc.touch_mkt_channel,
       COUNT(*)
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.se_calendar sc ON sts.event_tstamp::DATE = sc.date_value
    INNER JOIN se.data.scv_touch_attribution sta ON sts.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE sts.event_tstamp BETWEEN '2021-12-01' AND CURRENT_DATE
GROUP BY 1, 2;

USE WAREHOUSE pipe_xlarge;

-- week on week direct spv traffic for dach and uk
SELECT sc.se_week,
       sc.se_year,
       stmc.touch_mkt_channel,
       COUNT(*) AS spvs
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.se_calendar sc ON sts.event_tstamp::DATE = sc.date_value
    INNER JOIN se.data.scv_touch_attribution sta ON sts.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE sts.event_tstamp BETWEEN '2020-12-01' AND CURRENT_DATE
  AND stmc.touch_mkt_channel = 'Direct'
  AND stmc.touch_affiliate_territory IN ('DE', 'UK', 'AU', 'CH')
GROUP BY 1, 2, 3;

SELECT sc.se_week,
       sc.se_year,
       stmc.touch_mkt_channel,
       COUNT(*) AS sessions
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.se_calendar sc ON stba.touch_start_tstamp::DATE = sc.date_value
    INNER JOIN se.data.scv_touch_attribution sta ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp BETWEEN '2020-12-01' AND CURRENT_DATE
  AND stmc.touch_mkt_channel = 'Direct'
  AND stmc.touch_affiliate_territory IN ('DE', 'UK', 'AU', 'CH')
GROUP BY 1, 2, 3;


SELECT sts.*,
       stmc.*,
       stba.*
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.se_calendar sc ON sts.event_tstamp::DATE = sc.date_value
    INNER JOIN se.data.scv_touch_attribution sta ON sts.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
    INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sc.se_week = 53
  AND sc.se_year = 2021
  AND stmc.touch_mkt_channel = 'Direct'
  AND stmc.touch_affiliate_territory IN ('DE', 'UK', 'AU', 'CH')
ORDER BY sts.touch_id, event_tstamp;

SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_id = '0006622e2e988cd11d86ce090245ac5345e6a52e456f380453046c1743e50903'
  AND se.data.se_week(stba.touch_start_tstamp::DATE) = 53
  AND stba.touch_start_tstamp >= '2021-12-20';

USE WAREHOUSE pipe_xlarge;

--channel category by day
SELECT stba.touch_start_tstamp::DATE                           AS date,
       SUM(IFF(channel_category = 'Test', 1, 0))               AS test,
       SUM(IFF(channel_category = 'Email - Triggers', 1, 0))   AS email_triggers,
       SUM(IFF(channel_category = 'Paid', 1, 0))               AS paid,
       SUM(IFF(channel_category = 'Free', 1, 0))               AS free,
       SUM(IFF(channel_category = 'Email - Newsletter', 1, 0)) AS email_newsletter,
       SUM(IFF(channel_category = 'Other', 1, 0))              AS other,
       COUNT(*)                                                AS total_sessions
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2021-12-01'
GROUP BY 1;

--uk + dach channel category by day
SELECT stba.touch_start_tstamp::DATE                           AS date,
       SUM(IFF(channel_category = 'Test', 1, 0))               AS test,
       SUM(IFF(channel_category = 'Email - Triggers', 1, 0))   AS email_triggers,
       SUM(IFF(channel_category = 'Paid', 1, 0))               AS paid,
       SUM(IFF(channel_category = 'Free', 1, 0))               AS free,
       SUM(IFF(channel_category = 'Email - Newsletter', 1, 0)) AS email_newsletter,
       SUM(IFF(channel_category = 'Other', 1, 0))              AS other,
       COUNT(*)                                                AS total_sessions
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2021-12-01'
  AND stmc.touch_affiliate_territory IN ('UK', 'DE', 'AT', 'CH')
GROUP BY 1;


--uk + dach channel  by day
SELECT stba.touch_start_tstamp::DATE                                  AS date,
       SUM(IFF(touch_mkt_channel = 'Affiliate Program', 1, 0))        AS affiliate_program,
       SUM(IFF(touch_mkt_channel = 'Blog', 1, 0))                     AS blog,
       SUM(IFF(touch_mkt_channel = 'Direct', 1, 0))                   AS direct,
       SUM(IFF(touch_mkt_channel = 'Display CPA', 1, 0))              AS display_cpa,
       SUM(IFF(touch_mkt_channel = 'Display CPL', 1, 0))              AS display_cpl,
       SUM(IFF(touch_mkt_channel = 'Email - Newsletter', 1, 0))       AS email_newsletter,
       SUM(IFF(touch_mkt_channel = 'Email - Other', 1, 0))            AS email_other,
       SUM(IFF(touch_mkt_channel = 'Email - Triggers', 1, 0))         AS email_triggers,
       SUM(IFF(touch_mkt_channel = 'Media', 1, 0))                    AS media,
       SUM(IFF(touch_mkt_channel = 'Organic Search Brand', 1, 0))     AS organic_search_brand,
       SUM(IFF(touch_mkt_channel = 'Organic Search Non-Brand', 1, 0)) AS organic_search_non_brand,
       SUM(IFF(touch_mkt_channel = 'Organic Social', 1, 0))           AS organic_social,
       SUM(IFF(touch_mkt_channel = 'Other', 1, 0))                    AS other,
       SUM(IFF(touch_mkt_channel = 'PPC - Brand', 1, 0))              AS ppc_brand,
       SUM(IFF(touch_mkt_channel = 'PPC - Non Brand CPA', 1, 0))      AS ppc_non_brand_cpa,
       SUM(IFF(touch_mkt_channel = 'PPC - Non Brand CPL', 1, 0))      AS ppc_non_brand_cpl,
       SUM(IFF(touch_mkt_channel = 'PPC - Undefined', 1, 0))          AS ppc_undefined,
       SUM(IFF(touch_mkt_channel = 'Paid Social CPA', 1, 0))          AS paid_social_cpa,
       SUM(IFF(touch_mkt_channel = 'Paid Social CPL', 1, 0))          AS paid_social_cpl,
       SUM(IFF(touch_mkt_channel = 'Partner', 1, 0))                  AS partner,
       SUM(IFF(touch_mkt_channel = 'Test', 1, 0))                     AS test,
       SUM(IFF(touch_mkt_channel = 'YouTube', 1, 0))                  AS youtube,
       COUNT(*)                                                       AS total_sessions
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2021-12-01'
  AND stmc.touch_affiliate_territory IN ('UK', 'DE', 'AT', 'CH')
GROUP BY 1;

-- uk sessions by day
SELECT stba.touch_start_tstamp::DATE AS date,
       COUNT(*)                      AS total_sessions
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2021-12-01'
  AND stmc.touch_affiliate_territory = 'UK'
GROUP BY 1;

SELECT DISTINCT scv_touch_marketing_channel.touch_mkt_channel
FROM se.data.scv_touch_marketing_channel;


SELECT stba.attributed_user_id
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= '2021-12-25'

