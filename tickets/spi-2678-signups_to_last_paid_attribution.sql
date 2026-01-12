/*
Assumptions:
- sign up tstamp from camila is within 1min variance of web tracking
- sign ups from 2018-01-01 onwards
- server side tracking didn't go into place until 2020-02-28

*/

WITH user_sign_up_data AS (
    SELECT
        sua.shiro_user_id,
        sua.signup_tstamp,
        sua.original_affiliate_name,
        sua.member_original_affiliate_classification
    FROM se.data.se_user_attributes sua
    WHERE sua.signup_tstamp >= '2018-01-01'
)
SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN user_sign_up_data usud ON TRY_TO_NUMBER(stba.attributed_user_id) = usud.shiro_user_id
    AND usud.signup_tstamp BETWEEN TIMESTAMPADD(MIN, -1, stba.touch_start_tstamp) AND TIMESTAMPADD(MIN, 1, stba.touch_end_tstamp);

-- of the 30.1M sign ups I can attribute back, 17.9M.
USE WAREHOUSE pipe_4xlarge;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.signup_sessions_lobster_20221003 AS
WITH user_sign_up_data AS (
    SELECT
        sua.shiro_user_id,
        sua.signup_tstamp,
        sua.original_affiliate_name,
        sua.original_affiliate_territory,
        sua.member_original_affiliate_classification
    FROM se.data.se_user_attributes sua
    WHERE sua.signup_tstamp >= '2018-01-01'
)
SELECT
    stba.touch_id,
    stba.stitched_identity_type,
    stba.attributed_user_id,
    stba.touch_start_tstamp,
    stba.touch_end_tstamp,
    stmc.touch_mkt_channel  AS last_click_channel,
    stmc2.touch_mkt_channel AS last_paid_channel,
    usud.*
FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN user_sign_up_data usud ON TRY_TO_NUMBER(stba.attributed_user_id) = usud.shiro_user_id
    AND usud.signup_tstamp BETWEEN TIMESTAMPADD(MIN, -1, stba.touch_start_tstamp) AND TIMESTAMPADD(MIN, 1, stba.touch_end_tstamp)
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
    INNER JOIN se.data.scv_touch_attribution sta ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
    INNER JOIN se.data.scv_touch_marketing_channel stmc2 ON sta.attributed_touch_id = stmc2.touch_id
;

SELECT *
FROM scratch.robinpatel.signup_sessions_lobster_20221003
WHERE last_click_channel != last_paid_channel;
-- of the 17.9M, 1.5M have a different last click to last paid channel

SELECT DISTINCT
    su.last_paid_channel
FROM scratch.robinpatel.signup_sessions_lobster_20221003 su;



/*MEMBER_ORIGINAL_AFFILIATE_CLASSIFICATION
PPC_BRAND_CPA
ORGANIC
DIRECT
OTHER
BLOG
DEALCHECKER
DISPLAY_CPL
DISPLAY_CPA
PPC_NON_BRAND_CPL
PARTNER
PPC_BRAND_CPL
MEDIA
unknown
PARTNER_WHITE_LABEL
PPC_NON_BRAND_CPA
PAID_SOCIAL_CPL
AFFILIATE_PROGRAM
PAID_SOCIAL_CPA
  */

/*LAST_PAID_CHANNEL
Paid Social CPA
Other
Media
Partner
Organic Search Non-Brand
Paid Social CPL
Organic Search Brand
Direct
PPC - Brand
YouTube
Blog
PPC - Non Brand CPL
PPC - Non Brand CPA
Email - Other
Test
PPC - Undefined
Email - Triggers
Email - Newsletter
Affiliate Program
Organic Social
Display CPL
Display CPA
*/



WITH user_sign_up_data AS (
    SELECT
        sua.shiro_user_id,
        sua.signup_tstamp,
        sua.original_affiliate_name,
        sua.original_affiliate_territory,
        sua.member_original_affiliate_classification
    FROM dbt_dev.dbt_robinpatel_staging.base_dwh__user_attributes sua
    WHERE sua.signup_tstamp >= '2018-01-01'
)
SELECT
    stba.touch_id,
    stba.stitched_identity_type,
    stba.attributed_user_id,
    stba.touch_start_tstamp,
    stba.touch_end_tstamp,
    stmc.touch_mkt_channel  AS last_click_channel,
    stmc2.touch_mkt_channel AS last_paid_channel,
    CASE
        WHEN stmc2.touch_mkt_channel
            IN (
                'Affiliate Program',
                'Display CPA',
                'Paid Social CPA',
                'PPC - Non Brand CPA'
                 ) THEN 'CPA'
        WHEN stmc2.touch_mkt_channel
            IN (
                'Display CPL',
                'Paid Social CPL',
                'PPC - Non Brand CPL'
                 ) THEN 'CPL'
        ELSE 'Other'
        END                 AS last_paid_channel_group,
    usud.shiro_user_id,
    usud.signup_tstamp,
    usud.original_affiliate_name,
    usud.original_affiliate_territory,
    usud.member_original_affiliate_classification,
    CASE
        WHEN usud.member_original_affiliate_classification
            IN (
                'PPC_NON_BRAND_CPA',
                'AFFILIATE_PROGRAM',
                'PAID_SOCIAL_CPA',
                'DISPLAY_CPA'
                 ) THEN 'CPA'
        WHEN usud.member_original_affiliate_classification
            IN (
                'DISPLAY_CPL',
                'PAID_SOCIAL_CPL',
                'PPC_NON_BRAND_CPL'
                 ) THEN 'CPL'
        ELSE 'Other'
        END                 AS affiliate_category_group
FROM dbt_dev.dbt_robinpatel_staging.base_scv__module_touch_basic_attributes stba
    INNER JOIN user_sign_up_data usud
               ON TRY_TO_NUMBER(stba.attributed_user_id) = usud.shiro_user_id
                   AND usud.signup_tstamp BETWEEN TIMESTAMPADD(MIN, -1, stba.touch_start_tstamp) AND TIMESTAMPADD(MIN, 1, stba.touch_end_tstamp)
    INNER JOIN dbt_dev.dbt_robinpatel_staging.base_scv__module_touch_marketing_channel stmc
               ON stba.touch_id = stmc.touch_id
    INNER JOIN dbt_dev.dbt_robinpatel_staging.base_scv__module_touch_attribution sta
               ON stba.touch_id = sta.touch_id
                   AND sta.attribution_model = 'last paid'
    INNER JOIN dbt_dev.dbt_robinpatel_staging.base_scv__module_touch_marketing_channel stmc2
               ON sta.attributed_touch_id = stmc2.touch_id


SELECT *
FROM dbt.bi_data_platform.dp_cohort_member_signups_by_channel dcmsbc;

SELECT
    last_paid_channel_group = affiliate_category_group AS channels_match,
    COUNT(*)
FROM dbt.bi_data_platform.dp_cohort_member_signups_by_channel dcmsbc
GROUP BY 1;

-- of the 17.9M sessions that I've associated to a sign up, only 1.2M of them have a different last paid channel to a last click one (using channel groupings of CPA/CPL/Other).

SELECT
    last_paid_channel_group,
    affiliate_category_group,
    COUNT(*)
FROM dbt.bi_data_platform.dp_cohort_member_signups_by_channel dcmsbc
WHERE last_paid_channel_group != affiliate_category_group
GROUP BY 1, 2;

-- 908,271 will move from 'Other' to a paid channel

SELECT
    affiliate_category_group,
    COUNT(*)
FROM dbt.bi_data_platform.dp_cohort_member_signups_by_channel dcmsbc
GROUP BY 1

-- 8,117,445 originally set as non paid;


SELECT * FROm dbt.bi_data_platform.dp_cohort_member_signups_by_channel dcmsbc;